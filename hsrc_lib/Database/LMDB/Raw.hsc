{-# LANGUAGE ForeignFunctionInterface, DeriveDataTypeable #-}

-- | This module is a thin wrapper above lmdb.h.
--
-- This wraps most of the opaque integral types and enumerations.
-- Error return codes are transformed into Haskell exceptions.
--
-- Notes:
-- * Relocation functions aren't supported.
-- * Unix mode fixed at 660 (read-write for user+group).
-- * 
module Database.LMDB.Raw
    ( LMDB_Version(..), lmdb_version, lmdb_dyn_version
    , LMDB_Error(..), MDB_ErrCode(..)

    , MDB_env, MDB_txn, MDB_txnid, MDB_cursor, MDB_dbi
    , MDB_val, mv_size, mv_data
    , MDB_stat, ms_psize, ms_depth, ms_branch_pages, ms_leaf_pages, ms_overflow_pages, ms_entries
    , MDB_envinfo, me_mapaddr, me_mapsize, me_last_pgno, me_last_txnid, me_maxreaders, me_numreaders
    , CmpFn, wrapCmpFn
    , MDB_EnvFlag(..), MDB_DbFlag(..)
    , MDB_WriteFlag(..), MDB_WriteFlags, compileWriteFlags
    , MDB_cursor_op(..)
{-
    -- | Environment Operations
    , mdb_env_create
    , mdb_env_open
    , mdb_env_copy
    , mdb_env_copyfd
    , mdb_env_stat
    , mdb_env_info
    , mdb_env_sync
    , mdb_env_close
    , mdb_env_set_flags
    , mdb_env_get_flags
    , mdb_env_get_path
    , mdb_env_get_fd
    , mdb_env_set_mapsize
    , mdb_env_set_maxreaders
    , mdb_env_get_maxreaders
    , mdb_env_set_maxdbs
    , mdb_env_get_maxkeysize

    -- | Transactions
    , mdb_txn_begin
    , mdb_txn_env
    , mdb_txn_commit
    , mdb_txn_abort
    , mdb_txn_reset
    , mdb_txn_renew

    -- | Databases
    , mdb_dbi_open
    , mdb_stat
    , mdb_dbi_flags
    , mdb_dbi_close
    , mdb_drop, mdb_clear
    , mdb_set_compare
    , mdb_set_dupsort
    -- , mdb_set_relfunc
    -- , mdb_set_relctx
    
    -- | Access
    , mdb_get
    , mdb_put
    , mdb_del

    -- | Cursors
    , mdb_cursor_open
    , mdb_cursor_close
    , mdb_cursor_renew
    , mdb_cursor_txn
    , mdb_cursor_dbi
    , mdb_cursor_get
    , mdb_cursor_put
    , mdb_cursor_del
    , mdb_cursor_count

    -- | Misc
    , mdb_cmp
    , mdb_dcmp
    , mdb_reader_list
    , mdb_reader_check
-}
    ) where

#include <lmdb.h>

import Foreign
import Foreign.C
import Control.Applicative
import Control.Monad
import Control.Exception
import qualified Data.Array.Unboxed as A
import Data.Monoid
import qualified Data.List as L
import Data.Typeable

#let alignment t = "%lu", (unsigned long)offsetof(struct {char x__; t (y__); }, y__)

{-
import Foreign.Marshal.Alloc
import Foreign.Storable
import Foreign.Ptr
import Control.Applicative
import Control.Monad
import Data.Bits
import Data.Maybe
import System.IO.Error
import System.IO (FilePath)
import Foreign.ForeignPtr
import Data.ByteString (ByteString)
import qualified Data.ByteString.Internal as BSI
import Data.Word
import Control.Exception.Extensible
import Data.Typeable
-}

-- | Version information for LMDB. Two potentially different versions
-- can be obtained: lmdb_version returns the version at the time of 
-- binding (via C preprocessor macros) and lmdb_dyn_version returns a
-- version for the bound library.
--
-- These bindings to Haskell will refuse to open the database when
-- the dynamic version of LMDB is different in the major or minor 
-- fields.
data LMDB_Version = LMDB_Version
    { v_major :: {-# UNPACK #-} !Int
    , v_minor :: {-# UNPACK #-} !Int
    , v_patch :: {-# UNPACK #-} !Int
    , v_text  :: !String
    } deriving (Eq, Ord, Show)

-- | Version of LMDB when the Haskell-LMDB binding was compiled.
lmdb_version :: LMDB_Version
lmdb_version = LMDB_Version
    { v_major = #const MDB_VERSION_MAJOR
    , v_minor = #const MDB_VERSION_MINOR
    , v_patch = #const MDB_VERSION_PATCH
    , v_text  = #const_str MDB_VERSION_STRING
    }

foreign import ccall "lmdb.h mdb_version" 
    _mdb_version :: Ptr CInt -> Ptr CInt -> Ptr CInt -> IO CString

-- Haskell seems to have difficulty inferring the `Ptr CInt` from 
-- the _mdb_version call. (This seriously annoys me.)
peekCInt :: Ptr CInt -> IO CInt
peekCInt = peek

-- | Version of LMDB linked to the current Haskell process.
lmdb_dyn_version :: IO LMDB_Version
lmdb_dyn_version =
    let szInt = sizeOf (undefined :: CInt) in
    allocaBytes (3 * szInt) $ \ pMajor -> do
        let pMinor = pMajor `plusPtr` szInt
        let pPatch = pMinor `plusPtr` szInt
        cvText <- _mdb_version pMajor pMinor pPatch 
        vMajor <- fromIntegral <$> peekCInt pMajor
        vMinor <- fromIntegral <$> peekCInt pMinor
        vPatch <- fromIntegral <$> peekCInt pPatch
        vText  <- peekCString cvText
        return $! LMDB_Version
            { v_major = vMajor
            , v_minor = vMinor
            , v_patch = vPatch
            , v_text  = vText
            }

-- | LMDB_Error is the exception type thrown in case a function from
-- the LMDB API does not return successfully. Clients should be 
-- prepared to catch exceptions from any LMDB operation.
data LMDB_Error = LMDB_Error 
    { e_context     :: String 
    , e_description :: String 
    , e_code        :: Either Int MDB_ErrCode
    } deriving (Eq, Ord, Show, Typeable)
instance Exception LMDB_Error

-- | Opaque structure for LMDB environment.
newtype MDB_env = MDB_env (Ptr MDB_env)

-- | Opaque structure for LMDB transaction.
newtype MDB_txn = MDB_txn (Ptr MDB_txn)

-- | Identifier for a transaction.
newtype MDB_txnid = MDB_txnid { _txnid :: CSize } deriving (Ord, Eq, Show)

-- | Opaque structure for LMDB cursor.
newtype MDB_cursor = MDB_cursor (Ptr MDB_cursor)

-- | Handle for a database in the environment.
newtype MDB_dbi = MDB_dbi { _dbi :: CUInt } 

-- | A value stored in the database. Be cautious; committing the
-- transaction that obtained a value should also invalidate it;
-- avoid capturing MDB_val in a lazy value. A 'safe' interface
-- similar to STRef will be provided in another module.
data MDB_val = MDB_val
    { mv_size :: {-# UNPACK #-} !CSize
    , mv_data :: {-# UNPACK #-} !(Ptr Word8)
    }

data MDB_stat = MDB_stat
    { ms_psize :: {-# UNPACK #-} !CUInt
    , ms_depth :: {-# UNPACK #-} !CUInt
    , ms_branch_pages :: {-# UNPACK #-} !CSize
    , ms_leaf_pages :: {-# UNPACK #-} !CSize
    , ms_overflow_pages :: {-# UNPACK #-} !CSize
    , ms_entries :: {-# UNPACK #-} !CSize
    } deriving (Eq, Ord, Show)

data MDB_envinfo = MDB_envinfo
    { me_mapaddr :: {-# UNPACK #-} !(Ptr ())
    , me_mapsize :: {-# UNPACK #-} !CSize
    , me_last_pgno :: {-# UNPACK #-} !CSize
    , me_last_txnid :: {-# UNPACK #-} !MDB_txnid
    , me_maxreaders :: {-# UNPACK #-} !CUInt
    , me_numreaders :: {-# UNPACK #-} !CUInt
    } deriving (Eq, Ord, Show)

-- | User-defined comparison functions for keys. 
-- (Corresponds to: ByteString -> ByteString -> Ord)
type CmpFn = Ptr MDB_val -> Ptr MDB_val -> IO CInt
foreign import ccall "wrapper"  wrapCmpFn :: CmpFn -> IO (FunPtr CmpFn)

-- | Environment flags from lmdb.h
data MDB_EnvFlag
    = MDB_FIXEDMAP
    | MDB_NOSUBDIR
    | MDB_NOSYNC
    | MDB_RDONLY
    | MDB_NOMETASYNC
    | MDB_WRITEMAP
    | MDB_MAPASYNC
    | MDB_NOTLS
    | MDB_NOLOCK
    | MDB_NORDAHEAD
    | MDB_NOMEMINIT
    deriving (Eq, Ord, Bounded, A.Ix, Show)

envFlags :: [(MDB_EnvFlag, Int)]
envFlags =
    [(MDB_FIXEDMAP, #const MDB_FIXEDMAP)
    ,(MDB_NOSUBDIR, #const MDB_NOSUBDIR)
    ,(MDB_NOSYNC, #const MDB_NOSYNC)
    ,(MDB_RDONLY, #const MDB_RDONLY)
    ,(MDB_NOMETASYNC, #const MDB_NOMETASYNC)
    ,(MDB_WRITEMAP, #const MDB_WRITEMAP)
    ,(MDB_MAPASYNC, #const MDB_MAPASYNC)
    ,(MDB_NOTLS, #const MDB_NOTLS)
    ,(MDB_NOLOCK, #const MDB_NOLOCK)
    ,(MDB_NORDAHEAD, #const MDB_NORDAHEAD)
    ,(MDB_NOMEMINIT, #const MDB_NOMEMINIT)
    ]

envFlagsArray :: A.UArray MDB_EnvFlag Int
envFlagsArray = A.accumArray (.|.) 0 (minBound, maxBound) envFlags

data MDB_DbFlag
    = MDB_REVERSEKEY
    | MDB_DUPSORT
    | MDB_INTEGERKEY
    | MDB_DUPFIXED
    | MDB_INTEGERDUP
    | MDB_REVERSEDUP
    | MDB_CREATE
    deriving (Eq, Ord, Bounded, A.Ix, Show)

dbFlags :: [(MDB_DbFlag, Int)]
dbFlags =
    [(MDB_REVERSEKEY, #const MDB_REVERSEKEY)
    ,(MDB_DUPSORT, #const MDB_DUPSORT)
    ,(MDB_INTEGERKEY, #const MDB_INTEGERKEY)
    ,(MDB_DUPFIXED, #const MDB_DUPFIXED)
    ,(MDB_INTEGERDUP, #const MDB_INTEGERDUP)
    ,(MDB_REVERSEDUP, #const MDB_REVERSEDUP)
    ,(MDB_CREATE, #const MDB_CREATE)
    ]

dbFlagsArray :: A.UArray MDB_DbFlag Int
dbFlagsArray = A.accumArray (.|.) 0 (minBound,maxBound) dbFlags

data MDB_WriteFlag 
    = MDB_NOOVERWRITE
    | MDB_NODUPDATA
    | MDB_CURRENT
    | MDB_RESERVE
    | MDB_APPEND
    | MDB_APPENDDUP
    | MDB_MULTIPLE
    deriving (Eq, Ord, Bounded, A.Ix, Show)

writeFlags :: [(MDB_WriteFlag, Int)]
writeFlags =
    [(MDB_NOOVERWRITE, #const MDB_NOOVERWRITE)
    ,(MDB_NODUPDATA, #const MDB_NODUPDATA)
    ,(MDB_CURRENT, #const MDB_CURRENT)
    ,(MDB_RESERVE, #const MDB_RESERVE)
    ,(MDB_APPEND, #const MDB_APPEND)
    ,(MDB_APPENDDUP, #const MDB_APPENDDUP)
    ,(MDB_MULTIPLE, #const MDB_MULTIPLE)
    ]

writeFlagsArray :: A.UArray MDB_WriteFlag Int
writeFlagsArray = A.accumArray (.|.) 0 (minBound,maxBound) writeFlags

-- | compiled write flags, corresponding to a [WriteFlag] list. Used
-- because writes are frequent enough that we want to avoid building
-- from a list on a per-write basis.
newtype MDB_WriteFlags = MDB_WriteFlags CInt

-- | compile a list of write flags. 
compileWriteFlags :: [MDB_WriteFlag] -> MDB_WriteFlags
compileWriteFlags = MDB_WriteFlags . L.foldl' addWF 0 where
    addWF n wf = n .|. fromIntegral (writeFlagsArray A.! wf)

data MDB_cursor_op
    = MDB_FIRST
    | MDB_FIRST_DUP
    | MDB_GET_BOTH
    | MDB_GET_BOTH_RANGE
    | MDB_GET_CURRENT
    | MDB_GET_MULTIPLE
    | MDB_LAST
    | MDB_LAST_DUP
    | MDB_NEXT
    | MDB_NEXT_DUP
    | MDB_NEXT_MULTIPLE
    | MDB_NEXT_NODUP
    | MDB_PREV
    | MDB_PREV_DUP
    | MDB_PREV_NODUP
    | MDB_SET
    | MDB_SET_KEY
    | MDB_SET_RANGE
    deriving (Eq, Ord, Bounded, A.Ix, Show)

cursorOps :: [(MDB_cursor_op, Int)]
cursorOps =
    [(MDB_FIRST, #const MDB_FIRST)
    ,(MDB_FIRST_DUP, #const MDB_FIRST_DUP)
    ,(MDB_GET_BOTH, #const MDB_GET_BOTH)
    ,(MDB_GET_BOTH_RANGE, #const MDB_GET_BOTH_RANGE)
    ,(MDB_GET_CURRENT, #const MDB_GET_CURRENT)
    ,(MDB_GET_MULTIPLE, #const MDB_GET_MULTIPLE)
    ,(MDB_LAST, #const MDB_LAST)
    ,(MDB_LAST_DUP, #const MDB_LAST_DUP)
    ,(MDB_NEXT, #const MDB_NEXT)
    ,(MDB_NEXT_DUP, #const MDB_NEXT_DUP)
    ,(MDB_NEXT_MULTIPLE, #const MDB_NEXT_MULTIPLE)
    ,(MDB_NEXT_NODUP, #const MDB_NEXT_NODUP)
    ,(MDB_PREV, #const MDB_PREV)
    ,(MDB_PREV_DUP, #const MDB_PREV_DUP)
    ,(MDB_PREV_NODUP, #const MDB_PREV_NODUP)
    ,(MDB_SET, #const MDB_SET)
    ,(MDB_SET_KEY, #const MDB_SET_KEY)
    ,(MDB_SET_RANGE, #const MDB_SET_RANGE)
    ]

cursorOpsArray :: A.UArray MDB_cursor_op Int 
cursorOpsArray = A.accumArray (flip const) minBound (minBound,maxBound) cursorOps

-- | Error codes from MDB. Note, however, that this API for MDB will mostly
-- use exceptions for any non-successful return codes. This is mostly included
-- because I feel the binding would be incomplete otherwise.
--
-- (The MDB_SUCCESS return value is excluded.)
data MDB_ErrCode
    = MDB_KEYEXIST
    | MDB_NOTFOUND
    | MDB_PAGE_NOTFOUND
    | MDB_CORRUPTED
    | MDB_PANIC
    | MDB_VERSION_MISMATCH
    | MDB_INVALID
    | MDB_MAP_FULL
    | MDB_DBS_FULL
    | MDB_READERS_FULL
    | MDB_TLS_FULL
    | MDB_TXN_FULL
    | MDB_CURSOR_FULL
    | MDB_PAGE_FULL
    | MDB_MAP_RESIZED
    | MDB_INCOMPATIBLE
    | MDB_BAD_RSLOT
    | MDB_BAD_TXN
    | MDB_BAD_VALSIZE
    deriving (Eq, Ord, Bounded, A.Ix, Show)

errCodes :: [(MDB_ErrCode, Int)]
errCodes =
    [(MDB_KEYEXIST, #const MDB_KEYEXIST)
    ,(MDB_NOTFOUND, #const MDB_NOTFOUND)
    ,(MDB_PAGE_NOTFOUND, #const MDB_PAGE_NOTFOUND)
    ,(MDB_CORRUPTED, #const MDB_CORRUPTED)
    ,(MDB_PANIC, #const MDB_PANIC)
    ,(MDB_VERSION_MISMATCH, #const MDB_VERSION_MISMATCH)
    ,(MDB_INVALID, #const MDB_INVALID)
    ,(MDB_MAP_FULL, #const MDB_MAP_FULL)
    ,(MDB_DBS_FULL, #const MDB_DBS_FULL)
    ,(MDB_READERS_FULL, #const MDB_READERS_FULL)
    ,(MDB_TLS_FULL, #const MDB_TLS_FULL)
    ,(MDB_TXN_FULL, #const MDB_TXN_FULL)
    ,(MDB_CURSOR_FULL, #const MDB_CURSOR_FULL)
    ,(MDB_PAGE_FULL, #const MDB_PAGE_FULL)
    ,(MDB_MAP_RESIZED, #const MDB_MAP_RESIZED)
    ,(MDB_INCOMPATIBLE, #const MDB_INCOMPATIBLE)
    ,(MDB_BAD_RSLOT, #const MDB_BAD_RSLOT)
    ,(MDB_BAD_TXN, #const MDB_BAD_TXN)
    ,(MDB_BAD_VALSIZE, #const MDB_BAD_VALSIZE)
    ]

_numToErrVal :: Int -> Either Int MDB_ErrCode
_numToErrVal code = 
    case L.find ((== code) . snd) errCodes of
        Nothing -> Left code
        Just (ec,_) -> Right ec

foreign import ccall "lmdb.h mdb_strerror" _mdb_strerror :: CInt -> IO CString 

_throwLMDBErrNum :: String -> CInt -> IO noReturn
_throwLMDBErrNum context errNum = do
    description <- peekCString =<< _mdb_strerror errNum
    throwIO $! LMDB_Error
        { e_context = context
        , e_description = description
        , e_code = _numToErrVal (fromIntegral errNum)
        }


foreign import ccall "lmdb.h mdb_env_create" _mdb_env_create :: Ptr (Ptr MDB_env) -> IO CInt


type MDB_mode_t = #type mdb_mode_t









    


{-
cmpBytesToCmpFn :: (ByteString -> ByteString -> Ord) -> CmpFn
cmpBytesToCmpFn cmp vL vR = do
    lBytes <- valToVolatileByteString vL
    rBytes <- valToVolatileByteString vR
    return $! case cmp lBytes rBytes of
        LT -> -1
        EQ -> 0
        GT -> 1

-- | Create a user-defined comparison funcion over ByteStrings
wrapCmpBytes :: (ByteString -> ByteString -> Ord) -> MDB_cmp_func
wrapCmpBytes = as_MDB_cmp_func . cmpBytesToCmpFn
    
-- | Convert a value to a bytestring in O(1) time. Note, however,
-- that this bytestring refers into a memory-mapped page in the 
-- database, which may be reused after the transaction that obtained
-- the value is dropped. Developers must be careful to ensure the
-- bytestring doesn't stick around in any lazy computations.
--
-- Consider use of the safer, higher level API that will strongly 
-- associate a value with a particular transaction.
valToBytes :: MDB_val -> IO ByteString
valToBytes (MDB_val sz pd) = do
    fpd <- newForeignPtr_ pd
    return $! B.fromForeignPtr fpd 0 (fromIntegral sz)
    
-}


















instance Storable MDB_val where
    alignment _ = #{alignment MDB_val}
    sizeOf _ = #{size MDB_val}
    peek ptr = do
        sz <- #{peek MDB_val, mv_size} ptr
        pd <- #{peek MDB_val, mv_data} ptr
        return $! MDB_val sz pd
    poke ptr (MDB_val sz pd) = do
        #{poke MDB_val, mv_size} ptr sz
        #{poke MDB_val, mv_data} ptr pd

instance Storable MDB_stat where
    alignment _ = #{alignment MDB_stat}
    sizeOf _ = #{size MDB_stat}
    peek ptr = do
        psize <- #{peek MDB_stat, ms_psize} ptr
        depth <- #{peek MDB_stat, ms_depth} ptr
        branch_pages <- #{peek MDB_stat, ms_branch_pages} ptr
        leaf_pages <- #{peek MDB_stat, ms_leaf_pages} ptr
        overflow_pages <- #{peek MDB_stat, ms_overflow_pages} ptr
        entries <- #{peek MDB_stat, ms_entries} ptr
        return $! MDB_stat
            { ms_psize = psize
            , ms_depth = depth
            , ms_branch_pages = branch_pages
            , ms_leaf_pages = leaf_pages
            , ms_overflow_pages = overflow_pages
            , ms_entries = entries
            }
    poke ptr val = do
        #{poke MDB_stat, ms_psize} ptr (ms_psize val)
        #{poke MDB_stat, ms_depth} ptr (ms_depth val)
        #{poke MDB_stat, ms_branch_pages} ptr (ms_branch_pages val)
        #{poke MDB_stat, ms_leaf_pages} ptr (ms_leaf_pages val)
        #{poke MDB_stat, ms_overflow_pages} ptr (ms_overflow_pages val)
        #{poke MDB_stat, ms_entries} ptr (ms_entries val)

instance Storable MDB_envinfo where
    alignment _ = #{alignment MDB_envinfo}
    sizeOf _ = #{size MDB_envinfo}
    peek ptr = do
        mapaddr <- #{peek MDB_envinfo, me_mapaddr} ptr
        mapsize <- #{peek MDB_envinfo, me_mapsize} ptr
        last_pgno <- #{peek MDB_envinfo, me_last_pgno} ptr
        last_txnid <- #{peek MDB_envinfo, me_last_txnid} ptr
        maxreaders <- #{peek MDB_envinfo, me_maxreaders} ptr
        numreaders <- #{peek MDB_envinfo, me_numreaders} ptr
        return $! MDB_envinfo 
            { me_mapaddr = mapaddr
            , me_mapsize = mapsize
            , me_last_pgno = last_pgno
            , me_last_txnid = MDB_txnid last_txnid
            , me_maxreaders = maxreaders
            , me_numreaders = numreaders
            }
    poke ptr val = do
        #{poke MDB_envinfo, me_mapaddr} ptr (me_mapaddr val)
        #{poke MDB_envinfo, me_mapsize} ptr (me_mapsize val)
        #{poke MDB_envinfo, me_last_pgno} ptr (me_last_pgno val)
        #{poke MDB_envinfo, me_last_txnid} ptr (_txnid $ me_last_txnid val)
        #{poke MDB_envinfo, me_maxreaders} ptr (me_maxreaders val)
        #{poke MDB_envinfo, me_numreaders} ptr (me_numreaders val)


