{-# LANGUAGE ForeignFunctionInterface, EmptyDataDecls #-}

-- | This module is a thin wrapper above lmdb.h.
module Database.LMDB.Raw
    ( LMDB_Version, lmdb_version
    , MDB_env, MDB_txn, MDB_cursor, MDB_dbi
    , MDB_val, mv_size, mv_data
    , MDB_stat, ms_psize, ms_depth, ms_branch_pages, ms_leaf_pages, ms_overflow_pages, ms_entries
    , MDB_envinfo, me_mapaddr, me_mapsize, me_last_pgno, me_last_txnid, me_maxreaders, me_numreaders
    , CmpFn, wrapCmpFn
    , MDB_EnvFlag(..), MDB_DbFlag(..)
    , MDB_WriteFlag(..), MDB_WriteFlags, compileWriteFlags
    ) where

#include <lmdb.h>

import Foreign
import Foreign.C
import Control.Exception
import qualified Data.Array.Unboxed as A
import Data.Monoid
import qualified Data.List as L

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
    { v_major :: Int
    , v_minor :: Int
    , v_patch :: Int
    , v_date  :: String
    } deriving (Eq, Ord, Show)

-- | Version of LMDB when the Haskell-LMDB binding was compiled.
lmdb_version :: LMDB_Version
lmdb_version = LMDB_Version
    { v_major = #const MDB_VERSION_MAJOR
    , v_minor = #const MDB_VERSION_MINOR
    , v_patch = #const MDB_VERSION_PATCH
    , v_date  = #const_str MDB_VERSION_DATE
    }

-- | Opaque structure for LMDB environment.
data MDB_env

-- | Opaque structure for LMDB transaction.
data MDB_txn

-- | Opaque structure for LMDB cursor.
data MDB_cursor

-- | Handle for a database in the environment.
newtype MDB_dbi = MDB_dbi CUInt

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
    , me_last_txnid :: {-# UNPACK #-} !CSize
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
    deriving (Eq, Ord, Bounded, A.Ix)

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
    deriving (Ord, Eq, A.Ix, Bounded)

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
    deriving (Ord, Eq, Bounded, A.Ix)

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
            , me_last_txnid = last_txnid
            , me_maxreaders = maxreaders
            , me_numreaders = numreaders
            }
    poke ptr val = do
        #{poke MDB_envinfo, me_mapaddr} ptr (me_mapaddr val)
        #{poke MDB_envinfo, me_mapsize} ptr (me_mapsize val)
        #{poke MDB_envinfo, me_last_pgno} ptr (me_last_pgno val)
        #{poke MDB_envinfo, me_last_txnid} ptr (me_last_txnid val)
        #{poke MDB_envinfo, me_maxreaders} ptr (me_maxreaders val)
        #{poke MDB_envinfo, me_numreaders} ptr (me_numreaders val)



{-
#define MDB_FIXEDMAP	0x01
	/** no environment directory */
#define MDB_NOSUBDIR	0x4000
	/** don't fsync after commit */
#define MDB_NOSYNC		0x10000
	/** read only */
#define MDB_RDONLY		0x20000
	/** don't fsync metapage after commit */
#define MDB_NOMETASYNC		0x40000
	/** use writable mmap */
#define MDB_WRITEMAP		0x80000
	/** use asynchronous msync when #MDB_WRITEMAP is used */
#define MDB_MAPASYNC		0x100000
	/** tie reader locktable slots to #MDB_txn objects instead of to threads */
#define MDB_NOTLS		0x200000
	/** don't do any locking, caller must manage their own locks */
#define MDB_NOLOCK		0x400000
	/** don't do readahead (no effect on Windows) */
#define MDB_NORDAHEAD	0x800000
	/** don't initialize malloc'd memory before writing to datafile */
#define MDB_NOMEMINIT	0x1000000
-}


{-
/** @defgroup  errors	Return Codes
 *
 *	BerkeleyDB uses -30800 to -30999, we'll go under them
 *	@{
 */
	/**	Successful result */
#define MDB_SUCCESS	 0
	/** key/data pair already exists */
#define MDB_KEYEXIST	(-30799)
	/** key/data pair not found (EOF) */
#define MDB_NOTFOUND	(-30798)
	/** Requested page not found - this usually indicates corruption */
#define MDB_PAGE_NOTFOUND	(-30797)
	/** Located page was wrong type */
#define MDB_CORRUPTED	(-30796)
	/** Update of meta page failed, probably I/O error */
#define MDB_PANIC		(-30795)
	/** Environment version mismatch */
#define MDB_VERSION_MISMATCH	(-30794)
	/** File is not a valid MDB file */
#define MDB_INVALID	(-30793)
	/** Environment mapsize reached */
#define MDB_MAP_FULL	(-30792)
	/** Environment maxdbs reached */
#define MDB_DBS_FULL	(-30791)
	/** Environment maxreaders reached */
#define MDB_READERS_FULL	(-30790)
	/** Too many TLS keys in use - Windows only */
#define MDB_TLS_FULL	(-30789)
	/** Txn has too many dirty pages */
#define MDB_TXN_FULL	(-30788)
	/** Cursor stack too deep - internal error */
#define MDB_CURSOR_FULL	(-30787)
	/** Page has not enough space - internal error */
#define MDB_PAGE_FULL	(-30786)
	/** Database contents grew beyond environment mapsize */
#define MDB_MAP_RESIZED	(-30785)
	/** MDB_INCOMPATIBLE: Operation and DB incompatible, or DB flags changed */
#define MDB_INCOMPATIBLE	(-30784)
	/** Invalid reuse of reader locktable slot */
#define MDB_BAD_RSLOT		(-30783)
	/** Transaction cannot recover - it must be aborted */
#define MDB_BAD_TXN			(-30782)
	/** Too big key/data, key is empty, or wrong DUPFIXED size */
#define MDB_BAD_VALSIZE		(-30781)
#define MDB_LAST_ERRCODE	MDB_BAD_VALSIZE
-}

-- | Access the current (Major,Minor,Patch) version of LMDB for the
-- bound library. If this is different from lmdb_version, it might
-- be useful to rebuild the lmdb package.
lmdb_dyn_version :: IO LMDB_Version
lmdb_dyn_version = error "TODO"





