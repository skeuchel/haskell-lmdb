{-# LANGUAGE OverloadedStrings #-}

module Database.LMDB where

import           Database.LMDB.C

import           Control.Concurrent     (MVar, ThreadId, isCurrentThreadBound,
                                         myThreadId, newEmptyMVar, putMVar,
                                         runInBoundThread, takeMVar)
import           Control.Exception      (throwIO)
import           Control.Monad          (unless, when, (>=>))
import           Control.Monad.Catch    (Exception, MonadMask, bracket,
                                         bracket_, mask_)
import           Control.Monad.IO.Class (MonadIO, liftIO)
import           Data.Bits              (Bits (..))
import           Data.ByteString        (ByteString)
import qualified Data.ByteString        as BS
import qualified Data.ByteString.Char8  as BC
import           Data.Char              (ord)
import           Data.IORef             (modifyIORef', newIORef, readIORef)
import           Data.Maybe             (isNothing)
import           Data.Monoid            ((<>))
import           Data.Typeable          (Typeable)
import           Foreign.C              (CInt, CString, peekCString,
                                         withCString)
import           Foreign.Marshal.Alloc  (alloca, allocaBytes)
import           Foreign.Ptr            (FunPtr, Ptr, freeHaskellFunPtr,
                                         nullPtr, plusPtr)
import           Foreign.Storable       (Storable (..))
import           System.IO              (FilePath)

--------------------------------------------------------------------------------

-- | Version information for LMDB. Two potentially different versions
-- can be obtained: lmdb_version returns the version at the time of
-- binding (via C preprocessor macros) and lmdb_dyn_version returns a
-- version for the bound library.
--
-- These bindings to Haskell will refuse to open the database when
-- the dynamic version of LMDB is different in the major or minor
-- fields.
data LMDB_Version = LMDB_Version
    { verMajor :: {-# UNPACK #-} !Int
    , verMinor :: {-# UNPACK #-} !Int
    , verPatch :: {-# UNPACK #-} !Int
    , verText  :: {-# UNPACK #-} !ByteString
    } deriving (Eq, Ord, Show)

-- | Version of LMDB when the Haskell-LMDB binding was compiled.
lmdb_version :: LMDB_Version
lmdb_version = LMDB_Version
    { verMajor = mdb_VERSION_MAJOR
    , verMinor = mdb_VERSION_MINOR
    , verPatch = mdb_VERSION_PATCH
    , verText  = BC.pack mdb_VERSION_STRING
    }

-- | Version of LMDB linked to the current Haskell process.
lmdb_dyn_version :: MonadIO m => m LMDB_Version
lmdb_dyn_version = liftIO $
    alloca $ \pMajor ->
    alloca $ \pMinor ->
    alloca $ \pPatch -> do
    pStr <- mdb_version pMajor pMinor pPatch
    LMDB_Version
      <$> (fromIntegral <$> peek pMajor)
      <*> (fromIntegral <$> peek pMinor)
      <*> (fromIntegral <$> peek pPatch)
      <*> BS.packCString pStr

-- | LMDB_Error is the exception type thrown in case a function from
-- the LMDB API does not return successfully. Clients should be
-- prepared to catch exceptions from any LMDB operation.
data LMDB_Error = LMDB_Error
    { errCtx  :: {-# UNPACK #-} !ByteString
    , errDesc :: {-# UNPACK #-} !ByteString
    , errCode :: {-# UNPACK #-} !MDB_return_code
    } deriving (Eq, Show, Typeable)
instance Exception LMDB_Error

--------------------------------------------------------------------------------

-- | Opaque structure for LMDB environment.
--
-- The environment additionally contains an MVar to enforce at most
-- one lightweight Haskell thread is writing at a time. This is
-- necessary so long as LMDB uses a long-lived mutex for writes, as
-- in v0.9.10.
--
data Env = Env
    { -- Opaque pointer to LMDB object
        envRaw   :: {-# UNPACK #-} !(MDB_env ())
    , -- Write lock
        envWLock :: {-# UNPACK #-} !(MVar ThreadId)
    }


-- Haskell-level writer lock for the environment.
--
-- This is necessary because otherwise multiple Haskell threads on the same OS
-- thread can acquire the same lock (in windows) or deadlock (in posix
-- systems). LMDB doesn't really support M:N writer threads.
--
-- Note: this will also enforce that the caller is operating in a bound
-- thread. So we can only _lockEnv from a bound thread, and we can only
-- _unlockEnv from the same thread.

_lockErr, _unlockErr :: LMDB_Error
_lockErr = LMDB_Error
    { errCtx  = "locking LMDB for write in Haskell layer"
    , errDesc = "must lock from a 'bound' thread!"
    , errCode = MDB_PANIC
    }
_unlockErr = LMDB_Error
    { errCtx  = "unlock Haskell layer LMDB after write"
    , errDesc = "calling thread does not own the lock!"
    , errCode = MDB_PANIC
    }

hasFlag :: Bits a => a -> a -> Bool
hasFlag a b = (a .&. b) /= zeroBits

_lockEnv :: MonadIO m => Env -> m ()
_lockEnv env = liftIO $ do
    safeThread <-
           isCurrentThreadBound
      <||> -- Why would we require no locking here?
           (hasFlag MDB_NOLOCK <$> envGetFlags env)
    unless safeThread (throwIO _lockErr)
    self <- myThreadId
    putMVar (envWLock env) self
  where
    getA <||> getB = getA >>= \a ->
      if a then return True else getB

_unlockEnv :: MonadIO m => Env -> m ()
_unlockEnv env = liftIO $ do
    let wlock = envWLock env
    self <- myThreadId
    mask_ $ do
        owner <- takeMVar wlock
        unless (self == owner) $ do
            putMVar wlock owner -- oops!
            throwIO _unlockErr

--------------------------------------------------------------------------------

-- | Opaque structure for LMDB transaction.
data Txn = Txn
    { txnRaw    :: {-# UNPACK #-} !(MDB_txn ())
    , -- | Environment that owns this transaction.
      txnEnv    :: {-# UNPACK #-} !Env
    , -- | Is this a read-write transaction? (vs read-only)
      txnWrite  :: !Bool
    , -- | Parent transaction, if any
      txnParent :: !(Maybe Txn)
    }

-- | Identifier for a transaction.
newtype TxnId = TxnId
    { txn_id :: MDB_txnid
    } deriving (Eq, Ord, Show)

newtype Dbi k v = Dbi
    { fromDbi :: MDB_dbi () k v
    }

-- | Opaque structure for LMDB cursor.
data Cursor k v = Cursor
    { crsRaw :: {-# UNPACK #-} !(MDB_cursor () k v)
    , crsDbi :: {-# UNPACK #-} !(Dbi k v)
    , crsTxn :: !Txn
    }

-- | A value stored in the database. Be cautious; committing the transaction
-- that obtained a value should also invalidate it; avoid capturing 'Val' in a
-- lazy value. A safe interface similar to 'STRef' could be provided by another
-- module.
type Value = MDB_val
type Stat = MDB_stat
type EnvInfo = MDB_envinfo

throwLMDBErrNum :: MonadIO m => ByteString -> MDB_return_code -> m noReturn
throwLMDBErrNum ctx ret = liftIO $ do
    desc <- BS.packCString (mdb_strerror ret)
    throwIO $! LMDB_Error ctx desc ret
{-# NOINLINE throwLMDBErrNum #-}

checkSuccess :: MonadIO m => ByteString -> MDB_return_code -> m ()
checkSuccess ctx rc = unless (rc == MDB_SUCCESS) (throwLMDBErrNum ctx rc)
{-# INLINE checkSuccess #-}

-- this match function is a bit relaxed, e.g. it will accept
-- LMDB 0.9.10 with 0.9.8, so long as the first two numbers
-- match.
versionMatch :: LMDB_Version -> LMDB_Version -> Bool
versionMatch vA vB = matchMajor && matchMinor
  where
    matchMajor = verMajor vA == verMajor vB
    matchMinor = verMinor vA == verMinor vB

lmdb_validate_version_match :: MonadIO m => m ()
lmdb_validate_version_match = liftIO $ do
    let vStat = lmdb_version
    vDyn <- lmdb_dyn_version
    unless (versionMatch vStat vDyn) $ throwIO $!
        LMDB_Error
        { errCtx  = "lmdb_validate_version_match"
        , errDesc = BC.pack $
                       "Haskell bindings: " ++ show vStat
                    ++ "\tDynamic library: " ++ show vDyn
        , errCode = MDB_VERSION_MISMATCH
        }

--------------------------------------------------------------------------------

-- | Allocate an environment object. This doesn't open the environment.
--
-- After creation, but before opening, please use:
--
--   mdb_env_set_mapsize
--   mdb_env_set_maxreaders
--   mdb_env_set_maxdbs
--
-- Then, just after opening, you should create a transaction to open
-- all the databases (MDB_dbi and MDB_dbi' values) your application
-- will use.
--
-- The typical use case would then involve keeping all these open
-- until your application either shuts down or crashes.
--
-- In addition to normal LMDB errors, this operation may throw an
-- MDB_VERSION_MISMATCH if the Haskell LMDB bindings don't match
-- the dynamic version. If this happens, you'll need to rebuild the
-- lmdb Haskell package.
envCreate :: MonadIO m => m Env
envCreate = liftIO $
    alloca $ \pEnv -> do
    lmdb_validate_version_match
    mdb_env_create pEnv
        >>= checkSuccess "envCreate"
    Env <$> peek pEnv <*> newEmptyMVar

-- | open or build a database in the filesystem. The named directory
-- must already exist and be writeable. Before opening, be sure to
-- at least apply `mdb_env_set_mapsize`.
--
-- After opening the environment, you should open the databases:
--
--    Create the environment.
--    Open a transaction.
--    Open all DBI handles the app will need.
--    Commit the transaction.
--    Use those DBI handles in subsequent transactions
envOpen :: MonadIO m => Env -> FilePath -> MDB_env_flags -> m ()
envOpen env fp envFlags = liftIO $
    withCString fp $ \cfp ->
    mdb_env_open (envRaw env) cfp flags' mode
      >>= checkSuccess "envOpen"
  where
    flags' = envFlags .|. MDB_NOTLS
    mode   = fromIntegral $ ord '\o660'

-- | Copy the environment to an empty (but existing) directory.
--
-- Note: the LMDB copy operation temporarily grabs the writer mutex.
-- Unfortunately, this greatly complicates the binding to Haskell.
-- This interface, mdb_env_copy, conservatively blocks all writers
-- in the same process for the entire duration of copy.
--
-- Recommendation: Don't use this function in the same process with
-- writers. Consider use of the `mdb_copy` command line utility if
-- you need hot copies.
envCopy :: MonadIO m => Env -> FilePath -> m ()
envCopy env fp = liftIO $
    runInBoundThread $
    bracket_ (_lockEnv env) (_unlockEnv env) $
    withCString fp $
        mdb_env_copy (envRaw env)
            >=> checkSuccess "envCopy"

-- | obtain statistics for environment
envStat :: MonadIO m => Env -> m MDB_stat
envStat env = liftIO $
    alloca $ \pStats -> do
    mdb_env_stat (envRaw env) pStats
        >>= checkSuccess "envStat"
    peek pStats

-- | obtain ad-hoc information about the environment.
envInfo :: MonadIO m => Env -> m EnvInfo
envInfo env = liftIO $
    alloca $ \pEnvInfo -> do
    mdb_env_info (envRaw env) pEnvInfo
        >>= checkSuccess "envInfo"
    peek pEnvInfo

-- | Initiate synchronization of environment with disk. However, if
-- the MDB_NOSYNC or MDB_MAPASYNC flags are active, this won't wait
-- for the operation to finish. Cf. mdb_env_sync_flush.
envSync :: MonadIO m => Env -> m ()
envSync env = liftIO $
    mdb_env_sync (envRaw env) 0
        >>= checkSuccess "envSync"

-- | Force buffered writes to disk before returning.
envSyncFlush :: MonadIO m => Env -> m ()
envSyncFlush env = liftIO $
    mdb_env_sync (envRaw env) 1
        >>= checkSuccess "envSyncFlush"

-- | Close the environment. The Env object should not be used by
-- any operations during or after closing.
envClose :: MonadIO m => Env -> m ()
envClose env = liftIO $
    _lockEnv env >> mdb_env_close (envRaw env)

-- | Set flags for the environment.
envSetFlags :: MonadIO m => Env -> MDB_env_flags -> m ()
envSetFlags env flags = liftIO $
    mdb_env_set_flags (envRaw env) flags 1
        >>= checkSuccess "envSetFlags"

-- | Unset flags for the environment.
envUnsetFlags :: MonadIO m => Env -> MDB_env_flags -> m ()
envUnsetFlags env flags = liftIO $
    mdb_env_set_flags (envRaw env) flags 0
        >>= checkSuccess "envUnsetFlags"

-- | View the current set of flags for the environment.
envGetFlags :: MonadIO m => Env -> m MDB_env_flags
envGetFlags env = liftIO $ alloca $ \pFlags -> do
    mdb_env_get_flags (envRaw env) pFlags
        >>= checkSuccess "envGetFlags"
    peek pFlags

-- | Obtain filesystem path for this environment.
envGetPath :: MonadIO m => Env -> m FilePath
envGetPath env = liftIO $ alloca $ \pcfp -> do
    mdb_env_get_path (envRaw env) pcfp
        >>= checkSuccess "mdb_env_get_path"
    peek pcfp >>= peekCString

-- | Set the memory map size, in bytes, for this environment. This
-- determines the maximum size for the environment and databases,
-- but typically only a small fraction of the database is in memory
-- at any given moment.
--
-- It is not a problem to set this to a very large number, hundreds
-- of gigabytes or even terabytes, assuming a sufficiently large
-- address space. It should be set to a multiple of page size.
--
-- The default map size is 1MB, intentionally set low to force
-- developers to select something larger.
envSetMapsize :: MonadIO m => Env -> Int -> m ()
envSetMapsize env nBytes = liftIO $
    mdb_env_set_mapsize (envRaw env) (fromIntegral nBytes)
        >>= checkSuccess "mdb_env_set_mapsize"

-- | Set the maximum number of concurrent readers.
envSetMaxreaders :: MonadIO m => Env -> Int -> m ()
envSetMaxreaders env nReaders = liftIO $
    mdb_env_set_maxreaders (envRaw env) (fromIntegral nReaders)
        >>= checkSuccess "mdb_env_set_maxreaders"

-- | Get the maximum number of concurrent readers.
envGetMaxreaders :: MonadIO m => Env -> m Int
envGetMaxreaders env = liftIO $ alloca $ \pCount -> do
    mdb_env_get_maxreaders (envRaw env) pCount
        >>= checkSuccess "mdb_env_get_maxreaders"
    fromIntegral <$> peek pCount

-- | Set the maximum number of named databases. LMDB is designed to
-- support a small handful of databases.
envSetMaxDbs :: MonadIO m => Env -> Int -> m ()
envSetMaxDbs env nDBs = liftIO $
    mdb_env_set_maxdbs (envRaw env) (fromIntegral nDBs)
        >>= checkSuccess "mdb_env_set_maxdbs"

-- | Key sizes in LMDB are determined by a compile-time constant,
-- defaulting to 511 bytes. This function returns the maximum.
envGetMaxkeysize :: MonadIO m => Env -> m Int
envGetMaxkeysize env = liftIO $
    fromIntegral <$> mdb_env_get_maxkeysize (envRaw env)

-- | Begin a new transaction, possibly read-only, with a possible parent.
--
--     mdb_txn_begin env parent bReadOnly
--
-- NOTE: Unless your Env was created with MDB_NOLOCK, it is necessary
-- that read-write transactions be created and completed in one Haskell
-- 'bound' thread, e.g. via forkOS or runInBoundThread. The bound threads
-- are necessary because LMDB uses OS-level mutexes which track the thread
-- ID of their owning thread.
--
-- This LMDB adapter includes its own MVar mutex to prevent more than one
-- Haskell-level thread from trying to write at the same time.
--
-- The hierarchical transactions are useful for read-write transactions.
-- They allow trying something out then aborting if it doesn't work. But
-- only one child should be active at a time, all in the same OS thread.
txnBegin :: MonadIO m => Env -> Maybe Txn -> Bool -> m Txn
txnBegin env parent bReadOnly = liftIO $ alloca $ \pChildTxn -> mask_ $ do
    let bWriteTxn     = not bReadOnly
        bLockForWrite = bWriteTxn && isNothing parent
        pParent       = maybe (MDB_txn nullPtr) txnRaw parent
        iFlags | bReadOnly = MDB_RDONLY
               | otherwise = zeroBits

    -- allow only one toplevel write operation at a time.
    when bLockForWrite (_lockEnv env)
    rc <- mdb_txn_begin (envRaw env) pParent iFlags pChildTxn
    unless (rc == MDB_SUCCESS) $
        when bLockForWrite (_unlockEnv env) >>
        throwLMDBErrNum "mdb_txn_begin" rc

    childTxn <- peek pChildTxn
    return $! Txn
        { txnRaw    = childTxn
        , txnEnv    = env
        , txnWrite  = bWriteTxn
        , txnParent = parent
        }

-- compute whether this transaction should hold the write lock.
-- If it does, unlock it. Otherwise, return.
_unlockTxn :: MonadIO m => Txn -> m ()
_unlockTxn txn = when bHasLock (_unlockEnv (txnEnv txn))
  where bHasLock = txnWrite txn && isNothing (txnParent txn)

-- | Commit a transaction. Don't use the transaction after this.
txnCommit :: MonadIO m => Txn -> m ()
txnCommit txn = liftIO $ mask_ $
    mdb_txn_commit (txnRaw txn) >>= \rc ->
    _unlockTxn txn >>
    checkSuccess "txnCommit" rc

-- | Abort a transaction. Don't use the transaction after this.
txnAbort :: MonadIO m => Txn -> m ()
txnAbort txn = liftIO $ mask_ $
    mdb_txn_abort (txnRaw txn) >>
    _unlockTxn txn

-- | Abort a read-only transaction, but don't destroy it.
-- Keep it available for mdb_txn_renew.
txnReset :: MonadIO m => Txn -> m ()
txnReset txn = liftIO $ mdb_txn_reset (txnRaw txn)

-- | Renew a read-only transaction that was previously _reset.
txnRenew :: MonadIO m => Txn -> m ()
txnRenew txn = liftIO $
    mdb_txn_renew (txnRaw txn)
        >>= checkSuccess "mdb_txn_renew"

txnId :: MonadIO m => Txn -> m TxnId
txnId txn = TxnId <$> liftIO (mdb_txn_id (txnRaw txn))

-- | Open a database that supports user-defined comparisons, but
-- has slightly more FFI overhead for reads and writes.
--
-- LMDB supports a small set of named databases, plus one 'main'
-- database using the null argument for the database name.
dbiOpen :: MonadIO m => Txn -> Maybe String -> MDB_db_flags -> m (Dbi k v)
dbiOpen txn dbName flags = liftIO $
    withCStringMaybe dbName $ \cdbName ->
    alloca $ \pDBI -> do
    mdb_dbi_open (txnRaw txn) cdbName flags pDBI
        >>= checkSuccess "dbiOpen"
    Dbi <$> peek pDBI

-- | database statistics
dbiStat :: MonadIO m => Txn -> Dbi k v -> m MDB_stat
dbiStat txn dbi = liftIO $
    alloca $ \pStat -> do
    mdb_stat (txnRaw txn) (fromDbi dbi) pStat
      >>= checkSuccess "dbiStat"
    peek pStat

-- | review flags from database
dbiFlags :: MonadIO m => Txn -> Dbi k v -> m MDB_db_flags
dbiFlags txn dbi = liftIO $
    alloca $ \pFlags -> do
    mdb_dbi_flags (txnRaw txn) (fromDbi dbi) pFlags
      >>= checkSuccess "dbiFlags"
    peek pFlags

-- | close the database handle.
--
-- Note: the normal use-case for LMDB is to open all the database
-- handles up front, then hold onto them until the application is
-- closed or crashed. In that case, you don't need to bother with
-- closing database handles.
dbiClose :: MonadIO m => Env -> Dbi k v -> m ()
dbiClose env dbi = liftIO $ mdb_dbi_close (envRaw env) (fromDbi dbi)

-- | remove the database and close the handle; don't use MDB_dbi
-- after this
dbiDrop :: MonadIO m => Txn -> Dbi k v -> m ()
dbiDrop txn dbi = liftIO $
    mdb_drop (txnRaw txn) (fromDbi dbi) 1
        >>= checkSuccess "dbiDrop"

-- | clear contents of database, reset to empty
dbiClear :: MonadIO m => Txn -> Dbi k v -> m ()
dbiClear txn dbi = liftIO $
    mdb_drop (txnRaw txn) (fromDbi dbi) 0
        >>= checkSuccess "dbiClear"

-- | Set a user-defined key comparison function for a database.
dbiSetCompare :: MonadIO m => Txn -> Dbi k v -> FunPtr (MDB_cmp_func k) -> m ()
dbiSetCompare txn dbi fcmp = liftIO $
    mdb_set_compare (txnRaw txn) (fromDbi dbi) fcmp
        >>= checkSuccess "mdb_set_compare"

-- | Set a user-defined data comparison operator for MDB_DUPSORT databases.
dbiSetDupsort :: MonadIO m => Txn -> Dbi k v -> FunPtr (MDB_cmp_func k) -> m ()
dbiSetDupsort txn dbi fcmp = liftIO $
    mdb_set_dupsort (txnRaw txn) (fromDbi dbi) fcmp
        >>= checkSuccess "mdb_set_dupsort"

-- zero datum
zed :: MDB_val a
zed = MDB_val 0 nullPtr

-- | Access a value by key. Returns Nothing if the key is not found.
dbiGet :: MonadIO m => Txn -> Dbi k v -> Value k -> m (Maybe (Value v))
dbiGet txn dbi key = liftIO $
    withKVPtrs key zed $ \pKey pVal ->
    mdb_get (txnRaw txn) (fromDbi dbi) pKey pVal
      >>= checkGet "dbiGet" pVal
{-# INLINE dbiGet #-}

checkGet :: MonadIO m
    => ByteString
    -> Ptr (Value a)
    -> MDB_return_code
    -> m (Maybe (Value a))
checkGet ctx pVal rc = liftIO $ case rc of
    MDB_SUCCESS  -> Just <$> peek pVal
    MDB_NOTFOUND -> return Nothing
    _            -> throwLMDBErrNum ctx rc
{-# INLINE checkGet #-}

-- | Add a (key,value) pair to the database.
--
-- Returns False on MDB_KEYEXIST, and True on MDB_SUCCESS. Any other
-- return value from LMDB results in an exception. The MDB_KEYEXIST
-- result can be returned only if certain write flags are enabled.
dbiPut :: MonadIO m
    => MDB_write_flags
    -> Txn
    -> Dbi k v
    -> Value k
    -> Value v
    -> m Bool
dbiPut wf txn dbi key val = liftIO $
    withKVPtrs key val $ \pKey pVal ->
    mdb_put (txnRaw txn) (fromDbi dbi) pKey pVal wf
        >>= checkPut "dbiPut"
{-# INLINE dbiPut #-}

checkPut :: MonadIO m => ByteString -> MDB_return_code -> m Bool
checkPut ctx rc = case rc of
    MDB_SUCCESS  -> return True
    MDB_KEYEXIST -> return False
    _            -> throwLMDBErrNum ctx rc
{-# INLINE checkPut #-}

--------------------------------------------------------------------------------

-- | Allocate space for data under a given key. This space must be
-- filled before the write transaction commits. The idea here is to
-- avoid an extra allocation.
--
--     mdb_reserve flags txn dbi key byteCount
--
-- Note: not safe to use with MDB_DUPSORT.
-- Note: MDB_KEYEXIST will result in an exception here.
dbiReserve :: MonadIO m
  => MDB_write_flags -> Txn -> Dbi k v -> Value k -> Int -> m (Value v)
dbiReserve wf txn dbi key szBytes = liftIO $
    withKVPtrs key (reserveData szBytes) $ \pKey pVal -> do
    mdb_put (txnRaw txn) (fromDbi dbi) pKey pVal (MDB_RESERVE<>wf)
        >>= checkSuccess "dbiReserve"
    peek pVal
{-# INLINE dbiReserve #-}

reserveData :: Int -> Value v
reserveData szBytes = MDB_val (fromIntegral szBytes) nullPtr

--------------------------------------------------------------------------------

-- | Delete a given key, or a specific (key,value) pair in case of
-- MDB_DUPSORT. This function will return False on a MDB_NOTFOUND
-- result, and True on MDB_SUCCESS.
--
-- Note: Ideally, LMDB would match the value even without MDB_DUPSORT.
-- But it doesn't. Under the hood, the data is replaced by a null ptr
-- if MDB_DUPSORT is not enabled (v0.9.10).
dbiDel :: MonadIO m => Txn -> Dbi k v -> Value k -> Maybe (Value v) -> m Bool
dbiDel txn dbi key mbVal = liftIO $
    withKVOptPtrs key mbVal (mdb_del (txnRaw txn) (fromDbi dbi))
        >>= checkDel "dbiDel"
{-# INLINE dbiDel #-}

checkDel :: MonadIO m => ByteString -> MDB_return_code -> m Bool
checkDel ctx rc = case rc of
    MDB_SUCCESS  -> return True
    MDB_NOTFOUND -> return False
    _            -> throwLMDBErrNum ctx rc
{-# INLINE checkDel #-}

intOrdering :: CInt -> Ordering
intOrdering = (`compare` 0)

-- | compare two values as keys in a database
dbiCmp :: MonadIO m => Txn -> Dbi k v -> Value k -> Value k -> m Ordering
dbiCmp txn dbi k1 k2 = liftIO $
    intOrdering <$> withKVPtrs k1 k2 (mdb_cmp (txnRaw txn) (fromDbi dbi))
{-# INLINE dbiCmp #-}

-- | compare two values as data in an MDB_DUPSORT database
dbiDCmp :: MonadIO m => Txn -> Dbi k v -> Value k -> Value k -> m Ordering
dbiDCmp txn dbi k1 k2 = liftIO $
    intOrdering <$> withKVPtrs k1 k2 (mdb_dcmp (txnRaw txn) (fromDbi dbi))
{-# INLINE dbiDCmp #-}

-- | open a cursor for the database.
crsOpen :: MonadIO m => Txn -> Dbi k v -> m (Cursor k v)
crsOpen txn dbi = liftIO $
    alloca $ \pCursor -> do
    mdb_cursor_open (txnRaw txn) (fromDbi dbi) pCursor
        >>= checkSuccess "cursorOpen"
    cursor <- peek pCursor
    return $! Cursor
        { crsRaw = cursor
        , crsDbi = dbi
        , crsTxn = txn
        }

-- | Low-level mdb_cursor_get operation, with direct control of how pointers to
-- values are allocated, whether an argument is a nullPtr, and so on.
--
-- In this case, False is returned for 'MDB_NOTFOUND' (in which case the
-- cursor should not be moved), and True is returned for 'MDB_SUCCESS'. Any
-- other return value from LMDB will result in an exception.
--
-- Depending on the MDB_cursor_op, additional values may be returned
-- via the pointers. At the moment
crsGet :: MonadIO m
    => MDB_cursor_op -> Cursor k v
    -> Ptr (Value k) -> Ptr (Value v)
    -> m Bool
crsGet op crs pKey pData = liftIO $
  mdb_cursor_get (crsRaw crs) pKey pData op
    >>= checkCrsGet "crsGet"
{-# INLINE crsGet #-}

checkCrsGet :: MonadIO m => ByteString -> MDB_return_code -> m Bool
checkCrsGet ctx rc = case rc of
    MDB_SUCCESS  -> return True
    MDB_NOTFOUND -> return False
    _            -> throwLMDBErrNum ctx rc
{-# INLINE checkCrsGet #-}

-- | Low-level 'mdb_cursor_put' operation.
--
-- As with dbiPut, this returns True on MDB_SUCCESS and False for MDB_KEYEXIST,
-- and otherwise throws an exception.
crsPut :: MonadIO m => MDB_write_flags -> Cursor k v -> Value k ->
  Value v -> m Bool
crsPut wf crs key val = liftIO $
    withKVPtrs key val $ \pKey pVal ->
    mdb_cursor_put (crsRaw crs) pKey pVal wf
        >>= checkCrsPut "crsPut"
{-# INLINE crsPut #-}

checkCrsPut :: MonadIO m => ByteString -> MDB_return_code -> m Bool
checkCrsPut ctx rc = case rc of
    MDB_SUCCESS  -> return True
    MDB_KEYEXIST -> return False
    _            -> throwLMDBErrNum ctx rc
{-# INLINE checkCrsPut #-}

-- | Delete the value at the cursor.
crsDel :: MonadIO m => MDB_write_flags -> Cursor k v -> m ()
crsDel wf crs = liftIO $ mdb_cursor_del (crsRaw crs) wf >>= checkCrsDel "crsDel"
{-# INLINE crsDel #-}

checkCrsDel :: MonadIO m => ByteString -> MDB_return_code -> m ()
checkCrsDel = checkSuccess
{-# INLINE checkCrsDel #-}

-- | Close a cursor. don't use after this. In general, cursors should
-- be closed before their associated transaction is commited or aborted.
cursorClose :: MonadIO m => Cursor k v -> m ()
cursorClose crs = liftIO $ mdb_cursor_close (crsRaw crs)

-- | count number of duplicate data items at cursor's current location.
crsCount :: MonadIO m => Cursor k v -> m Int
crsCount crs = liftIO $
    alloca $ \pCount -> do
    mdb_cursor_count (crsRaw crs) pCount
        >>= checkSuccess "crsCount"
    fromIntegral <$> peek pCount
{-# INLINE crsCount #-}

--------------------------------------------------------------------------------

-- | Access a value by key. Returns Nothing if the key is not found.
unsafeDbiGet :: MonadIO m => Txn -> Dbi k v -> Value k -> m (Maybe (Value v))
unsafeDbiGet txn dbi key = liftIO $
    withKVPtrs key zed $ \pKey pVal ->
    unsafe_mdb_get (txnRaw txn) (fromDbi dbi) pKey pVal
        >>= checkGet "unsafeDbiGet" pVal
{-# INLINE unsafeDbiGet #-}

unsafeDbiPut :: MonadIO m
    => MDB_write_flags -> Txn -> Dbi k v
    -> Value k -> Value v -> m Bool
unsafeDbiPut wf txn dbi key val = liftIO $
    withKVPtrs key val $ \pKey pVal ->
    unsafe_mdb_put (txnRaw txn) (fromDbi dbi) pKey pVal wf
        >>= checkPut "unsafeDbiPut"
{-# INLINE unsafeDbiPut #-}

unsafeDbiReserve :: MonadIO m
    => MDB_write_flags -> Txn -> Dbi k v
    -> Value k -> Int -> m (Value v)
unsafeDbiReserve wf txn dbi key szBytes = liftIO $
  withKVPtrs key (reserveData szBytes) $ \pKey pVal -> do
    unsafe_mdb_put (txnRaw txn) (fromDbi dbi) pKey pVal (MDB_RESERVE<>wf)
      >>= checkSuccess "unsafeDbiReserve"
    peek pVal
{-# INLINE unsafeDbiReserve #-}

unsafeDbiDel :: MonadIO m
    => Txn -> Dbi k v
    -> Value k -> Maybe (Value v) -> m Bool
unsafeDbiDel txn dbi key mbVal = liftIO $
    withKVOptPtrs key mbVal (unsafe_mdb_del (txnRaw txn) (fromDbi dbi))
        >>= checkDel "unsafeDbiDel"
{-# INLINE unsafeDbiDel #-}

unsafeCrsGet :: MonadIO m
    => MDB_cursor_op -> Cursor k v
    -> Ptr (Value k) -> Ptr (Value v) -> m Bool
unsafeCrsGet op crs pKey pData = liftIO $
    unsafe_mdb_cursor_get (crsRaw crs) pKey pData op
        >>= checkCrsGet "unsafeCrsGet"
{-# INLINE unsafeCrsGet #-}

unsafeCrsPut :: MonadIO m
    => MDB_write_flags -> Cursor k v
    -> Value k -> Value v -> m Bool
unsafeCrsPut wf crs key val = liftIO $
    withKVPtrs key val $ \pKey pVal ->
    unsafe_mdb_cursor_put (crsRaw crs) pKey pVal wf
        >>= checkCrsPut "unsafeCrsPut"
{-# INLINE unsafeCrsPut #-}

unsafeCrsDel :: MonadIO m => MDB_write_flags -> Cursor k v -> m ()
unsafeCrsDel wf crs = liftIO $
    unsafe_mdb_cursor_del (crsRaw crs) wf
        >>= checkCrsDel "unsafeCrsDel"
{-# INLINE unsafeCrsDel #-}

unsafeDbiCmp :: MonadIO m => Txn -> Dbi k v -> Value k -> Value k -> m Ordering
unsafeDbiCmp txn dbi k1 k2 = liftIO $
    intOrdering <$> withKVPtrs k1 k2 (unsafe_mdb_cmp (txnRaw txn) (fromDbi dbi))
{-# INLINE unsafeDbiCmp #-}

-- | compare two values as data in an MDB_DUPSORT database
unsafeDbiDCmp :: MonadIO m => Txn -> Dbi k v -> Value k -> Value k -> m Ordering
unsafeDbiDCmp txn dbi k1 k2 = liftIO $
    intOrdering <$> withKVPtrs k1 k2 (unsafe_mdb_dcmp (txnRaw txn) (fromDbi dbi))
{-# INLINE unsafeDbiDCmp #-}

unsafeCrsCount :: MonadIO m => Cursor k v -> m Int
unsafeCrsCount crs = liftIO $
    alloca $ \pCount -> do
    unsafe_mdb_cursor_count (crsRaw crs) pCount
        >>= checkSuccess "unsafeCrsCount"
    fromIntegral <$> peek pCount
{-# INLINE unsafeCrsCount #-}

--------------------------------------------------------------------------------

-- | Check for stale readers, and return number of stale readers cleared.
readerCheck :: MonadIO m => Env -> m Int
readerCheck env = liftIO $
    alloca $ \pCount -> do
    mdb_reader_check (envRaw env) pCount
        >>= checkSuccess "readerCheck"
    fromIntegral <$> peek pCount

-- | Dump entries from reader lock table (for human consumption)
readerList :: MonadIO m => Env -> m [ByteString]
readerList env = liftIO $ do
    rf <- newIORef []
    let onMsg :: MDB_msg_func ()
        onMsg cs _ = do
            msg <- BS.packCString cs
            modifyIORef' rf (msg:)
            return 0

    withMsgFunc onMsg $ \pMsgFunc ->
      mdb_reader_list (envRaw env) pMsgFunc nullPtr
        >>= checkSuccess "readerList" . fromIntegral
    readIORef rf

-- | use a nullable CString
withCStringMaybe :: Maybe String -> (CString -> IO a) -> IO a
withCStringMaybe Nothing  f = f nullPtr
withCStringMaybe (Just s) f = withCString s f

withMsgFunc ::
  MDB_msg_func a -> (FunPtr (MDB_msg_func a) -> IO b) -> IO b
withMsgFunc f = bracket (wrapMsgFunc f) freeHaskellFunPtr

-- | utility function: prepare pointers suitable for mdb_cursor_get.
withKVPtrs ::
       Value k
    -> Value v
    -> (Ptr (Value k) -> Ptr (Value v) -> IO a)
    -> IO a
withKVPtrs k v fn =
    allocaBytes (2 * sizeOf k) $ \pK ->
    let pV = pK `plusPtr` sizeOf k in
    poke pK k >> poke pV v >> fn pK pV
{-# INLINE withKVPtrs #-}

-- | variation on withKVPtrs with nullable value.
withKVOptPtrs ::
       Value k -> Maybe (Value v)
    -> (Ptr (Value k) -> Ptr (Value v) -> IO a)
    -> IO a
withKVOptPtrs k (Just v) fn = withKVPtrs k v fn
withKVOptPtrs k Nothing fn  = alloca $ \pK -> poke pK k >> fn pK nullPtr

--------------------------------------------------------------------------------

withCursor :: (MonadMask m, MonadIO m)
    => Txn -> Dbi k v -> (Cursor k v -> m a) -> m a
withCursor txn dbi = bracket (crsOpen txn dbi) cursorClose

--------------------------------------------------------------------------------
