Lightning MDB bindings for Haskell
==================================

# Lightning MDB

Lightning MDB (LMDB) is a Berkeley DB (BDB) replacement developed for the OpenLDAP project, primarily by Howard Chu and Symas Corp. Besides raw performance, LMDB has a lot to offer: 

* commercial-friendly BSD-style Open LDAP license
* stable latencies suitable for real-time
* tight memory usage without double-buffering
* simplicity, transparency, comprehensibility

While key-value databases are becoming more popular, it seems a lot of them are adding lots of features (new table types, clever optimizations with thread-local storage, etc.) and new features frequently come with new bugs. At least so far, it seems LMDB has avoided this trend and focused instead on refining the few features it has. LMDB does just one thing - memory-mapped B-trees - and does it very well.

That said, LMDB's performance is an attractive feature. The read performance is outstanding. The write performance doesn't scale as well compared to LevelDB and its derivatives, but is considerably more efficient than Berkeley DB.

Known weaknesses of LMDB:

* Must decide read-only vs. read-write transaction in advance.
* Database doesn't shrink. (Might be improved later.)
* Growing the database requires halting it at least briefly.
* Lack of large-block compression across multiple keys and values. 
* High relative overhead for very small key-value pairs.
* Single threading for the writers.

# Haskell Bindings

The challenges in making LMDB available to Haskell involve:

1. interaction with Haskell's green threads model
2. minimize unnecessary copies of data.

LMDB offers an `MDB_NOLOCK` option that is very tempting. I could perform all the locking logic at the Haskell layer. Unfortunately, that would also undermine nice features like LMDB's hot backups and external debugging utilities. Instead, I'll be leveraging `MDB_NOTLS` for the readers, and forcing writer transactions into a bound thread.

To minimize copies of data, I want to use `Data.ByteString.Internal.fromForeignPtr` to reference binary structures without copying them. However, data structures referenced this way are *fragile*: they cannot be used outside the current transaction. So I plan to hide them within a data structure bound to the current transaction in a manner similar to STRefs. This requires a more medium-level binding between LMDB and Haskell.

Developers will be able to access the fragile structure via explicitly unsafe actions, and will have relatively safe operations for indexing or taking slices.

# Experimental Extensions

It seems feasible to me to implement an STM-like layer above LMDB. Doing so could mitigate a few of LMDB's weaknesses, in particular:

* Readers could be silently promoted to Read-Writers. 
* Non-conflicting writers may be serialized logically.
* Writers may be merged and batched, amortizing fsync.

I'm also tempted to support automatic resizing of the database, but that seems much less relevant. Setting a large database shouldn't be a problem.

Further, it may be very useful to be able to 'freeze' operations on a database, waiting for all active readers and writers to exit. This could be very useful in cases where we might wish to resize the database dynamically. OTOH, it might not be a big deal to just choose a big number to start with.




# Hints

Before starting any other threads:
  Create the environment.
  Open a transaction.
  Open all DBI handles the app will need.
  Commit the transaction.

