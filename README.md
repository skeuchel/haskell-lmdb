haskell-lmdb
============

Bindings for LMDB from Haskell. This will be released as just the `lmdb` package on hackage, since the Haskell aspect is implicit. To install haskell-lmdb, you'll need to first install the lmdb development files (e.g. `sudo apt-get install liblmdb-dev` would work in Ubuntu). You'll also need to `cabal install c2hs` if you don't already have it.

# Lightning MDB

[Lightning MDB](http://symas.com/mdb/) (LMDB) is a Berkeley DB replacement developed for the OpenLDAP project, primarily by Howard Chu and Symas Corp. Besides its performance, which is impressive, LMDB has a lot to offer: 

* commercial-friendly BSD-style Open LDAP license
* stable latencies suitable for real-time
* tight memory usage without double-buffering
* simplicity, transparency, comprehensibility

While key-value databases are becoming more popular, it seems many of them are adding lots of features (new table types, clever optimizations with thread-local storage, etc.). This makes me wary: new features frequently come with new bugs. At least so far, it seems LMDB has avoided this trend and focused instead on refining the relatively few features it has. LMDB does one thing - memory-mapped B-trees - and does so very well.

LMDB is read-optimized, and outperforms most existing databases for read-only transactions. Write performance is also good, outperforming most other B-tree databases such as Berkeley DB. Log structured merge trees, such as LevelDB, are write-optimized and will tend to outperform LMDB in that area. LMDB is a very good choice for a read-heavy workload.

Known weaknesses of LMDB:

* Writer is single-threaded; doesn't merge non-conflicting writes.
* Read-only transactions are decided ahead of time, no promotion.
* Does not detect and merge non-conflicting read-write operations.
* The database only ever grows. No direct support for compacting.
* Set a maximum database size (weakness or strength?) when opening.

Many of these weaknesses could be mitigated through a high-level binding, e.g. leveraging STM-like abstractions. But this package aims to present LMDB as it exists, with only a thin layer for extra safety.

# Haskell Bindings

The challenges in making LMDB available to Haskell involve:

1. interaction with Haskell's green threads model
2. minimize unnecessary copies of data.

Fortunately, LMDB supports an `MDB_NOTLS` option to support the M:N threading models. Unfortunately, this only impacts read-only transactions; a writer transaction must operate in a specific bound thread. This is doable, but a little annoying. It would be preferable if `MDB_NOTLS` also applied to writers and the write locks.

To avoid copies of data, it is possible to use `fromForeignPointer` from the `Data.ByteString.Internal` package. However, we'll need to be careful because this foriegn pointer may become invalid after a transaction. A potential approach is to protect these pointers via the type system, in a manner similar to STRef, and limit them to simple indexing and slicing operations (and fmap'd functions).

# Hints

Before starting any other threads:
  Create the environment.
  Open a transaction.
  Open all DBI handles the app will need.
  Commit the transaction.

