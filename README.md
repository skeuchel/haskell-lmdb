haskell-lmdb
============

Bindings for LMDB from Haskell. This will be released as just the `lmdb` package on hackage, since the Haskell aspect is implicit. To install haskell-lmdb, you'll need to first install the lmdb development files (e.g. `sudo apt-get install liblmdb-dev` works in at least my version of Ubuntu). 

These bindings are developed against LMDB 0.9.10. 

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

This package aims to present LMDB as it exists, with a relatively thin layer for safety.

# Haskell Bindings

There are two main bindings to LMDB from Haskell. 

1. `Database.LMDB.Raw`, which is mostly faithful to `#include <lmdb.h>` in C.
2. `Database.LMDB`, which provides a monadic interface with bytestring values.

The raw interface does follow Haskell conventions, e.g. representing flag fields with lists or unexpected errors via exceptions. However, there are many safety caveats for the raw interface:

* write transactions must use an OS bound thread
* an `MDB_val` mustn't be used outside its transaction
* read-only transactions cannot be used for writes
* don't open the same environment file twice

The `Database.LMDB` interface offers some greater convenience and safety, and should have a negligible impact on performance.

*Aside:* These bindings use an internal mutex to prevent multiple concurrent write transactions from the Haskell layer. M:N threading + concurrent writers is a buggy combination for LMDB 0.9.10. 
