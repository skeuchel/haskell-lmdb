haskell-lmdb
============

Bindings for LMDB from Haskell. This will be released as just the `lmdb` package on hackage, since the Haskell aspect is implicit. To install haskell-lmdb, you'll need to first install the lmdb development files (e.g. `sudo apt-get install liblmdb-dev` works in at least my version of Ubuntu). 

These bindings are developed against LMDB 0.9.10. If LMDB is updated in a significant way that e.g. adds new features or relaxes old constraints, and it isn't clear that I've already noticed, please file an issue report! Or a pull request. 

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

# Haskell Bindings to LMDB

From a Haskell programmer's perspective, LMDB's API is very ad-hoc. Functions do many different things based on flags, and it's difficult to tell what a function does or returns just by studying the type signatures. Transactions and cursors and databases all have many different, implicit 'types' - e.g. there are `MDB_DUPSORT` databases that allow multiple values for a key, and `MDB_DUPFIXED` databases with fixed-size elements allowing calls to `MDB_MULTIPLE`. The same transaction type is used for both read-only and read-write transactions.

For this binding, I'm focusing on a useful subset of the LMDB API:

* put,get,del of key,value pairs
* reserve space for large values
* use cursors to read database

At the moment, I only have a 'raw' interface, `Database.LMDB.Raw`, which is mostly faithful to `#include <lmdb.h>` in C. The raw interface does follow Haskell FFI conventions, e.g. representing flag fields with lists or errors via exceptions. However, there are many safety caveats for the raw interface:

* write transactions must use an OS bound thread
* don't open the same environment file twice!
* an `MDB_val` mustn't be used outside its transaction
* the normal LMDB API flags and functions cautions

LMDB's writer mutex, which is held for the full duration of a write transaction, causes all sorts of problems for Haskell's lightweight threads. I currently introduce a simple `MVar` mutex at the `MDB_env` layer. Opening the same environment twice (concurrently) would bypass this. Fortunately, the expected use-case for LMDB is to open an environment once when the application starts up, then keep it open until the application shuts down or crashes.

Safe FFI calls are enough overhead to significantly impact a microbenchmark reading the LMDB database (especially if we read it sequentially). This might not be a big deal for 'real' code, where we perform any significant processing of returned values. But, in the interest of performance freaks like me, the 'raw' interface exports the read/write/cursor operations twice: once with `MDB_dbi` and again for `MDB_dbi'` (and similar for cursors). The difference between these is that the `MDB_dbi` allows user-defined comparisons but requires 'safe' FFI calls (which adds ~100ns overhead), while `MDB_dbi'` uses unsafe FFI calls but forbids user-defined comparisons (which adds ~10ns overhead). 

Likely, most users neither need nor want safe FFI for most databases.

I would like to later add some higher level interfaces, but I'll mostly be adding features as I need them. Anyone who needs more is welcome to provide a pull request. 



