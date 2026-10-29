# SQLite VFS on Google Cloud Storage Rapid

A native Rust SQLite Virtual File System (VFS) loadable extension (`.so`) designed specifically for **Google Cloud Storage (GCS) Rapid** zonal buckets.

It combines standard atomic object semantics for main database files with **GCS Rapid appendable objects** for SQLite Write-Ahead Logs (`-wal`), streaming all I/O via bidirectional gRPC channels (**Bidi streams**).

---

## Key Features

1. **Atomic Database Object Semantics**:
   - The primary SQLite database file is managed using standard atomic object semantics.
   - Durably commits pages and changes using generation-preconditioned atomic overwrites.
   - Reads page ranges using `google-cloud-storage` gRPC Bidi streaming (`BidiReadObject`).

2. **GCS Rapid WAL Engine**:
   - SQLite `-wal` files leverage GCS Rapid zonal storage with **atomic appendability** (`open_appendable_object`, `reopen_appendable_object`, and `append`).
   - Transaction commits synchronously flush WAL frames using client write queue flush (`flush().await`), providing 100% crash durability with sub-millisecond tail latency.
   - Full support for WAL generation rollover and checkpointing (`wal_checkpoint(TRUNCATE)`).

3. **High-Performance Bidirectional Streaming (Bidi Streams)**:
   - Uses `google-cloud-storage` with the `bidi` feature enabled.
   - Both main database reads and WAL appends execute over multiplexed gRPC Bidi streams, avoiding HTTP/1.1 and REST stalls.

4. **Explicit Shared Memory (`-shm`) Prohibition**:
   - SQLite shared memory files (`-shm`) rely on POSIX shared memory or mmap across local processes. Because GCS storage is distributed across VMs and networks, local `-shm` coordination is unsafe.
   - Shared memory files are strictly prohibited with explicit, actionable error messages guiding the user to execute `PRAGMA locking_mode = EXCLUSIVE;`.

5. **Production Diagnostics & Rich Error Reporting**:
   - Detailed diagnostics provided via SQLite's error logging hook (`sqlite3_log`), VFS `xGetLastError`, and `tracing`.
   - Actionable troubleshooting guidance when opening files or misconfiguring pragmas.

6. **Drop-in Vanilla SQLite Compatibility**:
   - Compiles to a standard shared object (`libsqlite3_gcsrapid_vfs.so`).
   - Loadable into unmodified vanilla `sqlite3` (from Ubuntu/Debian `apt-get install sqlite3`) via `.load` or `sqlite3_load_extension()`.

---

## Architecture

```
+-------------------------------------------------------------------+
|               Vanilla SQLite3 (apt-get / system lib)              |
+-------------------------------------------------------------------+
                                  |
                                  | VFS Callbacks (xOpen, xRead, xWrite, xSync)
                                  v
+-------------------------------------------------------------------+
|                sqlite3-gcsrapid-vfs Extension                     |
+-------------------------------------------------------------------+
       |                                           |
       | Main DB Operations                        | WAL Operations
       v                                           v
+-------------------------------+   +-------------------------------+
|      GcsDbFile Engine         |   |      GcsWalFile Engine        |
| - Atomic object commit        |   | - Atomic appendability        |
| - Page cache & dirty tracking |   | - Unflushed buffer merge      |
| - Preconditioned generation   |   | - Rapid appendable writer     |
+-------------------------------+   +-------------------------------+
       |                                           |
       | BidiReadObject / Finalize                 | BidiWriteObject (Append/Flush)
       +---------------------+---------------------+
                             |
                             v
+-------------------------------------------------------------------+
|      google-cloud-storage (v1.21.0 gRPC Bidi streaming client)     |
+-------------------------------------------------------------------+
                             |
                             v
+-------------------------------------------------------------------+
|           Google Cloud Storage Rapid Zonal Bucket                 |
|       (e.g., gs://sqlite-on-gcs-rapid-us-central1-a)              |
+-------------------------------------------------------------------+
```

---

## Building the Extension

### Prerequisites
- Rust 1.80+ (with Cargo)
- `libsqlite3-dev` (for headers) and `sqlite3` CLI

```bash
# Build the optimized shared library
cargo build --release
```

The compiled shared object will be located at:
```
target/release/libsqlite3_gcsrapid_vfs.so
```

---

## Usage with Vanilla SQLite3 CLI

### 1. Launch `sqlite3` and Load Extension
```bash
sqlite3 :memory:
```

```sql
-- 1. Load the GCS Rapid VFS extension
.load target/release/libsqlite3_gcsrapid_vfs.so

-- Verify the VFS is registered
.vfslist
-- (Displays: gcsrapid, unix)

-- 2. Open a database directly on GCS Rapid
.open file:gs://sqlite-on-gcs-rapid-us-central1-a/my_database.db?vfs=gcsrapid

-- 3. Set locking mode to EXCLUSIVE (required for WAL on distributed storage)
PRAGMA locking_mode = EXCLUSIVE;

-- 4. Enable Write-Ahead Log (WAL) mode
PRAGMA journal_mode = WAL;

-- 5. Execute standard SQL transactions
CREATE TABLE customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    balance REAL NOT NULL
);

INSERT INTO customers VALUES (1, 'Alice Corp', 12500.50);
INSERT INTO customers VALUES (2, 'Bob Systems', 8400.00);

SELECT * FROM customers;

-- 6. Checkpoint WAL into the main database file
PRAGMA wal_checkpoint(TRUNCATE);

.exit
```

---

## Shared Memory (`-shm`) Prohibition Rules

If you forget to specify `PRAGMA locking_mode = EXCLUSIVE;` before entering WAL mode or issuing queries, SQLite will attempt to allocate a shared memory file (`<dbname>-shm`). The VFS intercepts and rejects this immediately with:

```
Runtime error: unable to open database file (14)
GCS VFS: Shared memory files (*-shm) are prohibited because database files are stored in GCS.
To operate with WAL mode on GCS, execute 'PRAGMA locking_mode = EXCLUSIVE;' before accessing the database.
```

By enforcing `EXCLUSIVE` locking mode, SQLite handles all WAL index state internally in process memory without attempting POSIX shared memory calls over cloud storage.

---

## Running Tests

### 1. Offline Unit Tests
Verify URI parsing, error buffers, zero-fill short read semantics, and SHM detection:
```bash
cargo test --test unit_tests
```

### 2. End-to-End Integration Tests Against Live GCS Rapid
Verify cross-process persistence, table creation, WAL append flushes, checkpoints, and GCS bucket verification against a live Rapid bucket:
```bash
# Optional: set custom Rapid bucket (defaults to sqlite-on-gcs-rapid-us-central1-a)
export GCS_RAPID_BUCKET="sqlite-on-gcs-rapid-us-central1-a"

cargo test --test integration_tests -- --nocapture
```

Or run the integration bash test suite directly:
```bash
./tests/e2e_sqlite3_cli.sh target/release/libsqlite3_gcsrapid_vfs.so
```

---

## License
Apache-2.0 or MIT.
