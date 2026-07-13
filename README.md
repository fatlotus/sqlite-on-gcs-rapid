# SQLite3 Append-Only VFS on Google Cloud Storage (GCS)

This project implements a custom SQLite3 Virtual File System (VFS) C++ backend designed for append-only storage engines, with specific support for high-performance Google Cloud Storage (GCS) Rapid (zonal) buckets. 

Because files are append-only, the backend translates random-access logical page reads and writes into sequential appends on physical storage using an in-memory block translation layer and crash recovery scanner.

---

## Architecture & File Format

The database file is represented as a sequence of **4105-byte records**:
*   **Block Index** (`int64_t`, 8 bytes): The logical 4k block index (or `-1` to represent a truncate event).
*   **Data** (`uint8_t[4096]`, 4096 bytes): The 4k database page payload.
*   **Validity Flag** (`uint8_t`, 1 byte): `1` if the block is good/complete, or `0` if it is garbage/padding written to repair a partial block after a crash.

### Crash Recovery Scan
Upon opening a database:
1.  The physical file size is verified. If the size is not a multiple of 4105 bytes, the trailing incomplete write is padded with garbage bytes (`is_good = 0`) to align subsequent appends.
2.  The file is scanned sequentially from offset 0.
3.  Each valid record (where `is_good == 1`) reconstructs the block mapping in an in-memory `std::vector<int64_t>`. Later records automatically overwrite previous mappings for the same block index.
4.  Truncations (`block_index == -1`) prune out-of-bounds mappings and update the logical file size.

---

## Build Instructions

This project is built using **Bazel**.

Bazel automatically manages all dependencies (including Abseil, GoogleTest, and `google-cloud-cpp`).

```bash
# Build the SQLite3 loadable VFS extension shared library
bazel build //:libsqlite3_gcsvfs.so

# Run all VFS unit, end-to-end, and integration tests
bazel test //...
```

The compiled shared library will be located at `bazel-bin/libsqlite3_gcsvfs.so`.

---

## Quick Start / Demo with Prebuilt Extension

You can download and run a demo with the prebuilt extension directly without compiling from source.

### 1. Download the Extension
Use `curl` to download the appropriate prebuilt library from the [releases page](https://github.com/fatlotus/sqlite-on-gcs-rapid/releases/tag/v0.0.3):

**On macOS (ARM64):**
```bash
curl -L -o libsqlite3_gcsvfs.dylib https://github.com/fatlotus/sqlite-on-gcs-rapid/releases/download/v0.0.3/libsqlite3_gcsvfs-macos-arm64.dylib
```

**On Linux (x86_64):**
```bash
curl -L -o libsqlite3_gcsvfs.so https://github.com/fatlotus/sqlite-on-gcs-rapid/releases/download/v0.0.3/libsqlite3_gcsvfs-linux-x86_64.so
```

> [!NOTE]
> Mac OS will need to use the Homebrew cask version of sqlite3 in order to load an extension. The default system `sqlite3` disables loading external extensions for security.

### 2. Run the Demo
Launch the Homebrew-installed `sqlite3` shell and run the following commands to create and query a table using the `"appendonly"` VFS backend:

```sql
-- 1. Load the extension (omit the file extension; SQLite automatically appends .dylib or .so)
.load ./libsqlite3_gcsvfs

-- 2. Enable URI filename support
PRAGMA uri = ON;

-- 3. Open a local database file using the appendonly VFS
.open file:demo.db?vfs=appendonly

-- 4. Set page size to 4k (mandatory for the block mapper layer)
PRAGMA page_size = 4096;

-- 5. Create a table, insert records, and query
CREATE TABLE demo_table(id INTEGER PRIMARY KEY, message TEXT);
INSERT INTO demo_table (message) VALUES ('SQLite on GCS Rapid VFS!');
SELECT * FROM demo_table;

-- 6. Exit
.exit
```

---

## Operating the VFS Extension with Vanilla SQLite3

The built library is a standard SQLite3 loadable extension (https://sqlite.org/loadext.html). When loaded, it registers the `"appendonly"` VFS.

### 1. Configure Credentials
Ensure your environment is authenticated with Google Cloud Application Default Credentials (ADC) so the GCS SDK can connect to your bucket:
```bash
gcloud auth application-default login
```

### 2. Loading the Extension and Querying GCS
Because the database connection must be open using the `"appendonly"` VFS, you cannot pass the GCS URL directly on the command line if the extension is not yet loaded. Instead, you load the extension in an in-memory session, then use the `.open` command with a GCS URI filename:

```bash
# Start vanilla sqlite3 shell
bazel-bin/external/sqlite3+/shell
```

Inside the interactive prompt:
```sql
-- 1. Load the extension
.load bazel-bin/libsqlite3_gcsvfs.so

-- 2. Enable URI filename support
PRAGMA uri = ON;

-- 3. Open the database using the GCS URI filename and specify the VFS
.open file:gcs://my-sqlite-rapid-bucket/db.sqlite?vfs=appendonly

-- 4. Set page size to 4k (mandatory for the block mapper layer)
PRAGMA page_size = 4096;

-- 5. Standard SQL operations
CREATE TABLE test(val TEXT);
INSERT INTO test VALUES ('GCS C++ SDK');
SELECT * FROM test;
.exit
```

When you exit, the connection writer completes and finalizes the upload stream, committing the database object to Google Cloud Storage.

---

## Database Compaction

Because this VFS is append-only, every database page write adds a new record to the end of the physical file on GCS. Over time, as pages are modified, the file will contain outdated versions of blocks (garbage) and truncated block markers, leading to storage overhead and slower startup scan times.

The **Compactor Tool** solves this by transactionally rewriting the file:
1. It downloads the active database file (safeguarded by obtaining its current object generation).
2. It runs a sequential recovery scan locally via the `BlockMapper` to reconstruct the block mapping, identifying only the latest active blocks.
3. It transactionally streams only the active, valid blocks back to GCS.
4. The write operation uses GCS preconditions (`If-Generation-Match`) to guarantee transactional safety: if the database has been modified during compaction, the upload fails and no data is lost.

### Running the Compactor

Ensure your environment is authenticated with Application Default Credentials (ADC).

```bash
# Build the compactor tool
bazel build //:compactor

# Option 1: Run against a GCS database object using its gs:// URI
bazel run //:compactor -- gs://my-sqlite-rapid-bucket/db.sqlite

# Option 2: Run by passing the bucket and object names separately
bazel run //:compactor -- my-sqlite-rapid-bucket db.sqlite
```

---


## GCS Bucket Creation

### Creating a GCS Rapid (Zonal) Bucket
Zonal buckets reside in a single availability zone and utilize the **Rapid** storage class for ultra-low latency. Zonal buckets require **uniform bucket-level access** and **hierarchical namespaces** to be enabled:

```bash
gcloud storage buckets create gs://my-sqlite-rapid-bucket \
    --location=us-east4-a \
    --default-storage-class=RAPID \
    --uniform-bucket-level-access \
    --enable-hierarchical-namespace
```
*(Note: Zonal / Rapid buckets are currently in preview and require whitelisting on your GCP project. If your project is not whitelisted, the command will return a 400 Location Constraint error. Only GCS Rapid zonal buckets support the **Appendable Object** feature; standard regional or multi-regional buckets do not support appends and cannot be used as a fallback for this VFS.)*

