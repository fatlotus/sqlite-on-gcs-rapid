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

## GCS Bucket Creation

### Creating a GCS Rapid (Zonal) Bucket
Zonal buckets reside in a single availability zone and utilize the **Rapid** storage class for ultra-low latency. Zonal buckets require **uniform bucket-level access** and **hierarchical namespaces** to be enabled:

```bash
gcloud storage buckets create gs://my-sqlite-rapid-bucket \
    --location=us-east4-a \
    --uniform-bucket-level-access \
    --enable-hierarchical-namespace
```
*(Note: Zonal / Rapid buckets are currently in preview and require whitelisting on your GCP project. If your project is not whitelisted, the command will return a 400 Location Constraint error.)*

### Fallback: Standard Hierarchical Namespace Bucket
If your GCP project is not yet whitelisted for GCS Rapid buckets, you can use standard regional buckets with hierarchical namespaces enabled:
```bash
gcloud storage buckets create gs://my-sqlite-standard-bucket \
    --location=us-east4 \
    --uniform-bucket-level-access \
    --enable-hierarchical-namespace
```
