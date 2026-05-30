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

## Getting Started

### 1. Build the Code
The project is built using Bazel and modern C++17. You can compile in two configurations:

#### Option A: Offline / Mock Client Configuration (Recommended for local tests)
Compiles against a fast, high-performance mock client to allow complete local unit and integration testing without requiring internet access or active GCP credentials.
```bash
bazel build //:sqlite3_cli
bazel test //...
```

To run this configuration against real GCS buckets using the `gcloud` CLI as a fallback sync engine, set `USE_REAL_GCS=1` in your environment:
```bash
USE_REAL_GCS=1 bazel-bin/sqlite3_cli gcs://<your-bucket-name>/db.sqlite
```

#### Option B: Real Google Cloud Storage C++ SDK Configuration
Downloads and compiles the official `google-cloud-cpp` GCS client library and communicates natively via gRPC/REST APIs using your active Application Default Credentials (ADC).
```bash
bazel build //:sqlite3_cli --define=use_real_gcs_sdk=true --features=-layering_check --host_features=-layering_check --features=-use_header_modules --host_features=-use_header_modules
```

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

---

## Operating the SQLite3 GCS CLI

The custom `sqlite3_cli` binary registers the `"appendonly"` VFS backend and automatically enforces 4k page alignment (`PRAGMA page_size = 4096`) required by the block mapper.

### 1. Configure Credentials (for Real SDK mode)
Ensure your environment is authenticated with Application Default Credentials:
```bash
gcloud auth application-default login
```

### 2. Run Database Operations
You can run query statements directly from the command line:
```bash
# Create a table and insert rows
bazel-bin/sqlite3_cli gcs://my-sqlite-standard-bucket/db.sqlite \
    "CREATE TABLE users(id INT, name TEXT); INSERT INTO users VALUES (1, 'Alice');"

# Query the rows
bazel-bin/sqlite3_cli gcs://my-sqlite-standard-bucket/db.sqlite \
    "SELECT * FROM users;"
```

### 3. Interactive REPL Mode
If no SQL command is provided, the CLI enters an interactive prompt:
```bash
bazel-bin/sqlite3_cli gcs://my-sqlite-standard-bucket/db.sqlite
```
Inside the prompt, type standard SQL commands or type `.exit` to sync and close:
```sql
sqlite> CREATE TABLE test(val TEXT);
sqlite> INSERT INTO test VALUES ('GCS C++ SDK');
sqlite> SELECT * FROM test;
sqlite> .exit
```
When you exit or sync, the connection writer completes and finalizes the upload stream, committing the database object to Google Cloud Storage.
