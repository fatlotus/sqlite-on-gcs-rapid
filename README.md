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

This project can be built using either **Bazel** or **CMake**.

### 1. Build and Run Tests using Bazel

Bazel automatically manages all dependencies (including Abseil, GoogleTest, and `google-cloud-cpp`).

```bash
# Build the SQLite3 CLI binary
bazel build //:sqlite3_cli

# Run all VFS and storage unit tests
bazel test //...
```

The compiled binary will be located at `bazel-bin/sqlite3_cli`.

### 2. Build and Run Tests using CMake

To build with CMake, make sure you have the required dependencies (Abseil, GoogleTest, SQLite3, and the Google Cloud Storage C++ SDK) installed. You can manage them using a package manager like `vcpkg`.

#### With vcpkg integration:
```bash
# Configure the project pointing to the vcpkg toolchain
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE=~/vcpkg/scripts/buildsystems/vcpkg.cmake

# Compile targets
cmake --build build

# Run unit tests
cd build && ctest --output-on-failure
```

The compiled binary will be located at `build/sqlite3_cli`.

---

## Operating the SQLite3 GCS CLI

The custom `sqlite3_cli` binary registers the `"appendonly"` VFS backend and automatically enforces 4k page alignment (`PRAGMA page_size = 4096`) required by the block mapper.

### 1. Configure Credentials
Ensure your environment is authenticated with Google Cloud Application Default Credentials (ADC) so the GCS SDK can connect to your bucket:
```bash
gcloud auth application-default login
```

### 2. Run Database Queries Directly
You can run query statements directly from the command line by passing the `gcs://` URI and the SQL commands:
```bash
# Using Bazel binary:
bazel-bin/sqlite3_cli gcs://my-sqlite-rapid-bucket/db.sqlite \
    "CREATE TABLE users(id INT, name TEXT); INSERT INTO users VALUES (1, 'Alice');"

# Using CMake binary:
./build/sqlite3_cli gcs://my-sqlite-rapid-bucket/db.sqlite \
    "SELECT * FROM users;"
```

### 3. Interactive REPL Mode
If no SQL command is provided, the CLI enters an interactive prompt:
```bash
# Using Bazel binary:
bazel-bin/sqlite3_cli gcs://my-sqlite-rapid-bucket/db.sqlite

# Using CMake binary:
./build/sqlite3_cli gcs://my-sqlite-rapid-bucket/db.sqlite
```

Inside the interactive REPL prompt, type standard SQL commands or type `.exit` to sync and close:
```sql
sqlite> CREATE TABLE test(val TEXT);
sqlite> INSERT INTO test VALUES ('GCS C++ SDK');
sqlite> SELECT * FROM test;
sqlite> .exit
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
