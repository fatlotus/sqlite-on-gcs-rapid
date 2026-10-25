# SQLite3-on-Object Storage w/speed and durability

**Disclaimer**: This repository is not an official Google product. Do not use it in production. It is >97% slop (probably).

**Background**: sqlite has long been a useful primitive for adding schemas on top of lower level storage. Typically people go one of two ways with it:

1. You can use a bespoke Raft log for commits, and use a custom backend for the sqlite database. I believe this approach was first pioneered by rqlite (https://philipotoole.com/twelve-years-of-distributed-sqlite/), but has been used by many projects over the years.

    **Downsides**: You gotta run a server; the server needs to speak a bespoke protocol.

2. Alternatively, you can use object storage. Each write then uploads either the entire database, or writes a bunch of tiny objects to S3. This second approach is probably best exemplified by Litestream (https://litestream.io/), which streams out commits to the WAL as writes to object storage.

    **Downsides**: Uploading an object to typical object storage is pretty slow.

But- there's another way! Google Cloud Storage recently added support for appendable objects (https://docs.cloud.google.com/storage/docs/rapid/use-objects-in-zonal-buckets) — notably, it's faster to append to an object (~1ms) than it is to upload a new object (~10ms).

In that sense, you get the best of both worlds: no server is required, but you still get the durability of a Raft-level solution, and commit latency is much faster than Litestream.

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

-- 2. Open a local database file using the appendonly VFS
.open file:demo.db?vfs=appendonly

-- 3. Create a table, insert records, and query
CREATE TABLE demo_table(id INTEGER PRIMARY KEY, message TEXT);
INSERT INTO demo_table (message) VALUES ('SQLite on GCS Rapid VFS!');
SELECT * FROM demo_table;

-- 4. Exit
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

-- 2. Open the database using the GCS URI filename and specify the VFS
.open file:gcs://my-sqlite-rapid-bucket/db.sqlite?vfs=appendonly

-- 3. Standard SQL operations
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

## Log Import/Export CLI Tool

Because the append-only VFS format wraps the database in a custom record layout (4105-byte blocks), you cannot directly read the log-formatted database using standard, unmodified `sqlite3` without our VFS extension loaded.

The **Import/Export Tool** (`sqlite_log_tool`) allows you to easily convert between vanilla SQLite databases and our log-formatted append-only database format (which can be stored locally or on GCS).

### Features
*   **Import**: Converts a raw, standard SQLite database file into a log-formatted file. This is useful for migrating or uploading a standard database to GCS in an append-only, log-structured format.
*   **Export**: Reconstructs a standard, raw SQLite database file from a log-formatted file. The resulting database can then be opened by vanilla `sqlite3` or any other SQLite interface directly.

### Running the Tool

```bash
# Build the tool
bazel build //:sqlite_log_tool

# Export a log-formatted database (local or GCS) back to a raw database:
bazel-bin/sqlite_log_tool export gcs://my-sqlite-rapid-bucket/db.sqlite /path/to/local/raw.db
# Or from a local log:
bazel-bin/sqlite_log_tool export /path/to/local/log.db /path/to/local/raw.db

# Import a standard raw database into a log-formatted database (local or GCS):
bazel-bin/sqlite_log_tool import /path/to/local/raw.db gcs://my-sqlite-rapid-bucket/db.sqlite
# Or into a local log:
bazel-bin/sqlite_log_tool import /path/to/local/raw.db /path/to/local/log.db
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
*(Note: Only GCS Rapid zonal buckets support the **Appendable Object** feature; standard regional or multi-regional buckets do not support appends and cannot be used as a fallback for this VFS.)*
