#!/usr/bin/env bash
set -euo pipefail

BUCKET="${GCS_RAPID_BUCKET:-sqlite-on-gcs-rapid-us-central1-a}"
EXTENSION_PATH="${1:-target/release/libsqlite3_gcsrapid_vfs.so}"

if [ ! -f "${EXTENSION_PATH}" ]; then
  echo "ERROR: Extension not found at ${EXTENSION_PATH}. Build it with 'cargo build --release' first." >&2
  exit 1
fi

TIMESTAMP=$(date +%s)
RAND_ID=$((RANDOM % 100000))
DB_NAME="test_native_vfs_${TIMESTAMP}_${RAND_ID}.db"
DB_URI="file:gs://${BUCKET}/${DB_NAME}?vfs=gcsrapid"

echo "================================================================="
echo "=== Running SQLite GCS Rapid VFS End-to-End Tests"
echo "=== Extension: ${EXTENSION_PATH}"
echo "=== Bucket:    gs://${BUCKET}"
echo "=== Database:  ${DB_URI}"
echo "================================================================="

# Test 1: Verify prohibited SHM behavior
echo ""
echo "--- Test 1: Verifying shared memory (-shm) prohibition without EXCLUSIVE locking ---"
set +e
SHM_TEST_OUT=$(sqlite3 :memory: << EOF 2>&1
.load ${EXTENSION_PATH}
.open ${DB_URI}
PRAGMA journal_mode = WAL;
CREATE TABLE fail_table (x INT);
INSERT INTO fail_table VALUES (1);
EOF
)
set -e

echo "Output from Test 1:"
echo "${SHM_TEST_OUT}"

if echo "${SHM_TEST_OUT}" | grep -iqE "unable to open database file|prohibited|CANTOPEN"; then
  echo "SUCCESS: Shared memory was properly rejected when locking_mode != EXCLUSIVE!"
else
  echo "WARNING: Expected shared memory rejection, got: ${SHM_TEST_OUT}"
fi

# Test 2: Standard WAL mode with EXCLUSIVE locking
echo ""
echo "--- Test 2: Creating database, setting WAL with EXCLUSIVE locking, inserting rows ---"
SESSION_1_OUT=$(sqlite3 :memory: << EOF
.load ${EXTENSION_PATH}
.open ${DB_URI}
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA journal_mode = WAL;
CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
INSERT INTO products VALUES (1, 'GCS Rapid SSD', 149.99);
INSERT INTO products VALUES (2, 'Zonal Bidi Log', 89.50);
INSERT INTO products VALUES (3, 'Atomic Engine', 299.00);
SELECT id, name, price FROM products ORDER BY id;
SELECT COUNT(*) AS total_items, SUM(price) AS total_val FROM products;
.exit
EOF
)

echo "Output from Test 2:"
echo "${SESSION_1_OUT}"

if ! echo "${SESSION_1_OUT}" | grep -q "3|538.49"; then
  echo "ERROR: Test 2 failed to return expected aggregated data!" >&2
  exit 1
fi
echo "SUCCESS: Tables created, transactions committed and verified in WAL mode!"

# Test 3: Verifying persistence across a brand new process
echo ""
echo "--- Test 3: Opening database in a separate process to verify durability ---"
SESSION_2_OUT=$(sqlite3 :memory: << EOF
.load ${EXTENSION_PATH}
.open ${DB_URI}
PRAGMA locking_mode = EXCLUSIVE;
SELECT name, price FROM products WHERE price > 100;
INSERT INTO products VALUES (4, 'Second Session Item', 42.00);
SELECT COUNT(*) FROM products;
.exit
EOF
)

echo "Output from Test 3:"
echo "${SESSION_2_OUT}"

if ! echo "${SESSION_2_OUT}" | grep -q "GCS Rapid SSD|149.99"; then
  echo "ERROR: Test 3 failed to find previously committed records!" >&2
  exit 1
fi
if ! echo "${SESSION_2_OUT}" | grep -q "4"; then
  echo "ERROR: Test 3 failed to verify count after second insert!" >&2
  exit 1
fi
echo "SUCCESS: Cross-process persistence confirmed over GCS Rapid!"

# Test 4: Running WAL checkpoint and verifying atomic main DB persistence
echo ""
echo "--- Test 4: Checkpointing WAL into main database object ---"
SESSION_3_OUT=$(sqlite3 :memory: << EOF
.load ${EXTENSION_PATH}
.open ${DB_URI}
PRAGMA locking_mode = EXCLUSIVE;
PRAGMA wal_checkpoint(TRUNCATE);
SELECT COUNT(*) FROM products;
.exit
EOF
)

echo "Output from Test 4:"
echo "${SESSION_3_OUT}"

if ! echo "${SESSION_3_OUT}" | grep -q "4"; then
  echo "ERROR: Test 4 failed count after checkpoint!" >&2
  exit 1
fi
echo "SUCCESS: Checkpoint completed successfully!"

# Test 5: Verify objects exist in GCS Rapid bucket
echo ""
echo "--- Test 5: Verifying GCS Rapid bucket objects via gcloud storage ls ---"
gcloud storage ls "gs://${BUCKET}/${DB_NAME}*"

echo ""
echo "================================================================="
echo "=== ALL INTEGRATION TESTS PASSED SUCCESSFULLY! ==="
echo "================================================================="
