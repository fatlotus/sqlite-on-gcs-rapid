#!/bin/bash
set -euo pipefail

echo "=== Running E2E SQLite VFS Extension Shell Test ==="

# Set vanilla sqlite3 shell and extension paths directly from Bazel runfiles layout
SHELL_PATH="../sqlite3+/shell"
EXT_PATH="./libsqlite3_gcsvfs.so"

if [ ! -f "$SHELL_PATH" ]; then
  echo "Error: Could not find vanilla sqlite3 shell binary at $SHELL_PATH"
  exit 1
fi

if [ ! -f "$EXT_PATH" ]; then
  echo "Error: Could not find libsqlite3_gcsvfs shared library at $EXT_PATH"
  exit 1
fi

echo "Using SQLite Shell: $SHELL_PATH"
echo "Using VFS Extension: $EXT_PATH"

DB_FILE="e2e_shell_test.db"
rm -f "$DB_FILE"

# 1. Create a table and insert data
echo "Creating table and inserting data..."
"$SHELL_PATH" <<EOF
.load $EXT_PATH
PRAGMA uri=on;
.open file:$DB_FILE?vfs=appendonly
PRAGMA page_size = 4096;
CREATE TABLE shell_test (id INT, val TEXT);
INSERT INTO shell_test VALUES (1, 'Hello from Shell!');
.exit
EOF

if [ ! -f "$DB_FILE" ]; then
  echo "Error: Database file $DB_FILE was not created"
  exit 1
fi

# 2. Query data to verify persistence
echo "Querying data to verify persistence..."
OUTPUT=$("$SHELL_PATH" <<EOF
.load $EXT_PATH
PRAGMA uri=on;
.open file:$DB_FILE?vfs=appendonly
SELECT val FROM shell_test;
.exit
EOF
)

# Strip any carriage returns or extra whitespace
OUTPUT=$(echo "$OUTPUT" | tr -d '\r' | xargs)

echo "Result: $OUTPUT"
if [ "$OUTPUT" != "Hello from Shell!" ]; then
  echo "Error: Expected 'Hello from Shell!' but got '$OUTPUT'"
  exit 1
fi

rm -f "$DB_FILE"
echo "=== E2E SQLite VFS Extension Shell Test Passed! ==="
