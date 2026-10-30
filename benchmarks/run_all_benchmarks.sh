#!/usr/bin/env bash
set -euo pipefail

echo "==============================================================================="
echo "SQLite on GCS Rapid VFS: Comprehensive Queue Depth Benchmarks"
echo "Date: $(date -u)"
echo "Host: $(hostname)"
echo "Bucket: ${GCS_RAPID_BUCKET:-sqlite-on-gcs-rapid-us-central1-a}"
echo "==============================================================================="

export RUST_LOG=warn

echo ""
echo ">>> STEP 1/2: Running Native GCS Rapid Bidi Stream Benchmarks (Phases 1 & 2)..."
./target/release/bench_queue_depth

echo ""
echo ">>> STEP 2/2: Running End-to-End SQLite VFS Transaction Benchmarks (Phase 3)..."
python3 benchmarks/bench_sqlite_vfs_qd.py

echo ""
echo "==============================================================================="
echo "All Queue Depth Benchmarks Completed Successfully!"
echo "==============================================================================="
