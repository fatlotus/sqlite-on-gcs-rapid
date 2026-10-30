#!/usr/bin/env python3
"""
Benchmark 3: End-to-End SQLite VFS Transaction Queue Depth.
Uses vanilla sqlite3 CLI loaded with libsqlite3_gcsrapid_vfs.so on GCS Rapid.
Measures Throughput (TPS, MB/s) and Latency distributions across Queue Depths (1, 2, 4, 8, 16, 32).
"""

import os
import sys
import time
import subprocess


BUCKET = os.environ.get("GCS_RAPID_BUCKET", "sqlite-on-gcs-rapid-us-central1-a")
EXTENSION = os.environ.get("VFS_EXTENSION", "target/release/libsqlite3_gcsrapid_vfs.so")
PAGE_SIZE = 4096  # 4KB WAL frame

def run_qd(qd: int, num_tx_per_worker: int, session_id: int):
    procs = []
    # 1. Spawn workers and initialize SQLite with GCS Rapid VFS in WAL mode
    for worker_id in range(qd):
        db_uri = f"file:gs://{BUCKET}/bench_p3_{session_id}_{qd}_{worker_id}.db?vfs=gcsrapid"
        p = subprocess.Popen(
            ["sqlite3", ":memory:"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        init_sql = (
            f".load {EXTENSION}\n"
            f".open {db_uri}\n"
            f"PRAGMA locking_mode = EXCLUSIVE;\n"
            f"PRAGMA journal_mode = WAL;\n"
            f"CREATE TABLE IF NOT EXISTS bench_data (id INT PRIMARY KEY, val TEXT);\n"
            f"SELECT 'READY_{worker_id}';\n"
        )
        p.stdin.write(init_sql)
        p.stdin.flush()
        procs.append(p)

    for worker_id, p in enumerate(procs):
        ready_tag = f"READY_{worker_id}"
        while True:
            line = p.stdout.readline()
            if not line:
                p.wait()
                print(f"\n[ERROR] Worker {worker_id} exited prematurely with code {p.returncode} during initialization!")
                sys.exit(1)
            if ready_tag in line:
                break

    # 2. Warmup
    for worker_id, p in enumerate(procs):
        p.stdin.write(f"BEGIN IMMEDIATE;\nINSERT OR REPLACE INTO bench_data VALUES (-1, 'warm');\nCOMMIT;\nSELECT 'WARM_{worker_id}';\n")
        p.stdin.flush()
    for worker_id, p in enumerate(procs):
        warm_tag = f"WARM_{worker_id}"
        while True:
            line = p.stdout.readline()
            if not line or warm_tag in line:
                break

    # 3. Timed benchmark run
    latencies = []
    start_all = time.perf_counter()

    for tx_idx in range(num_tx_per_worker):
        tx_start = time.perf_counter()
        tag = f"TX_{tx_idx}"
        payload = "x" * 256

        # Dispatch write transaction across all workers in parallel
        for worker_id, p in enumerate(procs):
            sql = f"BEGIN IMMEDIATE;\nINSERT OR REPLACE INTO bench_data VALUES ({tx_idx}, '{payload}');\nCOMMIT;\nSELECT '{tag}_{worker_id}';\n"
            p.stdin.write(sql)
            p.stdin.flush()

        # Await completion across all in-flight workers
        for worker_id, p in enumerate(procs):
            worker_tag = f"{tag}_{worker_id}"
            while True:
                line = p.stdout.readline()
                if not line or worker_tag in line:
                    break

        batch_elapsed_ms = (time.perf_counter() - tx_start) * 1000.0
        # Per transaction effective latency under concurrency
        for _ in range(qd):
            latencies.append(batch_elapsed_ms)

    total_time_sec = time.perf_counter() - start_all

    # Cleanup processes
    for p in procs:
        try:
            p.stdin.write(".exit\n")
            p.stdin.flush()
            p.wait(timeout=2)
        except Exception:
            p.kill()

    total_txs = qd * num_tx_per_worker
    tps = total_txs / total_time_sec
    mb_per_sec = (total_txs * PAGE_SIZE / (1024.0 * 1024.0)) / total_time_sec

    lats = sorted(latencies)
    n = len(lats)
    mean = sum(lats) / n if n > 0 else 0.0

    return {
        "qd": qd,
        "tps": tps,
        "mb_s": mb_per_sec,
        "min": lats[0] if n > 0 else 0.0,
        "p50": lats[min(int(n * 0.50), n - 1)] if n > 0 else 0.0,
        "p90": lats[min(int(n * 0.90), n - 1)] if n > 0 else 0.0,
        "p95": lats[min(int(n * 0.95), n - 1)] if n > 0 else 0.0,
        "p99": lats[min(int(n * 0.99), n - 1)] if n > 0 else 0.0,
        "max": lats[-1] if n > 0 else 0.0,
        "mean": mean,
    }

def main():
    session_id = int(time.time())
    print("===============================================================================")
    print("Phase 3: End-to-End SQLite VFS Transaction Queue Depth (Vanilla SQLite CLI)")
    print(f"Bucket: gs://{BUCKET}")
    print(f"Extension: {EXTENSION}")
    print(f"Session ID: {session_id}")
    print("===============================================================================\n")

    qds = [1, 2, 4, 8, 16, 32]
    results = []

    for qd in qds:
        num_tx = 20 if qd <= 4 else 15
        print(f"Running SQLite VFS QD = {qd:2d}... ", end="", flush=True)
        stats = run_qd(qd, num_tx, session_id)
        print(f"-> {stats['tps']:6.1f} TPS | {stats['mb_s']:5.2f} MB/s | p50: {stats['p50']:5.2f}ms | p90: {stats['p90']:5.2f}ms | p99: {stats['p99']:5.2f}ms")
        results.append(stats)

    print("\n### 3. End-to-End SQLite VFS Transaction Queue Depth (Vanilla SQLite CLI)")
    print("| Queue Depth (QD) | Throughput (TPS) | Bandwidth (MB/s) | Min Latency (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max Latency (ms) | Mean (ms) |")
    print("|:----------------:|:----------------:|:----------------:|:----------------:|:--------:|:--------:|:--------:|:--------:|:----------------:|:---------:|")
    for r in results:
        print(f"| {r['qd']:16d} | {r['tps']:16.1f} | {r['mb_s']:16.2f} | {r['min']:16.2f} | {r['p50']:8.2f} | {r['p90']:8.2f} | {r['p95']:8.2f} | {r['p99']:8.2f} | {r['max']:16.2f} | {r['mean']:9.2f} |")

if __name__ == "__main__":
    main()
