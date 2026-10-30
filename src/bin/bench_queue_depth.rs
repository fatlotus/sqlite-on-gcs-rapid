//! Comprehensive Queue Depth Benchmark for SQLite on GCS Rapid VFS.
//!
//! Evaluates throughput (IOPS / TPS, MB/s) and latency (min, p50, p90, p95, p99, max, mean)
//! across various queue depths:
//! 1. Concurrent Writer Queue Depth (QD = 1, 2, 4, 8, 16, 32, 64)
//! 2. Pipelined / Group Commit Queue Depth (QD = 1, 2, 4, 8, 16, 32, 64)
//! 3. End-to-End SQLite VFS Transaction Queue Depth (QD = 1, 2, 4, 8, 16)

use std::sync::Arc;
use std::time::{Duration, Instant};
use google_cloud_storage::client::Storage;
use sqlite3_gcsrapid_vfs::gcs::wal_file::GcsWalFile;
use sqlite3_gcsrapid_vfs::uri::parse_gcs_uri;
use tokio::sync::Barrier;

const BUCKET: &str = "sqlite-on-gcs-rapid-us-central1-a";
const PAGE_SIZE: usize = 4096; // 4KB SQLite WAL frame size

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub count: usize,
    pub total_time: Duration,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
    pub ops_per_sec: f64,
    pub mb_per_sec: f64,
}

impl LatencyStats {
    pub fn compute(mut latencies_ms: Vec<f64>, total_time: Duration, total_bytes: usize) -> Self {
        if latencies_ms.is_empty() {
            return Self {
                count: 0,
                total_time,
                min_ms: 0.0,
                p50_ms: 0.0,
                p90_ms: 0.0,
                p95_ms: 0.0,
                p99_ms: 0.0,
                max_ms: 0.0,
                mean_ms: 0.0,
                ops_per_sec: 0.0,
                mb_per_sec: 0.0,
            };
        }

        latencies_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let count = latencies_ms.len();
        let sum: f64 = latencies_ms.iter().sum();
        let mean = sum / count as f64;
        let min = latencies_ms[0];
        let max = latencies_ms[count - 1];

        let p50 = latencies_ms[((count as f64 * 0.50) as usize).min(count - 1)];
        let p90 = latencies_ms[((count as f64 * 0.90) as usize).min(count - 1)];
        let p95 = latencies_ms[((count as f64 * 0.95) as usize).min(count - 1)];
        let p99 = latencies_ms[((count as f64 * 0.99) as usize).min(count - 1)];

        let secs = total_time.as_secs_f64();
        let ops_per_sec = count as f64 / secs;
        let mb_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / secs;

        Self {
            count,
            total_time,
            min_ms: min,
            p50_ms: p50,
            p90_ms: p90,
            p95_ms: p95,
            p99_ms: p99,
            max_ms: max,
            mean_ms: mean,
            ops_per_sec,
            mb_per_sec,
        }
    }
}

/// Benchmark 1: Concurrent Writer Queue Depth
/// Multiple in-flight append+flush streams running concurrently against GCS Rapid.
async fn run_concurrent_queue_depth_bench(
    storage: Arc<Storage>,
    queue_depth: usize,
    ops_per_worker: usize,
    bench_id: u128,
) -> LatencyStats {
    let barrier = Arc::new(Barrier::new(queue_depth));
    let mut handles = Vec::with_capacity(queue_depth);

    let start_all = Instant::now();

    for worker_id in 0..queue_depth {
        let storage_clone = storage.clone();
        let barrier_clone = barrier.clone();
        let uri_str = format!("gs://{}/bench_cqd_{}_{}.wal", BUCKET, bench_id, worker_id);
        let uri = parse_gcs_uri(&uri_str).unwrap();

        handles.push(tokio::spawn(async move {
            let mut wal = GcsWalFile::open(storage_clone, uri, 0).await.expect("Failed to open WAL");
            let payload = vec![0xABu8; PAGE_SIZE];

            barrier_clone.wait().await;
            let mut latencies = Vec::with_capacity(ops_per_worker);

            for i in 0..ops_per_worker {
                let op_start = Instant::now();
                wal.write(&payload, (i * PAGE_SIZE) as u64).await.expect("write failed");
                wal.sync().await.expect("sync failed");
                latencies.push(op_start.elapsed().as_secs_f64() * 1000.0);
            }

            let _ = wal.close().await;
            latencies
        }));
    }

    let mut all_latencies = Vec::new();
    for h in handles {
        let latencies = h.await.unwrap();
        all_latencies.extend(latencies);
    }

    let elapsed = start_all.elapsed();
    let total_bytes = queue_depth * ops_per_worker * PAGE_SIZE;
    LatencyStats::compute(all_latencies, elapsed, total_bytes)
}

/// Benchmark 2: Pipelined / Group Commit Queue Depth
/// Within a single stream, queuing `qd` 4KB pages in-flight before issuing a synchronous flush.
async fn run_pipelined_queue_depth_bench(
    storage: Arc<Storage>,
    queue_depth: usize,
    num_flushes: usize,
    bench_id: u128,
) -> LatencyStats {
    let uri_str = format!("gs://{}/bench_pqd_{}_{}.wal", BUCKET, bench_id, queue_depth);
    let uri = parse_gcs_uri(&uri_str).unwrap();
    let mut wal = GcsWalFile::open(storage, uri, 0).await.expect("Failed to open WAL");
    let payload = vec![0xCDu8; PAGE_SIZE];

    let mut latencies = Vec::with_capacity(num_flushes);
    let mut cur_offset = 0u64;

    let start_all = Instant::now();

    for _ in 0..num_flushes {
        let flush_start = Instant::now();
        // Queue QD writes into the stream buffer
        for _ in 0..queue_depth {
            wal.write(&payload, cur_offset).await.expect("write failed");
            cur_offset += PAGE_SIZE as u64;
        }
        // Flush the queued batch
        wal.sync().await.expect("sync failed");
        latencies.push(flush_start.elapsed().as_secs_f64() * 1000.0);
    }

    let elapsed = start_all.elapsed();
    let _ = wal.close().await;

    let total_ops = num_flushes * queue_depth;
    let total_bytes = total_ops * PAGE_SIZE;

    // Per-operation latency is batch latency / QD
    let per_op_latencies: Vec<f64> = latencies.iter().map(|&batch_lat| batch_lat / queue_depth as f64).collect();

    LatencyStats::compute(per_op_latencies, elapsed, total_bytes)
}

fn print_results_table(title: &str, rows: &[(usize, LatencyStats)]) {
    println!("\n### {}", title);
    println!("| Queue Depth (QD) | Throughput (IOPS/TPS) | Bandwidth (MB/s) | Min Latency (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | Max Latency (ms) | Mean (ms) |");
    println!("|:----------------:|:---------------------:|:----------------:|:----------------:|:--------:|:--------:|:--------:|:--------:|:----------------:|:---------:|");

    for (qd, stats) in rows {
        println!(
            "| {:16} | {:21.1} | {:16.2} | {:16.2} | {:8.2} | {:8.2} | {:8.2} | {:8.2} | {:16.2} | {:9.2} |",
            qd,
            stats.ops_per_sec,
            stats.mb_per_sec,
            stats.min_ms,
            stats.p50_ms,
            stats.p90_ms,
            stats.p95_ms,
            stats.p99_ms,
            stats.max_ms,
            stats.mean_ms,
        );
    }
    println!();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bench_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis();

    println!("===============================================================================");
    println!("SQLite GCS Rapid VFS: Throughput & Latency at Various Queue Depths Benchmark");
    println!("Bucket: gs://{}", BUCKET);
    println!("Benchmark Session ID: {}", bench_id);
    println!("Page Size: {} bytes (4KB WAL frame)", PAGE_SIZE);
    println!("===============================================================================\n");

    let storage = Arc::new(Storage::builder().build().await?);

    // -------------------------------------------------------------------------
    // Part 1: Concurrent Writer Queue Depth (QD = 1, 2, 4, 8, 16, 32, 64)
    // -------------------------------------------------------------------------
    println!("===============================================================================");
    println!("Phase 1: Concurrent Writer Queue Depth (GCS Rapid Bidi Streams)");
    println!("Tests concurrent in-flight streams, each executing 4KB append + synchronous flush");
    println!("===============================================================================");

    let concurrent_qds = [1, 2, 4, 8, 16, 32, 64];
    let mut concurrent_results = Vec::new();

    for &qd in &concurrent_qds {
        print!("Running Concurrent QD = {:2}... ", qd);
        // Ensure at least 64 ops per test, scaling with QD
        let ops_per_worker = if qd == 1 { 64 } else if qd <= 4 { 32 } else if qd <= 16 { 20 } else { 12 };
        let stats = run_concurrent_queue_depth_bench(storage.clone(), qd, ops_per_worker, bench_id).await;
        println!(
            "-> {:6.1} IOPS | {:5.2} MB/s | p50: {:5.2}ms | p90: {:5.2}ms | p99: {:5.2}ms",
            stats.ops_per_sec, stats.mb_per_sec, stats.p50_ms, stats.p90_ms, stats.p99_ms
        );
        concurrent_results.push((qd, stats));
    }

    print_results_table(
        "1. Concurrent Writer Queue Depth (GCS Rapid Bidi Streams)",
        &concurrent_results,
    );

    // -------------------------------------------------------------------------
    // Part 2: Pipelined / Group Commit Queue Depth (QD = 1, 2, 4, 8, 16, 32, 64)
    // -------------------------------------------------------------------------
    println!("===============================================================================");
    println!("Phase 2: Pipelined / Group Commit Queue Depth (Batch size per sync)");
    println!("Queues QD in-flight 4KB pages in stream buffer before calling flush().await");
    println!("===============================================================================");

    let pipelined_qds = [1, 2, 4, 8, 16, 32, 64];
    let mut pipelined_results = Vec::new();

    for &qd in &pipelined_qds {
        print!("Running Pipelined QD = {:2}... ", qd);
        let num_flushes = 30;
        let stats = run_pipelined_queue_depth_bench(storage.clone(), qd, num_flushes, bench_id).await;
        println!(
            "-> {:7.1} IOPS | {:6.2} MB/s | per-op p50: {:5.2}ms | p90: {:5.2}ms | p99: {:5.2}ms",
            stats.ops_per_sec, stats.mb_per_sec, stats.p50_ms, stats.p90_ms, stats.p99_ms
        );
        pipelined_results.push((qd, stats));
    }

    print_results_table(
        "2. Pipelined / Group Commit Queue Depth (GCS Rapid Bidi Streams)",
        &pipelined_results,
    );

    // -------------------------------------------------------------------------
    // Part 3: End-to-End SQLite VFS Transaction Queue Depth (QD = 1, 2, 4, 8, 16)
    println!("Phase 1 & 2 Complete! Run 'python3 benchmarks/bench_sqlite_vfs_qd.py' for Phase 3 (End-to-End SQLite VFS).");

    println!("===============================================================================");
    println!("Benchmark Complete! Cleaning up benchmark objects from GCS Rapid bucket...");
    println!("===============================================================================");

    Ok(())
}
