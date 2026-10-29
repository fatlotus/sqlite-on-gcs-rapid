//! Integration tests for SQLite GCS Rapid VFS using the vanilla sqlite3 CLI.

use std::process::Command;

#[test]
fn test_e2e_sqlite3_cli_with_gcs_rapid() {
    let script_path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/e2e_sqlite3_cli.sh");
    let ext_path = concat!(env!("CARGO_MANIFEST_DIR"), "/target/release/libsqlite3_gcsrapid_vfs.so");

    // Ensure the release extension is built
    let status = Command::new("cargo")
        .args(["build", "--release"])
        .status()
        .expect("Failed running cargo build --release");
    assert!(status.success(), "Failed to build release extension");

    // Run the end-to-end integration test suite
    let output = Command::new("bash")
        .arg(script_path)
        .arg(ext_path)
        .output()
        .expect("Failed executing e2e integration script");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("=== E2E STDOUT ===\n{}", stdout);
    if !stderr.is_empty() {
        eprintln!("=== E2E STDERR ===\n{}", stderr);
    }

    assert!(output.status.success(), "Integration test script failed!");
    assert!(stdout.contains("ALL INTEGRATION TESTS PASSED SUCCESSFULLY!"));
}
