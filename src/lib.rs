//! SQLite VFS extension using Google Cloud Storage Rapid.
//!
//! Provides:
//! - Standard atomic object semantics for main database files.
//! - GCS Rapid atomic appendable objects for SQLite WAL logs.
//! - High-performance gRPC Bidi streaming for both DB and WAL.
//! - Explicit prohibition of shared memory (-shm) files with helpful guidance.

pub mod error;
pub mod gcs;
pub mod io;
pub mod uri;
pub mod vfs;
