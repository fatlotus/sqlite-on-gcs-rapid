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

use std::sync::Once;
use crate::error::*;
use crate::vfs::register_gcs_vfs;

static INIT_TRACING: Once = Once::new();

fn init_logging() {
    INIT_TRACING.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,gcs_vfs=debug")),
            )
            .try_init();
    });
}

/// Return code for persistent VFS extensions (SQLite 3.25+) preventing dlclose unloading.
pub const SQLITE_OK_LOAD_PERMANENTLY: libc::c_int = 256;

/// Core extension entry point called by SQLite when loading this extension.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_extension_init(
    _db: *mut libc::c_void,
    _pz_err_msg: *mut *mut libc::c_char,
    _p_api: *const libc::c_void,
) -> libc::c_int {
    init_logging();

    tracing::info!(target: "gcs_vfs", "Initializing SQLite GCS Rapid VFS extension...");

    match register_gcs_vfs(false) {
        Ok(()) => {
            tracing::info!(target: "gcs_vfs", "GCS Rapid VFS ('gcsrapid') registered successfully.");
            SQLITE_OK_LOAD_PERMANENTLY
        }
        Err(rc) => {
            let msg = "Failed to register GCS Rapid VFS with SQLite";
            tracing::error!(target: "gcs_vfs", "{} (code: {})", msg, rc);
            rc
        }
    }
}

/// Filename-derived entry point: `libsqlite3_gcsrapid_vfs.so` -> `sqlite3_gcsrapidvfs_init`.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_gcsrapidvfs_init(
    db: *mut libc::c_void,
    pz_err_msg: *mut *mut libc::c_char,
    p_api: *const libc::c_void,
) -> libc::c_int {
    sqlite3_extension_init(db, pz_err_msg, p_api)
}

/// Additional alias with underscores: `sqlite3_gcsrapid_vfs_init`.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_gcsrapid_vfs_init(
    db: *mut libc::c_void,
    pz_err_msg: *mut *mut libc::c_char,
    p_api: *const libc::c_void,
) -> libc::c_int {
    sqlite3_extension_init(db, pz_err_msg, p_api)
}

/// Additional alias: `sqlite3_sqlite3gcsrapidvfs_init`.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_sqlite3gcsrapidvfs_init(
    db: *mut libc::c_void,
    pz_err_msg: *mut *mut libc::c_char,
    p_api: *const libc::c_void,
) -> libc::c_int {
    sqlite3_extension_init(db, pz_err_msg, p_api)
}

/// Additional alias: `sqlite3_sqlite3_gcsrapid_vfs_init`.
#[no_mangle]
pub unsafe extern "C" fn sqlite3_sqlite3_gcsrapid_vfs_init(
    db: *mut libc::c_void,
    pz_err_msg: *mut *mut libc::c_char,
    p_api: *const libc::c_void,
) -> libc::c_int {
    sqlite3_extension_init(db, pz_err_msg, p_api)
}
