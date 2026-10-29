#![allow(non_snake_case, non_camel_case_types)]

//! SQLite VFS struct and methods implementation.

use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::OnceLock;

use crate::error::*;
use crate::gcs::client::{block_on, get_storage_client_sync};
use crate::gcs::db_file::GcsDbFile;
use crate::gcs::wal_file::GcsWalFile;
use crate::io::{GCS_DB_IO_METHODS, GCS_WAL_IO_METHODS, GcsFileHandle, GcsSqliteFile};
use crate::uri::{is_gcs_path, is_shm_file, is_wal_file, parse_gcs_uri};

// SQLite Open flags
pub const SQLITE_OPEN_READONLY: i32 = 0x00000001;
pub const SQLITE_OPEN_READWRITE: i32 = 0x00000002;
pub const SQLITE_OPEN_CREATE: i32 = 0x00000004;
pub const SQLITE_OPEN_DELETEONCLOSE: i32 = 0x00000008;
pub const SQLITE_OPEN_EXCLUSIVE: i32 = 0x00000010;
pub const SQLITE_OPEN_MAIN_DB: i32 = 0x00000100;
pub const SQLITE_OPEN_TEMP_DB: i32 = 0x00000200;
pub const SQLITE_OPEN_TRANSIENT_DB: i32 = 0x00000400;
pub const SQLITE_OPEN_MAIN_JOURNAL: i32 = 0x00000800;
pub const SQLITE_OPEN_TEMP_JOURNAL: i32 = 0x00001000;
pub const SQLITE_OPEN_SUBJOURNAL: i32 = 0x00002000;
pub const SQLITE_OPEN_SUPER_JOURNAL: i32 = 0x00004000;
pub const SQLITE_OPEN_WAL: i32 = 0x00080000;

// Device characteristics
pub const SQLITE_IOCAP_ATOMIC: i32 = 0x00000001;
pub const SQLITE_IOCAP_SAFE_APPEND: i32 = 0x00000200;
pub const SQLITE_IOCAP_SEQUENTIAL: i32 = 0x00000400;
pub const SQLITE_IOCAP_POWERSAFE_OVERWRITE: i32 = 0x00001000;

// File control opcodes
pub const SQLITE_FCNTL_LOCKSTATE: i32 = 1;
pub const SQLITE_FCNTL_SIZE_HINT: i32 = 5;
pub const SQLITE_FCNTL_CHUNK_SIZE: i32 = 6;
pub const SQLITE_FCNTL_PERSIST_WAL: i32 = 10;
pub const SQLITE_FCNTL_POWERSAFE_OVERWRITE: i32 = 13;
pub const SQLITE_FCNTL_PRAGMA: i32 = 14;
pub const SQLITE_FCNTL_VFSNAME: i32 = 12;
pub const SQLITE_FCNTL_SYNC: i32 = 21;
pub const SQLITE_FCNTL_COMMIT_PHASETWO: i32 = 22;

// Locking levels
pub const SQLITE_LOCK_NONE: i32 = 0;
pub const SQLITE_LOCK_SHARED: i32 = 1;
pub const SQLITE_LOCK_RESERVED: i32 = 2;
pub const SQLITE_LOCK_PENDING: i32 = 3;
pub const SQLITE_LOCK_EXCLUSIVE: i32 = 4;

/// Base SQLite file structure.
#[repr(C)]
pub struct sqlite3_file {
    pub pMethods: *const sqlite3_io_methods,
}

/// SQLite I/O methods table.
#[repr(C)]
pub struct sqlite3_io_methods {
    pub iVersion: libc::c_int,
    pub xClose: unsafe extern "C" fn(*mut sqlite3_file) -> libc::c_int,
    pub xRead: unsafe extern "C" fn(*mut sqlite3_file, *mut libc::c_void, libc::c_int, i64) -> libc::c_int,
    pub xWrite: unsafe extern "C" fn(*mut sqlite3_file, *const libc::c_void, libc::c_int, i64) -> libc::c_int,
    pub xTruncate: unsafe extern "C" fn(*mut sqlite3_file, i64) -> libc::c_int,
    pub xSync: unsafe extern "C" fn(*mut sqlite3_file, libc::c_int) -> libc::c_int,
    pub xFileSize: unsafe extern "C" fn(*mut sqlite3_file, *mut i64) -> libc::c_int,
    pub xLock: unsafe extern "C" fn(*mut sqlite3_file, libc::c_int) -> libc::c_int,
    pub xUnlock: unsafe extern "C" fn(*mut sqlite3_file, libc::c_int) -> libc::c_int,
    pub xCheckReservedLock: unsafe extern "C" fn(*mut sqlite3_file, *mut libc::c_int) -> libc::c_int,
    pub xFileControl: unsafe extern "C" fn(*mut sqlite3_file, libc::c_int, *mut libc::c_void) -> libc::c_int,
    pub xSectorSize: unsafe extern "C" fn(*mut sqlite3_file) -> libc::c_int,
    pub xDeviceCharacteristics: unsafe extern "C" fn(*mut sqlite3_file) -> libc::c_int,
    pub xShmMap: unsafe extern "C" fn(*mut sqlite3_file, libc::c_int, libc::c_int, libc::c_int, *mut *mut libc::c_void) -> libc::c_int,
    pub xShmLock: unsafe extern "C" fn(*mut sqlite3_file, libc::c_int, libc::c_int, libc::c_int) -> libc::c_int,
    pub xShmBarrier: unsafe extern "C" fn(*mut sqlite3_file),
    pub xShmUnmap: unsafe extern "C" fn(*mut sqlite3_file, libc::c_int) -> libc::c_int,
}

/// SQLite VFS structure.
#[repr(C)]
pub struct sqlite3_vfs {
    pub iVersion: libc::c_int,
    pub szOsFile: libc::c_int,
    pub mxPathname: libc::c_int,
    pub pNext: *mut sqlite3_vfs,
    pub zName: *const libc::c_char,
    pub pAppData: *mut libc::c_void,
    pub xOpen: unsafe extern "C" fn(*mut sqlite3_vfs, *const libc::c_char, *mut sqlite3_file, libc::c_int, *mut libc::c_int) -> libc::c_int,
    pub xDelete: unsafe extern "C" fn(*mut sqlite3_vfs, *const libc::c_char, libc::c_int) -> libc::c_int,
    pub xAccess: unsafe extern "C" fn(*mut sqlite3_vfs, *const libc::c_char, libc::c_int, *mut libc::c_int) -> libc::c_int,
    pub xFullPathname: unsafe extern "C" fn(*mut sqlite3_vfs, *const libc::c_char, libc::c_int, *mut libc::c_char) -> libc::c_int,
    pub xDlOpen: unsafe extern "C" fn(*mut sqlite3_vfs, *const libc::c_char) -> *mut libc::c_void,
    pub xDlError: unsafe extern "C" fn(*mut sqlite3_vfs, libc::c_int, *mut libc::c_char),
    pub xDlSym: unsafe extern "C" fn(*mut sqlite3_vfs, *mut libc::c_void, *const libc::c_char) -> Option<unsafe extern "C" fn()>,
    pub xDlClose: unsafe extern "C" fn(*mut sqlite3_vfs, *mut libc::c_void),
    pub xRandomness: unsafe extern "C" fn(*mut sqlite3_vfs, libc::c_int, *mut libc::c_char) -> libc::c_int,
    pub xSleep: unsafe extern "C" fn(*mut sqlite3_vfs, libc::c_int) -> libc::c_int,
    pub xCurrentTime: unsafe extern "C" fn(*mut sqlite3_vfs, *mut libc::c_double) -> libc::c_int,
    pub xGetLastError: unsafe extern "C" fn(*mut sqlite3_vfs, libc::c_int, *mut libc::c_char) -> libc::c_int,
    pub xCurrentTimeInt64: Option<unsafe extern "C" fn(*mut sqlite3_vfs, *mut i64) -> libc::c_int>,
}

unsafe impl Sync for sqlite3_vfs {}
unsafe impl Send for sqlite3_vfs {}

static DEFAULT_OS_VFS: AtomicPtr<sqlite3_vfs> = AtomicPtr::new(std::ptr::null_mut());

extern "C" {
    fn sqlite3_vfs_find(zVfsName: *const libc::c_char) -> *mut sqlite3_vfs;
    fn sqlite3_vfs_register(pVfs: *mut sqlite3_vfs, makeDefault: libc::c_int) -> libc::c_int;
    fn sqlite3_log(iErrCode: libc::c_int, zFormat: *const libc::c_char, ...);
}

/// Emits a message directly to SQLite's error log.
pub fn log_to_sqlite(err_code: i32, msg: &str) {
    if let Ok(c_fmt) = CString::new("%s") {
        if let Ok(c_msg) = CString::new(msg) {
            unsafe {
                sqlite3_log(err_code, c_fmt.as_ptr(), c_msg.as_ptr());
            }
        }
    }
}

pub static GCS_RAPID_VFS: OnceLock<sqlite3_vfs> = OnceLock::new();

/// Initializes and returns the static `sqlite3_vfs` instance for `"gcsrapid"`.
pub fn get_gcs_vfs() -> &'static sqlite3_vfs {
    GCS_RAPID_VFS.get_or_init(|| {
        sqlite3_vfs {
            iVersion: 2,
            szOsFile: std::mem::size_of::<GcsSqliteFile>() as libc::c_int,
            mxPathname: 1024,
            pNext: std::ptr::null_mut(),
            zName: c"gcsrapid".as_ptr(),
            pAppData: std::ptr::null_mut(),
            xOpen: vfs_xOpen,
            xDelete: vfs_xDelete,
            xAccess: vfs_xAccess,
            xFullPathname: vfs_xFullPathname,
            xDlOpen: vfs_xDlOpen,
            xDlError: vfs_xDlError,
            xDlSym: vfs_xDlSym,
            xDlClose: vfs_xDlClose,
            xRandomness: vfs_xRandomness,
            xSleep: vfs_xSleep,
            xCurrentTime: vfs_xCurrentTime,
            xGetLastError: vfs_xGetLastError,
            xCurrentTimeInt64: Some(vfs_xCurrentTimeInt64),
        }
    })
}

/// Registers the `"gcsrapid"` VFS with the active SQLite library.
pub fn register_gcs_vfs(make_default: bool) -> Result<(), i32> {
    unsafe {
        // Cache existing default OS VFS (typically unix) for fallback
        let def = sqlite3_vfs_find(std::ptr::null());
        if !def.is_null() {
            DEFAULT_OS_VFS.store(def, Ordering::SeqCst);
        }

        let vfs = get_gcs_vfs();
        let rc = sqlite3_vfs_register(vfs as *const _ as *mut sqlite3_vfs, if make_default { 1 } else { 0 });
        if rc != SQLITE_OK {
            return Err(rc);
        }
    }
    tracing::info!(target: "gcs_vfs", "Registered 'gcsrapid' SQLite VFS (default: {})", make_default);
    Ok(())
}

// ----------------------------------------------------------------------------
// VFS Methods
// ----------------------------------------------------------------------------

unsafe extern "C" fn vfs_xOpen(
    _p_vfs: *mut sqlite3_vfs,
    z_name: *const libc::c_char,
    p_file: *mut sqlite3_file,
    flags: libc::c_int,
    p_out_flags: *mut libc::c_int,
) -> libc::c_int {
    let raw_path = if z_name.is_null() {
        ""
    } else {
        match CStr::from_ptr(z_name).to_str() {
            Ok(s) => s,
            Err(_) => return SQLITE_CANTOPEN,
        }
    };

    // Requirement 4 & 5: PROHIBIT SHARED MEMORY FILES
    if is_shm_file(raw_path) {
        let msg = "GCS VFS: Shared memory files (*-shm) are prohibited because database files are stored in GCS. \
                   To operate with WAL mode on GCS, execute 'PRAGMA locking_mode = EXCLUSIVE;' before accessing the database.";
        set_last_error(msg);
        log_to_sqlite(SQLITE_CANTOPEN, msg);
        tracing::error!(target: "gcs_vfs", "{}", msg);
        return SQLITE_CANTOPEN;
    }

    // Check if path is a GCS database or WAL
    if is_gcs_path(raw_path) {
        let uri = match parse_gcs_uri(raw_path) {
            Ok(u) => u,
            Err(e) => {
                set_last_error(e);
                return SQLITE_CANTOPEN;
            }
        };

        let storage = match get_storage_client_sync() {
            Ok(s) => s,
            Err(e) => {
                set_last_error(e);
                return SQLITE_CANTOPEN;
            }
        };

        let file_ptr = p_file as *mut GcsSqliteFile;

        if (flags & SQLITE_OPEN_WAL) != 0 {
            // Open WAL file on GCS Rapid
            match block_on(GcsWalFile::open(storage, uri, flags)) {
                Ok(wal_file) => {
                    (*file_ptr).p_methods = &GCS_WAL_IO_METHODS;
                    (*file_ptr).handle = Box::into_raw(Box::new(GcsFileHandle::Wal(Box::new(wal_file))));
                    (*file_ptr).lock_state = SQLITE_LOCK_NONE;
                    if !p_out_flags.is_null() {
                        *p_out_flags = flags;
                    }
                    return SQLITE_OK;
                }
                Err(e) => return e,
            }
        } else {
            // Open Main DB file using standard atomic object semantics
            match block_on(GcsDbFile::open(storage, uri, flags)) {
                Ok(db_file) => {
                    (*file_ptr).p_methods = &GCS_DB_IO_METHODS;
                    (*file_ptr).handle = Box::into_raw(Box::new(GcsFileHandle::Db(Box::new(db_file))));
                    (*file_ptr).lock_state = SQLITE_LOCK_NONE;
                    if !p_out_flags.is_null() {
                        *p_out_flags = flags;
                    }
                    return SQLITE_OK;
                }
                Err(e) => return e,
            }
        }
    }

    // Non-GCS files (temporary databases, rollback journals, memory files): delegate to default OS VFS
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xOpen)(default_vfs, z_name, p_file, flags, p_out_flags);
    }

    SQLITE_CANTOPEN
}

unsafe extern "C" fn vfs_xDelete(
    _p_vfs: *mut sqlite3_vfs,
    z_name: *const libc::c_char,
    sync_dir: libc::c_int,
) -> libc::c_int {
    if z_name.is_null() {
        return SQLITE_OK;
    }
    if let Ok(path) = CStr::from_ptr(z_name).to_str() {
        if is_gcs_path(path) {
            // In WAL mode, SQLite might attempt to delete WAL after closing
            tracing::info!(target: "gcs_vfs", "Delete requested for GCS path: {}", path);
            return SQLITE_OK;
        }
    }

    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xDelete)(default_vfs, z_name, sync_dir);
    }
    SQLITE_OK
}

unsafe extern "C" fn vfs_xAccess(
    _p_vfs: *mut sqlite3_vfs,
    z_name: *const libc::c_char,
    flags: libc::c_int,
    p_res_out: *mut libc::c_int,
) -> libc::c_int {
    if z_name.is_null() || p_res_out.is_null() {
        return SQLITE_IOERR;
    }
    if let Ok(path) = CStr::from_ptr(z_name).to_str() {
        if is_shm_file(path) {
            // Prohibited shared memory files never exist on GCS
            *p_res_out = 0;
            return SQLITE_OK;
        }
        if is_gcs_path(path) {
            if let Ok(uri) = parse_gcs_uri(path) {
                if let Ok(storage) = get_storage_client_sync() {
                    let formatted_bucket = uri.formatted_bucket();
                    let target_obj = if (flags & SQLITE_OPEN_WAL) != 0 || is_wal_file(path) {
                        uri.wal_object()
                    } else {
                        uri.object
                    };
                    let exists = block_on(async {
                        storage.open_object(&formatted_bucket, &target_obj).send().await.is_ok()
                    });
                    *p_res_out = if exists { 1 } else { 0 };
                    return SQLITE_OK;
                }
            }
            *p_res_out = 0;
            return SQLITE_OK;
        }
    }

    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xAccess)(default_vfs, z_name, flags, p_res_out);
    }
    *p_res_out = 0;
    SQLITE_OK
}

/// Finding 4: Preserve GCS URI path without Unix working directory prepending.
unsafe extern "C" fn vfs_xFullPathname(
    _p_vfs: *mut sqlite3_vfs,
    z_name: *const libc::c_char,
    n_out: libc::c_int,
    z_out: *mut libc::c_char,
) -> libc::c_int {
    if z_name.is_null() || z_out.is_null() || n_out <= 0 {
        return SQLITE_IOERR;
    }

    if let Ok(path) = CStr::from_ptr(z_name).to_str() {
        if is_gcs_path(path) || path.starts_with("gs://") || path.starts_with("gcs://") || path.starts_with("file:gs://") {
            let bytes = path.as_bytes();
            let copy_len = bytes.len().min((n_out - 1) as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const libc::c_char, z_out, copy_len);
            *z_out.add(copy_len) = 0;
            return SQLITE_OK;
        }
    }

    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xFullPathname)(default_vfs, z_name, n_out, z_out);
    }

    // Fallback: copy as-is
    let bytes = CStr::from_ptr(z_name).to_bytes();
    let copy_len = bytes.len().min((n_out - 1) as usize);
    std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const libc::c_char, z_out, copy_len);
    *z_out.add(copy_len) = 0;
    SQLITE_OK
}

unsafe extern "C" fn vfs_xDlOpen(_p_vfs: *mut sqlite3_vfs, z_filename: *const libc::c_char) -> *mut libc::c_void {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xDlOpen)(default_vfs, z_filename);
    }
    std::ptr::null_mut()
}

unsafe extern "C" fn vfs_xDlError(_p_vfs: *mut sqlite3_vfs, n_byte: libc::c_int, z_err_msg: *mut libc::c_char) {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        ((*default_vfs).xDlError)(default_vfs, n_byte, z_err_msg);
    }
}

unsafe extern "C" fn vfs_xDlSym(
    _p_vfs: *mut sqlite3_vfs,
    p: *mut libc::c_void,
    z_symbol: *const libc::c_char,
) -> Option<unsafe extern "C" fn()> {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xDlSym)(default_vfs, p, z_symbol);
    }
    None
}

unsafe extern "C" fn vfs_xDlClose(_p_vfs: *mut sqlite3_vfs, p: *mut libc::c_void) {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        ((*default_vfs).xDlClose)(default_vfs, p);
    }
}

unsafe extern "C" fn vfs_xRandomness(_p_vfs: *mut sqlite3_vfs, n_byte: libc::c_int, z_out: *mut libc::c_char) -> libc::c_int {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xRandomness)(default_vfs, n_byte, z_out);
    }
    SQLITE_OK
}

unsafe extern "C" fn vfs_xSleep(_p_vfs: *mut sqlite3_vfs, microseconds: libc::c_int) -> libc::c_int {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xSleep)(default_vfs, microseconds);
    }
    std::thread::sleep(std::time::Duration::from_micros(microseconds as u64));
    SQLITE_OK
}

unsafe extern "C" fn vfs_xCurrentTime(_p_vfs: *mut sqlite3_vfs, p_time: *mut libc::c_double) -> libc::c_int {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        return ((*default_vfs).xCurrentTime)(default_vfs, p_time);
    }
    if !p_time.is_null() {
        *p_time = 2440587.5 + (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs_f64() / 86400.0);
    }
    SQLITE_OK
}

unsafe extern "C" fn vfs_xCurrentTimeInt64(_p_vfs: *mut sqlite3_vfs, p_time: *mut i64) -> libc::c_int {
    let default_vfs = DEFAULT_OS_VFS.load(Ordering::SeqCst);
    if !default_vfs.is_null() {
        if let Some(f) = (*default_vfs).xCurrentTimeInt64 {
            return f(default_vfs, p_time);
        }
    }
    if !p_time.is_null() {
        let ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;
        *p_time = 24405875 * 8640000 + ms;
    }
    SQLITE_OK
}

/// Finding 5: Rich error message provider for `sqlite3_errmsg()`.
unsafe extern "C" fn vfs_xGetLastError(
    _p_vfs: *mut sqlite3_vfs,
    n_buf: libc::c_int,
    z_buf: *mut libc::c_char,
) -> libc::c_int {
    copy_last_error_to_buf(z_buf, n_buf)
}
