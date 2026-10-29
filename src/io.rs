#![allow(non_snake_case, non_camel_case_types)]

//! SQLite `sqlite3_io_methods` implementations for GCS DB and Rapid WAL files.

use crate::error::*;
use crate::gcs::client::block_on;
use crate::gcs::db_file::GcsDbFile;
use crate::gcs::wal_file::GcsWalFile;
use crate::vfs::*;

/// Representation of an open SQLite file handle in the GCS VFS.
pub enum GcsFileHandle {
    Db(Box<GcsDbFile>),
    Wal(Box<GcsWalFile>),
}

/// Layout of `sqlite3_file` allocated by SQLite using `vfs.szOsFile`.
#[repr(C)]
pub struct GcsSqliteFile {
    /// SQLite base file struct. Must be the first field!
    pub p_methods: *const sqlite3_io_methods,
    /// Pointer to our heap-allocated `GcsFileHandle`
    pub handle: *mut GcsFileHandle,
    /// In-memory lock state (SQLITE_LOCK_NONE .. SQLITE_LOCK_EXCLUSIVE)
    pub lock_state: i32,
}

// ----------------------------------------------------------------------------
// DB IO Methods
// ----------------------------------------------------------------------------

pub static GCS_DB_IO_METHODS: sqlite3_io_methods = sqlite3_io_methods {
    iVersion: 2,
    xClose: db_xClose,
    xRead: db_xRead,
    xWrite: db_xWrite,
    xTruncate: db_xTruncate,
    xSync: db_xSync,
    xFileSize: db_xFileSize,
    xLock: file_xLock,
    xUnlock: file_xUnlock,
    xCheckReservedLock: file_xCheckReservedLock,
    xFileControl: db_xFileControl,
    xSectorSize: file_xSectorSize,
    xDeviceCharacteristics: db_xDeviceCharacteristics,
    xShmMap: prohibited_xShmMap,
    xShmLock: prohibited_xShmLock,
    xShmBarrier: prohibited_xShmBarrier,
    xShmUnmap: prohibited_xShmUnmap,
};

pub static GCS_WAL_IO_METHODS: sqlite3_io_methods = sqlite3_io_methods {
    iVersion: 2,
    xClose: wal_xClose,
    xRead: wal_xRead,
    xWrite: wal_xWrite,
    xTruncate: wal_xTruncate,
    xSync: wal_xSync,
    xFileSize: wal_xFileSize,
    xLock: file_xLock,
    xUnlock: file_xUnlock,
    xCheckReservedLock: file_xCheckReservedLock,
    xFileControl: wal_xFileControl,
    xSectorSize: file_xSectorSize,
    xDeviceCharacteristics: wal_xDeviceCharacteristics,
    xShmMap: prohibited_xShmMap,
    xShmLock: prohibited_xShmLock,
    xShmBarrier: prohibited_xShmBarrier,
    xShmUnmap: prohibited_xShmUnmap,
};

// ----------------------------------------------------------------------------
// Main DB Implementations
// ----------------------------------------------------------------------------

unsafe extern "C" fn db_xClose(p_file: *mut sqlite3_file) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() {
        return SQLITE_OK;
    }
    let mut boxed = Box::from_raw(f.handle);
    f.handle = std::ptr::null_mut();

    if let GcsFileHandle::Db(ref mut db) = *boxed {
        if let Err(e) = block_on(db.close()) {
            return e;
        }
    }
    SQLITE_OK
}

unsafe extern "C" fn db_xRead(
    p_file: *mut sqlite3_file,
    z_buf: *mut libc::c_void,
    i_amt: libc::c_int,
    i_ofst: i64,
) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || i_amt <= 0 || i_ofst < 0 {
        return SQLITE_IOERR_READ;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Db(ref mut db) = handle {
        let slice = std::slice::from_raw_parts_mut(z_buf as *mut u8, i_amt as usize);
        match block_on(db.read(slice, i_ofst as u64)) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_READ
    }
}

unsafe extern "C" fn db_xWrite(
    p_file: *mut sqlite3_file,
    z_buf: *const libc::c_void,
    i_amt: libc::c_int,
    i_ofst: i64,
) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || i_amt <= 0 || i_ofst < 0 {
        return SQLITE_IOERR_WRITE;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Db(ref mut db) = handle {
        let slice = std::slice::from_raw_parts(z_buf as *const u8, i_amt as usize);
        match db.write(slice, i_ofst as u64) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_WRITE
    }
}

unsafe extern "C" fn db_xTruncate(p_file: *mut sqlite3_file, size: i64) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || size < 0 {
        return SQLITE_IOERR_TRUNCATE;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Db(ref mut db) = handle {
        match db.truncate(size as u64) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_TRUNCATE
    }
}

unsafe extern "C" fn db_xSync(p_file: *mut sqlite3_file, _flags: libc::c_int) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() {
        return SQLITE_IOERR_FSYNC;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Db(ref mut db) = handle {
        match block_on(db.sync()) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_FSYNC
    }
}

unsafe extern "C" fn db_xFileSize(p_file: *mut sqlite3_file, p_size: *mut i64) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || p_size.is_null() {
        return SQLITE_IOERR_FSTAT;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Db(ref db) = handle {
        *p_size = db.file_size() as i64;
        SQLITE_OK
    } else {
        SQLITE_IOERR_FSTAT
    }
}

unsafe extern "C" fn db_xFileControl(
    _p_file: *mut sqlite3_file,
    op: libc::c_int,
    p_arg: *mut libc::c_void,
) -> libc::c_int {
    match op {
        SQLITE_FCNTL_VFSNAME => {
            if !p_arg.is_null() {
                let out_ptr = p_arg as *mut *const libc::c_char;
                *out_ptr = c"gcsrapid".as_ptr();
                return SQLITE_OK;
            }
        }
        SQLITE_FCNTL_PRAGMA => {
            if !p_arg.is_null() {
                let az_arg = p_arg as *mut *mut libc::c_char;
                let pragma_name = *az_arg.add(1);
                if !pragma_name.is_null() {
                    let name_str = std::ffi::CStr::from_ptr(pragma_name).to_string_lossy();
                    if name_str.eq_ignore_ascii_case("journal_mode") {
                        tracing::info!(target: "gcs_vfs",
                            "PRAGMA journal_mode evaluated on GCS DB. Ensure 'PRAGMA locking_mode = EXCLUSIVE;' is set for WAL.");
                    }
                }
            }
        }
        SQLITE_FCNTL_POWERSAFE_OVERWRITE => return SQLITE_OK,
        SQLITE_FCNTL_PERSIST_WAL => return SQLITE_OK,
        _ => {}
    }
    SQLITE_NOTFOUND
}

unsafe extern "C" fn db_xDeviceCharacteristics(_p_file: *mut sqlite3_file) -> libc::c_int {
    // Main DB uses standard atomic object semantics
    SQLITE_IOCAP_ATOMIC | SQLITE_IOCAP_POWERSAFE_OVERWRITE
}

// ----------------------------------------------------------------------------
// WAL Implementations
// ----------------------------------------------------------------------------

unsafe extern "C" fn wal_xClose(p_file: *mut sqlite3_file) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() {
        return SQLITE_OK;
    }
    let mut boxed = Box::from_raw(f.handle);
    f.handle = std::ptr::null_mut();

    if let GcsFileHandle::Wal(ref mut wal) = *boxed {
        if let Err(e) = block_on(wal.close()) {
            return e;
        }
    }
    SQLITE_OK
}

unsafe extern "C" fn wal_xRead(
    p_file: *mut sqlite3_file,
    z_buf: *mut libc::c_void,
    i_amt: libc::c_int,
    i_ofst: i64,
) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || i_amt <= 0 || i_ofst < 0 {
        return SQLITE_IOERR_READ;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Wal(ref mut wal) = handle {
        let slice = std::slice::from_raw_parts_mut(z_buf as *mut u8, i_amt as usize);
        match block_on(wal.read(slice, i_ofst as u64)) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_READ
    }
}

unsafe extern "C" fn wal_xWrite(
    p_file: *mut sqlite3_file,
    z_buf: *const libc::c_void,
    i_amt: libc::c_int,
    i_ofst: i64,
) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || i_amt <= 0 || i_ofst < 0 {
        return SQLITE_IOERR_WRITE;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Wal(ref mut wal) = handle {
        let slice = std::slice::from_raw_parts(z_buf as *const u8, i_amt as usize);
        match block_on(wal.write(slice, i_ofst as u64)) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_WRITE
    }
}

unsafe extern "C" fn wal_xTruncate(p_file: *mut sqlite3_file, size: i64) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || size < 0 {
        return SQLITE_IOERR_TRUNCATE;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Wal(ref mut wal) = handle {
        match block_on(wal.truncate(size as u64)) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_TRUNCATE
    }
}

unsafe extern "C" fn wal_xSync(p_file: *mut sqlite3_file, _flags: libc::c_int) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() {
        return SQLITE_IOERR_FSYNC;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Wal(ref mut wal) = handle {
        match block_on(wal.sync()) {
            Ok(()) => SQLITE_OK,
            Err(e) => e,
        }
    } else {
        SQLITE_IOERR_FSYNC
    }
}

unsafe extern "C" fn wal_xFileSize(p_file: *mut sqlite3_file, p_size: *mut i64) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if f.handle.is_null() || p_size.is_null() {
        return SQLITE_IOERR_FSTAT;
    }
    let handle = &mut *f.handle;
    if let GcsFileHandle::Wal(ref wal) = handle {
        *p_size = wal.file_size() as i64;
        SQLITE_OK
    } else {
        SQLITE_IOERR_FSTAT
    }
}

unsafe extern "C" fn wal_xFileControl(
    _p_file: *mut sqlite3_file,
    op: libc::c_int,
    p_arg: *mut libc::c_void,
) -> libc::c_int {
    match op {
        SQLITE_FCNTL_VFSNAME => {
            if !p_arg.is_null() {
                let out_ptr = p_arg as *mut *const libc::c_char;
                *out_ptr = c"gcsrapid".as_ptr();
                return SQLITE_OK;
            }
        }
        SQLITE_FCNTL_PERSIST_WAL => return SQLITE_OK,
        _ => {}
    }
    SQLITE_NOTFOUND
}

unsafe extern "C" fn wal_xDeviceCharacteristics(_p_file: *mut sqlite3_file) -> libc::c_int {
    // WAL file uses GCS Rapid atomic appendability
    SQLITE_IOCAP_SAFE_APPEND | SQLITE_IOCAP_SEQUENTIAL
}

// ----------------------------------------------------------------------------
// Shared File Locking and Sectors
// ----------------------------------------------------------------------------

unsafe extern "C" fn file_xLock(p_file: *mut sqlite3_file, e_lock: libc::c_int) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if e_lock > f.lock_state {
        f.lock_state = e_lock;
    }
    SQLITE_OK
}

unsafe extern "C" fn file_xUnlock(p_file: *mut sqlite3_file, e_lock: libc::c_int) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if e_lock < f.lock_state {
        f.lock_state = e_lock;
    }
    SQLITE_OK
}

unsafe extern "C" fn file_xCheckReservedLock(
    p_file: *mut sqlite3_file,
    p_res_out: *mut libc::c_int,
) -> libc::c_int {
    let f = &mut *(p_file as *mut GcsSqliteFile);
    if !p_res_out.is_null() {
        *p_res_out = if f.lock_state >= SQLITE_LOCK_RESERVED { 1 } else { 0 };
    }
    SQLITE_OK
}

unsafe extern "C" fn file_xSectorSize(_p_file: *mut sqlite3_file) -> libc::c_int {
    4096
}

// ----------------------------------------------------------------------------
// Shared Memory Methods (EXPLICITLY PROHIBITED PER REQUIREMENTS 4 & 5)
// ----------------------------------------------------------------------------

unsafe extern "C" fn prohibited_xShmMap(
    _p_file: *mut sqlite3_file,
    _i_pg: libc::c_int,
    _pgsz: libc::c_int,
    _b_extend: libc::c_int,
    _pp: *mut *mut libc::c_void,
) -> libc::c_int {
    let msg = "GCS VFS: Shared memory (-shm) is prohibited for GCS-backed databases. \
               You MUST set 'PRAGMA locking_mode = EXCLUSIVE;' before accessing WAL databases on GCS.";
    set_last_error(msg);
    tracing::error!(target: "gcs_vfs", "{}", msg);
    SQLITE_CANTOPEN
}

unsafe extern "C" fn prohibited_xShmLock(
    _p_file: *mut sqlite3_file,
    _offset: libc::c_int,
    _n: libc::c_int,
    _flags: libc::c_int,
) -> libc::c_int {
    SQLITE_OK
}

unsafe extern "C" fn prohibited_xShmBarrier(_p_file: *mut sqlite3_file) {}

unsafe extern "C" fn prohibited_xShmUnmap(
    _p_file: *mut sqlite3_file,
    _delete_flag: libc::c_int,
) -> libc::c_int {
    SQLITE_OK
}
