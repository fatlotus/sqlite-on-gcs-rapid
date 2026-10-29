//! SQLite error codes, extended result codes, and thread-local error messaging.

use std::cell::RefCell;
use std::ffi::CString;

// Standard SQLite result codes
pub const SQLITE_OK: i32 = 0;
pub const SQLITE_ERROR: i32 = 1;
pub const SQLITE_PERM: i32 = 3;
pub const SQLITE_ABORT: i32 = 4;
pub const SQLITE_BUSY: i32 = 5;
pub const SQLITE_READONLY: i32 = 8;
pub const SQLITE_IOERR: i32 = 10;
pub const SQLITE_CORRUPT: i32 = 11;
pub const SQLITE_NOTFOUND: i32 = 12;
pub const SQLITE_CANTOPEN: i32 = 14;
pub const SQLITE_MISUSE: i32 = 21;

// Extended SQLite error codes
pub const SQLITE_IOERR_READ: i32 = SQLITE_IOERR | (1 << 8);
pub const SQLITE_IOERR_SHORT_READ: i32 = SQLITE_IOERR | (2 << 8);
pub const SQLITE_IOERR_WRITE: i32 = SQLITE_IOERR | (3 << 8);
pub const SQLITE_IOERR_FSYNC: i32 = SQLITE_IOERR | (4 << 8);
pub const SQLITE_IOERR_TRUNCATE: i32 = SQLITE_IOERR | (6 << 8);
pub const SQLITE_IOERR_FSTAT: i32 = SQLITE_IOERR | (7 << 8);
pub const SQLITE_IOERR_UNLOCK: i32 = SQLITE_IOERR | (8 << 8);
pub const SQLITE_IOERR_RDLOCK: i32 = SQLITE_IOERR | (9 << 8);
pub const SQLITE_IOERR_DELETE: i32 = SQLITE_IOERR | (10 << 8);
pub const SQLITE_IOERR_CLOSE: i32 = SQLITE_IOERR | (16 << 8);
pub const SQLITE_CANTOPEN_NOTEMPDIR: i32 = SQLITE_CANTOPEN | (1 << 8);
pub const SQLITE_CANTOPEN_ISDIR: i32 = SQLITE_CANTOPEN | (2 << 8);
pub const SQLITE_CANTOPEN_FULLPATH: i32 = SQLITE_CANTOPEN | (3 << 8);

thread_local! {
    static LAST_ERROR: RefCell<Option<String>> = const { RefCell::new(None) };
}

/// Sets the thread-local error message for retrieval by `xGetLastError` / `sqlite3_errmsg()`.
pub fn set_last_error(msg: impl Into<String>) {
    let s = msg.into();
    tracing::warn!(target: "gcs_vfs", "{}", s);
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = Some(s);
    });
}

/// Retrieves and clears the thread-local error message.
pub fn get_last_error() -> Option<String> {
    LAST_ERROR.with(|cell| cell.borrow_mut().take())
}

/// Copies the last error string into SQLite's buffer for `xGetLastError`.
pub fn copy_last_error_to_buf(buf: *mut libc::c_char, buf_len: libc::c_int) -> libc::c_int {
    if buf.is_null() || buf_len <= 0 {
        return SQLITE_OK;
    }
    let msg = LAST_ERROR.with(|cell| cell.borrow().clone()).unwrap_or_default();
    if msg.is_empty() {
        unsafe { *buf = 0 };
        return SQLITE_OK;
    }
    if let Ok(c_str) = CString::new(msg) {
        let bytes = c_str.as_bytes_with_nul();
        let copy_len = bytes.len().min(buf_len as usize);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr() as *const libc::c_char, buf, copy_len);
            *buf.add(copy_len - 1) = 0; // ensure null-terminated
        }
    }
    SQLITE_OK
}
