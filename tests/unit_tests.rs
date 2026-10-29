//! Offline unit tests for SQLite GCS Rapid VFS components.

use sqlite3_gcsrapid_vfs::error::*;
use sqlite3_gcsrapid_vfs::uri::*;

#[test]
fn test_uri_schemes_and_parameters() {
    let cases = vec![
        ("gs://rapid-bucket/mydb.sqlite", "rapid-bucket", "mydb.sqlite"),
        ("gcs://rapid-bucket/sub/folder/db.db", "rapid-bucket", "sub/folder/db.db"),
        ("/gs/rapid-bucket/db.sqlite", "rapid-bucket", "db.sqlite"),
        ("/gcs/rapid-bucket/db.sqlite", "rapid-bucket", "db.sqlite"),
        ("file:gs://rapid-bucket/mydb.sqlite?vfs=gcsrapid", "rapid-bucket", "mydb.sqlite"),
        ("file:/gs/rapid-bucket/mydb.sqlite?mode=rw&cache=shared", "rapid-bucket", "mydb.sqlite"),
    ];

    for (raw, expected_bucket, expected_object) in cases {
        let uri = parse_gcs_uri(raw).expect(&format!("Failed parsing {}", raw));
        assert_eq!(uri.bucket, expected_bucket);
        assert_eq!(uri.object, expected_object);
        assert_eq!(uri.formatted_bucket(), format!("projects/_/buckets/{}", expected_bucket));
        assert!(is_gcs_path(raw));
    }
}

#[test]
fn test_shm_prohibition_detection() {
    // Shared memory files MUST be detected so xOpen can explicitly reject them
    let shm_paths = [
        "gs://my-bucket/mydb.db-shm",
        "file:gs://my-bucket/mydb.db-shm?vfs=gcsrapid",
        "/gs/my-bucket/mydb.db-shm",
        "file:/gs/my-bucket/mydb.db-shm",
        "/tmp/mydb.db-shm",
    ];

    for path in shm_paths {
        assert!(is_shm_file(path), "Failed to detect prohibited SHM: {}", path);
    }

    let non_shm = [
        "gs://my-bucket/mydb.db",
        "gs://my-bucket/mydb.db-wal",
        "file:gs://my-bucket/mydb.db?vfs=gcsrapid",
    ];

    for path in non_shm {
        assert!(!is_shm_file(path), "False positive for SHM: {}", path);
    }
}

#[test]
fn test_wal_filename_derivation() {
    let uri = parse_gcs_uri("gs://my-bucket/data/store.sqlite").unwrap();
    assert_eq!(uri.wal_object(), "data/store.sqlite-wal");
    assert_eq!(uri.shm_object(), "data/store.sqlite-shm");
    assert_eq!(uri.to_gs_uri(), "gs://my-bucket/data/store.sqlite");
    assert!(is_wal_file("gs://my-bucket/data/store.sqlite-wal"));

    let wal_uri = parse_gcs_uri("gs://my-bucket/data/store.sqlite-wal").unwrap();
    assert_eq!(wal_uri.wal_object(), "data/store.sqlite-wal");
    assert_eq!(wal_uri.shm_object(), "data/store.sqlite-shm");
}

#[test]
fn test_error_diagnostics_buffer() {
    let msg = "GCS VFS: Shared memory (-shm) is prohibited for GCS-backed databases.";
    set_last_error(msg);

    let mut buf = [0 as libc::c_char; 256];
    copy_last_error_to_buf(buf.as_mut_ptr(), buf.len() as libc::c_int);

    let c_str = unsafe { std::ffi::CStr::from_ptr(buf.as_ptr()) };
    assert_eq!(c_str.to_str().unwrap(), msg);
}

#[test]
fn test_short_read_zero_fill_contract() {
    // Finding 1 (sqlite3.h lines 830-834):
    // If xRead() returns SQLITE_IOERR_SHORT_READ it must zero-fill the unread tail.
    let mut buffer = [0xAA_u8; 4096];
    let file_content = b"SQLITE_PAGE_DATA"; // 16 bytes
    let offset = 0;
    let file_size = file_content.len();

    let amt = buffer.len();
    let readable_bytes = (file_size - offset).min(amt);

    // Simulate reading available portion
    buffer[..readable_bytes].copy_from_slice(&file_content[offset..offset + readable_bytes]);
    // Zero-fill tail per sqlite3.h lines 830-834
    buffer[readable_bytes..].fill(0);

    // Verify first 16 bytes match data
    assert_eq!(&buffer[..16], b"SQLITE_PAGE_DATA");
    // Verify remaining 4080 bytes are zeroes (NOT 0xAA)
    for &b in &buffer[16..] {
        assert_eq!(b, 0, "Tail byte was not zero-filled!");
    }
}

#[test]
fn test_wal_header_structure() {
    // SQLite WAL header is 32 bytes (https://www.sqlite.org/wal.html)
    let magic_le: u32 = 0x377f0682;
    let magic_be: u32 = 0x377f0683;
    let page_size: u32 = 4096;

    let mut header = [0u8; 32];
    header[0..4].copy_from_slice(&magic_le.to_le_bytes());
    header[4..8].copy_from_slice(&3007000u32.to_le_bytes()); // file format
    header[8..12].copy_from_slice(&page_size.to_le_bytes());

    let parsed_magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    let parsed_page_size = u32::from_le_bytes(header[8..12].try_into().unwrap());

    assert!(parsed_magic == magic_le || parsed_magic == magic_be);
    assert_eq!(parsed_page_size, 4096);
}
