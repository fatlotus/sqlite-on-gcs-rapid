//! GCS URI parsing and validation.

use std::fmt;

/// Represents a parsed GCS location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsUri {
    pub bucket: String,
    pub object: String,
}

impl GcsUri {
    /// Formats bucket name for `google-cloud-storage` gRPC API (`projects/_/buckets/<bucket>`).
    pub fn formatted_bucket(&self) -> String {
        format_bucket_name(&self.bucket)
    }

    /// Returns the corresponding WAL object name for this database object.
    pub fn wal_object(&self) -> String {
        format!("{}-wal", self.object)
    }

    /// Returns the corresponding SHM object name (which is prohibited).
    pub fn shm_object(&self) -> String {
        format!("{}-shm", self.object)
    }

    /// Constructs a full `gs://` URI.
    pub fn to_gs_uri(&self) -> String {
        format!("gs://{}/{}", self.bucket, self.object)
    }
}

impl fmt::Display for GcsUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "gs://{}/{}", self.bucket, self.object)
    }
}

/// Helper to format bucket names according to GCS requirements (`projects/_/buckets/<name>`).
pub fn format_bucket_name(name: &str) -> String {
    let clean = name.trim_start_matches("gs://").trim_start_matches("gcs://");
    if clean.starts_with("projects/") {
        clean.to_string()
    } else {
        format!("projects/_/buckets/{}", clean.trim_matches('/'))
    }
}

/// Checks if a path or filename represents a shared memory (`-shm`) file.
pub fn is_shm_file(path: &str) -> bool {
    let clean = path.split('?').next().unwrap_or(path);
    clean.ends_with("-shm") || clean.ends_with("-shm-wal") || clean.contains(".db-shm")
}

/// Checks if a path represents a WAL file (`-wal`).
pub fn is_wal_file(path: &str) -> bool {
    let clean = path.split('?').next().unwrap_or(path);
    clean.ends_with("-wal")
}

/// Attempts to parse a raw SQLite filename or URI string into a `GcsUri`.
///
/// Supported formats:
/// - `gs://<bucket>/<object>`
/// - `gcs://<bucket>/<object>`
/// - `/gs/<bucket>/<object>`
/// - `/gcs/<bucket>/<object>`
/// - `file:gs://<bucket>/<object>?vfs=gcsrapid`
/// - `file:/gs/<bucket>/<object>`
pub fn parse_gcs_uri(raw_path: &str) -> Result<GcsUri, String> {
    if raw_path.is_empty() {
        return Err("Path is empty".to_string());
    }

    // Strip "file:" prefix if present
    let mut s = raw_path;
    if let Some(stripped) = s.strip_prefix("file:") {
        s = stripped;
    }

    // Strip query parameters (?vfs=... etc.)
    if let Some(pos) = s.find('?') {
        s = &s[..pos];
    }

    // Detect prefix
    let rest = if let Some(stripped) = s.strip_prefix("gs://") {
        stripped
    } else if let Some(stripped) = s.strip_prefix("gcs://") {
        stripped
    } else if let Some(stripped) = s.strip_prefix("/gs/") {
        stripped
    } else if let Some(stripped) = s.strip_prefix("/gcs/") {
        stripped
    } else {
        return Err(format!(
            "Not a valid GCS URI (must start with gs://, gcs://, /gs/, or /gcs/): '{}'",
            raw_path
        ));
    };

    let trimmed = rest.trim_start_matches('/');
    if let Some(first_slash) = trimmed.find('/') {
        let bucket = trimmed[..first_slash].trim().to_string();
        let object = trimmed[first_slash + 1..].trim().to_string();

        if bucket.is_empty() {
            return Err(format!("Missing GCS bucket name in '{}'", raw_path));
        }
        if object.is_empty() {
            return Err(format!("Missing GCS object path in '{}'", raw_path));
        }

        Ok(GcsUri { bucket, object })
    } else {
        Err(format!(
            "Invalid GCS URI (missing object name after bucket): '{}'",
            raw_path
        ))
    }
}

/// Checks whether a given path string targets GCS.
pub fn is_gcs_path(raw_path: &str) -> bool {
    parse_gcs_uri(raw_path).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_gcs_uri_schemes() {
        let uri = parse_gcs_uri("gs://my-bucket/mydb.sqlite").unwrap();
        assert_eq!(uri.bucket, "my-bucket");
        assert_eq!(uri.object, "mydb.sqlite");
        assert_eq!(uri.formatted_bucket(), "projects/_/buckets/my-bucket");
        assert_eq!(uri.wal_object(), "mydb.sqlite-wal");
        assert_eq!(uri.shm_object(), "mydb.sqlite-shm");

        let uri2 = parse_gcs_uri("gcs://my-bucket/folder/sub/db.sqlite").unwrap();
        assert_eq!(uri2.bucket, "my-bucket");
        assert_eq!(uri2.object, "folder/sub/db.sqlite");

        let uri3 = parse_gcs_uri("/gs/my-bucket/mydb.sqlite").unwrap();
        assert_eq!(uri3.bucket, "my-bucket");
        assert_eq!(uri3.object, "mydb.sqlite");

        let uri4 = parse_gcs_uri("file:gs://my-bucket/mydb.sqlite?vfs=gcsrapid&mode=rw").unwrap();
        assert_eq!(uri4.bucket, "my-bucket");
        assert_eq!(uri4.object, "mydb.sqlite");
    }

    #[test]
    fn test_is_shm_file() {
        assert!(is_shm_file("gs://my-bucket/db.sqlite-shm"));
        assert!(is_shm_file("file:gs://my-bucket/db.sqlite-shm?vfs=gcsrapid"));
        assert!(is_shm_file("/tmp/mydb.db-shm"));
        assert!(!is_shm_file("gs://my-bucket/db.sqlite-wal"));
        assert!(!is_shm_file("gs://my-bucket/db.sqlite"));
    }

    #[test]
    fn test_is_wal_file() {
        assert!(is_wal_file("gs://my-bucket/db.sqlite-wal"));
        assert!(is_wal_file("file:gs://my-bucket/db.sqlite-wal?vfs=gcsrapid"));
        assert!(!is_wal_file("gs://my-bucket/db.sqlite-shm"));
        assert!(!is_wal_file("gs://my-bucket/db.sqlite"));
    }

    #[test]
    fn test_invalid_uris() {
        assert!(parse_gcs_uri("").is_err());
        assert!(parse_gcs_uri("local.db").is_err());
        assert!(parse_gcs_uri("/tmp/mydb.sqlite").is_err());
        assert!(parse_gcs_uri("gs://my-bucket/").is_err());
        assert!(parse_gcs_uri("gs://my-bucket").is_err());
    }
}
