//! Main DB file engine using standard atomic object semantics on GCS.

use std::collections::BTreeMap;
use std::sync::Arc;
use bytes::Bytes;
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;

use crate::error::*;
use crate::uri::GcsUri;

pub const PAGE_SIZE: usize = 4096;

/// Main SQLite database file representation on Google Cloud Storage.
pub struct GcsDbFile {
    pub uri: GcsUri,
    storage: Arc<Storage>,
    /// Active gRPC Bidi streaming descriptor for reading persisted ranges
    desc: Option<ObjectDescriptor>,
    /// Generation number for optimistic concurrency and atomic overwrites
    generation: Option<i64>,
    /// Logical size of the database file in bytes
    size: u64,
    /// Dirty page cache mapping page index -> 4096-byte dirty page data
    dirty_pages: BTreeMap<u64, Vec<u8>>,
    /// Base in-memory cache for fast page access
    cached_pages: BTreeMap<u64, Vec<u8>>,
}

impl GcsDbFile {
    /// Opens or creates the main database file in GCS.
    pub async fn open(storage: Arc<Storage>, uri: GcsUri, flags: i32) -> Result<Self, i32> {
        let formatted_bucket = uri.formatted_bucket();
        let is_create = (flags & crate::vfs::SQLITE_OPEN_CREATE) != 0;

        tracing::info!(target: "gcs_vfs", "Opening main DB file: gs://{}/{} (create: {})", uri.bucket, uri.object, is_create);

        let desc_res = storage.open_object(&formatted_bucket, &uri.object).send().await;

        match desc_res {
            Ok(desc) => {
                let obj = desc.object();
                let size = obj.size as u64;
                let generation = Some(obj.generation);
                tracing::info!(target: "gcs_vfs", "Found existing DB: gs://{}/{} (size: {}, gen: {:?})", uri.bucket, uri.object, size, generation);
                Ok(Self {
                    uri,
                    storage,
                    desc: Some(desc),
                    generation,
                    size,
                    dirty_pages: BTreeMap::new(),
                    cached_pages: BTreeMap::new(),
                })
            }
            Err(e) => {
                let err_str = e.to_string();
                if is_create {
                    tracing::info!(target: "gcs_vfs", "Creating brand new DB: gs://{}/{}", uri.bucket, uri.object);
                    // Generation 0 guarantees creation fails if object already exists
                    Ok(Self {
                        uri,
                        storage,
                        desc: None,
                        generation: Some(0),
                        size: 0,
                        dirty_pages: BTreeMap::new(),
                        cached_pages: BTreeMap::new(),
                    })
                } else {
                    set_last_error(format!("GCS DB object gs://{}/{} not found: {}", uri.bucket, uri.object, err_str));
                    Err(SQLITE_CANTOPEN)
                }
            }
        }
    }

    /// Reads `amt` bytes at `offset` into `buf`.
    ///
    /// Short reads are guaranteed to be zero-filled per sqlite3.h lines 830-834.
    pub async fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), i32> {
        let amt = buf.len();
        if amt == 0 {
            return Ok(());
        }

        let logical_end = self.size;
        if offset >= logical_end {
            // Completely past EOF: zero-fill and return SHORT_READ
            buf.fill(0);
            return Err(SQLITE_IOERR_SHORT_READ);
        }

        let readable_bytes = ((logical_end - offset) as usize).min(amt);

        let start_page = offset / PAGE_SIZE as u64;
        let end_page = (offset + readable_bytes as u64 - 1) / PAGE_SIZE as u64;

        for page_idx in start_page..=end_page {
            let page_start = page_idx * PAGE_SIZE as u64;
            let page_data = if let Some(p) = self.dirty_pages.get(&page_idx) {
                p.clone()
            } else if let Some(p) = self.cached_pages.get(&page_idx) {
                p.clone()
            } else {
                let fetched = self.fetch_range_from_gcs(page_start, PAGE_SIZE).await?;
                self.cached_pages.insert(page_idx, fetched.clone());
                fetched
            };

            let overlap_start = offset.max(page_start);
            let overlap_end = (offset + readable_bytes as u64).min(page_start + PAGE_SIZE as u64);
            let buf_start = (overlap_start - offset) as usize;
            let buf_end = (overlap_end - offset) as usize;
            let page_off = (overlap_start - page_start) as usize;
            let copy_len = (overlap_end - overlap_start) as usize;

            if page_off < page_data.len() {
                let avail = (page_data.len() - page_off).min(copy_len);
                buf[buf_start..buf_start + avail].copy_from_slice(&page_data[page_off..page_off + avail]);
                if avail < copy_len {
                    buf[buf_start + avail..buf_end].fill(0);
                }
            } else {
                buf[buf_start..buf_end].fill(0);
            }
        }

        // Check if this was a short read
        if readable_bytes < amt {
            // Zero-fill the unread tail per sqlite3.h lines 830-834
            buf[readable_bytes..].fill(0);
            return Err(SQLITE_IOERR_SHORT_READ);
        }

        Ok(())
    }

    /// Fetches a byte range from GCS via gRPC data plane Bidi stream.
    async fn fetch_range_from_gcs(&mut self, start: u64, len: usize) -> Result<Vec<u8>, i32> {
        if self.desc.is_none() {
            // Brand new uncommitted DB has no remote content
            return Ok(vec![0u8; len]);
        }

        let desc = self.desc.as_ref().unwrap();
        let mut stream = desc.read_range(ReadRange::segment(start, len as u64)).await;
        let mut out = Vec::with_capacity(len);

        while let Some(chunk_res) = stream.next().await {
            match chunk_res {
                Ok(chunk) => out.extend_from_slice(&chunk),
                Err(e) => {
                    set_last_error(format!("BidiReadObject error on gs://{}/{}: {}", self.uri.bucket, self.uri.object, e));
                    return Err(SQLITE_IOERR_READ);
                }
            }
        }

        // If returned fewer bytes than requested, zero-fill the rest
        if out.len() < len {
            out.resize(len, 0);
        }

        Ok(out)
    }

    /// Writes `amt` bytes at `offset`.
    pub fn write(&mut self, buf: &[u8], offset: u64) -> Result<(), i32> {
        let amt = buf.len();
        if amt == 0 {
            return Ok(());
        }

        let new_size = self.size.max(offset + amt as u64);
        self.size = new_size;

        for (i, &byte) in buf.iter().enumerate() {
            let byte_offset = offset + i as u64;
            let page_idx = byte_offset / PAGE_SIZE as u64;
            let page_offset = (byte_offset % PAGE_SIZE as u64) as usize;

            let page = self.dirty_pages.entry(page_idx).or_insert_with(|| {
                // If page already in clean cache, copy it; else initialize zeroes
                if let Some(cached) = self.cached_pages.get(&page_idx) {
                    cached.clone()
                } else {
                    vec![0u8; PAGE_SIZE]
                }
            });

            if page_offset >= page.len() {
                page.resize(page_offset + 1, 0);
            }
            page[page_offset] = byte;
        }

        Ok(())
    }

    /// Truncates the database to `new_size`.
    pub fn truncate(&mut self, new_size: u64) -> Result<(), i32> {
        self.size = new_size;
        let last_page_idx = new_size / PAGE_SIZE as u64;
        let last_page_offset = (new_size % PAGE_SIZE as u64) as usize;

        // Discard pages beyond new_size
        self.dirty_pages.retain(|&idx, _| idx <= last_page_idx);
        self.cached_pages.retain(|&idx, _| idx <= last_page_idx);

        if let Some(page) = self.dirty_pages.get_mut(&last_page_idx) {
            page.truncate(last_page_offset);
        }
        if let Some(page) = self.cached_pages.get_mut(&last_page_idx) {
            page.truncate(last_page_offset);
        }

        Ok(())
    }

    /// Returns the logical file size.
    pub fn file_size(&self) -> u64 {
        self.size
    }

    /// Durably and atomically commits the database to Google Cloud Storage.
    pub async fn sync(&mut self) -> Result<(), i32> {
        if self.dirty_pages.is_empty() && self.desc.is_some() {
            // Nothing dirty to sync
            return Ok(());
        }

        tracing::info!(target: "gcs_vfs", "Syncing main DB gs://{}/{} (size: {}, dirty pages: {})",
            self.uri.bucket, self.uri.object, self.size, self.dirty_pages.len());

        let total_size = self.size as usize;
        let mut full_payload = vec![0u8; total_size];

        // 1. Populate base from clean cache / GCS
        let num_pages = (total_size + PAGE_SIZE - 1) / PAGE_SIZE;
        for page_idx in 0..num_pages as u64 {
            let page_start = (page_idx * PAGE_SIZE as u64) as usize;
            let page_len = PAGE_SIZE.min(total_size.saturating_sub(page_start));

            if let Some(dirty) = self.dirty_pages.get(&page_idx) {
                let copy_len = page_len.min(dirty.len());
                full_payload[page_start..page_start + copy_len].copy_from_slice(&dirty[..copy_len]);
            } else if let Some(clean) = self.cached_pages.get(&page_idx) {
                let copy_len = page_len.min(clean.len());
                full_payload[page_start..page_start + copy_len].copy_from_slice(&clean[..copy_len]);
            } else {
                let fetched = self.fetch_range_from_gcs(page_start as u64, page_len).await?;
                let copy_len = page_len.min(fetched.len());
                full_payload[page_start..page_start + copy_len].copy_from_slice(&fetched[..copy_len]);
                self.cached_pages.insert(page_idx, fetched);
            }
        }

        // 2. Commit atomically to GCS Rapid using open_appendable_object + append + finalize
        let formatted_bucket = self.uri.formatted_bucket();
        let mut builder = self.storage.open_appendable_object(&formatted_bucket, &self.uri.object);

        if let Some(gen) = self.generation {
            builder = builder.set_if_generation_match(gen);
        }

        let mut writer = match builder.send().await {
            Ok(w) => w,
            Err(e) => {
                set_last_error(format!("Failed opening appendable DB object gs://{}/{}: {}", self.uri.bucket, self.uri.object, e));
                return Err(SQLITE_IOERR_WRITE);
            }
        };

        if !full_payload.is_empty() {
            if let Err(e) = writer.append(Bytes::from(full_payload)).await {
                set_last_error(format!("Failed appending DB data to gs://{}/{}: {}", self.uri.bucket, self.uri.object, e));
                return Err(SQLITE_IOERR_WRITE);
            }
        }

        match writer.finalize().await {
            Ok(sealed_obj) => {
                self.generation = Some(sealed_obj.generation);
                self.size = sealed_obj.size as u64;

                // Move dirty pages into cached pages
                for (idx, page) in std::mem::take(&mut self.dirty_pages) {
                    self.cached_pages.insert(idx, page);
                }

                // Re-open Bidi descriptor
                match self.storage.open_object(&formatted_bucket, &self.uri.object).send().await {
                    Ok(new_desc) => self.desc = Some(new_desc),
                    Err(e) => {
                        set_last_error(format!("Failed re-opening DB descriptor gs://{}/{}: {}", self.uri.bucket, self.uri.object, e));
                        return Err(SQLITE_IOERR);
                    }
                }

                tracing::info!(target: "gcs_vfs", "Main DB gs://{}/{} committed atomically! New generation: {:?}",
                    self.uri.bucket, self.uri.object, self.generation);
                Ok(())
            }
            Err(e) => {
                set_last_error(format!("Atomic upload finalization failed on gs://{}/{}: {}", self.uri.bucket, self.uri.object, e));
                Err(SQLITE_IOERR_WRITE)
            }
        }
    }

    /// Closes the database file, ensuring any pending dirty pages are committed.
    pub async fn close(&mut self) -> Result<(), i32> {
        if !self.dirty_pages.is_empty() {
            self.sync().await?;
        }
        self.desc = None;
        self.dirty_pages.clear();
        self.cached_pages.clear();
        Ok(())
    }
}
