//! WAL file engine using GCS Rapid appendable objects with Bidi streaming.

use std::sync::Arc;
use bytes::Bytes;
use google_cloud_storage::appendable_object_writer::AppendableObjectWriter;
use google_cloud_storage::client::Storage;
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::object_descriptor::ObjectDescriptor;

use crate::error::*;
use crate::uri::GcsUri;

/// Write-Ahead Log (WAL) file representation on Google Cloud Storage Rapid.
pub struct GcsWalFile {
    pub uri: GcsUri,
    pub wal_object: String,
    storage: Arc<Storage>,
    /// GCS Rapid appendable object writer using BidiWriteObject gRPC stream
    writer: Option<AppendableObjectWriter>,
    /// Open descriptor for reading persisted chunks via BidiReadObject gRPC stream
    desc: Option<ObjectDescriptor>,
    /// Generation number of the appendable object
    generation: Option<i64>,
    /// Persisted byte size acknowledged by GCS Rapid
    persisted_size: u64,
    /// Memory buffer for bytes written via `write()` but not yet committed via `flush()`
    unflushed_buffer: Vec<u8>,
    /// Offset where `unflushed_buffer` begins
    unflushed_start_offset: u64,
    /// Total logical size (persisted + un-flushed)
    logical_size: u64,
}

impl GcsWalFile {
    /// Opens or reopens an appendable WAL object in GCS Rapid.
    pub async fn open(storage: Arc<Storage>, uri: GcsUri, _flags: i32) -> Result<Self, i32> {
        let wal_object = uri.wal_object();
        let formatted_bucket = uri.formatted_bucket();

        tracing::info!(target: "gcs_vfs", "Opening GCS Rapid WAL: gs://{}/{}", uri.bucket, wal_object);

        // Check if an existing unfinalized WAL object already exists
        let open_res = storage.open_object(&formatted_bucket, &wal_object).send().await;

        match open_res {
            Ok(desc) => {
                let gen = desc.object().generation;
                let size = desc.object().size as u64;
                tracing::info!(target: "gcs_vfs", "Reopening existing WAL gs://{}/{} (gen: {}, size: {})",
                    uri.bucket, wal_object, gen, size);

                match storage.reopen_appendable_object(&formatted_bucket, &wal_object, gen).send().await {
                    Ok(writer) => {
                        let persisted = writer.persisted_size() as u64;
                        Ok(Self {
                            uri,
                            wal_object,
                            storage,
                            writer: Some(writer),
                            desc: Some(desc),
                            generation: Some(gen),
                            persisted_size: persisted,
                            unflushed_buffer: Vec::new(),
                            unflushed_start_offset: persisted,
                            logical_size: persisted,
                        })
                    }
                    Err(e) => {
                        set_last_error(format!("Failed reopening appendable WAL gs://{}/{}: {}", uri.bucket, wal_object, e));
                        Err(SQLITE_CANTOPEN)
                    }
                }
            }
            Err(_) => {
                // WAL does not exist yet: create brand new appendable object
                tracing::info!(target: "gcs_vfs", "Creating new appendable WAL gs://{}/{}", uri.bucket, wal_object);
                match storage.open_appendable_object(&formatted_bucket, &wal_object).send().await {
                    Ok(writer) => {
                        // Open descriptor to track generation and size
                        let desc = storage.open_object(&formatted_bucket, &wal_object).send().await.ok();
                        let generation = desc.as_ref().map(|d| d.object().generation);
                        Ok(Self {
                            uri,
                            wal_object,
                            storage,
                            writer: Some(writer),
                            desc,
                            generation,
                            persisted_size: 0,
                            unflushed_buffer: Vec::new(),
                            unflushed_start_offset: 0,
                            logical_size: 0,
                        })
                    }
                    Err(e) => {
                        set_last_error(format!("Failed creating appendable WAL gs://{}/{}: {}", uri.bucket, wal_object, e));
                        Err(SQLITE_CANTOPEN)
                    }
                }
            }
        }
    }

    /// Appends bytes to the WAL stream.
    ///
    /// Per Finding 2: Detects `offset == 0` on an active WAL as a cycle restart,
    /// rolling over to a fresh appendable object.
    pub async fn write(&mut self, buf: &[u8], offset: u64) -> Result<(), i32> {
        let amt = buf.len();
        if amt == 0 {
            return Ok(());
        }

        let formatted_bucket = self.uri.formatted_bucket();

        // Finding 2: SQLite WAL restart via header overwrite at offset 0
        if offset == 0 && (self.persisted_size > 0 || !self.unflushed_buffer.is_empty()) {
            tracing::info!(target: "gcs_vfs", "WAL restart detected at offset 0! Rolling over appendable object gs://{}/{}",
                self.uri.bucket, self.wal_object);

            if let Some(old_writer) = self.writer.take() {
                let _ = old_writer.close().await;
            }

            // Open fresh appendable object
            match self.storage.open_appendable_object(&formatted_bucket, &self.wal_object).send().await {
                Ok(new_writer) => {
                    self.writer = Some(new_writer);
                    self.desc = self.storage.open_object(&formatted_bucket, &self.wal_object).send().await.ok();
                    self.persisted_size = 0;
                    self.unflushed_buffer.clear();
                    self.unflushed_start_offset = 0;
                    self.logical_size = 0;
                }
                Err(e) => {
                    set_last_error(format!("Failed rolling over WAL gs://{}/{}: {}", self.uri.bucket, self.wal_object, e));
                    return Err(SQLITE_IOERR_WRITE);
                }
            }
        }

        // Buffer the un-flushed bytes in memory so live xRead sees them immediately (Finding 3)
        if self.unflushed_buffer.is_empty() {
            self.unflushed_start_offset = offset;
        }

        let buffer_rel_start = (offset.saturating_sub(self.unflushed_start_offset)) as usize;
        let required_len = buffer_rel_start + amt;
        if self.unflushed_buffer.len() < required_len {
            self.unflushed_buffer.resize(required_len, 0);
        }
        self.unflushed_buffer[buffer_rel_start..buffer_rel_start + amt].copy_from_slice(buf);

        // Send to GCS Rapid AppendableObjectWriter queue
        if let Some(writer) = self.writer.as_mut() {
            if let Err(e) = writer.append(Bytes::copy_from_slice(buf)).await {
                set_last_error(format!("Failed appending chunk to WAL gs://{}/{}: {}", self.uri.bucket, self.wal_object, e));
                return Err(SQLITE_IOERR_WRITE);
            }
        } else {
            set_last_error("WAL writer is not open".to_string());
            return Err(SQLITE_IOERR_WRITE);
        }

        self.logical_size = self.logical_size.max(offset + amt as u64);
        Ok(())
    }

    /// Reads bytes from the WAL.
    ///
    /// Per Finding 3: Checks un-flushed write buffer first, then streams persisted bytes
    /// via BidiReadObject (`desc.read_range`).
    /// Per Finding 1: Zero-fills short reads per sqlite3.h lines 830-834.
    pub async fn read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), i32> {
        let amt = buf.len();
        if amt == 0 {
            return Ok(());
        }

        if offset >= self.logical_size {
            // Past EOF: zero-fill and return SHORT_READ
            buf.fill(0);
            return Err(SQLITE_IOERR_SHORT_READ);
        }

        let readable_bytes = ((self.logical_size - offset) as usize).min(amt);
        let read_slice = &mut buf[..readable_bytes];

        let req_start = offset;
        let req_end = offset + readable_bytes as u64;

        let unflushed_start = self.unflushed_start_offset;
        let unflushed_len = self.unflushed_buffer.len() as u64;
        let unflushed_end = unflushed_start + unflushed_len;

        // 1. Fetch persisted range from GCS Rapid via BidiReadObject if needed
        let persisted_limit = self.persisted_size.min(unflushed_start);
        if req_start < persisted_limit {
            let fetch_end = req_end.min(persisted_limit);
            let fetch_len = (fetch_end - req_start) as usize;
            let fetched = self.fetch_persisted_range(req_start, fetch_len).await?;
            let copy_len = fetched.len().min(fetch_len);
            read_slice[..copy_len].copy_from_slice(&fetched[..copy_len]);
            if copy_len < fetch_len {
                read_slice[copy_len..fetch_len].fill(0);
            }
        }

        // 2. Fetch unflushed in-memory buffer if needed
        if unflushed_len > 0 && req_end > unflushed_start && req_start < unflushed_end {
            let overlap_start = req_start.max(unflushed_start);
            let overlap_end = req_end.min(unflushed_end);
            let buf_offset = (overlap_start - req_start) as usize;
            let src_start = (overlap_start - unflushed_start) as usize;
            let copy_len = (overlap_end - overlap_start) as usize;
            read_slice[buf_offset..buf_offset + copy_len]
                .copy_from_slice(&self.unflushed_buffer[src_start..src_start + copy_len]);
        }

        // Short-read zero-fill guarantee (Finding 1)
        if readable_bytes < amt {
            buf[readable_bytes..].fill(0);
            return Err(SQLITE_IOERR_SHORT_READ);
        }

        Ok(())
    }

    /// Streams a persisted byte range from GCS Rapid via gRPC BidiReadObject.
    async fn fetch_persisted_range(&mut self, start: u64, len: usize) -> Result<Vec<u8>, i32> {
        if self.desc.is_none() {
            let formatted_bucket = self.uri.formatted_bucket();
            self.desc = self.storage.open_object(&formatted_bucket, &self.wal_object).send().await.ok();
        }

        if let Some(desc) = self.desc.as_ref() {
            let mut stream = desc.read_range(ReadRange::segment(start, len as u64)).await;
            let mut out = Vec::with_capacity(len);
            while let Some(chunk_res) = stream.next().await {
                match chunk_res {
                    Ok(chunk) => out.extend_from_slice(&chunk),
                    Err(e) => {
                        set_last_error(format!("BidiReadObject error on WAL gs://{}/{}: {}",
                            self.uri.bucket, self.wal_object, e));
                        return Err(SQLITE_IOERR_READ);
                    }
                }
            }
            Ok(out)
        } else {
            Ok(vec![0u8; len])
        }
    }

    /// Durably flushes all buffered appends to the GCS Rapid zonal storage.
    pub async fn sync(&mut self) -> Result<(), i32> {
        if self.unflushed_buffer.is_empty() {
            return Ok(());
        }

        tracing::info!(target: "gcs_vfs", "Syncing GCS Rapid WAL gs://{}/{} (flushing {} bytes)",
            self.uri.bucket, self.wal_object, self.unflushed_buffer.len());

        if let Some(writer) = self.writer.as_mut() {
            match writer.flush().await {
                Ok(new_persisted) => {
                    self.persisted_size = new_persisted as u64;
                    self.unflushed_buffer.clear();
                    self.unflushed_start_offset = self.persisted_size;

                    // Refresh Bidi descriptor
                    let formatted_bucket = self.uri.formatted_bucket();
                    self.desc = self.storage.open_object(&formatted_bucket, &self.wal_object).send().await.ok();

                    tracing::info!(target: "gcs_vfs", "WAL flush acknowledged! Persisted size: {} bytes", self.persisted_size);
                    Ok(())
                }
                Err(e) => {
                    set_last_error(format!("Failed flushing WAL to GCS Rapid gs://{}/{}: {}",
                        self.uri.bucket, self.wal_object, e));
                    Err(SQLITE_IOERR_FSYNC)
                }
            }
        } else {
            set_last_error("WAL writer not open on sync".to_string());
            Err(SQLITE_IOERR_FSYNC)
        }
    }

    /// Truncates the WAL.
    ///
    /// Truncating to 0 resets the log by rolling over to a fresh appendable object.
    pub async fn truncate(&mut self, new_size: u64) -> Result<(), i32> {
        if new_size == 0 {
            tracing::info!(target: "gcs_vfs", "Truncating WAL gs://{}/{} to 0", self.uri.bucket, self.wal_object);

            if let Some(writer) = self.writer.take() {
                let _ = writer.close().await;
            }

            let formatted_bucket = self.uri.formatted_bucket();
            match self.storage.open_appendable_object(&formatted_bucket, &self.wal_object).send().await {
                Ok(new_writer) => {
                    self.writer = Some(new_writer);
                    self.desc = self.storage.open_object(&formatted_bucket, &self.wal_object).send().await.ok();
                    self.persisted_size = 0;
                    self.unflushed_buffer.clear();
                    self.unflushed_start_offset = 0;
                    self.logical_size = 0;
                    Ok(())
                }
                Err(e) => {
                    set_last_error(format!("Failed recreating appendable WAL gs://{}/{}: {}",
                        self.uri.bucket, self.wal_object, e));
                    Err(SQLITE_IOERR_TRUNCATE)
                }
            }
        } else {
            self.logical_size = new_size;
            Ok(())
        }
    }

    /// Returns the logical file size.
    pub fn file_size(&self) -> u64 {
        self.logical_size
    }

    /// Returns the generation number of the appendable object.
    pub fn generation(&self) -> Option<i64> {
        self.generation
    }

    /// Closes the WAL writer cleanly, leaving it unfinalized for subsequent resumption.
    pub async fn close(&mut self) -> Result<(), i32> {
        if !self.unflushed_buffer.is_empty() {
            self.sync().await?;
        }
        if let Some(writer) = self.writer.take() {
            let _ = writer.close().await;
        }
        self.desc = None;
        self.unflushed_buffer.clear();
        Ok(())
    }
}
