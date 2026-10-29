//! GCS storage client initialization and Tokio runtime bridge.

use std::sync::{Arc, OnceLock};
use google_cloud_storage::client::Storage;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();
static STORAGE_CLIENT: OnceLock<Arc<Storage>> = OnceLock::new();

/// Returns a reference to the global multi-threaded Tokio runtime.
pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .thread_name("gcs-vfs-worker")
            .build()
            .expect("Failed to initialize Tokio runtime for GCS VFS")
    })
}

/// Executes an asynchronous future synchronously from C ABI contexts.
///
/// Uses `block_in_place` if currently inside an active Tokio context to avoid panics.
pub fn block_on<F: std::future::Future>(f: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(f))
    } else {
        get_runtime().block_on(f)
    }
}

/// Asynchronously gets or initializes the global GCS Storage client.
pub async fn get_storage_client() -> Result<Arc<Storage>, String> {
    if let Some(client) = STORAGE_CLIENT.get() {
        return Ok(client.clone());
    }

    let client = Storage::builder()
        .build()
        .await
        .map_err(|e| format!("Failed to create Google Cloud Storage client: {}", e))?;

    let arc = Arc::new(client);
    let _ = STORAGE_CLIENT.set(arc.clone());
    Ok(arc)
}

/// Synchronously gets or initializes the global GCS Storage client.
pub fn get_storage_client_sync() -> Result<Arc<Storage>, String> {
    block_on(get_storage_client())
}
