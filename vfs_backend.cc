#include "vfs_backend.h"

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT3
#include <fcntl.h>
#include <sys/stat.h>

#ifndef SQLITE_IOERR_DIRTY
#define SQLITE_IOERR_DIRTY (SQLITE_IOERR | (36 << 8))
#endif
#include <pthread.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "absl/status/status.h"
#include "block_mapper.h"
#include "gcs_storage.h"
#include "google/cloud/storage/client.h"
#include "local_storage.h"

namespace sqlite {

namespace {

class InMemoryStorage : public AppendOnlyStorage {
 public:
  InMemoryStorage() : size_(0) {}
  ~InMemoryStorage() override = default;

  absl::StatusOr<int64_t> Append(const uint8_t* data, size_t size) override {
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t offset = size_;
    if (size > 0 && data != nullptr) {
      data_.insert(data_.end(), data, data + size);
      size_ += size;
    }
    return offset;
  }

  absl::StatusOr<size_t> PRead(uint8_t* buf, size_t size,
                               int64_t offset) override {
    std::lock_guard<std::mutex> lock(mutex_);
    if (offset < 0) {
      return absl::InvalidArgumentError("Offset cannot be negative");
    }
    if (offset + size > static_cast<size_t>(size_)) {
      return absl::OutOfRangeError("Attempted to read past EOF");
    }
    if (size > 0 && buf != nullptr) {
      std::memcpy(buf, data_.data() + offset, size);
    }
    return size;
  }

  absl::StatusOr<int64_t> GetSize() override {
    std::lock_guard<std::mutex> lock(mutex_);
    return size_;
  }

  absl::Status Sync() override {
    return absl::OkStatus();
  }

  absl::Status Synchronize() override {
    return absl::OkStatus();
  }

 private:
  std::mutex mutex_;
  std::vector<uint8_t> data_;
  int64_t size_;
};

std::mutex g_journal_paths_mutex;
std::unordered_set<std::string> g_in_memory_journal_paths;

sqlite3_vfs* g_default_vfs = nullptr;

std::mutex g_registry_mutex;
std::unordered_map<std::string, AppendOnlyFile*> g_open_files;

static int xClose(sqlite3_file* pFile) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_open_files.erase(file->path);
  }
  file->mapper.~unique_ptr<BlockMapper>();
  file->path.std::string::~string();
  return SQLITE_OK;
}

static int xRead(sqlite3_file* pFile, void* pBuf, int iAmt,
                 sqlite3_int64 iOfst) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  if (!file->mapper) {
    return SQLITE_IOERR_READ;
  }
  int64_t logical_size = file->mapper->logical_size();
  if (iOfst >= logical_size) {
    std::memset(pBuf, 0, iAmt);
    return SQLITE_IOERR_SHORT_READ;
  } else if (iOfst + iAmt > logical_size) {
    int64_t read_len = logical_size - iOfst;
    absl::Status status =
        file->mapper->Read(reinterpret_cast<uint8_t*>(pBuf), read_len, iOfst);
    if (!status.ok()) {
      return SQLITE_IOERR_READ;
    }
    std::memset(reinterpret_cast<char*>(pBuf) + read_len, 0, iAmt - read_len);
    return SQLITE_IOERR_SHORT_READ;
  } else {
    absl::Status status =
        file->mapper->Read(reinterpret_cast<uint8_t*>(pBuf), iAmt, iOfst);
    if (!status.ok()) {
      return SQLITE_IOERR_READ;
    }
    return SQLITE_OK;
  }
}

static int xWrite(sqlite3_file* pFile, const void* pBuf, int iAmt,
                  sqlite3_int64 iOfst) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  if (!file->mapper) {
    return SQLITE_IOERR_WRITE;
  }
  absl::Status status =
      file->mapper->Write(reinterpret_cast<const uint8_t*>(pBuf), iAmt, iOfst);
  if (!status.ok()) {
    return SQLITE_IOERR_WRITE;
  }
  return SQLITE_OK;
}

static int xTruncate(sqlite3_file* pFile, sqlite3_int64 size) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  if (!file->mapper) {
    return SQLITE_IOERR_TRUNCATE;
  }
  absl::Status status = file->mapper->Truncate(size);
  if (!status.ok()) {
    return SQLITE_IOERR_TRUNCATE;
  }
  return SQLITE_OK;
}

static int xSync(sqlite3_file* pFile, int flags) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  if (!file->mapper) {
    return SQLITE_IOERR_DIRTY;
  }
  absl::Status status = file->mapper->Sync();
  if (!status.ok()) {
    return SQLITE_IOERR_DIRTY;
  }
  return SQLITE_OK;
}

static int xFileSize(sqlite3_file* pFile, sqlite3_int64* pSize) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  if (!file->mapper) {
    return SQLITE_IOERR;
  }
  *pSize = file->mapper->logical_size();
  return SQLITE_OK;
}

static int xLock(sqlite3_file* pFile, int eLock) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  if (file->lock_level == SQLITE_LOCK_NONE && eLock >= SQLITE_LOCK_SHARED) {
    if (file->mapper) {
      absl::Status status = file->mapper->Synchronize();
      if (!status.ok()) {
        return SQLITE_IOERR_LOCK;
      }
    }
  }
  file->lock_level = eLock;
  return SQLITE_OK;
}

static int xUnlock(sqlite3_file* pFile, int eLock) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  file->lock_level = eLock;
  return SQLITE_OK;
}

static int xCheckReservedLock(sqlite3_file* pFile, int* pResOut) {
  *pResOut = 0;
  return SQLITE_OK;
}

static int xFileControl(sqlite3_file* pFile, int op, void* pArg) {
  return SQLITE_NOTFOUND;
}

static int xSectorSize(sqlite3_file* pFile) { return 4096; }

static int xDeviceCharacteristics(sqlite3_file* pFile) {
  return SQLITE_IOCAP_SAFE_APPEND;
}

static const sqlite3_io_methods g_append_only_io_methods = {
    1,                     /* iVersion */
    xClose,                /* xClose */
    xRead,                 /* xRead */
    xWrite,                /* xWrite */
    xTruncate,             /* xTruncate */
    xSync,                 /* xSync */
    xFileSize,             /* xFileSize */
    xLock,                 /* xLock */
    xUnlock,               /* xUnlock */
    xCheckReservedLock,    /* xCheckReservedLock */
    xFileControl,          /* xFileControl */
    xSectorSize,           /* xSectorSize */
    xDeviceCharacteristics /* xDeviceCharacteristics */
};

static int xOpen(sqlite3_vfs* pVfs, const char* zName, sqlite3_file* pFile,
                 int flags, int* pOutFlags) {
  if (flags & SQLITE_OPEN_WAL) {
    return SQLITE_CANTOPEN;
  }
  if (zName == nullptr) {
    return SQLITE_CANTOPEN;
  }
  std::string path(zName);
  bool is_gcs = (path.rfind("gcs://", 0) == 0);
  bool is_journal = (flags & (SQLITE_OPEN_MAIN_JOURNAL | SQLITE_OPEN_TEMP_JOURNAL |
                              SQLITE_OPEN_SUBJOURNAL | SQLITE_OPEN_SUPER_JOURNAL));

  if (!is_gcs && !is_journal && !(flags & SQLITE_OPEN_MAIN_DB)) {
    return g_default_vfs->xOpen(g_default_vfs, zName, pFile, flags, pOutFlags);
  }

  {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    if (g_open_files.find(path) != g_open_files.end()) {
      return SQLITE_BUSY;
    }
  }

  if (is_journal) {
    std::lock_guard<std::mutex> lock(g_journal_paths_mutex);
    g_in_memory_journal_paths.insert(path);
  }

  std::unique_ptr<AppendOnlyStorage> storage;
  if (is_journal) {
    storage = std::make_unique<InMemoryStorage>();
  } else if (is_gcs) {
    std::string_view sub = std::string_view(path).substr(6);
    size_t first_slash = sub.find('/');
    if (first_slash == std::string_view::npos) {
      return SQLITE_CANTOPEN;
    }
    std::string bucket = std::string(sub.substr(0, first_slash));
    std::string object = std::string(sub.substr(first_slash + 1));

    auto storage_or = GcsRapidStorage::Create(bucket, object);
    if (!storage_or.ok()) {
      return SQLITE_CANTOPEN;
    }
    storage = std::move(storage_or.value());
  } else {
    auto storage_or = LocalStorage::Open(path);
    if (!storage_or.ok()) {
      return SQLITE_CANTOPEN;
    }
    storage = std::move(storage_or.value());
  }

  auto mapper = std::make_unique<BlockMapper>(std::move(storage));
  absl::Status init_status = mapper->Init();
  if (!init_status.ok()) {
    return SQLITE_CANTOPEN;
  }

  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  new (&file->mapper) std::unique_ptr<BlockMapper>(std::move(mapper));
  new (&file->path) std::string(std::move(path));
  file->lock_level = SQLITE_LOCK_NONE;
  file->base.pMethods = &g_append_only_io_methods;

  {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_open_files[file->path] = file;
  }

  if (pOutFlags) {
    *pOutFlags = flags;
  }
  return SQLITE_OK;
}

static int xDelete(sqlite3_vfs* pVfs, const char* zName, int syncDir) {
  if (zName == nullptr) {
    return SQLITE_IOERR_DELETE;
  }
  std::string path(zName);

  {
    std::lock_guard<std::mutex> lock(g_journal_paths_mutex);
    if (g_in_memory_journal_paths.find(path) != g_in_memory_journal_paths.end()) {
      g_in_memory_journal_paths.erase(path);
      return SQLITE_OK;
    }
  }

  bool is_gcs = (path.rfind("gcs://", 0) == 0);

  if (is_gcs) {
    std::string_view sub = std::string_view(path).substr(6);
    size_t first_slash = sub.find('/');
    if (first_slash == std::string_view::npos) {
      return SQLITE_IOERR_DELETE;
    }
    std::string bucket = std::string(sub.substr(0, first_slash));
    std::string object = std::string(sub.substr(first_slash + 1));
    google::cloud::storage::Client client(google::cloud::Options{});
    auto delete_status = client.DeleteObject(bucket, object);
    if (!delete_status.ok() &&
        delete_status.code() != google::cloud::StatusCode::kNotFound) {
      return SQLITE_IOERR_DELETE;
    }
    return SQLITE_OK;
  } else {
    return g_default_vfs->xDelete(g_default_vfs, zName, syncDir);
  }
}

static int xAccess(sqlite3_vfs* pVfs, const char* zName, int flags,
                   int* pResOut) {
  if (zName == nullptr) {
    *pResOut = 0;
    return SQLITE_OK;
  }
  std::string path(zName);

  {
    std::lock_guard<std::mutex> lock(g_journal_paths_mutex);
    if (g_in_memory_journal_paths.find(path) != g_in_memory_journal_paths.end()) {
      *pResOut = 1;
      return SQLITE_OK;
    }
  }

  bool is_gcs = (path.rfind("gcs://", 0) == 0);

  if (is_gcs) {
    std::string_view sub = std::string_view(path).substr(6);
    size_t first_slash = sub.find('/');
    if (first_slash == std::string_view::npos) {
      *pResOut = 0;
      return SQLITE_OK;
    }
    std::string bucket = std::string(sub.substr(0, first_slash));
    std::string object = std::string(sub.substr(first_slash + 1));
    bool exists = false;
    google::cloud::storage::Client client(google::cloud::Options{});
    auto metadata = client.GetObjectMetadata(bucket, object);
    exists = metadata.ok();
    if (flags == SQLITE_ACCESS_EXISTS) {
      *pResOut = exists ? 1 : 0;
    } else {
      *pResOut = 1;
    }
    return SQLITE_OK;
  } else {
    return g_default_vfs->xAccess(g_default_vfs, zName, flags, pResOut);
  }
}

static int xFullPathname(sqlite3_vfs* pVfs, const char* zName, int nOut,
                         char* zOut) {
  if (zName == nullptr) {
    return SQLITE_CANTOPEN;
  }
  std::string path(zName);
  if (path.rfind("gcs://", 0) == 0) {
    std::strncpy(zOut, zName, nOut - 1);
    zOut[nOut - 1] = '\0';
    return SQLITE_OK;
  }
  return g_default_vfs->xFullPathname(g_default_vfs, zName, nOut, zOut);
}

static void* xDlOpen(sqlite3_vfs* pVfs, const char* zFilename) {
  return g_default_vfs->xDlOpen(g_default_vfs, zFilename);
}

static void xDlError(sqlite3_vfs* pVfs, int nByte, char* zErrMsg) {
  g_default_vfs->xDlError(g_default_vfs, nByte, zErrMsg);
}

static void (*xDlSym(sqlite3_vfs* pVfs, void* pHandle,
                     const char* zSymbol))(void) {
  return g_default_vfs->xDlSym(g_default_vfs, pHandle, zSymbol);
}

static void xDlClose(sqlite3_vfs* pVfs, void* pHandle) {
  g_default_vfs->xDlClose(g_default_vfs, pHandle);
}

static int xRandomness(sqlite3_vfs* pVfs, int nByte, char* zOut) {
  return g_default_vfs->xRandomness(g_default_vfs, nByte, zOut);
}

static int xSleep(sqlite3_vfs* pVfs, int microseconds) {
  return g_default_vfs->xSleep(g_default_vfs, microseconds);
}

static int xCurrentTime(sqlite3_vfs* pVfs, double* pTime) {
  return g_default_vfs->xCurrentTime(g_default_vfs, pTime);
}

static int xGetLastError(sqlite3_vfs* pVfs, int nByte, char* zOut) {
  return g_default_vfs->xGetLastError(g_default_vfs, nByte, zOut);
}

static int xCurrentTimeInt64(sqlite3_vfs* pVfs, sqlite3_int64* pTime) {
  return g_default_vfs->xCurrentTimeInt64(g_default_vfs, pTime);
}

static sqlite3_vfs g_append_only_vfs = {
    2,                      /* iVersion (v2 supports xCurrentTimeInt64) */
    sizeof(AppendOnlyFile), /* szOsFile */
    1024,                   /* mxPathname */
    nullptr,                /* pNext */
    "appendonly",           /* zName */
    nullptr,                /* pAppData */
    xOpen,                  /* xOpen */
    xDelete,                /* xDelete */
    xAccess,                /* xAccess */
    xFullPathname,          /* xFullPathname */
    xDlOpen,                /* xDlOpen */
    xDlError,               /* xDlError */
    xDlSym,                 /* xDlSym */
    xDlClose,               /* xDlClose */
    xRandomness,            /* xRandomness */
    xSleep,                 /* xSleep */
    xCurrentTime,           /* xCurrentTime */
    xGetLastError,          /* xGetLastError */
    xCurrentTimeInt64       /* xCurrentTimeInt64 */
};

}  // namespace

absl::Status RegisterAppendOnlyVfs() {
  pthread_atfork(nullptr, nullptr, []() {
    {
      std::lock_guard<std::mutex> lock(g_registry_mutex);
      g_open_files.clear();
    }
    {
      std::lock_guard<std::mutex> lock(g_journal_paths_mutex);
      g_in_memory_journal_paths.clear();
    }
  });

  g_default_vfs = sqlite3_vfs_find(nullptr);
  if (!g_default_vfs) {
    return absl::InternalError("Failed to find default SQLite VFS");
  }
  g_append_only_vfs.mxPathname = g_default_vfs->mxPathname;
  g_append_only_vfs.szOsFile = std::max(
      static_cast<int>(sizeof(AppendOnlyFile)), g_default_vfs->szOsFile);
  int rc = sqlite3_vfs_register(&g_append_only_vfs, 0);
  if (rc != SQLITE_OK) {
    return absl::InternalError("Failed to register appendonly VFS");
  }
  return absl::OkStatus();
}

}  // namespace sqlite
