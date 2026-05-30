#include "vfs_backend.h"

#include <sqlite3.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>

#ifndef SQLITE_IOERR_DIRTY
#define SQLITE_IOERR_DIRTY (SQLITE_IOERR | (36<<8))
#endif
#include <unistd.h>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <algorithm>
#include <cstdlib>

#include "absl/status/status.h"
#include "block_mapper.h"
#include "local_storage.h"
#include "gcs_storage.h"
#ifndef USE_REAL_GCS_SDK
#include "gcs_client_mock.h"
#endif

#include <unordered_map>
#include <mutex>

namespace sqlite {

namespace {

sqlite3_vfs* g_default_vfs = nullptr;

std::mutex g_registry_mutex;
std::unordered_map<std::string, AppendOnlyFile*> g_open_files;

std::mutex g_gcs_paths_mutex;
std::unordered_map<std::string, std::string> g_gcs_paths; // local_path -> gcs_path

static int xClose(sqlite3_file* pFile) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_open_files.erase(file->path);
  }
  bool is_gcs = false;
  {
    std::lock_guard<std::mutex> lock(g_gcs_paths_mutex);
    is_gcs = g_gcs_paths.find(file->path) != g_gcs_paths.end();
  }
  if (is_gcs) {
    ::unlink(file->path.c_str());
  }
  file->mapper.~unique_ptr<BlockMapper>();
  file->path.std::string::~string();
  return SQLITE_OK;
}

static int xRead(sqlite3_file* pFile, void* pBuf, int iAmt, sqlite3_int64 iOfst) {
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
    absl::Status status = file->mapper->Read(reinterpret_cast<uint8_t*>(pBuf), read_len, iOfst);
    if (!status.ok()) {
      return SQLITE_IOERR_READ;
    }
    std::memset(reinterpret_cast<char*>(pBuf) + read_len, 0, iAmt - read_len);
    return SQLITE_IOERR_SHORT_READ;
  } else {
    absl::Status status = file->mapper->Read(reinterpret_cast<uint8_t*>(pBuf), iAmt, iOfst);
    if (!status.ok()) {
      return SQLITE_IOERR_READ;
    }
    return SQLITE_OK;
  }
}

static int xWrite(sqlite3_file* pFile, const void* pBuf, int iAmt, sqlite3_int64 iOfst) {
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  if (!file->mapper) {
    return SQLITE_IOERR_WRITE;
  }
  absl::Status status = file->mapper->Write(reinterpret_cast<const uint8_t*>(pBuf), iAmt, iOfst);
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
  return SQLITE_OK;
}

static int xUnlock(sqlite3_file* pFile, int eLock) {
  return SQLITE_OK;
}

static int xCheckReservedLock(sqlite3_file* pFile, int* pResOut) {
  *pResOut = 0;
  return SQLITE_OK;
}

static int xFileControl(sqlite3_file* pFile, int op, void* pArg) {
  return SQLITE_NOTFOUND;
}

static int xSectorSize(sqlite3_file* pFile) {
  return 4096;
}

static int xDeviceCharacteristics(sqlite3_file* pFile) {
  return SQLITE_IOCAP_SAFE_APPEND;
}

static const sqlite3_io_methods g_append_only_io_methods = {
  1,                          /* iVersion */
  xClose,                     /* xClose */
  xRead,                      /* xRead */
  xWrite,                     /* xWrite */
  xTruncate,                  /* xTruncate */
  xSync,                      /* xSync */
  xFileSize,                  /* xFileSize */
  xLock,                      /* xLock */
  xUnlock,                    /* xUnlock */
  xCheckReservedLock,         /* xCheckReservedLock */
  xFileControl,               /* xFileControl */
  xSectorSize,                /* xSectorSize */
  xDeviceCharacteristics      /* xDeviceCharacteristics */
};

static int xOpen(sqlite3_vfs* pVfs, const char* zName, sqlite3_file* pFile, int flags, int* pOutFlags) {
  std::cerr << "xOpen: " << (zName ? zName : "NULL") << " flags " << flags << std::endl;
  if (!(flags & SQLITE_OPEN_MAIN_DB)) {
    int rc = g_default_vfs->xOpen(g_default_vfs, zName, pFile, flags, pOutFlags);
    if (rc != SQLITE_OK) {
      int err = errno;
      std::cerr << "xOpen delegated returned " << rc << " errno " << err << " (" << std::strerror(err) << ")" << std::endl;
    } else {
      std::cerr << "xOpen delegated returned " << rc << std::endl;
    }
    return rc;
  }
  if (zName == nullptr) {
    return SQLITE_CANTOPEN;
  }
  std::string path(zName);
  std::string gcs_path;
  {
    std::lock_guard<std::mutex> lock(g_gcs_paths_mutex);
    auto it = g_gcs_paths.find(path);
    if (it != g_gcs_paths.end()) {
      gcs_path = it->second;
    }
  }
  std::cerr << "xOpen main DB path: " << path << " -> gcs_path: " << gcs_path << std::endl;
  if (gcs_path.empty() && path.rfind("gcs://", 0) == 0) {
    gcs_path = path;
  }

  bool is_gcs = !gcs_path.empty();

  {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    if (g_open_files.find(path) != g_open_files.end()) {
      return SQLITE_BUSY;
    }
  }

  if (is_gcs) {
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd >= 0) {
      ::close(fd);
    }
  }

  std::unique_ptr<AppendOnlyStorage> storage;
  if (is_gcs) {
    std::string_view sub = std::string_view(gcs_path).substr(6);
    size_t first_slash = sub.find('/');
    if (first_slash == std::string_view::npos) {
      if (is_gcs) ::unlink(path.c_str());
      return SQLITE_CANTOPEN;
    }
    std::string bucket = std::string(sub.substr(0, first_slash));
    std::string object = std::string(sub.substr(first_slash + 1));
    
#ifdef USE_REAL_GCS_SDK
    auto storage_or = GcsRapidStorage::Create(bucket, object);
#else
    auto storage_or = GcsRapidStorage::Create(std::make_shared<google::cloud::storage::AsyncClient>(), bucket, object);
#endif
    if (!storage_or.ok()) {
      if (is_gcs) ::unlink(path.c_str());
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
    if (is_gcs) {
      ::unlink(path.c_str());
    }
    return SQLITE_CANTOPEN;
  }
  
  auto* file = reinterpret_cast<AppendOnlyFile*>(pFile);
  new (&file->mapper) std::unique_ptr<BlockMapper>(std::move(mapper));
  new (&file->path) std::string(std::move(path));
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
  std::string gcs_path;
  {
    std::lock_guard<std::mutex> lock(g_gcs_paths_mutex);
    auto it = g_gcs_paths.find(path);
    if (it != g_gcs_paths.end()) {
      gcs_path = it->second;
    }
  }
  if (gcs_path.empty() && path.rfind("gcs://", 0) == 0) {
    gcs_path = path;
  }

  if (!gcs_path.empty()) {
    ::unlink(path.c_str());
    std::string_view sub = std::string_view(gcs_path).substr(6);
    size_t first_slash = sub.find('/');
    if (first_slash == std::string_view::npos) {
      return SQLITE_IOERR_DELETE;
    }
    std::string bucket = std::string(sub.substr(0, first_slash));
    std::string object = std::string(sub.substr(first_slash + 1));
#ifdef USE_REAL_GCS_SDK
    google::cloud::storage::Client client(google::cloud::Options{});
    auto delete_status = client.DeleteObject(bucket, object);
    if (!delete_status.ok()) {
      return SQLITE_IOERR_DELETE;
    }
#else
    std::string registry_key = bucket + "/" + object;
    {
      std::lock_guard<std::mutex> lock(google::cloud::storage::GetRegistryMutex());
      google::cloud::storage::GetMockRegistry().erase(registry_key);
    }
#endif
    return SQLITE_OK;
  } else {
    return g_default_vfs->xDelete(g_default_vfs, zName, syncDir);
  }
}

static int xAccess(sqlite3_vfs* pVfs, const char* zName, int flags, int* pResOut) {
  if (zName == nullptr) {
    *pResOut = 0;
    return SQLITE_OK;
  }
  std::string path(zName);
  std::string gcs_path;
  {
    std::lock_guard<std::mutex> lock(g_gcs_paths_mutex);
    auto it = g_gcs_paths.find(path);
    if (it != g_gcs_paths.end()) {
      gcs_path = it->second;
    }
  }
  if (gcs_path.empty() && path.rfind("gcs://", 0) == 0) {
    gcs_path = path;
  }

  if (!gcs_path.empty()) {
    std::string_view sub = std::string_view(gcs_path).substr(6);
    size_t first_slash = sub.find('/');
    if (first_slash == std::string_view::npos) {
      *pResOut = 0;
      return SQLITE_OK;
    }
    std::string bucket = std::string(sub.substr(0, first_slash));
    std::string object = std::string(sub.substr(first_slash + 1));
    bool exists = false;
#ifdef USE_REAL_GCS_SDK
    google::cloud::storage::Client client(google::cloud::Options{});
    auto metadata = client.GetObjectMetadata(bucket, object);
    exists = metadata.ok();
#else
    std::string registry_key = bucket + "/" + object;
    {
      std::lock_guard<std::mutex> lock(google::cloud::storage::GetRegistryMutex());
      exists = google::cloud::storage::GetMockRegistry().find(registry_key) != google::cloud::storage::GetMockRegistry().end();
    }
#endif
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

static int xFullPathname(sqlite3_vfs* pVfs, const char* zName, int nOut, char* zOut) {
  std::cerr << "xFullPathname: " << (zName ? zName : "NULL") << std::endl;
  if (zName == nullptr) {
    return SQLITE_CANTOPEN;
  }
  std::string path(zName);
  if (path.rfind("gcs://", 0) == 0) {
    std::string suffix = path.substr(6);
    std::replace(suffix.begin(), suffix.end(), '/', '_');
    std::string flat_local_path;
    const char* tmpdir = std::getenv("TEST_TMPDIR");
    if (tmpdir != nullptr) {
      flat_local_path = std::string(tmpdir) + "/gcs_" + suffix;
    } else {
      flat_local_path = "/tmp/gcs_" + suffix;
    }

    int rc = g_default_vfs->xFullPathname(g_default_vfs, flat_local_path.c_str(), nOut, zOut);
    std::cerr << "xFullPathname GCS mapped flat_local_path " << flat_local_path << " -> zOut " << zOut << " rc " << rc << std::endl;
    if ((rc & 0xFF) == SQLITE_OK) {
      std::lock_guard<std::mutex> lock(g_gcs_paths_mutex);
      g_gcs_paths[zOut] = path;
    }
    return rc;
  }
  int rc = g_default_vfs->xFullPathname(g_default_vfs, zName, nOut, zOut);
  std::cerr << "xFullPathname local path " << zName << " -> zOut " << zOut << " rc " << rc << std::endl;
  return rc;
}

static void* xDlOpen(sqlite3_vfs* pVfs, const char* zFilename) {
  return g_default_vfs->xDlOpen(g_default_vfs, zFilename);
}

static void xDlError(sqlite3_vfs* pVfs, int nByte, char* zErrMsg) {
  g_default_vfs->xDlError(g_default_vfs, nByte, zErrMsg);
}

static void (*xDlSym(sqlite3_vfs* pVfs, void* pHandle, const char* zSymbol))(void) {
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
  if (g_default_vfs->iVersion >= 2 && g_default_vfs->xCurrentTimeInt64) {
    return g_default_vfs->xCurrentTimeInt64(g_default_vfs, pTime);
  }
  double t = 0.0;
  int rc = g_default_vfs->xCurrentTime(g_default_vfs, &t);
  if (rc == SQLITE_OK) {
    *pTime = static_cast<sqlite3_int64>(t * 86400000.0);
  }
  return rc;
}

static sqlite3_vfs g_append_only_vfs = {
  2,                              /* iVersion (v2 supports xCurrentTimeInt64) */
  sizeof(AppendOnlyFile),         /* szOsFile */
  1024,                           /* mxPathname */
  nullptr,                        /* pNext */
  "appendonly",                   /* zName */
  nullptr,                        /* pAppData */
  xOpen,                          /* xOpen */
  xDelete,                        /* xDelete */
  xAccess,                        /* xAccess */
  xFullPathname,                  /* xFullPathname */
  xDlOpen,                        /* xDlOpen */
  xDlError,                       /* xDlError */
  xDlSym,                         /* xDlSym */
  xDlClose,                       /* xDlClose */
  xRandomness,                    /* xRandomness */
  xSleep,                         /* xSleep */
  xCurrentTime,                   /* xCurrentTime */
  xGetLastError,                  /* xGetLastError */
  xCurrentTimeInt64               /* xCurrentTimeInt64 */
};

}  // namespace

absl::Status RegisterAppendOnlyVfs() {
  g_default_vfs = sqlite3_vfs_find(nullptr);
  if (!g_default_vfs) {
    return absl::InternalError("Failed to find default SQLite VFS");
  }
  g_append_only_vfs.mxPathname = g_default_vfs->mxPathname;
  g_append_only_vfs.szOsFile = std::max(static_cast<int>(sizeof(AppendOnlyFile)), g_default_vfs->szOsFile);
  int rc = sqlite3_vfs_register(&g_append_only_vfs, 0);
  if (rc != SQLITE_OK) {
    return absl::InternalError("Failed to register appendonly VFS");
  }
  return absl::OkStatus();
}

}  // namespace sqlite
