#include <sqlite3ext.h>
#include "vfs_backend.h"

SQLITE_EXTENSION_INIT1

extern "C" {

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_gcsvfs_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
  SQLITE_EXTENSION_INIT2(pApi);
  auto status = sqlite::RegisterAppendOnlyVfs();
  if (!status.ok()) {
    if (pzErrMsg) {
      *pzErrMsg = sqlite3_mprintf("%.*s", static_cast<int>(status.message().size()), status.message().data());
    }
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_extension_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
  return sqlite3_gcsvfs_init(db, pzErrMsg, pApi);
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_libsqlite3_gcsvfs_init(
    sqlite3 *db,
    char **pzErrMsg,
    const sqlite3_api_routines *pApi
) {
  return sqlite3_gcsvfs_init(db, pzErrMsg, pApi);
}

}
