#ifndef VFS_BACKEND_H_
#define VFS_BACKEND_H_

#include <sqlite3ext.h>
#include <memory>
#include <string>
#include "absl/status/status.h"
#include "block_mapper.h"

namespace sqlite {

struct AppendOnlyFile {
  sqlite3_file base;
  std::unique_ptr<BlockMapper> mapper;
  std::string path;
};

absl::Status RegisterAppendOnlyVfs();

}  // namespace sqlite

#endif  // VFS_BACKEND_H_
