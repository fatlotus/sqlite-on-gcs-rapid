#ifndef IMPORT_EXPORT_H_
#define IMPORT_EXPORT_H_

#include <memory>
#include <string>

#include "absl/status/status.h"
#include "storage.h"

namespace sqlite {

// Exports a log-formatted storage to a raw binary file.
absl::Status ExportLog(std::unique_ptr<AppendOnlyStorage> log_storage,
                       const std::string& raw_file_path);

// Imports a raw binary file into a log-formatted storage.
absl::Status ImportLog(const std::string& raw_file_path,
                       std::unique_ptr<AppendOnlyStorage> log_storage);

}  // namespace sqlite

#endif  // IMPORT_EXPORT_H_
