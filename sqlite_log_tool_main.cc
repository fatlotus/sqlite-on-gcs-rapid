#include <unistd.h>

#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "gcs_storage.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/grpc_plugin.h"
#include "import_export.h"
#include "local_storage.h"

void PrintUsage(const char* prog) {
  std::cerr << "Usage:\n";
  std::cerr << "  " << prog << " import <raw_sqlite3_db> <log_storage_path>\n";
  std::cerr << "  " << prog << " export <log_storage_path> <raw_sqlite3_db>\n";
  std::cerr << "\n";
  std::cerr << "Where <log_storage_path> can be a local file or a GCS path "
               "starting with gcs:// or gs://\n";
}

bool ParseGcsPath(const std::string& path, std::string* bucket,
                  std::string* object) {
  std::string_view sv(path);
  std::string_view sub;
  if (sv.rfind("gcs://", 0) == 0) {
    sub = sv.substr(6);
  } else if (sv.rfind("gs://", 0) == 0) {
    sub = sv.substr(5);
  } else {
    return false;
  }
  size_t first_slash = sub.find('/');
  if (first_slash == std::string_view::npos) {
    return false;
  }
  *bucket = std::string(sub.substr(0, first_slash));
  *object = std::string(sub.substr(first_slash + 1));
  return true;
}

absl::Status DeleteStoragePath(const std::string& path) {
  std::string bucket, object;
  if (ParseGcsPath(path, &bucket, &object)) {
    google::cloud::Options options =
        google::cloud::Options{}
            .set<google::cloud::storage_experimental::EnableGrpcMetricsOption>(
                false);
    google::cloud::storage::Client client(options);
    auto delete_status = client.DeleteObject(bucket, object);
    if (!delete_status.ok() &&
        delete_status.code() != google::cloud::StatusCode::kNotFound) {
      return absl::Status(static_cast<absl::StatusCode>(delete_status.code()),
                          delete_status.message());
    }
    return absl::OkStatus();
  } else {
    if (unlink(path.c_str()) < 0 && errno != ENOENT) {
      return absl::ErrnoToStatus(
          errno, "Failed to delete existing local file: " + path);
    }
    return absl::OkStatus();
  }
}

absl::StatusOr<std::unique_ptr<sqlite::AppendOnlyStorage>> OpenStoragePath(
    const std::string& path) {
  std::string bucket, object;
  if (ParseGcsPath(path, &bucket, &object)) {
    return sqlite::GcsRapidStorage::Create(bucket, object);
  } else {
    return sqlite::LocalStorage::Open(path);
  }
}

int main(int argc, char* argv[]) {
  absl::InitializeLog();

  if (argc != 4) {
    PrintUsage(argv[0]);
    return 1;
  }

  std::string mode = argv[1];
  if (mode == "import") {
    std::string raw_db = argv[2];
    std::string log_path = argv[3];

    std::cout << "Importing " << raw_db << " into " << log_path << "...\n";

    // For import, we delete the target first to ensure a fresh, empty log.
    absl::Status del_status = DeleteStoragePath(log_path);
    if (!del_status.ok()) {
      std::cerr << "Warning: failed to clear existing target log: "
                << del_status.message() << "\n";
    }

    auto storage_or = OpenStoragePath(log_path);
    if (!storage_or.ok()) {
      std::cerr << "Failed to open log storage: " << storage_or.status().message()
                << "\n";
      return 1;
    }

    absl::Status status =
        sqlite::ImportLog(raw_db, std::move(storage_or.value()));
    if (!status.ok()) {
      std::cerr << "Import failed: " << status.message() << "\n";
      return 1;
    }
    std::cout << "Import completed successfully!\n";

  } else if (mode == "export") {
    std::string log_path = argv[2];
    std::string raw_db = argv[3];

    std::cout << "Exporting " << log_path << " into " << raw_db << "...\n";

    auto storage_or = OpenStoragePath(log_path);
    if (!storage_or.ok()) {
      std::cerr << "Failed to open log storage: " << storage_or.status().message()
                << "\n";
      return 1;
    }

    absl::Status status =
        sqlite::ExportLog(std::move(storage_or.value()), raw_db);
    if (!status.ok()) {
      std::cerr << "Export failed: " << status.message() << "\n";
      return 1;
    }
    std::cout << "Export completed successfully!\n";

  } else {
    PrintUsage(argv[0]);
    return 1;
  }

  return 0;
}
