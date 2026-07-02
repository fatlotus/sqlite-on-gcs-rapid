#include "compactor.h"

#include <unistd.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <system_error>
#include <utility>
#include <vector>

#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "block_mapper.h"
#include "google/cloud/storage/client.h"
#include "google/cloud/storage/grpc_plugin.h"
#include "local_storage.h"

namespace sqlite {
namespace gcs = ::google::cloud::storage;

namespace {

absl::Status ConvertStatus(const google::cloud::Status& s) {
  if (s.ok()) return absl::OkStatus();
  absl::StatusCode code;
  switch (s.code()) {
    case google::cloud::StatusCode::kCancelled:
      code = absl::StatusCode::kCancelled;
      break;
    case google::cloud::StatusCode::kInvalidArgument:
      code = absl::StatusCode::kInvalidArgument;
      break;
    case google::cloud::StatusCode::kDeadlineExceeded:
      code = absl::StatusCode::kDeadlineExceeded;
      break;
    case google::cloud::StatusCode::kNotFound:
      code = absl::StatusCode::kNotFound;
      break;
    case google::cloud::StatusCode::kAlreadyExists:
      code = absl::StatusCode::kAlreadyExists;
      break;
    case google::cloud::StatusCode::kPermissionDenied:
      code = absl::StatusCode::kPermissionDenied;
      break;
    case google::cloud::StatusCode::kResourceExhausted:
      code = absl::StatusCode::kResourceExhausted;
      break;
    case google::cloud::StatusCode::kFailedPrecondition:
      code = absl::StatusCode::kFailedPrecondition;
      break;
    case google::cloud::StatusCode::kAborted:
      code = absl::StatusCode::kAborted;
      break;
    case google::cloud::StatusCode::kOutOfRange:
      code = absl::StatusCode::kOutOfRange;
      break;
    case google::cloud::StatusCode::kUnimplemented:
      code = absl::StatusCode::kUnimplemented;
      break;
    case google::cloud::StatusCode::kInternal:
      code = absl::StatusCode::kInternal;
      break;
    case google::cloud::StatusCode::kUnavailable:
      code = absl::StatusCode::kUnavailable;
      break;
    case google::cloud::StatusCode::kDataLoss:
      code = absl::StatusCode::kDataLoss;
      break;
    case google::cloud::StatusCode::kUnauthenticated:
      code = absl::StatusCode::kUnauthenticated;
      break;
    default:
      code = absl::StatusCode::kUnknown;
      break;
  }
  return absl::Status(code, s.message());
}

struct TempFileGuard {
  std::string path;
  ~TempFileGuard() {
    if (!path.empty()) {
      std::error_code ec;
      std::filesystem::remove(path, ec);
    }
  }
};

}  // namespace

absl::Status CompactGcsObject(const std::string& bucket,
                              const std::string& object) {
  auto options = google::cloud::Options{}.set<google::cloud::storage_experimental::EnableGrpcMetricsOption>(false);
  return CompactGcsObject(bucket, object, options);
}

absl::Status CompactGcsObject(const std::string& bucket,
                              const std::string& object,
                              const google::cloud::Options& options) {
  LOG(INFO) << "CompactGcsObject start: bucket=" << bucket << ", object=" << object;

  gcs::Client client(options);

  // 1. Get object metadata to obtain the current generation
  auto metadata = client.GetObjectMetadata(bucket, object);
  if (!metadata) {
    if (metadata.status().code() == google::cloud::StatusCode::kNotFound) {
      return absl::NotFoundError(absl::StrCat("GCS object not found: gs://", bucket, "/", object));
    }
    return ConvertStatus(metadata.status());
  }
  std::int64_t generation = metadata->generation();
  LOG(INFO) << "GCS object current generation: " << generation;

  // 2. Create a local temporary file to download to
  std::string temp_path = (std::filesystem::temp_directory_path() / "sqlite_compact_XXXXXX").string();
  int fd = mkstemp(&temp_path[0]);
  if (fd == -1) {
    return absl::InternalError("Failed to create temporary file");
  }
  close(fd);
  TempFileGuard guard{temp_path};

  // 3. Download the specific generation to the temporary file
  LOG(INFO) << "Downloading generation " << generation << " to " << temp_path;
  auto download_status = client.DownloadToFile(bucket, object, temp_path, gcs::Generation(generation));
  if (!download_status.ok()) {
    return ConvertStatus(download_status);
  }

  // 4. Open the temporary file via LocalStorage and run block recovery using BlockMapper
  auto storage_or = LocalStorage::Open(temp_path);
  if (!storage_or.ok()) {
    return storage_or.status();
  }
  BlockMapper mapper(std::move(storage_or.value()));
  absl::Status init_status = mapper.Init();
  if (!init_status.ok()) {
    return init_status;
  }

  int64_t logical_size = mapper.logical_size();
  LOG(INFO) << "Logical size recovered: " << logical_size << " bytes";

  // 5. Open a streaming upload for the transactional replacement
  LOG(INFO) << "Opening streaming upload for transactional overwrite";
  gcs::ObjectWriteStream write_stream = client.WriteObject(
      bucket, object, gcs::IfGenerationMatch(generation));
  if (!write_stream) {
    return ConvertStatus(write_stream.metadata().status());
  }

  // 6. Write active blocks sequentially
  absl::Status write_status = WriteCompactedRecords(mapper, write_stream);
  if (!write_status.ok()) {
    return write_status;
  }

  // 7. Finalize streaming upload
  write_stream.Close();
  if (!write_stream.metadata()) {
    return ConvertStatus(write_stream.metadata().status());
  }

  LOG(INFO) << "CompactGcsObject completed successfully. New generation: " << write_stream.metadata()->generation();
  return absl::OkStatus();
}

absl::Status WriteCompactedRecords(BlockMapper& mapper, std::ostream& os) {
  int64_t logical_size = mapper.logical_size();
  int64_t ceil_blocks = (logical_size + 4095) / 4096;

  int64_t compacted_blocks_count = 0;
  for (int64_t b = 0; b < ceil_blocks; ++b) {
    if (mapper.IsBlockMapped(b)) {
      uint8_t block_data[4096];
      absl::Status read_status = mapper.Read(block_data, sizeof(block_data), b * 4096);
      if (!read_status.ok()) {
        return read_status;
      }

      // Write block index (8 bytes)
      os.write(reinterpret_cast<const char*>(&b), sizeof(b));
      // Write block payload (4096 bytes)
      os.write(reinterpret_cast<const char*>(block_data), sizeof(block_data));
      // Write is_good flag (1 byte)
      uint8_t is_good = 1;
      os.write(reinterpret_cast<const char*>(&is_good), sizeof(is_good));

      if (!os.good()) {
        return absl::InternalError("Failed to write streaming payload to output stream");
      }

      compacted_blocks_count++;
    }
  }

  LOG(INFO) << "WriteCompactedRecords completed. Wrote " << compacted_blocks_count
            << " of " << ceil_blocks << " blocks.";
  return absl::OkStatus();
}

}  // namespace sqlite
