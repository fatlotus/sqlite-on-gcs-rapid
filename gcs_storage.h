#ifndef GCS_STORAGE_H_
#define GCS_STORAGE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "google/cloud/storage/async/client.h"
#include "google/cloud/storage/async/object_descriptor.h"
#include "storage.h"

namespace sqlite {

class GcsRapidStorage : public AppendOnlyStorage {
 public:
  GcsRapidStorage(
      std::shared_ptr<google::cloud::storage::AsyncClient> async_client,
      std::string bucket, std::string object,
      google::cloud::storage::AsyncWriter writer,
      google::cloud::storage::AsyncToken token,
      std::optional<google::cloud::storage::ObjectDescriptor> descriptor,
      int64_t initial_offset);

  ~GcsRapidStorage() override;

  static absl::StatusOr<std::unique_ptr<GcsRapidStorage>> Create(
      const std::string& bucket, const std::string& object);

  absl::StatusOr<int64_t> Append(const uint8_t* data, size_t size) override;
  absl::StatusOr<size_t> PRead(uint8_t* buf, size_t size,
                               int64_t offset) override;
  absl::StatusOr<int64_t> GetSize() override;
  absl::Status Sync() override;
  absl::Status Synchronize() override;

 private:
  std::shared_ptr<google::cloud::storage::AsyncClient> async_client_;
  std::string bucket_;
  std::string object_;

  std::mutex mutex_;
  google::cloud::storage::AsyncWriter writer_;
  google::cloud::storage::AsyncToken token_;
  std::optional<google::cloud::storage::ObjectDescriptor> descriptor_;
  int64_t initial_offset_ = 0;
  int64_t file_length_ = 0;
  bool has_local_writes_ = false;
  std::vector<uint8_t> buffer_;
};

}  // namespace sqlite

#endif
