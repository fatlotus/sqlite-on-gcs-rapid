#ifndef GCS_STORAGE_H_
#define GCS_STORAGE_H_

#include <cstdint>
#include <cstddef>
#include <string>
#include <memory>
#include <mutex>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "storage.h"

#include "google/cloud/storage/client.h"
#include "google/cloud/storage/async/client.h"

namespace google {
namespace cloud {
namespace storage_experimental {
using AsyncClient = ::google::cloud::storage::AsyncClient;
using AsyncObjectWriter = ::google::cloud::storage::AsyncWriter;
using AppendObjectToken = ::google::cloud::storage::AsyncToken;
}  // namespace storage_experimental
}  // namespace cloud
}  // namespace google

namespace sqlite {

namespace gcs = ::google::cloud::storage;
namespace gcs_ex = ::google::cloud::storage_experimental;

class GcsRapidStorage : public AppendOnlyStorage {
 public:
  ~GcsRapidStorage() override;

  static absl::StatusOr<std::unique_ptr<GcsRapidStorage>> Create(
      const std::string& bucket, const std::string& object);

  Future<absl::StatusOr<int64_t>> AppendAsync(const uint8_t* data, size_t size) override;
  absl::StatusOr<size_t> PRead(uint8_t* buf, size_t size, int64_t offset) override;
  absl::StatusOr<int64_t> GetSize() override;
  absl::Status Sync() override;

 private:
  GcsRapidStorage(
      std::shared_ptr<gcs_ex::AsyncClient> async_client,
      gcs::Client client,
      std::string bucket, std::string object,
      gcs_ex::AsyncObjectWriter writer,
      gcs_ex::AppendObjectToken token,
      int64_t initial_offset);

  std::shared_ptr<gcs_ex::AsyncClient> async_client_;
  gcs::Client client_;
  std::string bucket_;
  std::string object_;

  std::mutex mutex_;
  gcs_ex::AsyncObjectWriter writer_;
  gcs_ex::AppendObjectToken token_;
  int64_t current_offset_ = 0;
  Future<absl::StatusOr<int64_t>> last_write_fut_;
};

} // namespace sqlite

#endif // GCS_STORAGE_H_
