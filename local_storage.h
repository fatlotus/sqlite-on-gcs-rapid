#ifndef LOCAL_STORAGE_H_
#define LOCAL_STORAGE_H_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "storage.h"

namespace sqlite {

class LocalStorage : public AppendOnlyStorage {
 public:
  ~LocalStorage() override;

  // Factory method to open/create a file.
  static absl::StatusOr<std::unique_ptr<LocalStorage>> Open(
      const std::string& path);

  absl::StatusOr<int64_t> Append(const uint8_t* data, size_t size) override;
  absl::StatusOr<size_t> PRead(uint8_t* buf, size_t size,
                               int64_t offset) override;
  absl::StatusOr<int64_t> GetSize() override;
  absl::Status Sync() override;
  absl::Status Synchronize() override;

 private:
  explicit LocalStorage(int fd);

  int fd_;
  std::mutex mutex_;
};

}  // namespace sqlite

#endif  // LOCAL_STORAGE_H_
