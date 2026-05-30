#ifndef LOCAL_STORAGE_H_
#define LOCAL_STORAGE_H_

#include <cstdint>
#include <cstddef>
#include <string>
#include <memory>
#include <mutex>
#include <condition_variable>
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "storage.h"

#include <queue>
#include <thread>
#include <vector>

namespace sqlite {

class LocalStorage : public AppendOnlyStorage {
 public:
   ~LocalStorage() override;

  // Factory method to open/create a file.
  static absl::StatusOr<std::unique_ptr<LocalStorage>> Open(const std::string& path);

  Future<absl::StatusOr<int64_t>> AppendAsync(const uint8_t* data, size_t size) override;
  absl::StatusOr<size_t> PRead(uint8_t* buf, size_t size, int64_t offset) override;
  absl::StatusOr<int64_t> GetSize() override;
  absl::Status Sync() override;

 private:
  explicit LocalStorage(int fd);
  void WorkerLoop();
  absl::StatusOr<int64_t> AppendSync(const uint8_t* data, size_t size);

  struct Task {
    std::vector<uint8_t> data;
    std::promise<absl::StatusOr<int64_t>> promise;
  };

  int fd_;
  std::mutex mutex_;

  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<Task> queue_;
  bool stop_ = false;
  std::thread worker_thread_;
};

}  // namespace sqlite

#endif  // LOCAL_STORAGE_H_
