#include "local_storage.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <vector>

#ifdef __APPLE__
#define fdatasync(fd) fsync(fd)
#endif

namespace sqlite {

LocalStorage::LocalStorage(int fd) : fd_(fd) {
  worker_thread_ = std::thread(&LocalStorage::WorkerLoop, this);
}

LocalStorage::~LocalStorage() {
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    stop_ = true;
  }
  queue_cv_.notify_all();
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
  if (fd_ >= 0) {
    close(fd_);
  }
}

absl::StatusOr<std::unique_ptr<LocalStorage>> LocalStorage::Open(const std::string& path) {
  int fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
  if (fd < 0) {
    return absl::ErrnoToStatus(errno, "Failed to open file: " + path);
  }
  return std::unique_ptr<LocalStorage>(new LocalStorage(fd));
}

void LocalStorage::WorkerLoop() {
  while (true) {
    Task task;
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this]() { return stop_ || !queue_.empty(); });
      if (stop_ && queue_.empty()) {
        break;
      }
      task = std::move(queue_.front());
      queue_.pop();
    }
    task.promise.set_value(AppendSync(task.data.data(), task.data.size()));
  }
}

absl::StatusOr<int64_t> LocalStorage::AppendSync(const uint8_t* data, size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  off_t offset = lseek(fd_, 0, SEEK_END);
  if (offset < 0) {
    return absl::ErrnoToStatus(errno, "Failed to lseek to end of file");
  }
  if (size == 0) {
    return static_cast<int64_t>(offset);
  }

  size_t bytes_written = 0;
  while (bytes_written < size) {
    ssize_t res = write(fd_, data + bytes_written, size - bytes_written);
    if (res < 0) {
      if (errno == EINTR) {
        continue;
      }
      return absl::ErrnoToStatus(errno, "Failed to write to file");
    }
    bytes_written += res;
  }
  return static_cast<int64_t>(offset);
}

Future<absl::StatusOr<int64_t>> LocalStorage::AppendAsync(const uint8_t* data, size_t size) {
  Task task;
  if (size > 0 && data != nullptr) {
    task.data.assign(data, data + size);
  }
  auto fut = task.promise.get_future();

  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (stop_) {
      std::promise<absl::StatusOr<int64_t>> prom;
      prom.set_value(absl::FailedPreconditionError("Storage is closed"));
      return Future<absl::StatusOr<int64_t>>(prom.get_future());
    }
    queue_.push(std::move(task));
  }
  queue_cv_.notify_one();

  return Future<absl::StatusOr<int64_t>>(std::move(fut));
}

absl::Status LocalStorage::Sync() {
  auto fut = AppendAsync(nullptr, 0);
  auto res = fut.get();
  if (!res.ok()) {
    return res.status();
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (fdatasync(fd_) < 0) {
    return absl::ErrnoToStatus(errno, "Failed to fdatasync");
  }
  return absl::OkStatus();
}

absl::StatusOr<size_t> LocalStorage::PRead(uint8_t* buf, size_t size, int64_t offset) {
  if (offset < 0) {
    return absl::InvalidArgumentError("Offset cannot be negative");
  }

  size_t bytes_read = 0;
  while (bytes_read < size) {
    ssize_t res = pread(fd_, buf + bytes_read, size - bytes_read, offset + bytes_read);
    if (res < 0) {
      if (errno == EINTR) {
        continue;
      }
      return absl::ErrnoToStatus(errno, "Failed to pread from file");
    }
    if (res == 0) {
      // EOF reached.
      break;
    }
    bytes_read += res;
  }
  return bytes_read;
}

absl::StatusOr<int64_t> LocalStorage::GetSize() {
  std::lock_guard<std::mutex> lock(mutex_);
  struct stat st;
  if (fstat(fd_, &st) < 0) {
    return absl::ErrnoToStatus(errno, "Failed to fstat file");
  }
  return static_cast<int64_t>(st.st_size);
}

}  // namespace sqlite
