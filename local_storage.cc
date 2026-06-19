#include "local_storage.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <iostream>
#include <vector>

namespace sqlite {

LocalStorage::LocalStorage(int fd) : fd_(fd) {}

LocalStorage::~LocalStorage() {
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

absl::StatusOr<int64_t> LocalStorage::Append(const uint8_t* data, size_t size) {
  std::cerr << "LOG: LocalStorage::Append start: size=" << size << std::endl;
  std::lock_guard<std::mutex> lock(mutex_);

  off_t offset = lseek(fd_, 0, SEEK_END);
  if (offset < 0) {
    auto status = absl::ErrnoToStatus(errno, "Failed to lseek to end of file");
    std::cerr << "LOG: LocalStorage::Append end: error=" << status.message() << std::endl;
    return status;
  }
  if (size == 0) {
    std::cerr << "LOG: LocalStorage::Append end: offset=" << offset << std::endl;
    return static_cast<int64_t>(offset);
  }

  size_t bytes_written = 0;
  while (bytes_written < size) {
    ssize_t res = write(fd_, data + bytes_written, size - bytes_written);
    if (res < 0) {
      if (errno == EINTR) {
        continue;
      }
      auto status = absl::ErrnoToStatus(errno, "Failed to write to file");
      std::cerr << "LOG: LocalStorage::Append end: error=" << status.message() << std::endl;
      return status;
    }
    bytes_written += res;
  }
  std::cerr << "LOG: LocalStorage::Append end: offset=" << offset << std::endl;
  return static_cast<int64_t>(offset);
}

absl::Status LocalStorage::Sync() {
  std::cerr << "LOG: LocalStorage::Sync start" << std::endl;
  std::lock_guard<std::mutex> lock(mutex_);
  if (fsync(fd_) < 0) {
    auto status = absl::ErrnoToStatus(errno, "Failed to fsync");
    std::cerr << "LOG: LocalStorage::Sync end: error=" << status.message() << std::endl;
    return status;
  }
  std::cerr << "LOG: LocalStorage::Sync end: success" << std::endl;
  return absl::OkStatus();
}

absl::StatusOr<size_t> LocalStorage::PRead(uint8_t* buf, size_t size, int64_t offset) {
  std::cerr << "LOG: LocalStorage::PRead start: size=" << size << ", offset=" << offset << std::endl;
  if (offset < 0) {
    auto status = absl::InvalidArgumentError("Offset cannot be negative");
    std::cerr << "LOG: LocalStorage::PRead end: error=" << status.message() << std::endl;
    return status;
  }

  size_t bytes_read = 0;
  while (bytes_read < size) {
    ssize_t res = pread(fd_, buf + bytes_read, size - bytes_read, offset + bytes_read);
    if (res < 0) {
      if (errno == EINTR) {
        continue;
      }
      auto status = absl::ErrnoToStatus(errno, "Failed to pread from file");
      std::cerr << "LOG: LocalStorage::PRead end: error=" << status.message() << std::endl;
      return status;
    }
    if (res == 0) {
      // EOF reached.
      break;
    }
    bytes_read += res;
  }
  std::cerr << "LOG: LocalStorage::PRead end: bytes_read=" << bytes_read << std::endl;
  return bytes_read;
}

absl::StatusOr<int64_t> LocalStorage::GetSize() {
  std::cerr << "LOG: LocalStorage::GetSize start" << std::endl;
  std::lock_guard<std::mutex> lock(mutex_);
  struct stat st;
  if (fstat(fd_, &st) < 0) {
    auto status = absl::ErrnoToStatus(errno, "Failed to fstat file");
    std::cerr << "LOG: LocalStorage::GetSize end: error=" << status.message() << std::endl;
    return status;
  }
  std::cerr << "LOG: LocalStorage::GetSize end: size=" << st.st_size << std::endl;
  return static_cast<int64_t>(st.st_size);
}

}  // namespace sqlite
