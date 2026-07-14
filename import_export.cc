#include "import_export.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <memory>

#include "absl/status/status.h"
#include "block_mapper.h"

namespace sqlite {

absl::Status ExportLog(std::unique_ptr<AppendOnlyStorage> log_storage,
                       const std::string& raw_file_path) {
  BlockMapper mapper(std::move(log_storage));
  absl::Status status = mapper.Init();
  if (!status.ok()) {
    return status;
  }

  int64_t total_size = mapper.logical_size();
  int fd = open(raw_file_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) {
    return absl::ErrnoToStatus(
        errno, "Failed to open raw file for writing: " + raw_file_path);
  }

  int64_t offset = 0;
  uint8_t buffer[4096];
  while (offset < total_size) {
    int64_t to_read = std::min<int64_t>(4096, total_size - offset);
    absl::Status read_status = mapper.Read(buffer, to_read, offset);
    if (!read_status.ok()) {
      close(fd);
      return read_status;
    }

    int64_t written = 0;
    while (written < to_read) {
      ssize_t res = write(fd, buffer + written, to_read - written);
      if (res < 0) {
        if (errno == EINTR) continue;
        close(fd);
        return absl::ErrnoToStatus(
            errno, "Failed to write to raw file: " + raw_file_path);
      }
      written += res;
    }
    offset += to_read;
  }

  close(fd);
  return absl::OkStatus();
}

absl::Status ImportLog(const std::string& raw_file_path,
                       std::unique_ptr<AppendOnlyStorage> log_storage) {
  BlockMapper mapper(std::move(log_storage));
  absl::Status status = mapper.Init();
  if (!status.ok()) {
    return status;
  }

  int fd = open(raw_file_path.c_str(), O_RDONLY);
  if (fd < 0) {
    return absl::ErrnoToStatus(
        errno, "Failed to open raw file for reading: " + raw_file_path);
  }

  struct stat st;
  if (fstat(fd, &st) < 0) {
    close(fd);
    return absl::ErrnoToStatus(errno,
                               "Failed to stat raw file: " + raw_file_path);
  }
  int64_t raw_size = st.st_size;

  int64_t offset = 0;
  uint8_t buffer[4096];
  while (true) {
    ssize_t res = read(fd, buffer, sizeof(buffer));
    if (res < 0) {
      if (errno == EINTR) continue;
      close(fd);
      return absl::ErrnoToStatus(
          errno, "Failed to read from raw file: " + raw_file_path);
    }
    if (res == 0) {
      break;
    }

    absl::Status write_status = mapper.Write(buffer, res, offset);
    if (!write_status.ok()) {
      close(fd);
      return write_status;
    }
    offset += res;
  }
  close(fd);

  absl::Status truncate_status = mapper.Truncate(raw_size);
  if (!truncate_status.ok()) {
    return truncate_status;
  }

  return mapper.Sync();
}

}  // namespace sqlite
