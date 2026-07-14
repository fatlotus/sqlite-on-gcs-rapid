#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <algorithm>

#include "absl/status/status.h"
#include "block_mapper.h"
#include "local_storage.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  // Use a unique temporary file path to prevent collisions.
  char temp_path[] = "/tmp/fuzz_local_writer_XXXXXX";
  int fd = mkstemp(temp_path);
  if (fd < 0) {
    return 0;
  }

  // Write fuzzed input to the temporary file.
  size_t bytes_written = 0;
  while (bytes_written < size) {
    ssize_t res = write(fd, data + bytes_written, size - bytes_written);
    if (res < 0) {
      if (errno == EINTR) {
        continue;
      }
      close(fd);
      unlink(temp_path);
      return 0;
    }
    bytes_written += res;
  }
  close(fd);

  // Run the recovery and operations using BlockMapper.
  {
    auto storage_or = sqlite::LocalStorage::Open(temp_path);
    if (storage_or.ok()) {
      auto mapper = std::make_unique<sqlite::BlockMapper>(std::move(storage_or.value()));
      absl::Status status = mapper->Init();
      if (status.ok()) {
        // If Init succeeded, exercise Read/Write/Truncate.
        if (size >= 32) {
          // Read the last 32 bytes of input to determine fuzzed commands.
          const uint8_t* cmd_data = data + size - 32;
          
          // Command 1: Read
          int64_t read_offset;
          std::memcpy(&read_offset, cmd_data, 8);
          uint32_t read_len;
          std::memcpy(&read_len, cmd_data + 8, 4);

          // Command 2: Write
          int64_t write_offset;
          std::memcpy(&write_offset, cmd_data + 12, 8);
          uint32_t write_len;
          std::memcpy(&write_len, cmd_data + 20, 4);

          // Command 3: Truncate
          int64_t truncate_size;
          std::memcpy(&truncate_size, cmd_data + 24, 8);

          // Execute Read
          if (read_len > 0) {
            // Cap read_len to max 1MB to prevent excessive memory allocation in the fuzzer
            read_len = read_len % (1024 * 1024);
            std::vector<uint8_t> read_buf(read_len);
            (void)mapper->Read(read_buf.data(), read_buf.size(), read_offset);
          }

          // Execute Write
          if (write_len > 0) {
            // Cap write_len to max 1MB
            write_len = write_len % (1024 * 1024);
            std::vector<uint8_t> write_buf(write_len, 0);
            (void)mapper->Write(write_buf.data(), write_buf.size(), write_offset);
          }

          // Execute Truncate
          (void)mapper->Truncate(truncate_size);
        } else {
          // Fallback if input is too small to have command bytes.
          if (mapper->logical_size() > 0) {
            std::vector<uint8_t> read_buf(4096);
            size_t read_len = std::min(static_cast<int64_t>(read_buf.size()), mapper->logical_size());
            (void)mapper->Read(read_buf.data(), read_len, 0);
          }
          uint8_t write_buf[100] = {0};
          (void)mapper->Write(write_buf, sizeof(write_buf), 0);
        }
      }
    }
  }

  // Ensure clean up of the temporary file.
  unlink(temp_path);
  return 0;
}
