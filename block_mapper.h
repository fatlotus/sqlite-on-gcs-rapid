#ifndef BLOCK_MAPPER_H_
#define BLOCK_MAPPER_H_

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "storage.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <vector>

namespace sqlite {

class BlockMapper {
public:
  explicit BlockMapper(std::unique_ptr<AppendOnlyStorage> storage);
  ~BlockMapper() = default;

  // Disallow copy and assign
  BlockMapper(const BlockMapper &) = delete;
  BlockMapper &operator=(const BlockMapper &) = delete;

  // Perform crash recovery and initialize logical size and mapping.
  absl::Status Init();

  // Basic logical read/write operations.
  absl::Status Read(uint8_t *buf, size_t size, int64_t logical_offset);
  absl::Status Write(const uint8_t *buf, size_t size, int64_t logical_offset);

  // Truncate logical file.
  absl::Status Truncate(int64_t new_size);

  // Sync delegating to storage.
  absl::Status Sync();

  // Helper to get logical size.
  int64_t logical_size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return logical_size_;
  }

  // Helper to check if a block is mapped.
  bool IsBlockMapped(int64_t block_index) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (block_index >= 0 &&
        block_index < static_cast<int64_t>(logical_to_physical_.size())) {
      return logical_to_physical_[block_index] != -1;
    }
    return false;
  }

private:
  absl::Status ReadLocked(uint8_t *buf, size_t size,
                          int64_t logical_offset) const;

  std::unique_ptr<AppendOnlyStorage> storage_;
  mutable std::shared_mutex mutex_;

  // Maps logical block index to its physical offset in storage (offset + 8).
  std::vector<int64_t> logical_to_physical_;

  int64_t logical_size_ = 0;
  bool initialized_ = false;
};

} // namespace sqlite

#endif // BLOCK_MAPPER_H_
