#include "block_mapper.h"

#include <algorithm>
#include <cstring>
#include <vector>

namespace sqlite {

BlockMapper::BlockMapper(std::unique_ptr<AppendOnlyStorage> storage)
    : storage_(std::move(storage)) {}

absl::Status BlockMapper::Init() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (initialized_) {
    return absl::FailedPreconditionError("BlockMapper already initialized");
  }

  // Check physical file size P
  auto size_or = storage_->GetSize();
  if (!size_or.ok()) {
    return size_or.status();
  }
  int64_t P = size_or.value();

  // Handle alignment padding if needed
  if (P % 4105 != 0) {
    int64_t B = P % 4105;
    int64_t pad_size = 4105 - B;
    std::vector<uint8_t> padding(pad_size, 0);
    // last byte set to 0 (is_good = 0), which is already done since vector is
    // initialized to 0

    auto append_res = storage_->Append(padding.data(), padding.size());
    if (!append_res.ok()) {
      return append_res.status();
    }

    // Get the updated size
    auto new_size_or = storage_->GetSize();
    if (!new_size_or.ok()) {
      return new_size_or.status();
    }
    P = new_size_or.value();
  }

  // Scan storage in 4105-byte records
  int64_t offset = 0;
  while (offset < P) {
    uint8_t record_buf[4105];
    auto read_or = storage_->PRead(record_buf, 4105, offset);
    if (!read_or.ok()) {
      return read_or.status();
    }
    if (read_or.value() != 4105) {
      return absl::InternalError(
          "Corrupt database file or unexpected EOF during recovery");
    }

    uint8_t is_good = record_buf[4104];
    if (is_good == 1) {
      int64_t block_index;
      std::memcpy(&block_index, record_buf, sizeof(block_index));
      if (block_index < -1 || block_index >= 1000000) {
        return absl::InternalError("Invalid block index during recovery");
      }
      if (block_index >= 0) {
        if (block_index >= static_cast<int64_t>(logical_to_physical_.size())) {
          logical_to_physical_.resize(block_index + 1, -1);
        }
        logical_to_physical_[block_index] = offset + 8;
        logical_size_ = std::max(logical_size_, (block_index + 1) * 4096);
      } else if (block_index == -1) {
        int64_t new_size;
        std::memcpy(&new_size, record_buf + 8, sizeof(new_size));
        if (new_size < 0) {
          return absl::InternalError("Invalid size during recovery");
        }
        logical_size_ = new_size;
        int64_t ceil_blocks = (new_size + 4095) / 4096;
        logical_to_physical_.resize(ceil_blocks, -1);
      }
    }
    offset += 4105;
  }

  initialized_ = true;
  return absl::OkStatus();
}

absl::Status BlockMapper::Read(uint8_t* buf, size_t size,
                               int64_t logical_offset) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (!initialized_) {
    return absl::FailedPreconditionError("BlockMapper not initialized");
  }
  return ReadLocked(buf, size, logical_offset);
}

absl::Status BlockMapper::ReadLocked(uint8_t* buf, size_t size,
                                     int64_t logical_offset) const {
  if (logical_offset < 0) {
    return absl::InvalidArgumentError("logical_offset cannot be negative");
  }
  if (size == 0) {
    return absl::OkStatus();
  }

  int64_t remaining_size = size;
  int64_t current_offset = logical_offset;
  size_t buf_offset = 0;

  while (remaining_size > 0) {
    int64_t block_index = current_offset / 4096;
    int64_t block_offset = current_offset % 4096;
    int64_t block_len =
        std::min(static_cast<int64_t>(4096 - block_offset), remaining_size);

    bool mapped = false;
    int64_t physical_base = -1;
    if (block_index >= 0 &&
        block_index < static_cast<int64_t>(logical_to_physical_.size())) {
      physical_base = logical_to_physical_[block_index];
      if (physical_base != -1) {
        mapped = true;
      }
    }

    if (mapped) {
      int64_t physical_offset = physical_base + block_offset;
      auto read_or =
          storage_->PRead(buf + buf_offset, block_len, physical_offset);
      if (!read_or.ok()) {
        return read_or.status();
      }
      if (read_or.value() < static_cast<size_t>(block_len)) {
        return absl::InternalError("Short read from physical storage");
      }
    } else {
      std::memset(buf + buf_offset, 0, block_len);
    }

    remaining_size -= block_len;
    current_offset += block_len;
    buf_offset += block_len;
  }

  return absl::OkStatus();
}

absl::Status BlockMapper::Write(const uint8_t* buf, size_t size,
                                int64_t logical_offset) {
  if (logical_offset < 0) {
    return absl::InvalidArgumentError("logical_offset cannot be negative");
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (!initialized_) {
    return absl::FailedPreconditionError("BlockMapper not initialized");
  }
  if (size == 0) {
    return absl::OkStatus();
  }

  int64_t start_block = logical_offset / 4096;
  int64_t end_block = (logical_offset + size - 1) / 4096;

  for (int64_t b = start_block; b <= end_block; ++b) {
    int64_t block_start_logical_offset = b * 4096;
    int64_t write_start_offset =
        std::max(logical_offset, block_start_logical_offset);
    int64_t write_end_offset =
        std::min(logical_offset + static_cast<int64_t>(size),
                 block_start_logical_offset + 4096);
    int64_t block_len = write_end_offset - write_start_offset;
    int64_t block_off = write_start_offset - block_start_logical_offset;

    uint8_t block_data[4096];
    if (block_len < 4096) {
      // Perform Read-Modify-Write
      absl::Status read_status =
          ReadLocked(block_data, 4096, block_start_logical_offset);
      if (!read_status.ok()) {
        return read_status;
      }
      std::memcpy(block_data + block_off,
                  buf + (write_start_offset - logical_offset), block_len);
    } else {
      std::memcpy(block_data, buf + (write_start_offset - logical_offset),
                  4096);
    }

    // Construct 4105-byte record
    uint8_t record_buf[4105];
    int64_t block_index = b;
    std::memcpy(record_buf, &block_index, sizeof(int64_t));
    std::memcpy(record_buf + 8, block_data, 4096);
    record_buf[4104] = 1;  // is_good = 1

    // Append to storage
    auto append_res = storage_->Append(record_buf, 4105);
    if (!append_res.ok()) {
      return append_res.status();
    }
    int64_t physical_offset = append_res.value();

    // Map block
    if (b >= static_cast<int64_t>(logical_to_physical_.size())) {
      logical_to_physical_.resize(b + 1, -1);
    }
    logical_to_physical_[b] = physical_offset + 8;
  }

  logical_size_ =
      std::max(logical_size_, logical_offset + static_cast<int64_t>(size));
  return absl::OkStatus();
}

absl::Status BlockMapper::Truncate(int64_t new_size) {
  if (new_size < 0) {
    return absl::InvalidArgumentError("new_size cannot be negative");
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (!initialized_) {
    return absl::FailedPreconditionError("BlockMapper not initialized");
  }

  // Construct a truncate record
  uint8_t record_buf[4105];
  int64_t block_index = -1;
  std::memcpy(record_buf, &block_index, sizeof(int64_t));
  std::memcpy(record_buf + 8, &new_size, sizeof(int64_t));
  std::memset(record_buf + 16, 0, 4088);
  record_buf[4104] = 1;  // is_good = 1

  auto append_res = storage_->Append(record_buf, 4105);
  if (!append_res.ok()) {
    return append_res.status();
  }

  // Update mappings and logical_size_
  logical_size_ = new_size;
  int64_t ceil_blocks = (new_size + 4095) / 4096;
  logical_to_physical_.resize(ceil_blocks, -1);

  return absl::OkStatus();
}

absl::Status BlockMapper::Sync() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (!initialized_) {
    return absl::FailedPreconditionError("BlockMapper not initialized");
  }
  return storage_->Sync();
}

}  // namespace sqlite
