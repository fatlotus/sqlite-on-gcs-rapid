#include "block_mapper.h"

#include <algorithm>
#include <cstring>
#include <string>
#include <vector>

#include "metadata.pb.h"

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
  struct RecoveredRecord {
    int64_t offset;
    int64_t block_index;
    uint8_t is_good;
    int64_t new_size; // default to -1 if not metadata/truncate
  };
  std::vector<RecoveredRecord> recovered_records;

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
    int64_t block_index = 0;
    int64_t new_size = -1;

    if (is_good == 1 || is_good == 2) {
      std::memcpy(&block_index, record_buf, sizeof(block_index));
      if (block_index < -1 || block_index >= 1000000) {
        return absl::InternalError("Invalid block index during recovery");
      }

      if (block_index == -1) {
        int64_t proto_size;
        std::memcpy(&proto_size, record_buf + 8, sizeof(proto_size));
        if (proto_size < 0 || proto_size > 4088) {
          return absl::InternalError("Invalid proto size during recovery");
        }
        MetadataBlock metadata;
        if (!metadata.ParseFromArray(record_buf + 16, proto_size)) {
          return absl::InternalError("Failed to parse MetadataBlock during recovery");
        }
        if (metadata.has_new_size()) {
          new_size = metadata.new_size();
          if (new_size < 0 || new_size > 1000000LL * 4096) {
            return absl::InternalError("Invalid size in MetadataBlock during recovery");
          }
        }
      }
    }

    recovered_records.push_back({offset, block_index, is_good, new_size});
    offset += 4105;
  }

  // Determine which records are applied (backward pass)
  std::vector<bool> applied(recovered_records.size(), false);
  bool applied_next = false;
  for (int64_t i = static_cast<int64_t>(recovered_records.size()) - 1; i >= 0; --i) {
    if (recovered_records[i].is_good == 1) {
      applied_next = true;
    } else if (recovered_records[i].is_good == 0) {
      applied_next = false;
    } else if (recovered_records[i].is_good == 2) {
      // remains applied_next
    } else {
      applied_next = false; // treat unknown states as abort
    }
    applied[i] = applied_next;
  }

  // Apply the records forward to reconstruct logical state and mappings
  for (size_t i = 0; i < recovered_records.size(); ++i) {
    if (!applied[i]) {
      continue;
    }
    const auto& rec = recovered_records[i];
    if (rec.block_index >= 0) {
      if (rec.block_index >= static_cast<int64_t>(logical_to_physical_.size())) {
        logical_to_physical_.resize(rec.block_index + 1, -1);
      }
      logical_to_physical_[rec.block_index] = rec.offset + 8;
      logical_size_ = std::max(logical_size_, (rec.block_index + 1) * 4096);
    } else if (rec.block_index == -1) {
      if (rec.new_size != -1) {
        logical_size_ = rec.new_size;
        int64_t ceil_blocks = (rec.new_size + 4095) / 4096;
        logical_to_physical_.resize(ceil_blocks, -1);
      }
    }
  }

  initialized_ = true;
  return absl::OkStatus();
}

absl::Status BlockMapper::Read(uint8_t* buf, size_t size,
                               int64_t logical_offset) {
  if (logical_offset < 0 || logical_offset > 1000000LL * 4096 ||
      size > 1000000LL * 4096 - logical_offset) {
    return absl::InvalidArgumentError("Read offset/size is out of range");
  }
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
    bool found_in_pending = false;
    const uint8_t* pending_payload = nullptr;

    for (auto it = pending_records_.rbegin(); it != pending_records_.rend(); ++it) {
      if (it->block_index == block_index) {
        found_in_pending = true;
        pending_payload = it->payload;
        break;
      }
    }

    if (found_in_pending) {
      std::memcpy(buf + buf_offset, pending_payload + block_offset, block_len);
    } else {
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
    }

    remaining_size -= block_len;
    current_offset += block_len;
    buf_offset += block_len;
  }

  return absl::OkStatus();
}

absl::Status BlockMapper::Write(const uint8_t* buf, size_t size,
                                int64_t logical_offset) {
  if (logical_offset < 0 || logical_offset > 1000000LL * 4096 ||
      size > 1000000LL * 4096 - logical_offset) {
    return absl::InvalidArgumentError("Write offset/size is out of range");
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

    // Add to pending records
    PendingRecord pr;
    pr.block_index = b;
    std::memcpy(pr.payload, block_data, 4096);
    pending_records_.push_back(pr);
  }

  logical_size_ =
      std::max(logical_size_, logical_offset + static_cast<int64_t>(size));
  return absl::OkStatus();
}

absl::Status BlockMapper::Truncate(int64_t new_size) {
  if (new_size < 0 || new_size > 1000000LL * 4096) {
    return absl::InvalidArgumentError("new_size cannot be negative or exceed 4GB");
  }
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (!initialized_) {
    return absl::FailedPreconditionError("BlockMapper not initialized");
  }

  MetadataBlock metadata;
  metadata.set_new_size(new_size);

  std::string serialized;
  if (!metadata.SerializeToString(&serialized)) {
    return absl::InternalError("Failed to serialize MetadataBlock proto");
  }
  if (serialized.size() > 4088) {
    return absl::InternalError("Serialized MetadataBlock exceeds maximum metadata payload size");
  }

  // Construct a pending truncate record
  PendingRecord pr;
  pr.block_index = -1;
  
  int64_t proto_size = serialized.size();
  std::memcpy(pr.payload, &proto_size, sizeof(proto_size));
  std::memcpy(pr.payload + 8, serialized.data(), proto_size);
  std::memset(pr.payload + 8 + proto_size, 0, 4088 - proto_size);

  pending_records_.push_back(pr);

  // Update mappings and logical_size_
  logical_size_ = new_size;
  int64_t ceil_blocks = (new_size + 4095) / 4096;

  // Prune any pending records for blocks that are now out of bounds
  pending_records_.erase(
      std::remove_if(pending_records_.begin(), pending_records_.end(),
                     [ceil_blocks](const PendingRecord& r) {
                       return r.block_index >= ceil_blocks;
                     }),
      pending_records_.end());

  logical_to_physical_.resize(ceil_blocks, -1);

  return absl::OkStatus();
}

absl::Status BlockMapper::Sync() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  if (!initialized_) {
    return absl::FailedPreconditionError("BlockMapper not initialized");
  }

  if (pending_records_.empty()) {
    return storage_->Sync();
  }

  // Write all pending records sequentially
  for (size_t i = 0; i < pending_records_.size(); ++i) {
    const auto& pr = pending_records_[i];

    uint8_t record_buf[4105];
    std::memcpy(record_buf, &pr.block_index, sizeof(int64_t));
    std::memcpy(record_buf + 8, pr.payload, 4096);
    // Write state 2 for non-last blocks, and state 1 for the last block
    record_buf[4104] = (i == pending_records_.size() - 1) ? 1 : 2;

    auto append_res = storage_->Append(record_buf, 4105);
    if (!append_res.ok()) {
      return append_res.status();
    }
    int64_t physical_offset = append_res.value();

    if (pr.block_index >= 0) {
      if (pr.block_index >= static_cast<int64_t>(logical_to_physical_.size())) {
        logical_to_physical_.resize(pr.block_index + 1, -1);
      }
      logical_to_physical_[pr.block_index] = physical_offset + 8;
    }
  }

  pending_records_.clear();
  return storage_->Sync();
}

}  // namespace sqlite
