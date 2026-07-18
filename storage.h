#ifndef STORAGE_H_
#define STORAGE_H_

#include <cstddef>
#include <cstdint>
#include <future>
#include <type_traits>

#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace sqlite {

class AppendOnlyStorage {
 public:
  virtual ~AppendOnlyStorage() = default;

  virtual absl::StatusOr<int64_t> Append(const uint8_t* data, size_t size) = 0;
  virtual absl::StatusOr<size_t> PRead(uint8_t* buf, size_t size,
                                       int64_t offset) = 0;
  virtual absl::StatusOr<int64_t> GetSize() = 0;
  virtual absl::Status Sync() = 0;
  virtual absl::Status Synchronize() = 0;
};

}  // namespace sqlite

#endif  // STORAGE_H_
