#ifndef STORAGE_H_
#define STORAGE_H_

#include <cstdint>
#include <cstddef>
#include <future>
#include <type_traits>
#include <utility>
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace sqlite {

template <typename T>
class Future {
 public:
  Future() = default;
  Future(std::future<T> fut) : fut_(std::move(fut)) {}
  Future(std::shared_future<T> fut) : fut_(std::move(fut)) {}

  Future(const Future&) = default;
  Future& operator=(const Future&) = default;
  Future(Future&&) noexcept = default;
  Future& operator=(Future&&) noexcept = default;

  const T& get() const {
    return fut_.get();
  }

  void wait() const {
    fut_.wait();
  }

  bool valid() const {
    return fut_.valid();
  }

  template <typename F>
  auto then(F&& func) -> Future<std::invoke_result_t<F, T>> {
    using R = std::invoke_result_t<F, T>;
    auto fut_copy = fut_;
    auto decay_func = std::forward<F>(func);
    
    std::future<R> next_fut = std::async(std::launch::async, [fut_copy, f = std::move(decay_func)]() mutable -> R {
      return f(fut_copy.get());
    });
    
    return Future<R>(std::move(next_fut));
  }

 private:
  std::shared_future<T> fut_;
};

template <>
class Future<void> {
 public:
  Future() = default;
  Future(std::future<void> fut) : fut_(std::move(fut)) {}
  Future(std::shared_future<void> fut) : fut_(std::move(fut)) {}

  Future(const Future&) = default;
  Future& operator=(const Future&) = default;
  Future(Future&&) noexcept = default;
  Future& operator=(Future&&) noexcept = default;

  void get() const {
    fut_.get();
  }

  void wait() const {
    fut_.wait();
  }

  bool valid() const {
    return fut_.valid();
  }

  template <typename F>
  auto then(F&& func) -> Future<std::invoke_result_t<F>> {
    using R = std::invoke_result_t<F>;
    auto fut_copy = fut_;
    auto decay_func = std::forward<F>(func);
    
    std::future<R> next_fut = std::async(std::launch::async, [fut_copy, f = std::move(decay_func)]() mutable -> R {
      fut_copy.get();
      return f();
    });
    
    return Future<R>(std::move(next_fut));
  }

 private:
  std::shared_future<void> fut_;
};

class AppendOnlyStorage {
 public:
  virtual ~AppendOnlyStorage() = default;

  virtual Future<absl::StatusOr<int64_t>> AppendAsync(const uint8_t* data, size_t size) = 0;
  virtual absl::StatusOr<size_t> PRead(uint8_t* buf, size_t size, int64_t offset) = 0;
  virtual absl::StatusOr<int64_t> GetSize() = 0;
  virtual absl::Status Sync() = 0;
};

}  // namespace sqlite

#endif  // STORAGE_H_
