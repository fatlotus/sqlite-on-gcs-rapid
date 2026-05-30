#ifndef GCS_CLIENT_MOCK_H_
#define GCS_CLIENT_MOCK_H_

#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <future>
#include <cstring>
#include <algorithm>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>

#include "storage.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"

namespace google {
namespace cloud {

template <typename T>
using future = ::sqlite::Future<T>;

using Status = ::absl::Status;

template <typename T>
using StatusOr = ::absl::StatusOr<T>;

namespace storage {

class BucketName {
 public:
  explicit BucketName(std::string name) : name_(std::move(name)) {}
  const std::string& name() const { return name_; }
 private:
  std::string name_;
};

class WritePayload {
 public:
  WritePayload() = default;
  explicit WritePayload(std::string data) : data_(std::move(data)) {}
  explicit WritePayload(std::vector<uint8_t> data) : data_(data.begin(), data.end()) {}
  
  const std::string& data() const { return data_; }
 private:
  std::string data_;
};

class AppendObjectToken {
 public:
  AppendObjectToken() = default;
  explicit AppendObjectToken(std::string token) : token_(std::move(token)) {}
  
  const std::string& token() const { return token_; }
 private:
  std::string token_;
};

struct MockObjectData {
  std::mutex mutex;
  std::vector<uint8_t> data;
};

inline std::unordered_map<std::string, std::shared_ptr<MockObjectData>>& GetMockRegistry() {
  static auto* registry = new std::unordered_map<std::string, std::shared_ptr<MockObjectData>>();
  return *registry;
}

inline std::mutex& GetRegistryMutex() {
  static std::mutex registry_mutex;
  return registry_mutex;
}

class AsyncObjectWriter {
 public:
  AsyncObjectWriter() = default;
  AsyncObjectWriter(std::string bucket, std::string object)
      : bucket_(std::move(bucket)), object_(std::move(object)) {}
  
  AsyncObjectWriter(const AsyncObjectWriter&) = default;
  AsyncObjectWriter& operator=(const AsyncObjectWriter&) = default;
  AsyncObjectWriter(AsyncObjectWriter&&) noexcept = default;
  AsyncObjectWriter& operator=(AsyncObjectWriter&&) noexcept = default;

  google::cloud::future<google::cloud::StatusOr<AppendObjectToken>> Write(
      AppendObjectToken token, WritePayload payload) {
    auto bucket = bucket_;
    auto object = object_;
    auto task = [bucket = std::move(bucket), object = std::move(object), token = std::move(token), payload = std::move(payload)]() -> google::cloud::StatusOr<AppendObjectToken> {
      auto registry_key = bucket + "/" + object;
      std::shared_ptr<MockObjectData> obj_data;
      {
        std::lock_guard<std::mutex> lock(GetRegistryMutex());
        obj_data = GetMockRegistry()[registry_key];
        if (!obj_data) {
          obj_data = std::make_shared<MockObjectData>();
          GetMockRegistry()[registry_key] = obj_data;
        }
      }
      std::lock_guard<std::mutex> lock(obj_data->mutex);
      const auto& bytes = payload.data();
      obj_data->data.insert(obj_data->data.end(), bytes.begin(), bytes.end());
      return AppendObjectToken(std::to_string(obj_data->data.size()));
    };

    std::promise<google::cloud::StatusOr<AppendObjectToken>> prom;
    prom.set_value(task());
    return google::cloud::future<google::cloud::StatusOr<AppendObjectToken>>(prom.get_future());
  }

  google::cloud::future<google::cloud::StatusOr<std::string>> Finalize(AppendObjectToken token) {
    std::promise<google::cloud::StatusOr<std::string>> prom;
    prom.set_value(std::string("finalized"));
    return google::cloud::future<google::cloud::StatusOr<std::string>>(prom.get_future());
  }

 private:
  std::string bucket_;
  std::string object_;
};

class AsyncClient {
 public:
  AsyncClient() = default;

  google::cloud::future<google::cloud::StatusOr<std::pair<AsyncObjectWriter, AppendObjectToken>>>
  StartAppendableObjectUpload(BucketName bucket, std::string object_name) {
    auto bucket_name = bucket.name();
    auto task = [bucket_name, object_name = std::move(object_name)]() -> google::cloud::StatusOr<std::pair<AsyncObjectWriter, AppendObjectToken>> {
      auto registry_key = bucket_name + "/" + object_name;
      std::shared_ptr<MockObjectData> obj_data;
      {
        std::lock_guard<std::mutex> lock(GetRegistryMutex());
        obj_data = GetMockRegistry()[registry_key];
        if (!obj_data) {
          obj_data = std::make_shared<MockObjectData>();
          GetMockRegistry()[registry_key] = obj_data;
        }
      }
      std::lock_guard<std::mutex> lock(obj_data->mutex);

      const char* use_real_gcs = std::getenv("USE_REAL_GCS");
      if (use_real_gcs != nullptr) {
        std::string local_path = "/tmp/gcs_cache_" + bucket_name + "_" + object_name;
        for (size_t i = 5; i < local_path.size(); ++i) {
          if (local_path[i] == '/') {
            local_path[i] = '_';
          }
        }
        std::string cmd = "gcloud storage cp gs://" + bucket_name + "/" + object_name + " " + local_path;
        int status = std::system(cmd.c_str());
        if (status == 0) {
          int fd = open(local_path.c_str(), O_RDONLY);
          if (fd >= 0) {
            struct stat st;
            if (fstat(fd, &st) == 0) {
              obj_data->data.resize(st.st_size);
              ssize_t bytes_read = read(fd, obj_data->data.data(), st.st_size);
              if (bytes_read >= 0) {
                obj_data->data.resize(bytes_read);
              } else {
                obj_data->data.clear();
              }
            }
            close(fd);
          }
        }
      }

      std::string token_val = std::to_string(obj_data->data.size());
      AsyncObjectWriter writer(bucket_name, object_name);
      return std::make_pair(writer, AppendObjectToken(token_val));
    };

    std::promise<google::cloud::StatusOr<std::pair<AsyncObjectWriter, AppendObjectToken>>> prom;
    prom.set_value(task());
    return google::cloud::future<google::cloud::StatusOr<std::pair<AsyncObjectWriter, AppendObjectToken>>>(prom.get_future());
  }

  google::cloud::future<google::cloud::StatusOr<std::pair<AsyncObjectWriter, AppendObjectToken>>>
  StartAppendableObjectUpload(std::string bucket_name, std::string object_name) {
    return StartAppendableObjectUpload(BucketName(std::move(bucket_name)), std::move(object_name));
  }

  google::cloud::StatusOr<size_t> PRead(
      const std::string& bucket, const std::string& object, uint8_t* buf, size_t size, int64_t offset) {
    auto registry_key = bucket + "/" + object;
    std::shared_ptr<MockObjectData> obj_data;
    {
      std::lock_guard<std::mutex> lock(GetRegistryMutex());
      auto it = GetMockRegistry().find(registry_key);
      if (it == GetMockRegistry().end()) {
        return absl::NotFoundError("Object not found: " + registry_key);
      }
      obj_data = it->second;
    }
    std::lock_guard<std::mutex> lock(obj_data->mutex);
    if (offset < 0) {
      return absl::InvalidArgumentError("Offset cannot be negative");
    }
    if (static_cast<size_t>(offset) >= obj_data->data.size()) {
      return 0;
    }
    size_t to_read = std::min(size, obj_data->data.size() - static_cast<size_t>(offset));
    std::memcpy(buf, obj_data->data.data() + offset, to_read);
    return to_read;
  }

  google::cloud::StatusOr<int64_t> GetSize(const std::string& bucket, const std::string& object) {
    auto registry_key = bucket + "/" + object;
    std::shared_ptr<MockObjectData> obj_data;
    {
      std::lock_guard<std::mutex> lock(GetRegistryMutex());
      auto it = GetMockRegistry().find(registry_key);
      if (it == GetMockRegistry().end()) {
        return absl::NotFoundError("Object not found: " + registry_key);
      }
      obj_data = it->second;
    }
    std::lock_guard<std::mutex> lock(obj_data->mutex);
    return static_cast<int64_t>(obj_data->data.size());
  }
};

} // namespace storage

namespace storage_experimental = storage;

} // namespace cloud
} // namespace google

#endif // GCS_CLIENT_MOCK_H_
