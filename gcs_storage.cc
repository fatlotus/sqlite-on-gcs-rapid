#include "gcs_storage.h"

#ifdef USE_REAL_GCS_SDK

#include <utility>
#include <vector>
#include <future>
#include <iostream>

namespace sqlite {
namespace {

Future<absl::StatusOr<int64_t>> MakeCompletedFuture(absl::StatusOr<int64_t> val) {
  std::promise<absl::StatusOr<int64_t>> prom;
  prom.set_value(val);
  return Future<absl::StatusOr<int64_t>>(prom.get_future());
}

absl::Status ConvertStatus(const google::cloud::Status& s) {
  if (s.ok()) return absl::OkStatus();
  absl::StatusCode code;
  switch (s.code()) {
    case google::cloud::StatusCode::kCancelled: code = absl::StatusCode::kCancelled; break;
    case google::cloud::StatusCode::kInvalidArgument: code = absl::StatusCode::kInvalidArgument; break;
    case google::cloud::StatusCode::kDeadlineExceeded: code = absl::StatusCode::kDeadlineExceeded; break;
    case google::cloud::StatusCode::kNotFound: code = absl::StatusCode::kNotFound; break;
    case google::cloud::StatusCode::kAlreadyExists: code = absl::StatusCode::kAlreadyExists; break;
    case google::cloud::StatusCode::kPermissionDenied: code = absl::StatusCode::kPermissionDenied; break;
    case google::cloud::StatusCode::kResourceExhausted: code = absl::StatusCode::kResourceExhausted; break;
    case google::cloud::StatusCode::kFailedPrecondition: code = absl::StatusCode::kFailedPrecondition; break;
    case google::cloud::StatusCode::kAborted: code = absl::StatusCode::kAborted; break;
    case google::cloud::StatusCode::kOutOfRange: code = absl::StatusCode::kOutOfRange; break;
    case google::cloud::StatusCode::kUnimplemented: code = absl::StatusCode::kUnimplemented; break;
    case google::cloud::StatusCode::kInternal: code = absl::StatusCode::kInternal; break;
    case google::cloud::StatusCode::kUnavailable: code = absl::StatusCode::kUnavailable; break;
    case google::cloud::StatusCode::kDataLoss: code = absl::StatusCode::kDataLoss; break;
    case google::cloud::StatusCode::kUnauthenticated: code = absl::StatusCode::kUnauthenticated; break;
    default: code = absl::StatusCode::kUnknown; break;
  }
  return absl::Status(code, s.message());
}

} // namespace

GcsRapidStorage::GcsRapidStorage(
    std::shared_ptr<gcs_ex::AsyncClient> async_client,
    gcs::Client client,
    std::string bucket, std::string object,
    gcs_ex::AsyncObjectWriter writer,
    gcs_ex::AppendObjectToken token,
    int64_t initial_offset)
    : async_client_(std::move(async_client)),
      client_(std::move(client)),
      bucket_(std::move(bucket)),
      object_(std::move(object)),
      writer_(std::move(writer)),
      token_(std::move(token)),
      current_offset_(initial_offset),
      last_write_fut_(MakeCompletedFuture(0)) {}

GcsRapidStorage::~GcsRapidStorage() {
  (void)Sync();
  decltype(token_) t;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    t = std::move(token_);
  }
  if (t.valid()) {
    auto fut = writer_.Finalize(std::move(t));
    fut.wait();
    auto res = fut.get();
    if (!res.ok()) {
      std::cerr << "Failed to finalize GCS upload: " << res.status().message() << std::endl;
    } else {
      std::cerr << "Successfully finalized GCS upload." << std::endl;
    }
  }
}

absl::StatusOr<std::unique_ptr<GcsRapidStorage>> GcsRapidStorage::Create(
    const std::string& bucket, const std::string& object) {
  std::cerr << "USING REAL GCS C++ SDK CONNECTION" << std::endl;
  auto async_client = std::make_shared<gcs_ex::AsyncClient>(google::cloud::Options{});
  auto client = gcs::Client(google::cloud::Options{});
  
  // Query metadata to get initial size/offset
  auto metadata = client.GetObjectMetadata(bucket, object);
  int64_t initial_offset = 0;
  if (metadata) {
    initial_offset = static_cast<int64_t>(metadata->size());
  } else if (metadata.status().code() != google::cloud::StatusCode::kNotFound) {
    return ConvertStatus(metadata.status());
  }

  auto future = async_client->StartAppendableObjectUpload(
      gcs::BucketName(bucket), object);
  future.wait();
  auto result_or = future.get();
  if (!result_or.ok()) {
    return ConvertStatus(result_or.status());
  }
  auto& pair = result_or.value();
  return std::unique_ptr<GcsRapidStorage>(new GcsRapidStorage(
      std::move(async_client), std::move(client), bucket, object,
      std::move(pair.first), std::move(pair.second), initial_offset));
}

Future<absl::StatusOr<int64_t>> GcsRapidStorage::AppendAsync(const uint8_t* data, size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto payload = gcs::WritePayload(
      std::vector<uint8_t>(data, data + size));
  
  last_write_fut_ = last_write_fut_.then(
      [this, payload = std::move(payload), size](absl::StatusOr<int64_t> prev_status) mutable -> absl::StatusOr<int64_t> {
        if (!prev_status.ok()) {
          return prev_status.status();
        }
        
        gcs_ex::AppendObjectToken current_token;
        int64_t write_offset = 0;
        {
          std::lock_guard<std::mutex> token_lock(mutex_);
          current_token = std::move(token_);
          write_offset = current_offset_;
        }
        
        auto write_fut = writer_.Write(std::move(current_token), std::move(payload));
        write_fut.wait();
        auto result_or = write_fut.get();
        if (!result_or.ok()) {
          return ConvertStatus(result_or.status());
        }

        {
          std::lock_guard<std::mutex> token_lock(mutex_);
          token_ = std::move(result_or.value());
          current_offset_ += size;
        }
        return write_offset;
      });
      
  return last_write_fut_;
}

absl::Status GcsRapidStorage::Sync() {
  Future<absl::StatusOr<int64_t>> fut;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    fut = last_write_fut_;
  }
  if (fut.valid()) {
    fut.wait();
    auto res = fut.get();
    if (!res.ok()) {
      return res.status();
    }
  }
  return absl::OkStatus();
}

absl::StatusOr<size_t> GcsRapidStorage::PRead(uint8_t* buf, size_t size, int64_t offset) {
  auto stream = client_.ReadObject(bucket_, object_, gcs::ReadRange(offset, offset + size));
  if (!stream.status().ok()) {
    return ConvertStatus(stream.status());
  }
  stream.read(reinterpret_cast<char*>(buf), size);
  if (stream.bad()) {
    return ConvertStatus(stream.status());
  }
  return static_cast<size_t>(stream.gcount());
}

absl::StatusOr<int64_t> GcsRapidStorage::GetSize() {
  auto metadata = client_.GetObjectMetadata(bucket_, object_);
  if (!metadata) {
    if (metadata.status().code() == google::cloud::StatusCode::kNotFound) {
      return 0;
    }
    return ConvertStatus(metadata.status());
  }
  return static_cast<int64_t>(metadata->size());
}

} // namespace sqlite

#else // !USE_REAL_GCS_SDK

#include <utility>
#include <vector>
#include <future>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>

namespace sqlite {
namespace {

Future<absl::StatusOr<int64_t>> MakeCompletedFuture(absl::StatusOr<int64_t> val) {
  std::promise<absl::StatusOr<int64_t>> prom;
  prom.set_value(val);
  return Future<absl::StatusOr<int64_t>>(prom.get_future());
}

} // namespace

GcsRapidStorage::GcsRapidStorage(
    std::shared_ptr<google::cloud::storage::AsyncClient> client,
    std::string bucket, std::string object,
    google::cloud::storage::AsyncObjectWriter writer,
    google::cloud::storage::AppendObjectToken token)
    : client_(std::move(client)),
      bucket_(std::move(bucket)),
      object_(std::move(object)),
      writer_(std::move(writer)),
      token_(std::move(token)),
      last_write_fut_(MakeCompletedFuture(0)) {}

GcsRapidStorage::~GcsRapidStorage() {
  (void)Sync();
  decltype(token_) t;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    t = std::move(token_);
  }
  if (!t.token().empty()) {
    auto fut = writer_.Finalize(std::move(t));
    fut.wait();
    auto res = fut.get();
    if (!res.ok()) {
      std::cerr << "Failed to finalize GCS upload: " << res.status().message() << std::endl;
    } else {
      std::cerr << "Successfully finalized GCS upload." << std::endl;
    }
  }
}

absl::StatusOr<std::unique_ptr<GcsRapidStorage>> GcsRapidStorage::Create(
    std::shared_ptr<google::cloud::storage::AsyncClient> client,
    const std::string& bucket, const std::string& object) {
  std::cerr << "USING MOCK GCS CLIENT IN-MEMORY" << std::endl;
  auto future = client->StartAppendableObjectUpload(bucket, object);
  future.wait();
  auto result_or = future.get();
  if (!result_or.ok()) {
    return result_or.status();
  }
  auto& pair = result_or.value();
  return std::unique_ptr<GcsRapidStorage>(new GcsRapidStorage(
      std::move(client), bucket, object,
      std::move(pair.first), std::move(pair.second)));
}

Future<absl::StatusOr<int64_t>> GcsRapidStorage::AppendAsync(const uint8_t* data, size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto payload = google::cloud::storage::WritePayload(
      std::vector<uint8_t>(data, data + size));
  
  last_write_fut_ = last_write_fut_.then(
      [this, payload = std::move(payload)](absl::StatusOr<int64_t> prev_status) mutable -> absl::StatusOr<int64_t> {
        if (!prev_status.ok()) {
          return prev_status.status();
        }
        
        google::cloud::storage::AppendObjectToken current_token;
        {
          std::lock_guard<std::mutex> token_lock(mutex_);
          current_token = token_;
        }
        
        int64_t physical_offset = 0;
        try {
          physical_offset = std::stoll(current_token.token());
        } catch (...) {
          return absl::InternalError("Failed to parse token as offset");
        }
        
        auto write_fut = writer_.Write(std::move(current_token), std::move(payload));
        write_fut.wait();
        auto result_or = write_fut.get();
        if (!result_or.ok()) {
          return result_or.status();
        }

        {
          std::lock_guard<std::mutex> token_lock(mutex_);
          token_ = std::move(result_or.value());
        }
        return physical_offset;
      });
      
  return last_write_fut_;
}

absl::Status GcsRapidStorage::Sync() {
  Future<absl::StatusOr<int64_t>> fut;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    fut = last_write_fut_;
  }
  if (fut.valid()) {
    fut.wait();
    auto res = fut.get();
    if (!res.ok()) {
      return res.status();
    }
  }

  const char* use_real_gcs = std::getenv("USE_REAL_GCS");
  if (use_real_gcs != nullptr) {
    std::vector<uint8_t> data_copy;
    {
      std::lock_guard<std::mutex> lock(google::cloud::storage::GetRegistryMutex());
      auto key = bucket_ + "/" + object_;
      auto it = google::cloud::storage::GetMockRegistry().find(key);
      if (it != google::cloud::storage::GetMockRegistry().end()) {
        std::lock_guard<std::mutex> obj_lock(it->second->mutex);
        data_copy = it->second->data;
      }
    }

    std::string local_path = "/tmp/gcs_cache_" + bucket_ + "_" + object_;
    for (size_t i = 5; i < local_path.size(); ++i) {
      if (local_path[i] == '/') {
        local_path[i] = '_';
      }
    }

    int fd = open(local_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
      if (!data_copy.empty()) {
        ssize_t bytes_written = write(fd, data_copy.data(), data_copy.size());
        if (bytes_written != static_cast<ssize_t>(data_copy.size())) {
          close(fd);
          return absl::InternalError("Failed to write all bytes to local cache");
        }
      }
      close(fd);
    } else {
      return absl::InternalError("Failed to open local cache file for writing");
    }

    std::string cmd = "gcloud storage cp " + local_path + " gs://" + bucket_ + "/" + object_;
    int status = std::system(cmd.c_str());
    if (status != 0) {
      return absl::InternalError("gcloud storage cp command failed");
    }
  }

  return absl::OkStatus();
}

absl::StatusOr<size_t> GcsRapidStorage::PRead(uint8_t* buf, size_t size, int64_t offset) {
  return client_->PRead(bucket_, object_, buf, size, offset);
}

absl::StatusOr<int64_t> GcsRapidStorage::GetSize() {
  return client_->GetSize(bucket_, object_);
}

} // namespace sqlite

#endif // USE_REAL_GCS_SDK
