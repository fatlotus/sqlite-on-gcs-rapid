#include "gcs_storage.h"

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
