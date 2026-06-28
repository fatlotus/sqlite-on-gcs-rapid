#include "gcs_storage.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>
#include <utility>
#include <vector>

#include "absl/log/log.h"
#include "absl/strings/str_format.h"
#include "google/cloud/storage/grpc_plugin.h"

namespace sqlite {
namespace gcs = ::google::cloud::storage;

namespace {

absl::Status ConvertStatus(const google::cloud::Status& s) {
  if (s.ok()) return absl::OkStatus();
  absl::StatusCode code;
  switch (s.code()) {
    case google::cloud::StatusCode::kCancelled:
      code = absl::StatusCode::kCancelled;
      break;
    case google::cloud::StatusCode::kInvalidArgument:
      code = absl::StatusCode::kInvalidArgument;
      break;
    case google::cloud::StatusCode::kDeadlineExceeded:
      code = absl::StatusCode::kDeadlineExceeded;
      break;
    case google::cloud::StatusCode::kNotFound:
      code = absl::StatusCode::kNotFound;
      break;
    case google::cloud::StatusCode::kAlreadyExists:
      code = absl::StatusCode::kAlreadyExists;
      break;
    case google::cloud::StatusCode::kPermissionDenied:
      code = absl::StatusCode::kPermissionDenied;
      break;
    case google::cloud::StatusCode::kResourceExhausted:
      code = absl::StatusCode::kResourceExhausted;
      break;
    case google::cloud::StatusCode::kFailedPrecondition:
      code = absl::StatusCode::kFailedPrecondition;
      break;
    case google::cloud::StatusCode::kAborted:
      code = absl::StatusCode::kAborted;
      break;
    case google::cloud::StatusCode::kOutOfRange:
      code = absl::StatusCode::kOutOfRange;
      break;
    case google::cloud::StatusCode::kUnimplemented:
      code = absl::StatusCode::kUnimplemented;
      break;
    case google::cloud::StatusCode::kInternal:
      code = absl::StatusCode::kInternal;
      break;
    case google::cloud::StatusCode::kUnavailable:
      code = absl::StatusCode::kUnavailable;
      break;
    case google::cloud::StatusCode::kDataLoss:
      code = absl::StatusCode::kDataLoss;
      break;
    case google::cloud::StatusCode::kUnauthenticated:
      code = absl::StatusCode::kUnauthenticated;
      break;
    default:
      code = absl::StatusCode::kUnknown;
      break;
  }
  return absl::Status(code, s.message());
}

absl::StatusOr<std::vector<uint8_t>> ReadDescriptorRange(
    gcs::ObjectDescriptor& descriptor, std::int64_t offset,
    std::int64_t limit) {
  LOG(INFO) << "ReadDescriptorRange start: offset=" << offset
            << ", limit=" << limit;
  auto [reader, token] = descriptor.Read(offset, limit);
  std::vector<uint8_t> data;
  while (token.valid()) {
    auto read_fut = reader.Read(std::move(token));
    read_fut.wait();
    auto res_or = read_fut.get();
    if (!res_or.ok()) {
      LOG(ERROR) << "ReadDescriptorRange failed: " << res_or.status().message();
      return ConvertStatus(res_or.status());
    }
    auto& pair = res_or.value();
    auto const& payload = pair.first;
    token = std::move(pair.second);
    for (absl::string_view sv : payload.contents()) {
      data.insert(data.end(), sv.begin(), sv.end());
    }
  }
  LOG(INFO) << "ReadDescriptorRange success: returned " << data.size()
            << " bytes";
  return data;
}

}  // namespace

GcsRapidStorage::GcsRapidStorage(std::shared_ptr<gcs::AsyncClient> async_client,
                                 std::string bucket, std::string object,
                                 gcs::AsyncWriter writer, gcs::AsyncToken token,
                                 gcs::ObjectDescriptor descriptor,
                                 int64_t initial_offset)
    : async_client_(std::move(async_client)),
      bucket_(std::move(bucket)),
      object_(std::move(object)),
      writer_(std::move(writer)),
      token_(std::move(token)),
      descriptor_(std::move(descriptor)),
      initial_offset_(initial_offset),
      file_length_(initial_offset) {
  LOG(INFO) << "GcsRapidStorage::GcsRapidStorage start: bucket=" << bucket_
            << ", object=" << object_ << ", initial_offset=" << initial_offset_;
  LOG(INFO) << "GcsRapidStorage::GcsRapidStorage end";
}

GcsRapidStorage::~GcsRapidStorage() {
  LOG(INFO) << "GcsRapidStorage::~GcsRapidStorage start";
  (void)Sync();
  bool has_writes = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    has_writes = (file_length_ > initial_offset_);
  }
  if (has_writes) {
    auto close_fut = writer_.Close();
    close_fut.wait();
  }
  LOG(INFO) << "GcsRapidStorage::~GcsRapidStorage end";
}

absl::StatusOr<std::unique_ptr<GcsRapidStorage>> GcsRapidStorage::Create(
    const std::string& bucket, const std::string& object) {
  LOG(INFO) << "GcsRapidStorage::Create start: bucket=" << bucket
            << ", object=" << object;
  auto options =
      google::cloud::Options{}
          .set<google::cloud::storage_experimental::EnableGrpcMetricsOption>(
              false);
  auto async_client = std::make_shared<gcs::AsyncClient>(options);
  LOG(INFO) << "GcsRapidStorage::Create before MakeGrpcClient";
  auto client = gcs::MakeGrpcClient(options);

  // Query metadata to get initial size/offset
  LOG(INFO) << "GcsRapidStorage::Create: calling GetObjectMetadata";
  auto metadata = client.GetObjectMetadata(bucket, object);
  LOG(INFO) << "GcsRapidStorage::Create: GetObjectMetadata returned";
  int64_t initial_offset = 0;
  if (metadata) {
    initial_offset = static_cast<int64_t>(metadata->size());
  } else if (metadata.status().code() != google::cloud::StatusCode::kNotFound) {
    auto status = absl::Status(
        ConvertStatus(metadata.status()).code(),
        absl::StrFormat(
            "Failed to get GCS object metadata for bucket=%s object=%s: %s",
            bucket, object, metadata.status().message()));
    LOG(ERROR) << "GcsRapidStorage::Create end: error=" << status.message();
    return status;
  }

  std::pair<gcs::AsyncWriter, gcs::AsyncToken> pair;
  if (metadata) {
    LOG(INFO)
        << "GcsRapidStorage::Create: calling ResumeAppendableObjectUpload";
    auto future = async_client->ResumeAppendableObjectUpload(
        gcs::BucketName(bucket), object, metadata->generation());
    future.wait();
    auto result_or = future.get();
    LOG(INFO)
        << "GcsRapidStorage::Create: ResumeAppendableObjectUpload returned";
    if (!result_or.ok()) {
      auto status =
          absl::Status(ConvertStatus(result_or.status()).code(),
                       absl::StrFormat("Failed to resume appendable upload for "
                                       "bucket=%s object=%s generation=%d: %s",
                                       bucket, object, metadata->generation(),
                                       result_or.status().message()));
      LOG(ERROR) << "GcsRapidStorage::Create end: error=" << status.message();
      return status;
    }
    pair = std::move(result_or.value());
  } else {
    LOG(INFO) << "GcsRapidStorage::Create: calling StartAppendableObjectUpload";
    auto future = async_client->StartAppendableObjectUpload(
        gcs::BucketName(bucket), object);
    future.wait();
    auto result_or = future.get();
    LOG(INFO)
        << "GcsRapidStorage::Create: StartAppendableObjectUpload returned";
    if (!result_or.ok()) {
      auto status = absl::Status(
          ConvertStatus(result_or.status()).code(),
          absl::StrFormat(
              "Failed to start appendable upload for bucket=%s object=%s: %s",
              bucket, object, result_or.status().message()));
      LOG(ERROR) << "GcsRapidStorage::Create end: error=" << status.message();
      return status;
    }
    pair = std::move(result_or.value());
  }

  // Get the size of the (potentially unfinalized) object
  auto const& state = pair.first.PersistedState();
  if (std::holds_alternative<google::storage::v2::Object>(state)) {
    initial_offset = std::get<google::storage::v2::Object>(state).size();
  } else if (std::holds_alternative<std::int64_t>(state)) {
    initial_offset = std::get<std::int64_t>(state);
  }

  LOG(INFO) << "GcsRapidStorage::Create: calling Open descriptor";
  auto desc_future = async_client->Open(gcs::BucketName(bucket), object);
  desc_future.wait();
  auto desc_or = desc_future.get();
  LOG(INFO) << "GcsRapidStorage::Create: Open descriptor returned";
  if (!desc_or.ok()) {
    auto status = absl::Status(
        ConvertStatus(desc_or.status()).code(),
        absl::StrFormat(
            "Failed to open GCS object descriptor for bucket=%s object=%s: %s",
            bucket, object, desc_or.status().message()));
    LOG(ERROR) << "GcsRapidStorage::Create end: error=" << status.message();
    return status;
  }

  LOG(INFO) << "GcsRapidStorage::Create end: success";
  return std::unique_ptr<GcsRapidStorage>(new GcsRapidStorage(
      std::move(async_client), bucket, object, std::move(pair.first),
      std::move(pair.second), std::move(desc_or.value()), initial_offset));
}

absl::StatusOr<int64_t> GcsRapidStorage::Append(const uint8_t* data,
                                                size_t size) {
  LOG(INFO) << "GcsRapidStorage::Append start: size=" << size;
  std::lock_guard<std::mutex> lock(mutex_);

  int64_t write_offset = file_length_;
  if (size > 0 && data != nullptr) {
    buffer_.insert(buffer_.end(), data, data + size);
    file_length_ += size;
  }

  LOG(INFO) << "GcsRapidStorage::Append end: offset=" << write_offset
            << ", size=" << size;
  return write_offset;
}

absl::Status GcsRapidStorage::Sync() {
  LOG(INFO) << "GcsRapidStorage::Sync start";
  std::lock_guard<std::mutex> lock(mutex_);

  if (buffer_.empty()) {
    LOG(INFO) << "GcsRapidStorage::Sync end: no writes to sync";
    return absl::OkStatus();
  }

  auto payload = gcs::WritePayload(std::move(buffer_));
  buffer_.clear();

  auto write_fut = writer_.Write(std::move(token_), std::move(payload));
  write_fut.wait();
  auto result_or = write_fut.get();
  if (!result_or.ok()) {
    auto status = ConvertStatus(result_or.status());
    LOG(ERROR) << "GcsRapidStorage::Sync end: write failed: "
               << status.message();
    return status;
  }
  token_ = std::move(result_or.value());

  auto flush_fut = writer_.Flush();
  flush_fut.wait();
  auto flush_status = flush_fut.get();
  if (!flush_status.ok()) {
    auto status = ConvertStatus(flush_status);
    LOG(ERROR) << "GcsRapidStorage::Sync end: flush failed: "
               << status.message();
    return status;
  }

  LOG(INFO) << "GcsRapidStorage::Sync end: success";
  return absl::OkStatus();
}

absl::StatusOr<size_t> GcsRapidStorage::PRead(uint8_t* buf, size_t size,
                                              int64_t offset) {
  LOG(INFO) << "GcsRapidStorage::PRead start: offset=" << offset
            << ", size=" << size;
  std::lock_guard<std::mutex> lock(mutex_);

  if (offset < 0) {
    auto status = absl::InvalidArgumentError("Offset cannot be negative");
    LOG(ERROR) << "GcsRapidStorage::PRead end: error=" << status.message();
    return status;
  }

  if (offset + size > file_length_) {
    auto status = absl::OutOfRangeError(absl::StrFormat(
        "Attempted to read past EOF (offset=%d, size=%d, file_length=%d)",
        offset, size, file_length_));
    LOG(ERROR) << "GcsRapidStorage::PRead end: error=" << status.message();
    return status;
  }

  int64_t buffer_start_offset = file_length_ - buffer_.size();

  if (offset >= buffer_start_offset) {
    std::memcpy(buf, buffer_.data() + (offset - buffer_start_offset), size);
    LOG(INFO) << "GcsRapidStorage::PRead end: success (read from buffer)";
    return size;
  }

  size_t gcs_read_size = size;
  size_t buffer_read_size = 0;

  if (offset + size > buffer_start_offset) {
    gcs_read_size = buffer_start_offset - offset;
    buffer_read_size = offset + size - buffer_start_offset;
  }

  int backoff_ms = 10;
  int attempt = 0;
  constexpr int kMaxAttempts = 15;

  while (true) {
    attempt++;
    int64_t current_desc_size = 0;
    auto meta = descriptor_.metadata();
    if (meta.has_value()) {
      current_desc_size = meta->size();
    }

    if (offset + gcs_read_size > current_desc_size || attempt > 1) {
      auto desc_future = async_client_->Open(gcs::BucketName(bucket_), object_);
      desc_future.wait();
      auto desc_or = desc_future.get();
      if (desc_or.ok()) {
        descriptor_ = std::move(desc_or.value());
      }
    }

    auto data_or = ReadDescriptorRange(descriptor_, offset, gcs_read_size);

    bool is_out_of_range = false;
    if (!data_or.ok()) {
      if (data_or.status().code() == absl::StatusCode::kOutOfRange) {
        is_out_of_range = true;
      } else {
        auto status = absl::Status(
            data_or.status().code(),
            absl::StrFormat("GcsRapidStorage::PRead read failed: %s",
                            data_or.status().message()));
        LOG(ERROR) << "GcsRapidStorage::PRead end: error=" << status.message();
        return status;
      }
    } else if (data_or->size() < gcs_read_size) {
      is_out_of_range = true;
    }

    if (is_out_of_range) {
      if (attempt >= kMaxAttempts) {
        auto status = absl::DeadlineExceededError(
            absl::StrFormat("Timed out waiting for GCS object to be readable "
                            "at offset %d size %d",
                            offset, gcs_read_size));
        LOG(ERROR) << "GcsRapidStorage::PRead end: error=" << status.message();
        return status;
      }
      LOG(WARNING)
          << "GcsRapidStorage::PRead: Out of range read, backing off for "
          << backoff_ms << " ms (attempt " << attempt << ")";
      std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
      backoff_ms = std::min(backoff_ms * 2, 2000);
      continue;
    }

    std::memcpy(buf, data_or->data(), gcs_read_size);
    if (buffer_read_size > 0) {
      std::memcpy(buf + gcs_read_size, buffer_.data(), buffer_read_size);
    }
    LOG(INFO) << "GcsRapidStorage::PRead end: success";
    return size;
  }
}

absl::StatusOr<int64_t> GcsRapidStorage::GetSize() {
  LOG(INFO) << "GcsRapidStorage::GetSize start";
  std::lock_guard<std::mutex> lock(mutex_);
  LOG(INFO) << "GcsRapidStorage::GetSize end: size=" << file_length_;
  return file_length_;
}

}  // namespace sqlite
