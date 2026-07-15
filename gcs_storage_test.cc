#include "gcs_storage.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "google/cloud/storage/internal/async/token_impl.h"
#include "google/cloud/storage/mocks/mock_async_connection.h"
#include "google/cloud/storage/mocks/mock_async_object_descriptor_connection.h"
#include "google/cloud/storage/mocks/mock_async_reader_connection.h"
#include "google/cloud/storage/mocks/mock_async_writer_connection.h"
#include "gtest/gtest.h"

namespace sqlite {
namespace {

TEST(GcsRapidStorageTest, PReadSuccess) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();
  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));

  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  EXPECT_CALL(*mock_desc_conn, metadata())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke([]() {
        google::storage::v2::Object obj;
        obj.set_size(100);
        return obj;
      }));

  EXPECT_CALL(*mock_conn, options())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Return(google::cloud::Options{}));

  EXPECT_CALL(*mock_conn, Open(::testing::_))
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke(
          [mock_desc_conn](
              google::cloud::storage::AsyncConnection::OpenParams p) {
            return google::cloud::make_ready_future(
                google::cloud::StatusOr<std::shared_ptr<
                    google::cloud::storage::ObjectDescriptorConnection>>(
                    mock_desc_conn));
          }));

  auto* raw_desc_conn = mock_desc_conn.get();
  EXPECT_CALL(
      *raw_desc_conn,
      Read(::testing::Field(&google::cloud::storage::
                                ObjectDescriptorConnection::ReadParams::start,
                            10)))
      .WillOnce(::testing::Invoke([](google::cloud::storage::
                                         ObjectDescriptorConnection::ReadParams
                                             p) {
        EXPECT_EQ(p.length, 5);
        auto mock_reader_conn = std::make_unique<
            google::cloud::storage_mocks::MockAsyncReaderConnection>();
        auto* raw_reader_conn = mock_reader_conn.get();

        std::string response_data = "hello";
        google::cloud::storage::ReadPayload payload(
            std::vector<std::string>{response_data});

        EXPECT_CALL(*raw_reader_conn, Read())
            .WillOnce(::testing::InvokeWithoutArgs([payload = std::move(
                                                        payload)]() mutable {
              return google::cloud::make_ready_future(
                  google::cloud::storage::AsyncReaderConnection::ReadResponse(
                      std::move(payload)));
            }))
            .WillOnce(::testing::InvokeWithoutArgs([]() {
              return google::cloud::make_ready_future(
                  google::cloud::storage::AsyncReaderConnection::ReadResponse(
                      google::cloud::Status{}));
            }));

        return mock_reader_conn;
      }));
  google::cloud::storage::ObjectDescriptor descriptor(mock_desc_conn);

  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::move(descriptor), 100);

  uint8_t buf[5] = {0};
  auto res = storage.PRead(buf, 5, 10);
  ASSERT_TRUE(res.ok());
  EXPECT_EQ(res.value(), 5);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 5), "hello");
}

TEST(GcsRapidStorageTest, AppendAndSyncSuccess) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();

  EXPECT_CALL(*mock_writer_conn, Write(::testing::_))
      .WillOnce(
          ::testing::Invoke([](google::cloud::storage::WritePayload payload) {
            return google::cloud::make_ready_future(google::cloud::Status{});
          }));

  EXPECT_CALL(*mock_writer_conn, Flush(::testing::_))
      .Times(::testing::AnyNumber())
      .WillRepeatedly(
          ::testing::Invoke([](google::cloud::storage::WritePayload) {
            return google::cloud::make_ready_future(google::cloud::Status{});
          }));

  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));
  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  EXPECT_CALL(*mock_desc_conn, metadata())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke([]() {
        google::storage::v2::Object obj;
        obj.set_size(0);
        return obj;
      }));

  google::cloud::storage::ObjectDescriptor descriptor(mock_desc_conn);

  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::move(descriptor), 0);

  std::string data = "hello";
  auto append_res = storage.Append(
      reinterpret_cast<const uint8_t*>(data.data()), data.size());
  ASSERT_TRUE(append_res.ok());
  EXPECT_EQ(append_res.value(), 0);

  auto size_res = storage.GetSize();
  ASSERT_TRUE(size_res.ok());
  EXPECT_EQ(size_res.value(), 5);

  auto sync_res = storage.Sync();
  EXPECT_TRUE(sync_res.ok());
}

TEST(GcsRapidStorageTest, PReadOutOfBounds) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();
  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));
  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  google::cloud::storage::ObjectDescriptor descriptor(mock_desc_conn);

  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::move(descriptor), 10);

  uint8_t buf[5] = {0};
  auto res = storage.PRead(buf, 5, 8);  // 8 + 5 = 13 > 10
  ASSERT_FALSE(res.ok());
  EXPECT_EQ(res.status().code(), absl::StatusCode::kOutOfRange);
}

TEST(GcsRapidStorageTest, PReadRetryThenSuccess) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();
  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));
  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  EXPECT_CALL(*mock_desc_conn, metadata())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke([]() {
        google::storage::v2::Object obj;
        obj.set_size(100);
        return obj;
      }));

  EXPECT_CALL(*mock_conn, options())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Return(google::cloud::Options{}));

  EXPECT_CALL(*mock_conn, Open(::testing::_))
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke(
          [mock_desc_conn](
              google::cloud::storage::AsyncConnection::OpenParams p) {
            return google::cloud::make_ready_future(
                google::cloud::StatusOr<std::shared_ptr<
                    google::cloud::storage::ObjectDescriptorConnection>>(
                    mock_desc_conn));
          }));

  auto* raw_desc_conn = mock_desc_conn.get();
  EXPECT_CALL(
      *raw_desc_conn,
      Read(::testing::Field(&google::cloud::storage::
                                ObjectDescriptorConnection::ReadParams::start,
                            10)))
      .WillOnce(::testing::Invoke(
          [](google::cloud::storage::ObjectDescriptorConnection::ReadParams p) {
            auto mock_reader_conn = std::make_unique<
                google::cloud::storage_mocks::MockAsyncReaderConnection>();
            EXPECT_CALL(*mock_reader_conn, Read())
                .WillOnce(::testing::InvokeWithoutArgs([]() {
                  return google::cloud::make_ready_future(
                      google::cloud::storage::AsyncReaderConnection::
                          ReadResponse(google::cloud::Status(
                              google::cloud::StatusCode::kOutOfRange,
                              "Out of range")));
                }));
            return mock_reader_conn;
          }))
      .WillOnce(::testing::Invoke([](google::cloud::storage::
                                         ObjectDescriptorConnection::ReadParams
                                             p) {
        auto mock_reader_conn = std::make_unique<
            google::cloud::storage_mocks::MockAsyncReaderConnection>();
        auto* raw_reader_conn = mock_reader_conn.get();

        std::string response_data = "hello";
        google::cloud::storage::ReadPayload payload(
            std::vector<std::string>{response_data});

        EXPECT_CALL(*raw_reader_conn, Read())
            .WillOnce(::testing::InvokeWithoutArgs([payload = std::move(
                                                        payload)]() mutable {
              return google::cloud::make_ready_future(
                  google::cloud::storage::AsyncReaderConnection::ReadResponse(
                      std::move(payload)));
            }))
            .WillOnce(::testing::InvokeWithoutArgs([]() {
              return google::cloud::make_ready_future(
                  google::cloud::storage::AsyncReaderConnection::ReadResponse(
                      google::cloud::Status{}));
            }));

        return mock_reader_conn;
      }));

  google::cloud::storage::ObjectDescriptor descriptor(mock_desc_conn);

  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::move(descriptor), 100);

  uint8_t buf[5] = {0};
  auto res = storage.PRead(buf, 5, 10);
  ASSERT_TRUE(res.ok());
  EXPECT_EQ(res.value(), 5);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 5), "hello");
}

TEST(GcsRapidStorageTest, PReadRetryFailure) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();
  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));
  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  EXPECT_CALL(*mock_desc_conn, metadata())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke([]() {
        google::storage::v2::Object obj;
        obj.set_size(100);
        return obj;
      }));

  EXPECT_CALL(*mock_conn, options())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Return(google::cloud::Options{}));

  EXPECT_CALL(*mock_conn, Open(::testing::_))
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke(
          [mock_desc_conn](
              google::cloud::storage::AsyncConnection::OpenParams p) {
            return google::cloud::make_ready_future(
                google::cloud::StatusOr<std::shared_ptr<
                    google::cloud::storage::ObjectDescriptorConnection>>(
                    mock_desc_conn));
          }));

  auto* raw_desc_conn = mock_desc_conn.get();
  EXPECT_CALL(
      *raw_desc_conn,
      Read(::testing::Field(&google::cloud::storage::
                                ObjectDescriptorConnection::ReadParams::start,
                            10)))
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke(
          [](google::cloud::storage::ObjectDescriptorConnection::ReadParams p) {
            auto mock_reader_conn = std::make_unique<
                google::cloud::storage_mocks::MockAsyncReaderConnection>();
            EXPECT_CALL(*mock_reader_conn, Read())
                .WillOnce(::testing::InvokeWithoutArgs([]() {
                  return google::cloud::make_ready_future(
                      google::cloud::storage::AsyncReaderConnection::
                          ReadResponse(google::cloud::Status(
                              google::cloud::StatusCode::kOutOfRange,
                              "Out of range")));
                }));
            return mock_reader_conn;
          }));

  google::cloud::storage::ObjectDescriptor descriptor(mock_desc_conn);

  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::move(descriptor), 100);

  uint8_t buf[5] = {0};
  auto res = storage.PRead(buf, 5, 10);
  ASSERT_FALSE(res.ok());
  EXPECT_EQ(res.status().code(), absl::StatusCode::kDeadlineExceeded);
}

TEST(GcsRapidStorageTest, SyncWriteFailure) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();

  EXPECT_CALL(*mock_writer_conn, Write(::testing::_))
      .WillOnce(
          ::testing::Invoke([](google::cloud::storage::WritePayload payload) {
            return google::cloud::make_ready_future(google::cloud::Status(
                google::cloud::StatusCode::kInvalidArgument, "Write failed"));
          }));

  EXPECT_CALL(*mock_writer_conn, Flush(::testing::_))
      .Times(::testing::AnyNumber())
      .WillRepeatedly(
          ::testing::Invoke([](google::cloud::storage::WritePayload) {
            return google::cloud::make_ready_future(google::cloud::Status{});
          }));

  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));
  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  google::cloud::storage::ObjectDescriptor descriptor(mock_desc_conn);

  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::move(descriptor), 0);

  std::string data = "hello";
  auto append_res = storage.Append(
      reinterpret_cast<const uint8_t*>(data.data()), data.size());
  ASSERT_TRUE(append_res.ok());

  auto sync_res = storage.Sync();
  ASSERT_FALSE(sync_res.ok());
  EXPECT_EQ(sync_res.code(), absl::StatusCode::kInvalidArgument);
}

TEST(GcsRapidStorageTest, SyncFlushFailure) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();

  EXPECT_CALL(*mock_writer_conn, Write(::testing::_))
      .WillOnce(
          ::testing::Invoke([](google::cloud::storage::WritePayload payload) {
            return google::cloud::make_ready_future(google::cloud::Status{});
          }));

  EXPECT_CALL(*mock_writer_conn, Flush(::testing::_))
      .WillOnce(::testing::Invoke([](google::cloud::storage::WritePayload) {
        return google::cloud::make_ready_future(google::cloud::Status(
            google::cloud::StatusCode::kInternal, "Flush failed"));
      }))
      .WillRepeatedly(
          ::testing::Invoke([](google::cloud::storage::WritePayload) {
            return google::cloud::make_ready_future(google::cloud::Status{});
          }));

  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));
  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  google::cloud::storage::ObjectDescriptor descriptor(mock_desc_conn);

  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::move(descriptor), 0);

  std::string data = "hello";
  auto append_res = storage.Append(
      reinterpret_cast<const uint8_t*>(data.data()), data.size());
  ASSERT_TRUE(append_res.ok());

  auto sync_res = storage.Sync();
  ASSERT_FALSE(sync_res.ok());
  EXPECT_EQ(sync_res.code(), absl::StatusCode::kInternal);
}

TEST(GcsRapidStorageTest, PReadFromNonExistentObjectThenCreated) {
  auto mock_conn =
      std::make_shared<google::cloud::storage_mocks::MockAsyncConnection>();
  auto async_client =
      std::make_shared<google::cloud::storage::AsyncClient>(mock_conn);

  auto mock_writer_conn = std::make_unique<
      google::cloud::storage_mocks::MockAsyncWriterConnection>();
  auto* raw_writer_conn = mock_writer_conn.get();

  EXPECT_CALL(*mock_writer_conn, Write(::testing::_))
      .WillOnce(
          ::testing::Invoke([](google::cloud::storage::WritePayload payload) {
            return google::cloud::make_ready_future(google::cloud::Status{});
          }));

  EXPECT_CALL(*mock_writer_conn, Flush(::testing::_))
      .Times(::testing::AnyNumber())
      .WillRepeatedly(
          ::testing::Invoke([](google::cloud::storage::WritePayload) {
            return google::cloud::make_ready_future(google::cloud::Status{});
          }));

  google::cloud::storage::AsyncWriter writer(std::move(mock_writer_conn));
  google::cloud::storage::AsyncToken token =
      google::cloud::storage_internal::MakeAsyncToken(raw_writer_conn);

  auto mock_desc_conn = std::make_shared<
      google::cloud::storage_mocks::MockAsyncObjectDescriptorConnection>();
  EXPECT_CALL(*mock_desc_conn, metadata())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Invoke([]() {
        google::storage::v2::Object obj;
        obj.set_size(5);
        return obj;
      }));

  EXPECT_CALL(*mock_conn, options())
      .Times(::testing::AnyNumber())
      .WillRepeatedly(::testing::Return(google::cloud::Options{}));

  EXPECT_CALL(*mock_conn, Open(::testing::_))
      .WillOnce(::testing::Invoke(
          [mock_desc_conn](
              google::cloud::storage::AsyncConnection::OpenParams p) {
            return google::cloud::make_ready_future(
                google::cloud::StatusOr<std::shared_ptr<
                    google::cloud::storage::ObjectDescriptorConnection>>(
                    mock_desc_conn));
          }));

  auto* raw_desc_conn = mock_desc_conn.get();
  EXPECT_CALL(
      *raw_desc_conn,
      Read(::testing::Field(&google::cloud::storage::
                                ObjectDescriptorConnection::ReadParams::start,
                            0)))
      .WillOnce(::testing::Invoke([](google::cloud::storage::
                                         ObjectDescriptorConnection::ReadParams
                                             p) {
        EXPECT_EQ(p.length, 5);
        auto mock_reader_conn = std::make_unique<
            google::cloud::storage_mocks::MockAsyncReaderConnection>();
        auto* raw_reader_conn = mock_reader_conn.get();

        std::string response_data = "hello";
        google::cloud::storage::ReadPayload payload(
            std::vector<std::string>{response_data});

        EXPECT_CALL(*raw_reader_conn, Read())
            .WillOnce(::testing::InvokeWithoutArgs([payload = std::move(
                                                        payload)]() mutable {
              return google::cloud::make_ready_future(
                  google::cloud::storage::AsyncReaderConnection::ReadResponse(
                      std::move(payload)));
            }))
            .WillOnce(::testing::InvokeWithoutArgs([]() {
              return google::cloud::make_ready_future(
                  google::cloud::storage::AsyncReaderConnection::ReadResponse(
                      google::cloud::Status{}));
            }));

        return mock_reader_conn;
      }));

  // Create GcsRapidStorage with std::nullopt descriptor (as if it was a new object)
  GcsRapidStorage storage(async_client, "my-bucket", "my-object",
                          std::move(writer), std::move(token),
                          std::nullopt, 0);

  // 1. Trying to read before writing should return OutOfRange (empty file)
  uint8_t buf[5] = {0};
  auto pre_read_res = storage.PRead(buf, 5, 0);
  ASSERT_FALSE(pre_read_res.ok());
  EXPECT_EQ(pre_read_res.status().code(), absl::StatusCode::kOutOfRange);

  // 2. Append data and Sync
  std::string data = "hello";
  auto append_res = storage.Append(
      reinterpret_cast<const uint8_t*>(data.data()), data.size());
  ASSERT_TRUE(append_res.ok());
  EXPECT_EQ(append_res.value(), 0);

  auto sync_res = storage.Sync();
  EXPECT_TRUE(sync_res.ok());

  // 3. PRead should now trigger the dynamic Open and read from mock GCS
  auto read_res = storage.PRead(buf, 5, 0);
  ASSERT_TRUE(read_res.ok());
  EXPECT_EQ(read_res.value(), 5);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 5), "hello");
}

}  // namespace
}  // namespace sqlite
