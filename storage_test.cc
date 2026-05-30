#include "storage.h"
#include "local_storage.h"
#include "gcs_storage.h"
#include "gcs_client_mock.h"

#include <cstdlib>
#include <string>
#include <vector>
#include <thread>
#include <unistd.h>

#include "gtest/gtest.h"
#include "absl/status/status.h"

namespace sqlite {
namespace {

std::string GetTestFilePath() {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir != nullptr) {
    return std::string(tmpdir) + "/storage_test.db";
  }
  return "/tmp/storage_test.db";
}

class LocalStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_path_ = GetTestFilePath();
    // Ensure file doesn't exist before test starts.
    unlink(test_path_.c_str());
  }

  void TearDown() override {
    unlink(test_path_.c_str());
  }

  std::string test_path_;
};

TEST_F(LocalStorageTest, OpenNewFile) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());
  EXPECT_NE(storage, nullptr);

  auto size_or = storage->GetSize();
  ASSERT_TRUE(size_or.ok());
  EXPECT_EQ(size_or.value(), 0);
}

TEST_F(LocalStorageTest, BasicAppendAndRead) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  std::string data1 = "Hello, world!";
  auto fut1 = storage->AppendAsync(reinterpret_cast<const uint8_t*>(data1.data()), data1.size());
  fut1.wait();
  ASSERT_TRUE(fut1.valid());
  EXPECT_TRUE(fut1.get().ok());

  auto size_or = storage->GetSize();
  ASSERT_TRUE(size_or.ok());
  EXPECT_EQ(size_or.value(), data1.size());

  uint8_t buf[64] = {0};
  auto read_or = storage->PRead(buf, data1.size(), 0);
  ASSERT_TRUE(read_or.ok());
  EXPECT_EQ(read_or.value(), data1.size());
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), read_or.value()), data1);
}

TEST_F(LocalStorageTest, PReadOffsets) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  std::string data1 = "ABC";
  std::string data2 = "DEF";

  auto fut1 = storage->AppendAsync(reinterpret_cast<const uint8_t*>(data1.data()), data1.size());
  EXPECT_TRUE(fut1.get().ok());

  auto fut2 = storage->AppendAsync(reinterpret_cast<const uint8_t*>(data2.data()), data2.size());
  EXPECT_TRUE(fut2.get().ok());

  // Reading DEF starting at offset 3
  uint8_t buf[3] = {0};
  auto read_or = storage->PRead(buf, 3, 3);
  ASSERT_TRUE(read_or.ok());
  EXPECT_EQ(read_or.value(), 3);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 3), "DEF");

  // Reading ABC starting at offset 0
  auto read_or2 = storage->PRead(buf, 3, 0);
  ASSERT_TRUE(read_or2.ok());
  EXPECT_EQ(read_or2.value(), 3);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 3), "ABC");
}

TEST_F(LocalStorageTest, FutureThenChaining) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  std::string data = "Chaining test";
  Future<absl::StatusOr<int64_t>> append_fut = storage->AppendAsync(
      reinterpret_cast<const uint8_t*>(data.data()), data.size());

  Future<std::string> success_fut = append_fut.then([](absl::StatusOr<int64_t> status) {
    if (status.ok()) {
      return std::string("OK");
    }
    return std::string("ERROR");
  });

  Future<int> length_fut = success_fut.then([](std::string s) {
    return static_cast<int>(s.length());
  });

  length_fut.wait();
  EXPECT_EQ(length_fut.get(), 2);  // "OK".length() is 2
}

TEST_F(LocalStorageTest, ConcurrentAppends) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  constexpr int kNumThreads = 10;
  constexpr int kAppendsPerThread = 20;
  std::string append_data = "x"; // 1 byte

  std::vector<std::thread> threads;
  std::vector<Future<absl::StatusOr<int64_t>>> futures;
  std::mutex futures_mutex;

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < kAppendsPerThread; ++j) {
        auto fut = storage->AppendAsync(
            reinterpret_cast<const uint8_t*>(append_data.data()), append_data.size());
        std::lock_guard<std::mutex> lock(futures_mutex);
        futures.push_back(std::move(fut));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  for (auto& fut : futures) {
    EXPECT_TRUE(fut.get().ok());
  }

  auto size_or = storage->GetSize();
  ASSERT_TRUE(size_or.ok());
  EXPECT_EQ(size_or.value(), kNumThreads * kAppendsPerThread);
}

class GcsRapidStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    client_ = std::make_shared<google::cloud::storage::AsyncClient>();
    bucket_ = "test-bucket";
    object_ = "test-object";
    // Clear out the mock registry for clean tests.
    std::lock_guard<std::mutex> lock(google::cloud::storage::GetRegistryMutex());
    google::cloud::storage::GetMockRegistry().erase(bucket_ + "/" + object_);
  }

  std::shared_ptr<google::cloud::storage::AsyncClient> client_;
  std::string bucket_;
  std::string object_;
};

TEST_F(GcsRapidStorageTest, OpenNewObject) {
  auto storage_or = GcsRapidStorage::Create(client_, bucket_, object_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());
  EXPECT_NE(storage, nullptr);

  auto size_or = storage->GetSize();
  ASSERT_TRUE(size_or.ok());
  EXPECT_EQ(size_or.value(), 0);
}

TEST_F(GcsRapidStorageTest, BasicAppendAndRead) {
  auto storage_or = GcsRapidStorage::Create(client_, bucket_, object_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  std::string data1 = "Hello, GCS Rapid Storage!";
  auto fut1 = storage->AppendAsync(reinterpret_cast<const uint8_t*>(data1.data()), data1.size());
  fut1.wait();
  ASSERT_TRUE(fut1.valid());
  auto res1 = fut1.get();
  EXPECT_TRUE(res1.ok()) << res1.status().ToString();

  auto size_or = storage->GetSize();
  ASSERT_TRUE(size_or.ok());
  EXPECT_EQ(size_or.value(), data1.size());

  uint8_t buf[128] = {0};
  auto read_or = storage->PRead(buf, data1.size(), 0);
  ASSERT_TRUE(read_or.ok());
  EXPECT_EQ(read_or.value(), data1.size());
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), read_or.value()), data1);
}

TEST_F(GcsRapidStorageTest, PReadOffsets) {
  auto storage_or = GcsRapidStorage::Create(client_, bucket_, object_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  std::string data1 = "ABC";
  std::string data2 = "DEF";

  auto fut1 = storage->AppendAsync(reinterpret_cast<const uint8_t*>(data1.data()), data1.size());
  EXPECT_TRUE(fut1.get().ok());

  auto fut2 = storage->AppendAsync(reinterpret_cast<const uint8_t*>(data2.data()), data2.size());
  EXPECT_TRUE(fut2.get().ok());

  uint8_t buf[3] = {0};
  auto read_or = storage->PRead(buf, 3, 3);
  ASSERT_TRUE(read_or.ok());
  EXPECT_EQ(read_or.value(), 3);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 3), "DEF");

  auto read_or2 = storage->PRead(buf, 3, 0);
  ASSERT_TRUE(read_or2.ok());
  EXPECT_EQ(read_or2.value(), 3);
  EXPECT_EQ(std::string(reinterpret_cast<char*>(buf), 3), "ABC");
}

TEST_F(GcsRapidStorageTest, ConcurrentAppends) {
  auto storage_or = GcsRapidStorage::Create(client_, bucket_, object_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  constexpr int kNumThreads = 10;
  constexpr int kAppendsPerThread = 20;
  std::string append_data = "y"; // 1 byte

  std::vector<std::thread> threads;
  std::vector<Future<absl::StatusOr<int64_t>>> futures;
  std::mutex futures_mutex;

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < kAppendsPerThread; ++j) {
        auto fut = storage->AppendAsync(
            reinterpret_cast<const uint8_t*>(append_data.data()), append_data.size());
        std::lock_guard<std::mutex> lock(futures_mutex);
        futures.push_back(std::move(fut));
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  for (auto& fut : futures) {
    EXPECT_TRUE(fut.get().ok());
  }

  auto size_or = storage->GetSize();
  ASSERT_TRUE(size_or.ok());
  EXPECT_EQ(size_or.value(), kNumThreads * kAppendsPerThread);
}

}  // namespace
}  // namespace sqlite
