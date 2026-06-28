#include "storage.h"

#include <unistd.h>

#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include "absl/status/status.h"
#include "gcs_storage.h"
#include "gtest/gtest.h"
#include "local_storage.h"

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

  void TearDown() override { unlink(test_path_.c_str()); }

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
  auto res1 = storage->Append(reinterpret_cast<const uint8_t*>(data1.data()),
                              data1.size());
  ASSERT_TRUE(res1.ok());

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

  auto res1 = storage->Append(reinterpret_cast<const uint8_t*>(data1.data()),
                              data1.size());
  EXPECT_TRUE(res1.ok());

  auto res2 = storage->Append(reinterpret_cast<const uint8_t*>(data2.data()),
                              data2.size());
  EXPECT_TRUE(res2.ok());

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

TEST_F(LocalStorageTest, ConcurrentAppends) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto storage = std::move(storage_or.value());

  constexpr int kNumThreads = 10;
  constexpr int kAppendsPerThread = 20;
  std::string append_data = "x";  // 1 byte

  std::vector<std::thread> threads;

  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      for (int j = 0; j < kAppendsPerThread; ++j) {
        auto res = storage->Append(
            reinterpret_cast<const uint8_t*>(append_data.data()),
            append_data.size());
        EXPECT_TRUE(res.ok());
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  auto size_or = storage->GetSize();
  ASSERT_TRUE(size_or.ok());
  EXPECT_EQ(size_or.value(), kNumThreads * kAppendsPerThread);
}

}  // namespace
}  // namespace sqlite
