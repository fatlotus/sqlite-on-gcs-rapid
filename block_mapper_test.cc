#include "block_mapper.h"

#include <unistd.h>

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "local_storage.h"

namespace sqlite {
namespace {

std::string GetTestFilePath() {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir != nullptr) {
    return std::string(tmpdir) + "/block_mapper_test.db";
  }
  return "/tmp/block_mapper_test.db";
}

class BlockMapperTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_path_ = GetTestFilePath();
    // Ensure file doesn't exist before test starts.
    unlink(test_path_.c_str());
  }

  void TearDown() override { unlink(test_path_.c_str()); }

  std::string test_path_;
};

TEST_F(BlockMapperTest, BasicReadWrite) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto mapper = std::make_unique<BlockMapper>(std::move(storage_or.value()));
  ASSERT_TRUE(mapper->Init().ok());

  // Logical size should initially be 0.
  EXPECT_EQ(mapper->logical_size(), 0);

  // Write a full block (4096 bytes).
  std::vector<uint8_t> write_data(4096);
  for (int i = 0; i < 4096; ++i) {
    write_data[i] = static_cast<uint8_t>(i % 256);
  }

  auto write_status = mapper->Write(write_data.data(), write_data.size(), 0);
  ASSERT_TRUE(write_status.ok());
  EXPECT_EQ(mapper->logical_size(), 4096);
  EXPECT_TRUE(mapper->IsBlockMapped(0));

  // Read back the block.
  std::vector<uint8_t> read_data(4096, 0);
  auto read_status = mapper->Read(read_data.data(), read_data.size(), 0);
  ASSERT_TRUE(read_status.ok());
  EXPECT_EQ(read_data, write_data);

  // Read unmapped range, should return zeroes.
  std::vector<uint8_t> unmapped_data(100, 1);  // fill with 1s first
  read_status = mapper->Read(unmapped_data.data(), unmapped_data.size(), 4096);
  ASSERT_TRUE(read_status.ok());
  for (uint8_t val : unmapped_data) {
    EXPECT_EQ(val, 0);
  }
}

TEST_F(BlockMapperTest, ReadModifyWrite) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto mapper = std::make_unique<BlockMapper>(std::move(storage_or.value()));
  ASSERT_TRUE(mapper->Init().ok());

  // 1. Partial write to block 0 (write 10 bytes at offset 100)
  std::vector<uint8_t> p1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  auto status = mapper->Write(p1.data(), p1.size(), 100);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(mapper->logical_size(), 110);

  // Read back and check that [100, 110) has the data, and rest is zero.
  std::vector<uint8_t> read_buf(120, 99);
  status = mapper->Read(read_buf.data(), read_buf.size(), 0);
  ASSERT_TRUE(status.ok());
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(read_buf[i], 0);
  }
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(read_buf[100 + i], p1[i]);
  }
  for (int i = 110; i < 120; ++i) {
    EXPECT_EQ(read_buf[i], 0);
  }

  // 2. Partial write spanning block boundary (write 10 bytes at offset 4090,
  // which spans block 0 and 1)
  std::vector<uint8_t> p2 = {20, 21, 22, 23, 24, 25, 26, 27, 28, 29};
  status = mapper->Write(p2.data(), p2.size(), 4090);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(mapper->logical_size(), 4100);

  // Read back around the block boundary.
  std::vector<uint8_t> boundary_buf(20, 99);
  status = mapper->Read(boundary_buf.data(), boundary_buf.size(), 4085);
  ASSERT_TRUE(status.ok());

  // indices 0-4 correspond to logical offsets 4085-4089 (should be 0)
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(boundary_buf[i], 0);
  }
  // indices 5-14 correspond to logical offsets 4090-4099 (should be p2 data)
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(boundary_buf[5 + i], p2[i]);
  }
  // indices 15-19 correspond to logical offsets 4100-4104 (should be 0)
  for (int i = 15; i < 20; ++i) {
    EXPECT_EQ(boundary_buf[i], 0);
  }

  // Also make sure [100, 110) still has p1 data (Read-Modify-Write preserved
  // it)
  std::vector<uint8_t> p1_check(10, 0);
  status = mapper->Read(p1_check.data(), p1_check.size(), 100);
  ASSERT_TRUE(status.ok());
  EXPECT_EQ(p1_check, p1);
}

TEST_F(BlockMapperTest, Truncate) {
  auto storage_or = LocalStorage::Open(test_path_);
  ASSERT_TRUE(storage_or.ok());
  auto mapper = std::make_unique<BlockMapper>(std::move(storage_or.value()));
  ASSERT_TRUE(mapper->Init().ok());

  // Write to block 0, 1, 2 (logical offsets up to 10000)
  std::vector<uint8_t> data = {123};
  ASSERT_TRUE(mapper->Write(data.data(), data.size(), 50).ok());
  ASSERT_TRUE(mapper->Write(data.data(), data.size(), 4096).ok());
  ASSERT_TRUE(mapper->Write(data.data(), data.size(), 9000).ok());

  EXPECT_EQ(mapper->logical_size(), 9001);
  EXPECT_TRUE(mapper->IsBlockMapped(0));
  EXPECT_TRUE(mapper->IsBlockMapped(1));
  EXPECT_TRUE(mapper->IsBlockMapped(2));

  // Truncate to 5000 (covers block 0 and part of block 1, prunes block 2)
  ASSERT_TRUE(mapper->Truncate(5000).ok());
  EXPECT_EQ(mapper->logical_size(), 5000);
  EXPECT_TRUE(mapper->IsBlockMapped(0));
  EXPECT_TRUE(mapper->IsBlockMapped(1));
  EXPECT_FALSE(mapper->IsBlockMapped(2));

  // Truncate to 1000 (covers part of block 0, prunes block 1)
  ASSERT_TRUE(mapper->Truncate(1000).ok());
  EXPECT_EQ(mapper->logical_size(), 1000);
  EXPECT_TRUE(mapper->IsBlockMapped(0));
  EXPECT_FALSE(mapper->IsBlockMapped(1));

  // Truncate to 0 (prunes block 0)
  ASSERT_TRUE(mapper->Truncate(0).ok());
  EXPECT_EQ(mapper->logical_size(), 0);
  EXPECT_FALSE(mapper->IsBlockMapped(0));
}

TEST_F(BlockMapperTest, CrashRecovery) {
  // 1. Write some blocks
  {
    auto storage_or = LocalStorage::Open(test_path_);
    ASSERT_TRUE(storage_or.ok());
    auto mapper = std::make_unique<BlockMapper>(std::move(storage_or.value()));
    ASSERT_TRUE(mapper->Init().ok());

    std::vector<uint8_t> b0(4096, 7);
    std::vector<uint8_t> b1(4096, 8);
    ASSERT_TRUE(mapper->Write(b0.data(), b0.size(), 0).ok());
    ASSERT_TRUE(mapper->Write(b1.data(), b1.size(), 4096).ok());
  }

  // 2. Simulate crash by appending a partial record (e.g. write 100 bytes of
  // garbage)
  {
    auto storage_or = LocalStorage::Open(test_path_);
    ASSERT_TRUE(storage_or.ok());
    auto storage = std::move(storage_or.value());

    std::vector<uint8_t> garbage(100, 99);
    auto append_res = storage->Append(garbage.data(), garbage.size());
    ASSERT_TRUE(append_res.ok());
  }

  // 3. Reopen the BlockMapper on the same file
  {
    auto storage_or = LocalStorage::Open(test_path_);
    ASSERT_TRUE(storage_or.ok());
    auto storage = std::move(storage_or.value());

    // Check size before Init to verify we appended 100 bytes (2 * 4105 + 100 =
    // 8310)
    auto size_or = storage->GetSize();
    ASSERT_TRUE(size_or.ok());
    EXPECT_EQ(size_or.value(), 8310);

    auto mapper = std::make_unique<BlockMapper>(std::move(storage));
    ASSERT_TRUE(mapper->Init().ok());

    // Verify it padded the file correctly to a multiple of 4105.
    // 2 * 4105 + 100 + padding = 3 * 4105 = 12315
    // Let's open a temp storage just to verify physical size, or we can add a
    // way to check it. Wait, the mapper doesn't expose physical size, but we
    // can verify it via LocalStorage on the path, or by making a new
    // LocalStorage. Let's check size using a temporary open.
    {
      auto check_storage_or = LocalStorage::Open(test_path_);
      ASSERT_TRUE(check_storage_or.ok());
      auto size_or2 = check_storage_or.value()->GetSize();
      ASSERT_TRUE(size_or2.ok());
      EXPECT_EQ(size_or2.value(), 12315);
    }

    // Verify it recovered all valid block mappings
    EXPECT_EQ(mapper->logical_size(), 8192);
    EXPECT_TRUE(mapper->IsBlockMapped(0));
    EXPECT_TRUE(mapper->IsBlockMapped(1));

    // Verify the data is correct
    std::vector<uint8_t> r0(4096, 0);
    std::vector<uint8_t> r1(4096, 0);
    ASSERT_TRUE(mapper->Read(r0.data(), r0.size(), 0).ok());
    ASSERT_TRUE(mapper->Read(r1.data(), r1.size(), 4096).ok());
    for (uint8_t v : r0) EXPECT_EQ(v, 7);
    for (uint8_t v : r1) EXPECT_EQ(v, 8);

    // Verify it can continue writing
    std::vector<uint8_t> b2(4096, 9);
    ASSERT_TRUE(mapper->Write(b2.data(), b2.size(), 8192).ok());
    EXPECT_EQ(mapper->logical_size(), 12288);
    EXPECT_TRUE(mapper->IsBlockMapped(2));

    std::vector<uint8_t> r2(4096, 0);
    ASSERT_TRUE(mapper->Read(r2.data(), r2.size(), 8192).ok());
    for (uint8_t v : r2) EXPECT_EQ(v, 9);
  }
}

}  // namespace
}  // namespace sqlite
