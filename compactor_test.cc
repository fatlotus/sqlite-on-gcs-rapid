#include "compactor.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "block_mapper.h"
#include "local_storage.h"

namespace sqlite {
namespace {

using ::testing::_;
using ::testing::Return;

std::string GetTestFilePath() {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir != nullptr) {
    return std::string(tmpdir) + "/compactor_test.db";
  }
  return "/tmp/compactor_test.db";
}

TEST(CompactorTest, WriteCompactedRecordsSuccess) {
  std::string test_path = GetTestFilePath();
  unlink(test_path.c_str());

  // 1. Write some test blocks to a local file
  {
    auto storage_or = LocalStorage::Open(test_path);
    ASSERT_TRUE(storage_or.ok());
    BlockMapper mapper(std::move(storage_or.value()));
    ASSERT_TRUE(mapper.Init().ok());

    std::vector<uint8_t> block_data(4096, 'A');
    ASSERT_TRUE(mapper.Write(block_data.data(), block_data.size(), 0).ok());

    std::vector<uint8_t> block_data_b(4096, 'B');
    ASSERT_TRUE(mapper.Write(block_data_b.data(), block_data_b.size(), 4096).ok());

    // Overwrite block 0 with 'C'
    std::vector<uint8_t> block_data_c(4096, 'C');
    ASSERT_TRUE(mapper.Write(block_data_c.data(), block_data_c.size(), 0).ok());
  }

  // 2. Open via BlockMapper and write to stream
  std::stringstream ss;
  {
    auto storage_or = LocalStorage::Open(test_path);
    ASSERT_TRUE(storage_or.ok());
    BlockMapper mapper(std::move(storage_or.value()));
    ASSERT_TRUE(mapper.Init().ok());

    absl::Status status = WriteCompactedRecords(mapper, ss);
    ASSERT_TRUE(status.ok());
  }

  // 3. Verify the output stream structure
  std::string data = ss.str();
  // We expect exactly 2 records (block 0 with 'C', block 1 with 'B')
  // Total size: 2 * 4105 = 8210 bytes
  ASSERT_EQ(data.size(), 8210);

  // Parse record 0
  int64_t r0_idx;
  std::memcpy(&r0_idx, data.data(), 8);
  EXPECT_EQ(r0_idx, 0);
  for (int i = 8; i < 4104; ++i) {
    EXPECT_EQ(data[i], 'C');
  }
  EXPECT_EQ(static_cast<uint8_t>(data[4104]), 1);

  // Parse record 1
  int64_t r1_idx;
  std::memcpy(&r1_idx, data.data() + 4105, 8);
  EXPECT_EQ(r1_idx, 1);
  for (int i = 4105 + 8; i < 4105 + 4104; ++i) {
    EXPECT_EQ(data[i], 'B');
  }
  EXPECT_EQ(static_cast<uint8_t>(data[4105 + 4104]), 1);

  unlink(test_path.c_str());
}



}  // namespace
}  // namespace sqlite
