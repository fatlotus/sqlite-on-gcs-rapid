#include "vfs_backend.h"

#include <fcntl.h>
#include <unistd.h>
#include <sqlite3.h>
#include <cstdlib>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "absl/status/status.h"
#include "gcs_client_mock.h"

namespace sqlite {
namespace {

std::string GetTestFilePath(const std::string& name) {
  const char* tmpdir = std::getenv("TEST_TMPDIR");
  if (tmpdir != nullptr) {
    return std::string(tmpdir) + "/" + name;
  }
  return "/tmp/" + name;
}

void ExecuteOrDie(sqlite3* db, const std::string& sql) {
  char* err_msg = nullptr;
  int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err_msg);
  std::string err = err_msg ? err_msg : "";
  if (err_msg) {
    sqlite3_free(err_msg);
  }
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to execute: " << sql << ", Error: " << err;
}

std::vector<std::string> QueryUsers(sqlite3* db) {
  std::vector<std::string> names;
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db, "SELECT name FROM users ORDER BY id;", -1, &stmt, nullptr);
  if (rc != SQLITE_OK) {
    return names;
  }
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    const unsigned char* val = sqlite3_column_text(stmt, 0);
    names.push_back(val ? reinterpret_cast<const char*>(val) : "");
  }
  sqlite3_finalize(stmt);
  return names;
}

class VfsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto status = RegisterAppendOnlyVfs();
    ASSERT_TRUE(status.ok()) << status.message();
  }
};

TEST_F(VfsTest, LocalStorageIntegration) {
  std::string db_path = GetTestFilePath("local_integration_test.db");
  // Clean up any stale files
  unlink(db_path.c_str());
  unlink((db_path + "-journal").c_str());
  unlink((db_path + "-wal").c_str());

  // 1. Open the database using VFS "appendonly"
  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  // 2. Force SQLite to use 4096-byte database pages
  ExecuteOrDie(db, "PRAGMA page_size = 4096;");

  // 3. Create table and insert data
  ExecuteOrDie(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
  ExecuteOrDie(db, "INSERT INTO users (name) VALUES ('Alice'), ('Bob');");

  // 4. Query and verify
  auto users = QueryUsers(db);
  ASSERT_EQ(users.size(), 2);
  EXPECT_EQ(users[0], "Alice");
  EXPECT_EQ(users[1], "Bob");

  // 5. Close database
  sqlite3_close(db);

  // 6. Reopen database, query, and verify that the data is restored successfully
  db = nullptr;
  rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  users = QueryUsers(db);
  ASSERT_EQ(users.size(), 2);
  EXPECT_EQ(users[0], "Alice");
  EXPECT_EQ(users[1], "Bob");

  // Insert another row to prepare for crash test
  ExecuteOrDie(db, "INSERT INTO users (name) VALUES ('Charlie');");

  // Close database
  sqlite3_close(db);

  // 7. Simulate write crash: append 100 bytes of zeros to the physical file
  {
    int fd = open(db_path.c_str(), O_WRONLY | O_APPEND);
    ASSERT_GE(fd, 0);
    std::vector<uint8_t> garbage(100, 0);
    ssize_t written = write(fd, garbage.data(), garbage.size());
    ASSERT_EQ(written, 100);
    close(fd);
  }

  // 8. Reopen database, verify it recovers and reads/writes successfully
  db = nullptr;
  rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  users = QueryUsers(db);
  ASSERT_EQ(users.size(), 3);
  EXPECT_EQ(users[0], "Alice");
  EXPECT_EQ(users[1], "Bob");
  EXPECT_EQ(users[2], "Charlie");

  // Write new data
  ExecuteOrDie(db, "INSERT INTO users (name) VALUES ('David');");
  users = QueryUsers(db);
  ASSERT_EQ(users.size(), 4);
  EXPECT_EQ(users[3], "David");

  sqlite3_close(db);

  // Clean up
  unlink(db_path.c_str());
  unlink((db_path + "-journal").c_str());
}

TEST_F(VfsTest, GcsStorageIntegration) {
  std::string db_path = "gcs://test-bucket/db.sqlite";
  std::string registry_key = "test-bucket/db.sqlite";

  // Clean mock GCS registry
  {
    std::lock_guard<std::mutex> lock(google::cloud::storage::GetRegistryMutex());
    google::cloud::storage::GetMockRegistry().erase(registry_key);
    google::cloud::storage::GetMockRegistry().erase(registry_key + "-journal");
  }

  // 1. Open the database using VFS "appendonly"
  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  // 2. Force SQLite to use 4096-byte database pages
  ExecuteOrDie(db, "PRAGMA page_size = 4096;");

  // 3. Create table and insert data
  ExecuteOrDie(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
  ExecuteOrDie(db, "INSERT INTO users (name) VALUES ('Alice'), ('Bob');");

  // 4. Query and verify
  auto users = QueryUsers(db);
  ASSERT_EQ(users.size(), 2);
  EXPECT_EQ(users[0], "Alice");
  EXPECT_EQ(users[1], "Bob");

  // 5. Close database
  sqlite3_close(db);

  // 6. Reopen database, query, and verify that the data is restored successfully
  db = nullptr;
  rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  users = QueryUsers(db);
  ASSERT_EQ(users.size(), 2);
  EXPECT_EQ(users[0], "Alice");
  EXPECT_EQ(users[1], "Bob");

  // Insert another row to prepare for crash test
  ExecuteOrDie(db, "INSERT INTO users (name) VALUES ('Charlie');");

  // Close database
  sqlite3_close(db);

  // 7. Simulate write crash: append 100 bytes of zeros to the physical mock object
  {
    std::lock_guard<std::mutex> lock(google::cloud::storage::GetRegistryMutex());
    auto obj_data = google::cloud::storage::GetMockRegistry()[registry_key];
    ASSERT_NE(obj_data, nullptr);
    std::lock_guard<std::mutex> obj_lock(obj_data->mutex);
    obj_data->data.insert(obj_data->data.end(), 100, 0);
  }

  // 8. Reopen database, verify it recovers and reads/writes successfully
  db = nullptr;
  rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  users = QueryUsers(db);
  ASSERT_EQ(users.size(), 3);
  EXPECT_EQ(users[0], "Alice");
  EXPECT_EQ(users[1], "Bob");
  EXPECT_EQ(users[2], "Charlie");

  // Write new data
  ExecuteOrDie(db, "INSERT INTO users (name) VALUES ('David');");
  users = QueryUsers(db);
  ASSERT_EQ(users.size(), 4);
  EXPECT_EQ(users[3], "David");

  sqlite3_close(db);

  // Clean mock GCS registry
  {
    std::lock_guard<std::mutex> lock(google::cloud::storage::GetRegistryMutex());
    google::cloud::storage::GetMockRegistry().erase(registry_key);
    google::cloud::storage::GetMockRegistry().erase(registry_key + "-journal");
  }
}

TEST_F(VfsTest, ConcurrentOpenReturnsBusy) {
  std::string db_path = GetTestFilePath("concurrent_open_test.db");
  unlink(db_path.c_str());

  // 1. Open the database first time
  sqlite3* db1 = nullptr;
  int rc1 = sqlite3_open_v2(db_path.c_str(), &db1, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc1, SQLITE_OK);

  // 2. Try to open the database second time concurrently
  sqlite3* db2 = nullptr;
  int rc2 = sqlite3_open_v2(db_path.c_str(), &db2, SQLITE_OPEN_READWRITE, "appendonly");
  // It should fail with SQLITE_BUSY
  EXPECT_EQ(rc2, SQLITE_BUSY);
  if (db2) {
    sqlite3_close(db2);
  }

  // 3. Close the first database
  sqlite3_close(db1);

  // 4. Try to open it again after closing - should succeed
  sqlite3* db3 = nullptr;
  int rc3 = sqlite3_open_v2(db_path.c_str(), &db3, SQLITE_OPEN_READWRITE, "appendonly");
  EXPECT_EQ(rc3, SQLITE_OK);
  if (db3) {
    sqlite3_close(db3);
  }

  unlink(db_path.c_str());
}

}  // namespace
}  // namespace sqlite
