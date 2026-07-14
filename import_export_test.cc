#include "import_export.h"

#include <fcntl.h>
#include <sqlite3.h>
#include <unistd.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
#include "local_storage.h"
#include "vfs_backend.h"

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
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to execute: " << sql
                           << ", Error: " << err;
}

std::vector<std::string> QueryUsers(sqlite3* db) {
  std::vector<std::string> names;
  sqlite3_stmt* stmt = nullptr;
  int rc = sqlite3_prepare_v2(db, "SELECT name FROM users ORDER BY id;", -1,
                              &stmt, nullptr);
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

class ImportExportTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto status = RegisterAppendOnlyVfs();
    ASSERT_TRUE(status.ok()) << status.message();
  }
};

TEST_F(ImportExportTest, EndToEndImportExport) {
  std::string log_db_path = GetTestFilePath("e2e_log.db");
  std::string raw_db_path = GetTestFilePath("e2e_raw.db");
  std::string imported_log_db_path = GetTestFilePath("e2e_imported_log.db");

  // Clean up any stale files
  unlink(log_db_path.c_str());
  unlink(raw_db_path.c_str());
  unlink(imported_log_db_path.c_str());

  // 1. Create a "real sqlite3 log"
  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(log_db_path.c_str(), &db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                           "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  ExecuteOrDie(db, "PRAGMA page_size = 4096;");
  ExecuteOrDie(db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);");
  ExecuteOrDie(db, "INSERT INTO users (name) VALUES ('Alice'), ('Bob');");

  sqlite3_close(db);

  // 2. Export the log to a raw database file
  auto export_storage_or = LocalStorage::Open(log_db_path);
  ASSERT_TRUE(export_storage_or.ok());
  absl::Status status =
      ExportLog(std::move(export_storage_or.value()), raw_db_path);
  ASSERT_TRUE(status.ok()) << status.message();

  // 3. Open and read the exported raw database with the default VFS
  db = nullptr;
  rc = sqlite3_open_v2(raw_db_path.c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
  ASSERT_EQ(rc, SQLITE_OK);

  auto raw_users = QueryUsers(db);
  ASSERT_EQ(raw_users.size(), 2);
  EXPECT_EQ(raw_users[0], "Alice");
  EXPECT_EQ(raw_users[1], "Bob");

  sqlite3_close(db);

  // 4. Import the raw database into a new log-formatted database
  auto import_storage_or = LocalStorage::Open(imported_log_db_path);
  ASSERT_TRUE(import_storage_or.ok());
  status = ImportLog(raw_db_path, std::move(import_storage_or.value()));
  ASSERT_TRUE(status.ok()) << status.message();

  // 5. Open and read the imported log-formatted database using the "appendonly"
  // VFS
  db = nullptr;
  rc = sqlite3_open_v2(imported_log_db_path.c_str(), &db, SQLITE_OPEN_READONLY,
                       "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  auto imported_users = QueryUsers(db);
  ASSERT_EQ(imported_users.size(), 2);
  EXPECT_EQ(imported_users[0], "Alice");
  EXPECT_EQ(imported_users[1], "Bob");

  sqlite3_close(db);

  // Clean up
  unlink(log_db_path.c_str());
  unlink(raw_db_path.c_str());
  unlink(imported_log_db_path.c_str());
}

}  // namespace
}  // namespace sqlite
