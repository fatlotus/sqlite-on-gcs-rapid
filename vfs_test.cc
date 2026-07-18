#include <fcntl.h>
#include <sqlite3.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <string>
#include <vector>

#include "absl/status/status.h"
#include "gtest/gtest.h"
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
  int rc =
      sqlite3_open_v2(db_path.c_str(), &db,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
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

  // 6. Reopen database, query, and verify that the data is restored
  // successfully
  db = nullptr;
  rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE,
                       "appendonly");
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
  rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE,
                       "appendonly");
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

TEST_F(VfsTest, ConcurrentOpenReturnsBusy) {
  std::string db_path = GetTestFilePath("concurrent_open_test.db");
  unlink(db_path.c_str());

  // 1. Open the database first time
  sqlite3* db1 = nullptr;
  int rc1 =
      sqlite3_open_v2(db_path.c_str(), &db1,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc1, SQLITE_OK);

  // 2. Try to open the database second time concurrently
  sqlite3* db2 = nullptr;
  int rc2 = sqlite3_open_v2(db_path.c_str(), &db2, SQLITE_OPEN_READWRITE,
                            "appendonly");
  // It should fail with SQLITE_BUSY
  EXPECT_EQ(rc2, SQLITE_BUSY);
  if (db2) {
    sqlite3_close(db2);
  }

  // 3. Close the first database
  sqlite3_close(db1);

  // 4. Try to open it again after closing - should succeed
  sqlite3* db3 = nullptr;
  int rc3 = sqlite3_open_v2(db_path.c_str(), &db3, SQLITE_OPEN_READWRITE,
                            "appendonly");
  EXPECT_EQ(rc3, SQLITE_OK);
  if (db3) {
    sqlite3_close(db3);
  }

  unlink(db_path.c_str());
}

TEST_F(VfsTest, RejectWalMode) {
  std::string db_path = GetTestFilePath("wal_reject_test.db");
  unlink(db_path.c_str());

  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(db_path.c_str(), &db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  // Try to enable WAL mode
  char* err_msg = nullptr;
  rc = sqlite3_exec(db, "PRAGMA journal_mode = WAL;", nullptr, nullptr, &err_msg);
  if (err_msg) {
    sqlite3_free(err_msg);
  }

  // Check the active journal mode
  sqlite3_stmt* stmt = nullptr;
  rc = sqlite3_prepare_v2(db, "PRAGMA journal_mode;", -1, &stmt, nullptr);
  ASSERT_EQ(rc, SQLITE_OK);
  rc = sqlite3_step(stmt);
  ASSERT_EQ(rc, SQLITE_ROW);
  std::string mode = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
  sqlite3_finalize(stmt);

  // WAL mode should not be the active mode because we rejected SQLITE_OPEN_WAL
  EXPECT_NE(mode, "wal");

  sqlite3_close(db);
  unlink(db_path.c_str());
}

TEST_F(VfsTest, InMemoryJournalIntegration) {
  std::string db_path = GetTestFilePath("in_memory_journal_test.db");
  std::string journal_path = db_path + "-journal";
  unlink(db_path.c_str());
  unlink(journal_path.c_str());

  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(db_path.c_str(), &db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  ExecuteOrDie(db, "PRAGMA page_size = 4096;");
  ExecuteOrDie(db, "CREATE TABLE test (id INTEGER PRIMARY KEY, val TEXT);");

  // Start a transaction and write some data
  ExecuteOrDie(db, "BEGIN TRANSACTION;");
  ExecuteOrDie(db, "INSERT INTO test (val) VALUES ('A'), ('B');");

  // While transaction is active, a rollback journal is typically open.
  // Verify that NO journal file exists on disk!
  struct stat st;
  int stat_rc = stat(journal_path.c_str(), &st);
  EXPECT_NE(stat_rc, 0) << "Journal file should not be created on disk!";

  ExecuteOrDie(db, "COMMIT;");

  // Verify data reads back fine
  sqlite3_stmt* stmt = nullptr;
  rc = sqlite3_prepare_v2(db, "SELECT val FROM test ORDER BY id;", -1, &stmt, nullptr);
  ASSERT_EQ(rc, SQLITE_OK);
  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "A");
  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "B");
  sqlite3_finalize(stmt);

  sqlite3_close(db);
  unlink(db_path.c_str());
}

TEST_F(VfsTest, MultiProcessLeaseSynchronization) {
  std::string db_path = GetTestFilePath("sync_local_integration_test.db");
  unlink(db_path.c_str());
  unlink((db_path + "-journal").c_str());

  // 1. Parent opens database and creates schema
  sqlite3* parent_db = nullptr;
  int rc = sqlite3_open_v2(db_path.c_str(), &parent_db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  ExecuteOrDie(parent_db, "PRAGMA page_size = 4096;");
  ExecuteOrDie(parent_db, "CREATE TABLE sync_test (id INTEGER PRIMARY KEY, val TEXT);");
  ExecuteOrDie(parent_db, "INSERT INTO sync_test (val) VALUES ('parent_init');");

  // Read back to ensure parent caches the current DB state
  sqlite3_stmt* stmt = nullptr;
  rc = sqlite3_prepare_v2(parent_db, "SELECT val FROM sync_test ORDER BY id;", -1, &stmt, nullptr);
  ASSERT_EQ(rc, SQLITE_OK);
  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "parent_init");
  ASSERT_EQ(sqlite3_step(stmt), SQLITE_DONE);
  sqlite3_finalize(stmt);

  // Fork child process to write concurrently
  pid_t pid = fork();
  ASSERT_GE(pid, 0);

  if (pid == 0) {
    // Child Process:
    sqlite3* child_db = nullptr;
    int child_rc = sqlite3_open_v2(db_path.c_str(), &child_db,
                                   SQLITE_OPEN_READWRITE, "appendonly");
    if (child_rc != SQLITE_OK) {
      std::exit(1);
    }

    char* child_err = nullptr;
    child_rc = sqlite3_exec(child_db, "INSERT INTO sync_test (val) VALUES ('child_write');", nullptr, nullptr, &child_err);
    if (child_err) sqlite3_free(child_err);
    if (child_rc != SQLITE_OK) {
      std::exit(2);
    }

    sqlite3_close(child_db);
    std::exit(0);
  }

  // Parent Process:
  int status = 0;
  pid_t waited = waitpid(pid, &status, 0);
  ASSERT_EQ(waited, pid);
  ASSERT_TRUE(WIFEXITED(status));
  ASSERT_EQ(WEXITSTATUS(status), 0);

  // Now, parent starts a new read transaction. It should synchronize and see the child's write!
  rc = sqlite3_prepare_v2(parent_db, "SELECT val FROM sync_test ORDER BY id;", -1, &stmt, nullptr);
  ASSERT_EQ(rc, SQLITE_OK) << sqlite3_errmsg(parent_db);

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "parent_init");

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "child_write");

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_DONE);
  sqlite3_finalize(stmt);

  sqlite3_close(parent_db);
  unlink(db_path.c_str());
}

}  // namespace
}  // namespace sqlite
