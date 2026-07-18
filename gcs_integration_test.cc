#include <sqlite3.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace sqlite {
namespace {

std::string FindExtension() {
  const std::string path = "./libsqlite3_gcsvfs.so";
  if (access(path.c_str(), F_OK) == 0) {
    return path;
  }
  return "";
}

TEST(GcsIntegrationTest, LoadExtensionAndQueryGcs) {
  // Get GCS path from environment variable
  const char* gcs_url_env = std::getenv("INTEGRATION_TEST_GCS_URL");
  if (gcs_url_env == nullptr) {
    GTEST_SKIP() << "Skipping integration test because "
                    "INTEGRATION_TEST_GCS_URL is not set.";
  }
  std::string gcs_url(gcs_url_env);
  std::cout << "Running GCS Integration Test against: " << gcs_url << std::endl;

  // Open an in-memory database to load the extension
  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(":memory:", &db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
  ASSERT_EQ(rc, SQLITE_OK);

  // Enable extension loading
  rc = sqlite3_enable_load_extension(db, 1);
  ASSERT_EQ(rc, SQLITE_OK);

  // Find extension shared library
  std::string ext_path = FindExtension();
  ASSERT_FALSE(ext_path.empty()) << "Could not find built extension shared "
                                    "library (libsqlite3_gcsvfs.so/dylib)";
  std::cout << "Loading extension from: " << ext_path << std::endl;

  // Load the extension
  char* err_msg = nullptr;
  rc = sqlite3_load_extension(db, ext_path.c_str(), "sqlite3_gcsvfs_init",
                              &err_msg);
  std::string err = err_msg ? err_msg : "";
  if (err_msg) {
    sqlite3_free(err_msg);
  }
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to load extension: " << err;

  // Close the in-memory database
  sqlite3_close(db);

  // Open the GCS database using the "appendonly" VFS in read-write mode
  db = nullptr;
  rc = sqlite3_open_v2(gcs_url.c_str(), &db,
                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                       "appendonly");
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to open GCS database for write: "
                           << (db ? sqlite3_errmsg(db) : "unknown error");

  // Create table and insert rows
  char* sql_err = nullptr;
  rc = sqlite3_exec(db, "CREATE TABLE test_table(id INTEGER PRIMARY KEY, val TEXT);", nullptr, nullptr, &sql_err);
  std::string sql_err_str = sql_err ? sql_err : "";
  if (sql_err) sqlite3_free(sql_err);
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to create table: " << sql_err_str;

  rc = sqlite3_exec(db, "INSERT INTO test_table (val) VALUES ('hello');", nullptr, nullptr, &sql_err);
  sql_err_str = sql_err ? sql_err : "";
  if (sql_err) sqlite3_free(sql_err);
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to insert hello: " << sql_err_str;

  rc = sqlite3_exec(db, "INSERT INTO test_table (val) VALUES ('world');", nullptr, nullptr, &sql_err);
  sql_err_str = sql_err ? sql_err : "";
  if (sql_err) sqlite3_free(sql_err);
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to insert world: " << sql_err_str;

  // Read back data before closing
  sqlite3_stmt* stmt = nullptr;
  rc = sqlite3_prepare_v2(db, "SELECT val FROM test_table ORDER BY id;", -1, &stmt, nullptr);
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to prepare query: " << sqlite3_errmsg(db);

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "hello");

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "world");

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_DONE);
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  // Reopen the GCS database in read-only mode to verify persistence
  db = nullptr;
  rc = sqlite3_open_v2(gcs_url.c_str(), &db, SQLITE_OPEN_READONLY, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to reopen GCS database: "
                           << (db ? sqlite3_errmsg(db) : "unknown error");

  stmt = nullptr;
  rc = sqlite3_prepare_v2(db, "SELECT val FROM test_table ORDER BY id;", -1, &stmt, nullptr);
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to prepare query after reopen: " << sqlite3_errmsg(db);

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "hello");

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_ROW);
  EXPECT_EQ(std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0))), "world");

  ASSERT_EQ(sqlite3_step(stmt), SQLITE_DONE);
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  std::cout << "GCS read/write integration test completed successfully." << std::endl;
}

TEST(GcsIntegrationTest, MultiProcessLeaseSynchronization) {
  // Get GCS path from environment variable
  const char* gcs_url_env = std::getenv("INTEGRATION_TEST_GCS_URL");
  if (gcs_url_env == nullptr) {
    GTEST_SKIP() << "Skipping integration test because "
                    "INTEGRATION_TEST_GCS_URL is not set.";
  }
  std::string gcs_url(gcs_url_env);
  std::cout << "Running MultiProcess Lease Synchronization Test against: " << gcs_url << std::endl;

  // Find extension shared library
  std::string ext_path = FindExtension();
  ASSERT_FALSE(ext_path.empty()) << "Could not find built extension shared library";

  // 1. Parent: Open database and load extension
  sqlite3* parent_db = nullptr;
  int rc = sqlite3_open_v2(gcs_url.c_str(), &parent_db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
  ASSERT_EQ(rc, SQLITE_OK);

  rc = sqlite3_enable_load_extension(parent_db, 1);
  ASSERT_EQ(rc, SQLITE_OK);

  char* err_msg = nullptr;
  rc = sqlite3_load_extension(parent_db, ext_path.c_str(), "sqlite3_gcsvfs_init", &err_msg);
  ASSERT_EQ(rc, SQLITE_OK) << (err_msg ? err_msg : "");
  if (err_msg) sqlite3_free(err_msg);

  // Close and reopen using the appendonly VFS
  sqlite3_close(parent_db);
  rc = sqlite3_open_v2(gcs_url.c_str(), &parent_db,
                       SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK);

  char* sql_err = nullptr;
  rc = sqlite3_exec(parent_db, "PRAGMA page_size = 4096;", nullptr, nullptr, &sql_err);
  if (sql_err) sqlite3_free(sql_err);
  ASSERT_EQ(rc, SQLITE_OK);

  rc = sqlite3_exec(parent_db, "CREATE TABLE sync_test(id INTEGER PRIMARY KEY, val TEXT);", nullptr, nullptr, &sql_err);
  if (sql_err) sqlite3_free(sql_err);
  ASSERT_EQ(rc, SQLITE_OK);

  rc = sqlite3_exec(parent_db, "INSERT INTO sync_test (val) VALUES ('parent_init');", nullptr, nullptr, &sql_err);
  if (sql_err) sqlite3_free(sql_err);
  ASSERT_EQ(rc, SQLITE_OK);

  // Do a read to ensure parent caches the current DB state
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
    int child_rc = sqlite3_open_v2(gcs_url.c_str(), &child_db,
                                   SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
    if (child_rc != SQLITE_OK) {
      std::cerr << "Child: Failed to open db" << std::endl;
      std::exit(1);
    }

    child_rc = sqlite3_enable_load_extension(child_db, 1);
    if (child_rc != SQLITE_OK) {
      std::cerr << "Child: Failed to enable extension" << std::endl;
      std::exit(1);
    }

    char* child_err = nullptr;
    child_rc = sqlite3_load_extension(child_db, ext_path.c_str(), "sqlite3_gcsvfs_init", &child_err);
    if (child_rc != SQLITE_OK) {
      std::cerr << "Child: Failed to load extension: " << (child_err ? child_err : "") << std::endl;
      std::exit(1);
    }
    if (child_err) sqlite3_free(child_err);

    sqlite3_close(child_db);
    child_rc = sqlite3_open_v2(gcs_url.c_str(), &child_db,
                               SQLITE_OPEN_READWRITE, "appendonly");
    if (child_rc != SQLITE_OK) {
      std::cerr << "Child: Failed to reopen with VFS" << std::endl;
      std::exit(1);
    }

    char* child_sql_err = nullptr;
    child_rc = sqlite3_exec(child_db, "INSERT INTO sync_test (val) VALUES ('child_write');", nullptr, nullptr, &child_sql_err);
    if (child_rc != SQLITE_OK) {
      std::cerr << "Child: Insert failed: " << (child_sql_err ? child_sql_err : "") << std::endl;
      if (child_sql_err) sqlite3_free(child_sql_err);
      std::exit(1);
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
}

}  // namespace
}  // namespace sqlite
