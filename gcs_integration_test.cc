#include <sqlite3.h>
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

  // Open the GCS database using the "appendonly" VFS
  db = nullptr;
  rc =
      sqlite3_open_v2(gcs_url.c_str(), &db, SQLITE_OPEN_READONLY, "appendonly");
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to open GCS database: "
                           << (db ? sqlite3_errmsg(db) : "unknown error");

  // Query the database
  sqlite3_stmt* stmt = nullptr;
  rc = sqlite3_prepare_v2(db,
                          "SELECT name FROM sqlite_master WHERE type='table';",
                          -1, &stmt, nullptr);
  ASSERT_EQ(rc, SQLITE_OK) << "Failed to prepare query: " << sqlite3_errmsg(db);

  std::cout << "Tables in production GCS database:" << std::endl;
  int row_count = 0;
  while (sqlite3_step(stmt) == SQLITE_ROW) {
    row_count++;
    const unsigned char* name = sqlite3_column_text(stmt, 0);
    std::cout << " - " << (name ? reinterpret_cast<const char*>(name) : "")
              << std::endl;
  }
  sqlite3_finalize(stmt);
  sqlite3_close(db);

  std::cout << "Query completed successfully. Found " << row_count << " tables."
            << std::endl;
}

}  // namespace
}  // namespace sqlite
