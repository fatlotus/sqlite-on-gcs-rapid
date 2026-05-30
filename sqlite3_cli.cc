#include <iostream>
#include <string>
#include <vector>

#include <sqlite3.h>
#include "vfs_backend.h"

namespace {

struct CallbackState {
  bool headers_printed = false;
};

int SqliteCallback(void* data, int argc, char** argv, char** azColName) {
  auto* state = static_cast<CallbackState*>(data);
  if (!state->headers_printed) {
    for (int i = 0; i < argc; i++) {
      std::cout << (i > 0 ? "|" : "") << (azColName[i] ? azColName[i] : "");
    }
    std::cout << "\n";
    state->headers_printed = true;
  }
  for (int i = 0; i < argc; i++) {
    std::cout << (i > 0 ? "|" : "") << (argv[i] ? argv[i] : "");
  }
  std::cout << "\n";
  return 0;
}

bool ExecuteCommand(sqlite3* db, const std::string& sql) {
  char* err_msg = nullptr;
  CallbackState state;
  int rc = sqlite3_exec(db, sql.c_str(), SqliteCallback, &state, &err_msg);
  if (rc != SQLITE_OK) {
    std::cerr << "Error: " << (err_msg ? err_msg : "unknown error") << "\n";
    if (err_msg) {
      sqlite3_free(err_msg);
    }
    return false;
  }
  if (err_msg) {
    sqlite3_free(err_msg);
  }
  return true;
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <db_path> [sql_command]\n";
    return 1;
  }

  std::string db_path = argv[1];
  std::string sql_command;
  if (argc >= 3) {
    for (int i = 2; i < argc; ++i) {
      if (i > 2) {
        sql_command += " ";
      }
      sql_command += argv[i];
    }
  }

  auto vfs_status = sqlite::RegisterAppendOnlyVfs();
  if (!vfs_status.ok()) {
    std::cerr << "Failed to register appendonly VFS: " << vfs_status.message() << "\n";
    return 1;
  }

  sqlite3* db = nullptr;
  int rc = sqlite3_open_v2(db_path.c_str(), &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, "appendonly");
  if (rc != SQLITE_OK) {
    std::cerr << "Failed to open database: " << (db ? sqlite3_errmsg(db) : "out of memory") << "\n";
    if (db) {
      sqlite3_close(db);
    }
    return 1;
  }

  // Automatically runs `PRAGMA page_size = 4096;` on open to set block size.
  char* err_msg = nullptr;
  rc = sqlite3_exec(db, "PRAGMA page_size = 4096;", nullptr, nullptr, &err_msg);
  if (rc != SQLITE_OK) {
    std::cerr << "Failed to set page size: " << (err_msg ? err_msg : "unknown error") << "\n";
    if (err_msg) {
      sqlite3_free(err_msg);
    }
    sqlite3_close(db);
    return 1;
  }
  if (err_msg) {
    sqlite3_free(err_msg);
  }

  if (!sql_command.empty()) {
    if (!ExecuteCommand(db, sql_command)) {
      sqlite3_close(db);
      return 1;
    }
  } else {
    std::string line;
    std::cout << "Enter SQL commands (type '.exit' to quit):\n";
    while (true) {
      std::cout << "sqlite> " << std::flush;
      if (!std::getline(std::cin, line)) {
        break;
      }
      if (line == ".exit") {
        break;
      }
      if (line.empty()) {
        continue;
      }
      ExecuteCommand(db, line);
    }
  }

  sqlite3_close(db);
  return 0;
}
