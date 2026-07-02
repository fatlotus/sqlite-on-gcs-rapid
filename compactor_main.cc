#include <iostream>
#include <string>
#include <string_view>

#include "absl/log/initialize.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "compactor.h"

void PrintUsage(const char* prog) {
  std::cerr << "Usage: " << prog << " <bucket> <object>\n";
  std::cerr << "   or: " << prog << " gs://<bucket>/<object>\n";
}

int main(int argc, char* argv[]) {
  absl::InitializeLog();

  std::string bucket;
  std::string object;

  if (argc == 2) {
    std::string arg = argv[1];
    if (arg == "-h" || arg == "--help") {
      PrintUsage(argv[0]);
      return 0;
    }
    if (arg.rfind("gs://", 0) == 0) {
      std::string_view sub = std::string_view(arg).substr(5);
      size_t first_slash = sub.find('/');
      if (first_slash == std::string_view::npos) {
        std::cerr << "Error: Invalid GCS URI format: " << arg << "\n";
        PrintUsage(argv[0]);
        return 1;
      }
      bucket = std::string(sub.substr(0, first_slash));
      object = std::string(sub.substr(first_slash + 1));
    } else {
      std::cerr << "Error: Single argument must be a gs:// URI\n";
      PrintUsage(argv[0]);
      return 1;
    }
  } else if (argc == 3) {
    bucket = argv[1];
    object = argv[2];
  } else {
    PrintUsage(argv[0]);
    return 1;
  }

  if (bucket.empty() || object.empty()) {
    std::cerr << "Error: Bucket and object names cannot be empty\n";
    PrintUsage(argv[0]);
    return 1;
  }

  std::cout << "Starting compaction for gs://" << bucket << "/" << object << "...\n";
  absl::Status status = sqlite::CompactGcsObject(bucket, object);
  if (!status.ok()) {
    std::cerr << "Compaction failed: " << status.message() << "\n";
    return 1;
  }

  std::cout << "Compaction completed successfully!\n";
  return 0;
}
