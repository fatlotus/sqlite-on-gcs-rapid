#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <filesystem>
#include <cstdint>
#include <cstring>
#include <algorithm>

#include "absl/log/globals.h"
#include "absl/log/initialize.h"


namespace fs = std::filesystem;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size);

// Helper to load files
std::vector<uint8_t> LoadFile(const std::string& path) {
  std::ifstream f(path, std::ios::binary | std::ios::ate);
  if (!f.is_open()) return {};
  std::streamsize size = f.tellg();
  f.seekg(0, std::ios::beg);
  std::vector<uint8_t> buffer(size);
  if (f.read(reinterpret_cast<char*>(buffer.data()), size)) {
    return buffer;
  }
  return {};
}

// Simple mutation function
void Mutate(std::vector<uint8_t>& data, std::mt19937& rng) {
  if (data.empty()) {
    data.resize(4105, 0);
  }
  std::uniform_int_distribution<int> op_dist(0, 4);
  int op = op_dist(rng);
  
  std::uniform_int_distribution<size_t> pos_dist(0, data.size() - 1);
  size_t pos = pos_dist(rng);

  switch (op) {
    case 0: { // Mutate a byte
      std::uniform_int_distribution<uint16_t> val_dist(0, 255);
      data[pos] = static_cast<uint8_t>(val_dist(rng));
      break;
    }
    case 1: { // Mutate an 8-byte integer
      if (pos + 8 <= data.size()) {
        uint64_t val = 0;
        std::uniform_int_distribution<int> val_type(0, 5);
        switch (val_type(rng)) {
          case 0: val = 0; break;
          case 1: val = 0xffffffffffffffffULL; break; // -1
          case 2: val = 1000000; break;
          case 3: val = 0x7fffffffffffffffULL; break; // INT64_MAX
          case 4: val = 0x8000000000000000ULL; break; // INT64_MIN
          case 5: {
            std::uniform_int_distribution<uint64_t> rand_val;
            val = rand_val(rng);
            break;
          }
        }
        std::memcpy(&data[pos], &val, 8);
      }
      break;
    }
    case 2: { // Truncate
      std::uniform_int_distribution<size_t> size_dist(0, data.size());
      data.resize(size_dist(rng));
      break;
    }
    case 3: { // Append random bytes
      std::uniform_int_distribution<size_t> append_dist(1, 100);
      size_t to_append = append_dist(rng);
      for (size_t i = 0; i < to_append; ++i) {
        data.push_back(static_cast<uint8_t>(rng() % 256));
      }
      break;
    }
    case 4: { // Delete random range
      std::uniform_int_distribution<size_t> len_dist(1, std::min<size_t>(100, data.size()));
      size_t len = len_dist(rng);
      if (pos + len <= data.size()) {
        data.erase(data.begin() + pos, data.begin() + pos + len);
      }
      break;
    }
  }
}

int main(int argc, char** argv) {
  absl::InitializeLog();
  absl::SetMinLogLevel(absl::LogSeverityAtLeast::kWarning);
  absl::SetStderrThreshold(absl::LogSeverityAtLeast::kWarning);

  std::cout << "Starting custom mutation fuzzer..." << std::endl;
  std::vector<std::vector<uint8_t>> corpus;

  // Load seeds
  std::string corpus_dir = "testdata/corpus";
  if (argc > 1) {
    corpus_dir = argv[1];
  }

  for (const auto& entry : fs::directory_iterator(corpus_dir)) {
    if (entry.is_regular_file()) {
      auto data = LoadFile(entry.path().string());
      if (!data.empty()) {
        corpus.push_back(data);
        std::cout << "Loaded seed: " << entry.path().filename().string() << " (" << data.size() << " bytes)" << std::endl;
      }
    }
  }

  if (corpus.empty()) {
    std::cout << "No seeds found in " << corpus_dir << ". Using empty seed." << std::endl;
    corpus.push_back({});
  }

  std::random_device rd;
  std::mt19937 rng(rd());

  auto start_time = std::chrono::steady_clock::now();
  uint64_t iterations = 0;

  while (true) {
    iterations++;
    if (iterations % 1000 == 0) {
      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
          std::chrono::steady_clock::now() - start_time).count();
      std::cout << "Ran " << iterations << " iterations. Elapsed: " << elapsed << "s" << std::endl;
      
      // Stop if run duration exceeded (1 hour = 3600 seconds)
      if (elapsed >= 3600) {
        std::cout << "Fuzzing completed (1 hour limit reached)." << std::endl;
        break;
      }
    }

    // Select input from corpus
    std::uniform_int_distribution<size_t> corpus_dist(0, corpus.size() - 1);
    std::vector<uint8_t> input = corpus[corpus_dist(rng)];

    // Mutate multiple times
    std::uniform_int_distribution<int> mutate_count(1, 5);
    int count = mutate_count(rng);
    for (int i = 0; i < count; ++i) {
      Mutate(input, rng);
    }

    // Run fuzzer iteration
    LLVMFuzzerTestOneInput(input.data(), input.size());
  }

  return 0;
}
