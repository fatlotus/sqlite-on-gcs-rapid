#ifndef COMPACTOR_H_
#define COMPACTOR_H_

#include <string>

#include "absl/status/status.h"
#include "google/cloud/options.h"

namespace sqlite {

class BlockMapper;

// Transactionally compacts an append-only SQLite VFS database object on GCS.
// It downloads the object, reconstructs the block mapping using BlockMapper
// to prune out overwritten records and garbage, and then streams the active
// blocks back to GCS using a streaming upload.
// Transactional safety is guaranteed via an If-Generation-Match precondition:
// the upload fails if the object's generation changes after the download begins.
absl::Status CompactGcsObject(const std::string& bucket,
                              const std::string& object);

// Version of CompactGcsObject that accepts custom google::cloud::Options.
absl::Status CompactGcsObject(const std::string& bucket,
                              const std::string& object,
                              const google::cloud::Options& options);

// Reads active blocks from the BlockMapper and writes them sequentially
// to the output stream as append-only records.
absl::Status WriteCompactedRecords(BlockMapper& mapper, std::ostream& os);

}  // namespace sqlite

#endif  // COMPACTOR_H_
