#pragma once

#include <cstdint>
#include <list>
#include <optional>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class UniversalCompactionPolicy {
 public:
  struct SortedRunInfo {
    bool merging;
    // uint64_t num_bytes;
    uint64_t num_units;
  };
  virtual ~UniversalCompactionPolicy() {}

  // Returns [a, b]: status[a] to status[b] should be compacted into one
  // sorted run.
  // It should be guaranteed that non of sorted runs in [0, b] is merging.
  // If b == status.size(), it means merging status[a] to status[b-1] to L1
  virtual std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                                  std::list<SortedRunInfo>::const_iterator>>
  PickCompaction(const std::list<SortedRunInfo> &status) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
