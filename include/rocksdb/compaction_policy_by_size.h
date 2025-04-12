#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyBySize {
 public:
  struct SortedRunInfo {
    bool merging;
    uint64_t size;
    SortedRunInfo(bool _merging, uint64_t _size)
        : merging(_merging), size(_size) {}
  };
  virtual ~CompactionPolicyBySize() {}

  // REQUIRES: DB mutex held
  // Returns [a, b]: status[a] to status[b] should be compacted into one
  // sorted run.
  // It should be guaranteed that non of sorted runs in [0, b] is merging.
  // If b == status.size(), it means merging status[a] to status[b-1] to L1
  virtual std::optional<std::pair<size_t, size_t>> PickCompaction(
      size_t write_buffer_size, const std::vector<SortedRunInfo> &status) = 0;
};

}  // namespace ROCKSDB_NAMESPACE
