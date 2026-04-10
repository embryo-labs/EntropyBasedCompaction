#pragma once

#include <cstdint>
#include <list>
#include <optional>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyByCount {
 public:
  struct SortedRunInfo {
    // > 1 means merging
    size_t num_sorted_runs;
    // The count of merged flushed SSTables
    uint64_t count;

    bool is_merging() const { return num_sorted_runs > 1; }
  };
  virtual ~CompactionPolicyByCount() {}

  // REQUIRES: DB mutex held
  // Returns [a, b]: status[a] to status[b] should be compacted into one
  // sorted run.
  virtual std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                                  std::list<SortedRunInfo>::const_iterator>>
  PickCompaction(const std::list<SortedRunInfo> &status) = 0;

  static void print(const std::list<SortedRunInfo> &status);
};

}  // namespace ROCKSDB_NAMESPACE
