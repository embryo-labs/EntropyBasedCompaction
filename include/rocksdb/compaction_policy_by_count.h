#pragma once

#include <cstdint>
#include <list>
#include <optional>

#include "rocksdb/env.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyByCount {
 public:
  struct SortedRunInfo {
    bool merging;
    // The count of merged flushed SSTables
    uint64_t count;
  };
  virtual ~CompactionPolicyByCount() {}

  // REQUIRES: DB mutex held
  // Returns [a, b]: status[a] to status[b] should be compacted into one
  // sorted run.
  // It should be guaranteed that non of sorted runs in [0, b] is merging.
  // If b == status.size(), it means merging status[a] to status[b-1] to L1
  virtual std::optional<
      std::tuple<std::list<SortedRunInfo>::const_iterator,
                 std::list<SortedRunInfo>::const_iterator, Env::IOPriority>>
  PickCompaction(const std::list<SortedRunInfo> &status) = 0;

  void print(const std::list<SortedRunInfo> &status);
};

}  // namespace ROCKSDB_NAMESPACE
