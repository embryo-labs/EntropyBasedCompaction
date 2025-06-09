#pragma once

#include "rocksdb/compaction_policy_by_count.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyTiering : public CompactionPolicyByCount {
 public:
  CompactionPolicyTiering(size_t T) : T_(T) {}
  std::optional<
      std::tuple<std::list<SortedRunInfo>::const_iterator,
                 std::list<SortedRunInfo>::const_iterator, Env::IOPriority>>
  PickCompaction(const std::list<SortedRunInfo>& status) override;

 private:
  size_t T_;
};

}  // namespace ROCKSDB_NAMESPACE
