#pragma once

#include "rocksdb/compaction_policy_by_count.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyTiering : public CompactionPolicyByCount {
 public:
  CompactionPolicyTiering(size_t T) : T_(T) {}

  std::optional<std::pair<
      std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator,
      std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator>>
  PickCompaction(
      const std::list<CompactionPolicyByCount::SortedRunInfo> &status) override;

 private:
  size_t T_;
};

}  // namespace ROCKSDB_NAMESPACE
