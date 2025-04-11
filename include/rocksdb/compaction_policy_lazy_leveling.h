#pragma once

#include <vector>

#include "rocksdb/compaction_policy_by_count.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyLazyLeveling : public CompactionPolicyByCount {
 public:
  CompactionPolicyLazyLeveling(std::vector<size_t> &&num_sorted_runs)
      : num_sorted_runs_(std::move(num_sorted_runs)) {}
  std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                          std::list<SortedRunInfo>::const_iterator>>
  PickCompaction(const std::list<SortedRunInfo> &status) override;

 private:
  std::vector<size_t> num_sorted_runs_;
};

}  // namespace ROCKSDB_NAMESPACE
