#pragma once

#include "rocksdb/universal_compaction_policy.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyLazyLeveling : public UniversalCompactionPolicy {
 public:
  CompactionPolicyLazyLeveling(size_t size_ratio, size_t num_levels)
      : size_ratio_(size_ratio), num_levels_(num_levels) {}
  std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                          std::list<SortedRunInfo>::const_iterator>>
  PickCompaction(const std::list<SortedRunInfo> &status) override;

 private:
  size_t size_ratio_;
  size_t num_levels_;
};

}  // namespace ROCKSDB_NAMESPACE
