#pragma once

#include "rocksdb/compaction_policy_by_count.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyLazyLeveling : public CompactionPolicyByCount {
 public:
  CompactionPolicyLazyLeveling(size_t T, double C) : T_(T), C_(C) {}

  std::optional<std::pair<
      std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator,
      std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator>>
  PickCompaction(
      const std::list<CompactionPolicyByCount::SortedRunInfo> &status) override;

 private:
  size_t T_;
  double C_;
};

}  // namespace ROCKSDB_NAMESPACE
