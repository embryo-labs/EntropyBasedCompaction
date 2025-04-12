#pragma once

#include "rocksdb/compaction_policy_by_size.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyLazyLeveling : public CompactionPolicyBySize {
 public:
  CompactionPolicyLazyLeveling(std::vector<size_t>&& num_sorted_runs)
      : num_sorted_runs_(std::move(num_sorted_runs)) {}

  std::optional<std::pair<size_t, size_t>> PickCompaction(
      size_t write_buffer_size,
      const std::vector<SortedRunInfo>& status) override;

 private:
  inline uint64_t size_threshold(uint64_t target_size, size_t level);

  std::vector<size_t> num_sorted_runs_;
};

}  // namespace ROCKSDB_NAMESPACE
