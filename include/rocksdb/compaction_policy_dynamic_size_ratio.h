#pragma once

#include "rocksdb/compaction_policy_by_size.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicyDynamicSizeRatio : public CompactionPolicyBySize {
 public:
  CompactionPolicyDynamicSizeRatio(double C);

  const char* name() override { return "DynamicSizeRatio"; }
  size_t size_ratio() const { return T_; }

  std::optional<std::tuple<size_t, size_t, Env::IOPriority>> PickCompaction(
      size_t write_buffer_size, const std::vector<SortedRunInfo>& status,
      uint64_t last_level_size) override;

 private:
  const double C_;

  size_t T_;
  double sqrt_T_;
};

}  // namespace ROCKSDB_NAMESPACE
