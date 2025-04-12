#include "rocksdb/compaction_policy_lazy_leveling.h"

#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

inline uint64_t CompactionPolicyLazyLeveling::size_threshold(
    uint64_t target_size, size_t level) {
  assert(num_sorted_runs_.size() != 0);
  if (level == num_sorted_runs_.size() - 1) {
    return std::numeric_limits<uint64_t>::max();
  } else {
    return target_size * std::sqrt(num_sorted_runs_[level]);
  }
}

std::optional<std::pair<size_t, size_t>>
CompactionPolicyLazyLeveling::PickCompaction(
    size_t write_buffer_size, const std::vector<SortedRunInfo>& status) {
  size_t a = 0, b = 0;
  size_t level = 0;
  size_t target_size = write_buffer_size;
  size_t threshold = size_threshold(target_size, level);
  while (b != status.size()) {
    if (status[b].merging) break;
    if (status[b].size > threshold) {
      if (b - a >= num_sorted_runs_[level]) break;
      a = b;
      do {
        assert(level < num_sorted_runs_.size());
        target_size *= num_sorted_runs_[level];
        ++level;
        threshold = size_threshold(target_size, level);
      } while (status[b].size > threshold);
    }
    ++b;
  }
  if (b - a < num_sorted_runs_[level]) return std::nullopt;
  while (b - a > num_sorted_runs_[level]) {
    ++a;
  }
  if (level == num_sorted_runs_.size() - 1) {
    if (b < status.size()) {
      // The last flatten job is still in progress
      assert(status[b].merging);
      return std::nullopt;
    }
  } else {
    --b;
  }
  return std::make_pair(a, b);
}

}  // namespace ROCKSDB_NAMESPACE
