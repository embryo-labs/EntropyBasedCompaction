#include "rocksdb/compaction_policy_lazy_leveling.h"

#include <cassert>
#include <cinttypes>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

std::optional<std::pair<
    std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator,
    std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator>>
CompactionPolicyLazyLeveling::PickCompaction(
    const std::list<SortedRunInfo> &status) {
  auto a = status.begin();
  size_t level = 0;
  size_t count = 1;
  auto b = status.begin();
  // num = b - a;
  size_t num = 0;
  while (b != status.end()) {
    if (b->merging) break;
    if (b->count != count) {
      if (num >= num_sorted_runs_[level]) break;
      a = b;
      num = 0;
      do {
        assert(level < num_sorted_runs_.size());
        count *= num_sorted_runs_[level];
        ++level;
      } while (count < b->count);
      assert(count == b->count);
    }
    ++b;
    ++num;
  }
  if (num < num_sorted_runs_[level]) return std::nullopt;
  while (num > num_sorted_runs_[level]) {
    ++a;
    --num;
  }
  if (level == num_sorted_runs_.size() - 1) {
    assert(b == status.end());
  } else {
    --b;
  }
  return std::make_optional<std::pair<
      std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator,
      std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator>>(a,
                                                                            b);
}

}  // namespace ROCKSDB_NAMESPACE
