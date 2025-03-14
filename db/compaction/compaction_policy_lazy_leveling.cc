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
  size_t level = 1;
  size_t size = 1;
  auto b = status.begin();
  // num = b - a;
  size_t diff = 0;
  while (b != status.end()) {
    if (b->merging) break;
    if (b->size != size) {
      if (diff >= size_ratio_) break;
      a = b;
      diff = 0;
      do {
        ++level;
        size *= size_ratio_;
      } while (size < b->size);
      assert(size == b->size);
    }
    ++b;
    ++diff;
  }
  if (diff < size_ratio_) return std::nullopt;
  while (diff > size_ratio_) {
    ++a;
    --diff;
  }
  if (level == num_levels_ - 1) {
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
