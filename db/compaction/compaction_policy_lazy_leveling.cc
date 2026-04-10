#include "rocksdb/compaction_policy_lazy_leveling.h"

#include <cassert>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

std::optional<std::pair<
    std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator,
    std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator>>
CompactionPolicyLazyLeveling::PickCompaction(
    const std::list<CompactionPolicyByCount::SortedRunInfo> &status) {
  size_t count = 1;
  auto last_sr = status.end();
  if (last_sr == status.begin()) {
    return std::nullopt;
  }
  --last_sr;
  auto it = status.begin();
  auto i = it;
  size_t num = 0;  // it - i
  while (it != last_sr) {
    if (it->is_merging()) {
      break;
    }
    if (it->count != count) {
      if (num >= T_) {
        break;
      }
      do {
        assert(it->count > count);
        count *= T_;
      } while (it->count != count);
      i = it;
      num = 0;
    }
    ++it;
    ++num;
  }
  if (it == last_sr && num * count * C_ >= last_sr->count) {
    // Should merge with the last sorted run
    if (last_sr->is_merging()) {
      return std::nullopt;
    }
    return std::make_pair(i, last_sr);
  }
  if (num < T_) {
    return std::nullopt;
  }
  if (it->is_merging() && it->count == count * T_) {
    // A compaction at this level is already in progress
    return std::nullopt;
  }
  while (num > T_) {
    num -= 1;
    ++i;
  }
  --it;
  return std::make_pair(i, it);
}

}  // namespace ROCKSDB_NAMESPACE
