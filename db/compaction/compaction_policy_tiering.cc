#include "rocksdb/compaction_policy_tiering.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

static inline uint64_t lowbit(uint64_t x) { return x & -x; }

std::optional<std::tuple<
    std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator,
    std::list<CompactionPolicyByCount::SortedRunInfo>::const_iterator,
    Env::IOPriority>>
CompactionPolicyTiering::PickCompaction(
    const std::list<SortedRunInfo> &status) {
  auto i = status.begin();
  if (i == status.end() || i->merging) {
    return std::nullopt;
  }
  auto j = i;
  ++j;
  while (j != status.end() && !j->merging) {
    auto next = j;
    ++next;
    bool pick = false;
    if (i->count == j->count) {
      if (next == status.end()) {
        pick = true;
      } else if (next->merging) {
        if (j->count * 2 != next->count) {
          pick = true;
        }
      } else if (j->count != next->count) {
        pick = true;
      }
    }
    if (pick) {
      uint64_t x = j->count;
      assert(lowbit(x) == x);
      uint64_t ans = 0;
      assert(x > 0);
      while (x > 1) {
        x >>= 1;
        ans += 1;
      }
      assert(ans <= Env::IOPriority::IO_MID - 1);
      return std::make_tuple(
          i, j,
          static_cast<Env::IOPriority>(Env::IOPriority::IO_MID - 1 - ans));
    }
    i = j;
    j = next;
  }
  return std::nullopt;
}

}  // namespace ROCKSDB_NAMESPACE
