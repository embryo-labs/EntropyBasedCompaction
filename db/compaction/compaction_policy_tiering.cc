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
  size_t num = 1;
  auto j = i;
  ++j;
  while (j != status.end() && !j->merging) {
    if (j->count != i->count) {
      i = j;
      num = 1;
      ++j;
      continue;
    }
    num += 1;
    ++j;
    if (num < T_) {
      continue;
    }
    assert(num == T_);
    bool pick = false;
    if (j == status.end()) {
      pick = true;
    } else if (j->merging) {
      if (i->count * T_ != j->count) {
        pick = true;
      }
    } else if (i->count != j->count) {
      pick = true;
    }
    if (pick) {
      --j;
      uint64_t x = i->count;
      uint64_t ans = 0;
      assert(x > 0);
      while (x > 1) {
        assert(x % T_ == 0);
        x /= T_;
        ans += 1;
      }
      assert(ans <= Env::IOPriority::IO_MID - 1);
      return std::make_tuple(
          i, j,
          static_cast<Env::IOPriority>(Env::IOPriority::IO_MID - 1 - ans));
    }
    ++i;
    num -= 1;
  }
  return std::nullopt;
}

}  // namespace ROCKSDB_NAMESPACE
