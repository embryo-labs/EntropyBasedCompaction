#include "db/compaction/compaction_picker_tiering.h"

#include <cassert>
#include <cmath>

namespace ROCKSDB_NAMESPACE {

std::optional<std::pair<
    std::list<CompactionPickerByCount::SortedRunInfo>::const_iterator,
    std::list<CompactionPickerByCount::SortedRunInfo>::const_iterator>>
CompactionPickerTiering::PickRunsToCompact(
    const MutableCFOptions& mutable_cf_options) const {
  size_t T = mutable_cf_options.max_bytes_for_level_multiplier;
  size_t count = 1;
  auto it = sorted_runs_.begin();
  auto i = it;
  size_t num = 0;  // it - i
  while (it != sorted_runs_.end()) {
    if (it->is_merging()) {
      break;
    }
    if (it->count != count) {
      if (num >= T) {
        break;
      }
      do {
        assert(it->count > count);
        count *= T;
      } while (it->count != count);
      i = it;
      num = 0;
    }
    ++it;
    ++num;
  }
  if (num < T) {
    return std::nullopt;
  }
  if (it->is_merging() && it->count == count * T) {
    // A compaction at this level is already in progress
    return std::nullopt;
  }
  while (num > T) {
    num -= 1;
    ++i;
  }
  --it;
  return std::make_pair(i, it);
}

}  // namespace ROCKSDB_NAMESPACE
