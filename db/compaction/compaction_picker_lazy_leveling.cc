#include "db/compaction/compaction_picker_lazy_leveling.h"

#include <cassert>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

std::optional<CompactionPickerByCountSize::PickResult>
CompactionPickerLazyLeveling::PickRunsToCompact(
    const MutableCFOptions& mutable_cf_options) const {
  uint64_t T = mutable_cf_options.max_bytes_for_level_multiplier;
  double C = mutable_cf_options.C;

  uint64_t count = 1;
  auto last_sr = sorted_runs_.end();
  if (last_sr == sorted_runs_.begin()) {
    return std::nullopt;
  }
  --last_sr;
  auto it = sorted_runs_.begin();
  auto i = it;
  size_t num = 0;  // it - i
  uint64_t size = 0;
  while (it != last_sr) {
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
      size = 0;
    }
    size += it->size;
    ++it;
    ++num;
  }
  if (it == last_sr && size * C >= last_sr->size) {
    // Should merge with the last sorted run
    if (last_sr->is_merging()) {
      return std::nullopt;
    }
    return PickResult{
        .first = i,
        .last = last_sr,
    };
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
  return PickResult{
    .first = i,
    .last = it,
  };
}

}  // namespace ROCKSDB_NAMESPACE
