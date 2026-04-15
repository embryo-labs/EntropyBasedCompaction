#include "db/compaction/compaction_picker_bush.h"

#include <cassert>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

std::optional<CompactionPickerByCountSize::PickResult>
CompactionPickerBush::PickRunsToCompact(
    const MutableCFOptions& mutable_cf_options) const {
  constexpr int32_t last_level_bloom_bits = 10;
  const double p_L =
      std::exp(-last_level_bloom_bits * std::log(2) * std::log(2));

  uint64_t L = mutable_cf_options.num_logical_levels;
  if (L == 0) {
    fprintf(stderr, "Error: num_logical_levels not specified\n");
    std::abort();
  }
  auto last_sr = sorted_runs_.end();
  if (last_sr == sorted_runs_.begin()) {
    return std::nullopt;
  }
  --last_sr;

  uint64_t i = 1;
  uint64_t count = 1;
  uint64_t max_num_runs = std::pow(T, std::pow(X, L - 1 - i));
  auto it = sorted_runs_.begin();
  auto first = it;
  uint64_t num = 0;  // it - first
  while (it != sorted_runs_.end()) {
    if (it->is_merging()) {
      break;
    }
    if (it->count != count) {
      if (num >= max_num_runs || it == last_sr) {
        break;
      }
      do {
        assert(it->count > count);
        count *= max_num_runs;
        i += 1;
        assert(i <= L - 1);
        max_num_runs = std::pow(T, std::pow(X, L - 1 - i));
      } while (it->count != count);
      first = it;
      num = 0;
    }
    ++it;
    ++num;
  }
  if (num < max_num_runs) {
    return std::nullopt;
  }
  while (num > max_num_runs) {
    num -= 1;
    ++first;
  }
  if (it->is_merging() && it->count == count * max_num_runs) {
    // A compaction at this level is already in progress
    return std::nullopt;
  }
  if (it == last_sr && i == L - 1) {
    // Should merge with the last sorted run
    if (it->is_merging()) {
      return std::nullopt;
    }
    return PickResult{
        .first = first,
        .last = it,
        .bloom_millibits_per_key = last_level_bloom_bits * 1000,
    };
  }
  --it;
  uint64_t size_non_last = 0;
  for (auto t = sorted_runs_.begin(); t != last_sr; ++t) {
    size_non_last += t->size;
  }
  double C = std::max(1.0, (double)last_sr->size / size_non_last);
  double p_i = (p_L / (max_num_runs - 1) / C *
                std::pow((double)T / max_num_runs, 1.0 / (X - 1)) *
                (max_num_runs - 1) / max_num_runs);
  double bloom_bits_per_key = -std::log(p_i) / std::log(2) / std::log(2);
  printf("i %" PRIu64 ", max_num_runs %" PRIu64
         ", C %f, bloom_bits_per_key %f\n",
         i, max_num_runs, C, bloom_bits_per_key);
  return PickResult{
      .first = first,
      .last = it,
      .bloom_millibits_per_key = (uint32_t)(bloom_bits_per_key * 1000),
  };
}

}  // namespace ROCKSDB_NAMESPACE
