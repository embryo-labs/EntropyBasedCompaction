#include "rocksdb/compaction_policy_dynamic_tiering.h"

#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

const double sqrt2 = sqrt(2);

static inline size_t ceil_log2(size_t x) {
  assert(x > 0);
  size_t y = x;
  size_t b = 0;
  while (y >>= 1) {
    ++b;
  }
  return (x & (x - 1)) ? b + 1 : b;
}

static inline double abs_ratio(uint64_t a, uint64_t b) {
  return a < b ? (double)b / a : (double)a / b;
}

inline uint64_t CompactionPolicyDynamicTiering::size_threshold(
    uint64_t size, size_t category) {
  return (size << category) * sqrt2;
}

std::optional<std::pair<size_t, size_t>>
CompactionPolicyDynamicTiering::PickCompaction(
    size_t write_buffer_size, const std::vector<SortedRunInfo>& sorted_runs,
    uint64_t last_level_size) {
  if (sorted_runs.empty()) {
    return std::nullopt;
  }
  uint64_t buffer_level_max_sorted_run = last_level_size / C_;

  struct CategoryInfo {
    // May actually belong to the next category if this category is empty
    int first;
    size_t num;
    size_t num_non_merging;
    CategoryInfo(int _first) : first(_first), num(0), num_non_merging(0) {}
  };
  std::vector<CategoryInfo> category_info;
  size_t category = 0;
  uint64_t category_size_upper_bound = write_buffer_size * sqrt2;
  int sorted_run_index = 0;
  for (;;) {
    category_info.emplace_back(sorted_run_index);
    while ((size_t)sorted_run_index != sorted_runs.size() &&
           sorted_runs[sorted_run_index].size < category_size_upper_bound) {
      category_info.back().num += 1;
      if (!sorted_runs[sorted_run_index].merging) {
        category_info.back().num_non_merging += 1;
      }
      sorted_run_index += 1;
    }
    ++category;
    category_size_upper_bound = (write_buffer_size << category) * sqrt2;
    if (category_size_upper_bound * 2 > buffer_level_max_sorted_run) {
      break;
    }
  }
  while ((size_t)sorted_run_index != sorted_runs.size()) {
    category_info.back().num += 1;
    if (!sorted_runs[sorted_run_index].merging) {
      category_info.back().num_non_merging += 1;
    }
    sorted_run_index += 1;
  }
  printf("(num, num_non_merging) in each category: ");
  for (size_t i = 0; i < category_info.size(); ++i) {
    printf("(%zu,%zu) ", category_info[i].num,
           category_info[i].num_non_merging);
  }
  putchar('\n');

  std::vector<size_t> target_distance(category_info.size(), 0);
  category = category_info.size() - 1;
  target_distance[category] = 1;
  size_t distance = 1;
  while (category > 0) {
    if (category_info[category].num_non_merging > 0) {
      distance = std::max(distance,
                          ceil_log2(category_info[category].num_non_merging));
    }
    category -= 1;
    target_distance[category] = distance;
  }
  printf("Target distance: ");
  for (size_t i = 0; i < target_distance.size(); ++i) {
    printf("%zu,", target_distance[i]);
  }
  putchar('\n');
  fflush(stdout);

  for (size_t start = 0; start < category_info.size();
       start += target_distance[start]) {
    if (category_info[start].num_non_merging == 0) {
      continue;
    }

    int first_sorted_run = category_info[start].first;
    if (sorted_runs[first_sorted_run].merging) {
      continue;
    }

    size_t target = start + target_distance[start];
    size_t target_size;
    uint64_t upper_bound_size;
    if (target >= category_info.size()) {
      target = category_info.size();
      target_size = buffer_level_max_sorted_run;
      upper_bound_size = buffer_level_max_sorted_run;
    } else {
      target_size = write_buffer_size << target;
      upper_bound_size = target_size * sqrt2;
    }
    int last_sorted_run =
        category_info[target - 1].first + category_info[target - 1].num - 1;
    assert(last_sorted_run >= 0);
    assert((size_t)last_sorted_run != sorted_runs.size());

    uint64_t current_size = 0;
    sorted_run_index = last_sorted_run;
    for (; sorted_run_index >= first_sorted_run; sorted_run_index -= 1) {
      if (sorted_runs[sorted_run_index].merging) {
        break;
      }
      uint64_t size = sorted_runs[sorted_run_index].size;
      if (current_size + size >= upper_bound_size) {
        break;
      }
      if (current_size != 0 && abs_ratio(current_size + size, target_size) >=
                                   abs_ratio(current_size, target_size)) {
        break;
      }
      current_size += size;
    }
    first_sorted_run = sorted_run_index + 1;
    uint64_t min_size = (write_buffer_size << (target - 1)) * sqrt2;
    if (current_size < min_size) {
      continue;
    }
    if (first_sorted_run == category_info[start].first &&
        abs_ratio(current_size + (write_buffer_size << start), target_size) <
            abs_ratio(current_size, target_size)) {
      // If another sorted run can make the size of the output file closer to
      // target size, we wait.
      continue;
    }
    if (target == category_info.size()) {
      last_sorted_run += 1;
      assert((size_t)last_sorted_run == sorted_runs.size());
    }
    return std::make_pair(first_sorted_run, last_sorted_run);
  }
  return std::nullopt;
}

}  // namespace ROCKSDB_NAMESPACE
