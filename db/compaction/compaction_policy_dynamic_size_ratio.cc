#include "rocksdb/compaction_policy_dynamic_size_ratio.h"

#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

static inline double abs_ratio(uint64_t a, uint64_t b) {
  return a < b ? (double)b / a : (double)a / b;
}

CompactionPolicyDynamicSizeRatio::CompactionPolicyDynamicSizeRatio(double C)
    : C_(C), T_(2), sqrt_T_(sqrt(T_)) {}

// Compaction from class i to class i+1 has a priority of
// IO_LOW + num_classes - i
std::optional<std::tuple<size_t, size_t, Env::IOPriority>>
CompactionPolicyDynamicSizeRatio::PickCompaction(
    size_t write_buffer_size, const std::vector<SortedRunInfo>& sorted_runs,
    uint64_t last_level_size) {
  if (sorted_runs.empty()) {
    return std::nullopt;
  }
  uint64_t buffer_level_sorted_run_size = last_level_size / C_;

  struct CategoryInfo {
    uint64_t size;
    // May actually belong to the next category if this category is empty
    int first;
    size_t num_non_merging;
    size_t num_merging;
    CategoryInfo(uint64_t _size, int _first)
        : size(_size), first(_first), num_non_merging(0), num_merging(0) {}
  };
  for (;;) {
    std::vector<CategoryInfo> category_info;
    size_t category = 0;
    uint64_t category_size = write_buffer_size;
    int sorted_run_index = 0;
    for (;;) {
      category_info.emplace_back(category_size, sorted_run_index);
      while ((size_t)sorted_run_index != sorted_runs.size() &&
             sorted_runs[sorted_run_index].size < category_size * sqrt_T_) {
        if (sorted_runs[sorted_run_index].merging) {
          category_info.back().num_merging += 1;
        } else {
          category_info.back().num_non_merging += 1;
        }
        sorted_run_index += 1;
      }
      ++category;
      category_size *= T_;
      if (category_size * sqrt_T_ > buffer_level_sorted_run_size) {
        break;
      }
    }
    while ((size_t)sorted_run_index != sorted_runs.size()) {
      if (sorted_runs[sorted_run_index].merging) {
        category_info.back().num_merging += 1;
      } else {
        category_info.back().num_non_merging += 1;
      }
      sorted_run_index += 1;
    }
    printf("(num_non_merging, num_merging) in each category: ");
    for (size_t i = 0; i < category_info.size(); ++i) {
      printf("(%zu,%zu) ", category_info[i].num_non_merging,
             category_info[i].num_merging);
    }
    putchar('\n');

    size_t start;
    for (start = 0; start < category_info.size(); start += 1) {
      if (start < category_info.size() - 1) {
        if (category_info[start].num_non_merging > T_) {
          break;
        }
      } else {
        if (category_info[start].num_non_merging * category_info[start].size /
                sqrt_T_ >
            buffer_level_sorted_run_size) {
          break;
        }
      }
    }
    bool congested = false;
    if (start < category_info.size()) {
      congested = true;
      if (category_info.size() > 1) {
        T_ += 1;
        sqrt_T_ = sqrt(T_);
        printf("Increase T to %zu\n", T_);
        continue;
      }
    }

    assert(!category_info.empty());
    for (start = 0; start < category_info.size(); start += 1) {
      if (category_info[start].num_merging > 0) {
        return std::nullopt;
      }
      if (category_info[start].num_non_merging == 0) {
        continue;
      }

      int first_sorted_run = category_info[start].first;
      if (sorted_runs[first_sorted_run].merging) {
        continue;
      }

      uint64_t target_size;
      if (start + 1 == category_info.size()) {
        target_size = buffer_level_sorted_run_size;
      } else {
        target_size = category_info[start].size * T_;
      }
      int last_sorted_run =
          category_info[start].first + category_info[start].num_non_merging - 1;
      assert(last_sorted_run >= 0);
      assert((size_t)last_sorted_run != sorted_runs.size());

      uint64_t current_size = 0;
      sorted_run_index = last_sorted_run;
      for (; sorted_run_index >= first_sorted_run; sorted_run_index -= 1) {
        if (sorted_runs[sorted_run_index].merging) {
          break;
        }
        uint64_t size = sorted_runs[sorted_run_index].size;
        if (current_size != 0 && abs_ratio(current_size + size, target_size) >=
                                     abs_ratio(current_size, target_size)) {
          break;
        }
        current_size += size;
      }
      first_sorted_run = sorted_run_index + 1;
      if (current_size < category_info[start].size * sqrt_T_) {
        continue;
      }
      if (first_sorted_run == category_info[start].first &&
          (last_sorted_run - first_sorted_run + 1) < (int)T_ &&
          abs_ratio(current_size + category_info[start].size, target_size) <
              abs_ratio(current_size, target_size)) {
        // If another sorted run can make the size of the output file closer to
        // target size, we wait.
        continue;
      }
      if (start + 1 == category_info.size()) {
        last_sorted_run += 1;
        assert((size_t)last_sorted_run == sorted_runs.size());
      }
      auto io_priority = static_cast<Env::IOPriority>(
          Env::IO_LOW + category_info.size() - start);
      assert(io_priority > Env::IO_LOW);
      assert(io_priority < Env::IO_MID);
      return std::make_tuple(first_sorted_run, last_sorted_run, io_priority);
    }
    assert(start == category_info.size());
    if (!congested && T_ > 2) {
      T_ -= 1;
      sqrt_T_ = sqrt(T_);
      printf("Decrease T to %zu\n", T_);
    }
    return std::nullopt;
  }
}

}  // namespace ROCKSDB_NAMESPACE
