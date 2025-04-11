#include "rocksdb/compaction_policy_steady.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

bool CompactionPolicySteady::IsPreMergeStatus(
    const std::list<SortedRunInfo> &status) {
  for (auto [it_expected, it_coming] =
           std::make_tuple(pre_merge_status_.rbegin(), status.rbegin());
       it_expected != pre_merge_status_.rend(); ++it_expected, ++it_coming) {
    if (it_coming == status.rend() || it_coming->count != *it_expected) {
      return false;
    }
  }
  return true;
}

std::optional<std::pair<
    std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator,
    std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator>>
CompactionPolicySteady::PickCompaction(const std::list<SortedRunInfo> &status) {
  auto a = status.begin();
  size_t num_top_level_sorted_runs = 0;
  if (s_ > 1) {
    // See if we need to trigger a compaction from the top level to the main
    // level
    while (a != status.end() && a->count == 1) {
      assert(!a->merging);
      ++num_top_level_sorted_runs;
      ++a;
    }
    if (num_top_level_sorted_runs >= s_) {
      assert(a != status.begin());
      --a;
      auto b = a;
      for (size_t i = 1; i < s_; ++i) {
        --a;
      }
      return std::make_optional<std::pair<
          std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator,
          std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator>>(
          a, b);
    }
  }

  // We don't allow multiple compactions in the main level yet
  for (auto it = a; it != status.end(); ++it) {
    if (it->merging) {
      return std::nullopt;
    }
  }

  size_t num_main_level_sorted_runs = status.size() - num_top_level_sorted_runs;
  if (pre_merge_status_reached_) {
    if (!IsPreMergeStatus(status)) {
      // Move to the next step
      const auto &ori_steps = rounds_[n_];
      const Step &ori_step = ori_steps[step_];
      if (step_ == ori_steps.size() - 1) {
        n_ = ori_step.total;
        step_ = 0;
        pre_merge_status_.clear();
      } else {
        ++step_;
        size_t to_merge = 0;
        for (size_t i = 0; i < ori_step.compact; ++i) {
          to_merge += pre_merge_status_.front();
          pre_merge_status_.pop_front();
        }
        pre_merge_status_.push_front(to_merge);
      }
      while (pre_merge_status_.size() < rounds_[n_][step_].total) {
        pre_merge_status_.push_front(s_);
      }
      pre_merge_status_reached_ = IsPreMergeStatus(status);
    } else {
      // Somehow the previously picked compaction doesn't take effect. Try again
    }
  } else {
    // Check again
    pre_merge_status_reached_ = IsPreMergeStatus(status);
  }
  if (!pre_merge_status_reached_) return std::nullopt;
  const auto &steps = rounds_[n_];
  const Step &step = steps[step_];
  assert(pre_merge_status_.size() == step.total);
  assert(num_main_level_sorted_runs >= step.total);
  for (size_t i = 0; i < num_main_level_sorted_runs - step.total; ++i) {
    ++a;
  }
  auto b = a;
  for (size_t i = 1; i < step.compact; ++i) {
    ++b;
  }
  if (step_ == steps.size() - 1) {
    // Global compaction
    assert(step.compact == step.total);
    ++b;
    assert(b == status.end());
  }
  return std::make_optional<std::pair<
      std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator,
      std::list<UniversalCompactionPolicy::SortedRunInfo>::const_iterator>>(a,
                                                                            b);
}

}  // namespace ROCKSDB_NAMESPACE
