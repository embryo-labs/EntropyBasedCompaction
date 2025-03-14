#pragma once

#include <deque>
#include <unordered_map>
#include <vector>

#include "rocksdb/universal_compaction_policy.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPolicySteady : public UniversalCompactionPolicy {
 public:
  struct Step {
    size_t total;
    size_t compact;
    Step(size_t _total, size_t _compact) : total(_total), compact(_compact) {}
  };
  struct Policy {
    size_t R;
    // rounds[n] is the steps when the number of merging sorted runs from the
    // last round is n
    // The last step is a global compaction
    std::unordered_map<size_t, std::vector<Step>> rounds;
  };
  CompactionPolicySteady(size_t s, size_t R,
                         std::unordered_map<size_t, std::vector<Step>>&& rounds)
      : s_(s),
        R_(R),
        rounds_(std::move(rounds)),
        n_(0),
        step_(0),
        pre_merge_status_(rounds_[n_][step_].total, s_),
        pre_merge_status_reached_(false) {}
  std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                          std::list<SortedRunInfo>::const_iterator>>
  PickCompaction(const std::list<SortedRunInfo>& status) override;

 private:
  bool IsPreMergeStatus(const std::list<SortedRunInfo>& status);

  size_t s_;
  size_t R_;
  std::unordered_map<size_t, std::vector<Step>> rounds_;

  size_t n_;
  size_t step_;

  std::deque<size_t> pre_merge_status_;
  bool pre_merge_status_reached_;
};

}  // namespace ROCKSDB_NAMESPACE
