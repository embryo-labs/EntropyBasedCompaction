#pragma once

#include "db/compaction/compaction_picker_by_count_size.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerBush : public CompactionPickerByCountSize {
 public:
  CompactionPickerBush(const ImmutableOptions& ioptions,
                       const InternalKeyComparator* icmp)
      : CompactionPickerByCountSize(ioptions, icmp) {}

  std::optional<PickResult> PickRunsToCompact(
      const MutableCFOptions& mutable_cf_options) const override;

 private:
  static constexpr uint64_t T = 2;
  static constexpr uint64_t X = 2;
};

}  // namespace ROCKSDB_NAMESPACE
