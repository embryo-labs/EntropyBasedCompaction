#pragma once

#include "db/compaction/compaction_picker_by_count_size.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerLazyLeveling : public CompactionPickerByCountSize {
 public:
  CompactionPickerLazyLeveling(const ImmutableOptions& ioptions,
                               const InternalKeyComparator* icmp)
      : CompactionPickerByCountSize(ioptions, icmp) {}

  std::optional<PickResult> PickRunsToCompact(
      const MutableCFOptions& mutable_cf_options) const override;
};

}  // namespace ROCKSDB_NAMESPACE
