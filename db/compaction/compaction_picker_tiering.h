#pragma once

#include "db/compaction/compaction_picker_by_count.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerTiering : public CompactionPickerByCount {
 public:
  CompactionPickerTiering(const ImmutableOptions& ioptions,
                          const InternalKeyComparator* icmp)
      : CompactionPickerByCount(ioptions, icmp) {}

  std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                          std::list<SortedRunInfo>::const_iterator>>
  PickRunsToCompact(const MutableCFOptions& mutable_cf_options) const override;
};

}  // namespace ROCKSDB_NAMESPACE
