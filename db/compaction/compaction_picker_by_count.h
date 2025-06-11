#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerByCount : public CompactionPicker {
 public:
  CompactionPickerByCount(const ImmutableOptions& ioptions,
                          const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp) {}

  bool CalcNeedsCompaction(const ColumnFamilyData* cfd,
                           const MutableCFOptions& mutable_cf_options,
                           const VersionStorageInfo* vstorage) const override;
  bool NeedsCompaction(const ColumnFamilyData* cfd,
                       const VersionStorageInfo* vstorage) const override;

  Compaction* PickCompaction(
      ColumnFamilyData* cfd, const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer) override;

  void CompactionFinished(void* merging_info, uint64_t size) override;
  void Flushed(ReadOnlyMemTable* memtable) override;

 private:
  Compaction* PickMainLevelCompaction(
      ColumnFamilyData* cfd, const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer);

  std::list<CompactionPolicyByCount::SortedRunInfo> sorted_runs_;
};

}  // namespace ROCKSDB_NAMESPACE
