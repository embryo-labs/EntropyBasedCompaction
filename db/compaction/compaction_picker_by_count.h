#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerByCount : public CompactionPicker {
 public:
  CompactionPickerByCount(const ImmutableOptions& ioptions,
                          const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp), vstorage_(nullptr) {}

  bool CalcNeedsCompaction(const ColumnFamilyData* cfd,
                           const MutableCFOptions& mutable_cf_options,
                           const VersionStorageInfo* vstorage) const override;
  bool NeedsCompaction(const ColumnFamilyData* cfd,
                       const VersionStorageInfo* vstorage) const override;

  void CompactionFinished(const VersionStorageInfo* vstorage,
                          void* merging_info, uint64_t size) override;
  void Flushed(const VersionStorageInfo* vstorage,
               const FileMetaData& file) override;

  Compaction* PickCompaction(
      ColumnFamilyData* cfd, const std::string& cf_name,
      const MutableCFOptions& mutable_cf_options,
      const MutableDBOptions& mutable_db_options,
      const std::vector<SequenceNumber>& existing_snapshots,
      const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
      LogBuffer* log_buffer, bool require_max_output_level) override;

 private:
  std::list<CompactionPolicyByCount::SortedRunInfo> sorted_runs_;

  const VersionStorageInfo* vstorage_;
};

}  // namespace ROCKSDB_NAMESPACE
