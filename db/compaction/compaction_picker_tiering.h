#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerTiering : public CompactionPicker {
 public:
  CompactionPickerTiering(const ImmutableOptions& ioptions,
                          const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp), T_(0), vstorage_(nullptr) {}

  bool CalcNeedsCompaction(const ColumnFamilyData* cfd,
                           const MutableCFOptions& mutable_cf_options,
                           const VersionStorageInfo* vstorage) const override;

  bool NeedsCompaction(const ColumnFamilyData* cfd,
                       const VersionStorageInfo* vstorage) const override;

  void CompactionFinished(const VersionStorageInfo* vstorage, void* info,
                          uint64_t size) override;
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
  struct SortedRunInfo {
    // > 1 means merging
    size_t num_sorted_runs;
    uint64_t compaction_times;
    // For printing
    uint64_t count;

    bool is_merging() const { return num_sorted_runs > 1; }
  };

  void print_status() const;

  std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                          std::list<SortedRunInfo>::const_iterator>>
  PickCompaction(const MutableCFOptions& mutable_cf_options) const;

  mutable uint64_t T_;
  mutable std::list<SortedRunInfo> sorted_runs_;

  const VersionStorageInfo* vstorage_;
};

}  // namespace ROCKSDB_NAMESPACE
