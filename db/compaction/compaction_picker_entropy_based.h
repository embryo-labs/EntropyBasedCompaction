#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {
class CompactionPickerEntropyBased : public CompactionPicker {
 public:
  CompactionPickerEntropyBased(const ImmutableOptions& ioptions,
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
      LogBuffer* log_buffer, bool require_max_output_level = false) override;

  void CompactionFinished(const VersionStorageInfo* vstorage, void* info,
                          uint64_t size) override;
  void Flushed(const VersionStorageInfo* vstorage,
               const FileMetaData& file) override;

 private:
  struct SortedRunInfo {
    std::vector<uint64_t> sizes;
    Compaction* c;
    SortedRunInfo(std::vector<uint64_t>&& _sizes)
        : sizes(std::move(_sizes)), c(nullptr) {}
    bool is_merging() const { return sizes.size() > 1; }
  };

  void print_status() const;

  std::optional<std::pair<size_t, size_t>> PickCompaction(
      const MutableCFOptions& mutable_cf_options,
      VersionStorageInfo* vstorage) const;

  bool SetIOPriorityOfLastCompaction(std::list<SortedRunInfo>::const_iterator
                                         last_sorted_run_picked_for_compaction);

  std::list<SortedRunInfo> sorted_runs_;
};
}  // namespace ROCKSDB_NAMESPACE
