#pragma once

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerBenefitCost : public CompactionPicker {
 public:
  CompactionPickerBenefitCost(const ImmutableOptions& ioptions,
                              const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp), vstorage_(nullptr) {}

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
      LogBuffer* log_buffer, bool require_max_output_level) override;

  void CompactionFinished(const VersionStorageInfo* vstorage, void* info,
                          uint64_t size) override;
  void Flushed(const VersionStorageInfo* vstorage,
               const FileMetaData& file) override;

 private:
  struct SortedRunInfo {
    // > 1 means merging
    size_t num_sorted_runs;
    uint64_t size;
    double r_e;
    Compaction* c;

    SortedRunInfo(size_t _num_sorted_runs, uint64_t _size,
                  double _benefit_cost_ratio)
        : num_sorted_runs(_num_sorted_runs),
          size(_size),
          r_e(_benefit_cost_ratio),
          c(nullptr) {}

    bool is_merging() const { return num_sorted_runs > 1; }
  };

  void print_status() const;

  std::optional<std::pair<
      std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator,
      std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator>>
  PickCompaction(const MutableCFOptions& mutable_cf_options) const;

  // Return succeed or not
  bool SetIOPriorityOfLastCompaction(
      std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator
          last_sorted_run_picked_for_compaction);

  // Ideally this should be in vstorage
  std::list<SortedRunInfo> sorted_runs_;
  const VersionStorageInfo* vstorage_;
};

}  // namespace ROCKSDB_NAMESPACE
