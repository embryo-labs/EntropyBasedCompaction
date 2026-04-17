#pragma once

#include <cstdint>

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerVertiorizon : public CompactionPicker {
 public:
  CompactionPickerVertiorizon(const ImmutableOptions& ioptions,
                              const InternalKeyComparator* icmp)
      : CompactionPicker(ioptions, icmp),
        horizontal_levels_(
            ioptions.num_horizontal_levels,
            HorizontalLevelInfo{.compaction_counter = (int64_t)ioptions.k}),
        compaction_to_l1_in_progress_(false),
        vstorage_(nullptr) {
    if (ioptions.num_horizontal_levels == 0) {
      fprintf(stderr, "Error: num_horizontal_levels not specified!\n");
      std::abort();
    }
    if (ioptions.k == 0) {
      fprintf(stderr, "Error: k not specified!\n");
      std::abort();
    }
    n_ = 1;
    for (uint64_t i = 1; i <= ioptions.k; ++i) {
      // C(k + l - i, i) is integer, so every intermediate result is integer
      n_ = n_ * (ioptions.k + ioptions.num_horizontal_levels - i) / i;
    }
  }

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
  struct SortedRunInfo {
    // > 1 means merging
    size_t num_sorted_runs;
    uint64_t size;

    bool is_merging() const { return num_sorted_runs > 1; }
  };
  struct HorizontalLevelInfo {
    int64_t compaction_counter;
    std::deque<SortedRunInfo> runs;
  };

  uint64_t level1_capacity(const MutableCFOptions& mutable_cf_options) const;
  std::vector<CompactionInputFiles> PickCompactionToL2(
      const MutableCFOptions& mutable_cf_options, VersionStorageInfo* vstorage);

  struct PickResult {
    std::vector<HorizontalLevelInfo>::iterator it_start_level, it_next_level;
    std::vector<CompactionInputFiles> inputs;
  };
  std::optional<PickResult> PickFilesToCompact(
      const MutableCFOptions& mutable_cf_options, VersionStorageInfo* vstorage);

  void print_status();

  uint64_t n_;

  std::vector<HorizontalLevelInfo> horizontal_levels_;
  bool compaction_to_l1_in_progress_;
  const VersionStorageInfo* vstorage_;
};

}  // namespace ROCKSDB_NAMESPACE
