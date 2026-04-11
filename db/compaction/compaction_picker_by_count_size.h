#pragma once

#include <cstdint>
#include <list>
#include <optional>

#include "db/compaction/compaction_picker.h"

namespace ROCKSDB_NAMESPACE {

class CompactionPickerByCountSize : public CompactionPicker {
 public:
  CompactionPickerByCountSize(const ImmutableOptions& ioptions,
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

 protected:
  struct SortedRunInfo {
    // > 1 means merging
    size_t num_sorted_runs;
    // The count of merged flushed SSTables
    uint64_t count;
    uint64_t size;

    bool is_merging() const { return num_sorted_runs > 1; }
  };

  // REQUIRES: DB mutex held
  // Returns [a, b]: status[a] to status[b] should be compacted into one
  // sorted run.
  virtual std::optional<std::pair<std::list<SortedRunInfo>::const_iterator,
                                  std::list<SortedRunInfo>::const_iterator>>
  PickRunsToCompact(const MutableCFOptions& mutable_cf_options) const = 0;

  std::list<SortedRunInfo> sorted_runs_;

 private:
  void print_status();

  const VersionStorageInfo* vstorage_;
};

}  // namespace ROCKSDB_NAMESPACE
