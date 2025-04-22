#include "db/compaction/compaction_picker_by_count.h"

namespace ROCKSDB_NAMESPACE {

bool CompactionPickerByCount::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  if (vstorage->CompactionScore(0) >= 1) {
    return true;
  }
  assert(vstorage->num_levels() == 3);
  return !vstorage->LevelFiles(1).empty();
}

Compaction* CompactionPickerByCount::PickMainLevelCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  CompactionPolicyByCount* policy =
      std::get<0>(mutable_cf_options.compaction_policy.value()).get();
  assert(policy != nullptr);
  auto& sorted_run_status = cfd->sorted_run_status();
  cfd->print_sorted_run_status();
  auto ret = policy->PickCompaction(sorted_run_status);
  if (!ret.has_value()) return nullptr;
  auto [a, b] = ret.value();
  std::vector<CompactionInputFiles> inputs(1);
  int output_level;
  if (b == sorted_run_status.end()) {
    output_level = 1;
    --b;
    assert(vstorage->num_levels() >= 2);
    if (vstorage->NumLevelFiles(1) > 0) {
      // The last global compaction is not finished yet.
      return nullptr;
    }
  } else {
    output_level = 0;
  }
  inputs[0].level = 0;
  size_t i = 0;
  auto it = sorted_run_status.begin();
  const auto& l0_files = vstorage->LevelFiles(0);
  while (it != a) {
    assert(!it->merging);
    assert(!l0_files[i]->being_compacted);
    ++i;
    ++it;
  }
  size_t size = 0;
  for (;;) {
    assert(!it->merging);
    size += it->count;
    auto file = l0_files[i];
    assert(!file->being_compacted);
    inputs[0].files.push_back(file);
    if (it == b) break;
    ++i;
    ++it;
  }
  std::optional<std::list<CompactionPolicyByCount::SortedRunInfo>::iterator>
      merging_info;
  if (output_level == 0) {
    // Erase [a, b)
    auto b_mut = sorted_run_status.erase(a, b);
    b_mut->merging = true;
    b_mut->count = size;
    merging_info = std::make_optional(b_mut);
  } else {
    sorted_run_status.erase(a, sorted_run_status.end());
  }
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleUniversal),
      std::numeric_limits<uint64_t>::max(),
      GetCompressionType(vstorage, mutable_cf_options, output_level, 1,
                         /* multi path not supported yet */ false),
      /* compression not supported yet */ kNoCompression,
      GetCompressionOptions(mutable_cf_options, vstorage, output_level,
                            /* compression not supported yet */ false),
      mutable_cf_options.default_write_temperature,
      /* max_subcompactions */ 0, /* grandparents not supported yet */ {},
      /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr,
      /* is manual */ false, /* trim_ts */ "", /*score*/ 2333,
      false /* deletion_compaction */,
      /* l0_files_might_overlap */ true, CompactionReason::kPolicyByCount,
      BlobGarbageCollectionPolicy::kUseDefault, -1, merging_info,
      static_cast<Env::IOPriority>(output_level == 1
                                       ? Env::IOPriority::IO_LOW + 1
                                       : Env::IOPriority::IO_LOW + 2));
}

Compaction* CompactionPickerByCount::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  Compaction* c = PickMainLevelCompaction(
      cfd, cf_name, mutable_cf_options, mutable_db_options, existing_snapshots,
      snapshot_checker, vstorage, log_buffer);
  if (c) {
    return c;
  }
  return PickIncrementalLastLevelCompaction(cfd, cf_name, mutable_cf_options,
                                            mutable_db_options, vstorage);
}

}  // namespace ROCKSDB_NAMESPACE
