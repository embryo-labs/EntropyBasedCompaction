#include "db/compaction/compaction_picker_by_count.h"

namespace ROCKSDB_NAMESPACE {

bool CompactionPickerByCount::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  if (vstorage->num_levels() > 1) {
    assert(vstorage->num_levels() == 3);
    if (!vstorage->LevelFiles(1).empty()) {
      return true;
    }
  }
  CompactionPolicyByCount* policy =
      std::get<0>(mutable_cf_options.compaction_policy.value()).get();
  return policy->PickCompaction(sorted_runs_).has_value();
}
bool CompactionPickerByCount::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

Compaction* CompactionPickerByCount::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  assert(vstorage->num_levels() == 1);
  CompactionPolicyByCount::print(sorted_runs_);
  fflush(stdout);
  CompactionPolicyByCount* policy =
      std::get<0>(mutable_cf_options.compaction_policy.value()).get();
  assert(policy != nullptr);
  auto ret = policy->PickCompaction(sorted_runs_);
  if (!ret.has_value()) return nullptr;

  std::optional<BackgroundJobLimiter::CompactionHandle> limiter_handle;
  if (ioptions_.background_job_limiter) {
    limiter_handle = ioptions_.background_job_limiter->RegisterCompaction();
    if (!limiter_handle.has_value()) {
      return nullptr;
    }
  }

  auto [a, b, io_priority] = ret.value();
  std::vector<CompactionInputFiles> inputs(1);
  int output_level;
  if (b == sorted_runs_.end()) {
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
  auto it = sorted_runs_.begin();
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
  CompactionPolicyByCount::SortedRunInfo* merging_info;
  if (output_level == 0) {
    // Erase [a, b)
    auto b_mut = sorted_runs_.erase(a, b);
    b_mut->merging = true;
    b_mut->count = size;
    merging_info = &*b_mut;
  } else {
    sorted_runs_.erase(a, sorted_runs_.end());
    merging_info = nullptr;
  }
  policy->print(sorted_runs_);
  fflush(stdout);
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
      /* snapshot_checker */ nullptr, std::move(limiter_handle),
      CompactionReason::kPolicyByCount, /* trim_ts */ "", /*score*/ 2333,
      /* l0_files_might_overlap */ true,
      BlobGarbageCollectionPolicy::kUseDefault, -1, merging_info, io_priority);
}

void CompactionPickerByCount::CompactionFinished(
    const VersionStorageInfo* vstorage, void* merging_info, uint64_t size) {
  auto info =
      static_cast<CompactionPolicyByCount::SortedRunInfo*>(merging_info);
  assert(info->merging);
  info->merging = false;
  CompactionPolicyByCount::print(sorted_runs_);
  fflush(stdout);
}

void CompactionPickerByCount::Flushed(const VersionStorageInfo* vstorage,
                                      const FileMetaData& file) {
  sorted_runs_.push_front(CompactionPolicyByCount::SortedRunInfo{false, 1});
}

}  // namespace ROCKSDB_NAMESPACE
