#include "db/compaction/compaction_picker_by_count.h"

namespace ROCKSDB_NAMESPACE {

bool CompactionPickerByCount::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  if (vstorage_ != vstorage) {
    // sorted_runs_ not synced yet. Will try again shortly.
    return false;
  }
  assert(vstorage->num_levels() == 1);
  auto policy = mutable_cf_options.compaction_policy;
  return policy->PickCompaction(sorted_runs_).has_value();
}
bool CompactionPickerByCount::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

void CompactionPickerByCount::CompactionFinished(
    const VersionStorageInfo* vstorage, void* merging_info, uint64_t size) {
  vstorage_ = vstorage;
  auto info =
      static_cast<CompactionPolicyByCount::SortedRunInfo*>(merging_info);
  assert(info->is_merging());
  info->num_sorted_runs = 1;
  CompactionPolicyByCount::print(sorted_runs_);
}

void CompactionPickerByCount::Flushed(const VersionStorageInfo* vstorage,
                                      const FileMetaData& file) {
  vstorage_ = vstorage;
  sorted_runs_.push_front(
      CompactionPolicyByCount::SortedRunInfo{.num_sorted_runs = 1, .count = 1});
  CompactionPolicyByCount::print(sorted_runs_);
}

Compaction* CompactionPickerByCount::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  assert(vstorage->num_levels() == 1);
  auto policy = mutable_cf_options.compaction_policy;
  assert(policy != nullptr);
  auto ret = policy->PickCompaction(sorted_runs_);
  if (!ret.has_value()) return nullptr;

  auto [i, j] = ret.value();
  std::vector<CompactionInputFiles> inputs(1);
  inputs[0].level = 0;
  const auto& l0_files = vstorage->LevelFiles(0);
  size_t file_i = 0;
  auto it = sorted_runs_.begin();
  while (it != i) {
    file_i += it->num_sorted_runs;
    assert(file_i < l0_files.size());
    ++it;
  }

  auto first_to_erase = i;
  uint64_t num = 0;
  uint64_t count = 0;
  for (;;) {
    num += 1;
    count += i->count;
    auto file = l0_files.at(file_i);
    assert(!file->being_compacted);
    inputs[0].files.push_back(file);
    if (i == j) {
      break;
    }
    ++i;
    ++file_i;
  }

  std::optional<BackgroundJobLimiter::CompactionHandle> limiter_handle;
  if (ioptions_.background_job_limiter) {
    limiter_handle =
        ioptions_.background_job_limiter->RegisterCompaction(Reff(inputs));
    if (!limiter_handle.has_value()) {
      return nullptr;
    }
  }

  CompactionPolicyByCount::SortedRunInfo* merging_info;
  it = sorted_runs_.erase(first_to_erase, j);
  assert(it == j);
  it->num_sorted_runs = num;
  it->count = count;
  policy->print(sorted_runs_);
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), 0,
      MaxFileSizeForLevel(mutable_cf_options, 0, kCompactionStyleUniversal),
      std::numeric_limits<uint64_t>::max(),
      GetCompressionType(vstorage, mutable_cf_options, 0, 1,
                         /* multi path not supported yet */ false),
      /* compression not supported yet */ kNoCompression,
      GetCompressionOptions(mutable_cf_options, vstorage, 0,
                            /* compression not supported yet */ false),
      mutable_cf_options.default_write_temperature,
      /* max_subcompactions */ 0, /* grandparents not supported yet */ {},
      /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, std::move(limiter_handle),
      CompactionReason::kPolicyByCount, /* trim_ts */ "", /*score*/ 2333,
      /* l0_files_might_overlap */ true,
      BlobGarbageCollectionPolicy::kUseDefault, -1, &*it, Env::IO_LOW);
}

}  // namespace ROCKSDB_NAMESPACE
