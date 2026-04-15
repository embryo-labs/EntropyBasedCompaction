#include "db/compaction/compaction_picker_by_count.h"

namespace ROCKSDB_NAMESPACE {

void CompactionPickerByCount::print_status() {
  printf("[");
  auto it = sorted_runs_.begin();
  if (it == sorted_runs_.end()) {
    printf("]\n");
    fflush(stdout);
    return;
  }
  for (;;) {
    if (it->is_merging()) {
      printf("merging %zu ", it->num_sorted_runs);
    }
    printf("%" PRIu64, it->count);
    ++it;
    if (it == sorted_runs_.end()) {
      printf("]\n");
      fflush(stdout);
      break;
    }
    printf(", ");
  }
}

bool CompactionPickerByCount::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  if (vstorage_ != vstorage) {
    // sorted_runs_ not synced yet. Will try again shortly.
    return false;
  }
  assert(vstorage->num_levels() == 1);
  return PickRunsToCompact(mutable_cf_options).has_value();
}
bool CompactionPickerByCount::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

void CompactionPickerByCount::CompactionFinished(
    const VersionStorageInfo* vstorage, void* merging_info, uint64_t size) {
  vstorage_ = vstorage;
  auto info = static_cast<SortedRunInfo*>(merging_info);
  assert(info->is_merging());
  info->num_sorted_runs = 1;
  print_status();
}

void CompactionPickerByCount::Flushed(const VersionStorageInfo* vstorage,
                                      const FileMetaData& file) {
  vstorage_ = vstorage;
  sorted_runs_.push_front(SortedRunInfo{.num_sorted_runs = 1, .count = 1});
  print_status();
}

Compaction* CompactionPickerByCount::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  assert(vstorage->num_levels() == 1);
  auto ret = PickRunsToCompact(mutable_cf_options);
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

  SortedRunInfo* merging_info;
  it = sorted_runs_.erase(first_to_erase, j);
  assert(it == j);
  it->num_sorted_runs = num;
  it->count = count;
  print_status();
  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), 0,
      MaxFileSizeForLevel(mutable_cf_options, 0, kCompactionStyleUniversal),
      std::numeric_limits<uint64_t>::max(), 0,
      GetCompressionType(vstorage, mutable_cf_options, 0,
                         vstorage->base_level()),
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
