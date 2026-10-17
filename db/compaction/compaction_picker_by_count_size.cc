#include "db/compaction/compaction_picker_by_count_size.h"

#include <cinttypes>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

void CompactionPickerByCountSize::print_status() {
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
    printf("%" PRIu64 " %" PRIu64, it->count, it->size);
    ++it;
    if (it == sorted_runs_.end()) {
      printf("]\n");
      fflush(stdout);
      break;
    }
    printf(", ");
  }
}

bool CompactionPickerByCountSize::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  if (vstorage_ != vstorage) {
    // sorted_runs_ not synced yet. Will try again shortly.
    return false;
  }
  assert(vstorage->num_levels() == 1);
  return PickRunsToCompact(mutable_cf_options).has_value();
}
bool CompactionPickerByCountSize::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

void CompactionPickerByCountSize::CompactionFinished(
    const VersionStorageInfo* vstorage, void* merging_info, uint64_t size) {
  vstorage_ = vstorage;
  auto info = static_cast<SortedRunInfo*>(merging_info);
  assert(info->is_merging());
  info->num_sorted_runs = 1;
  info->size = size;
  print_status();
}

void CompactionPickerByCountSize::Flushed(const VersionStorageInfo* vstorage,
                                          const FileMetaData& file) {
  vstorage_ = vstorage;
  sorted_runs_.push_front(SortedRunInfo{
      .num_sorted_runs = 1, .count = 1, .size = file.fd.file_size});
  print_status();
}

Compaction* CompactionPickerByCountSize::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  assert(vstorage->num_levels() == 1);
  auto ret = PickRunsToCompact(mutable_cf_options);
  if (!ret.has_value()) return nullptr;

  const auto pick_result = ret.value();
  std::vector<CompactionInputFiles> inputs(1);
  inputs[0].level = 0;
  const auto& l0_files = vstorage->LevelFiles(0);
  size_t file_i = 0;
  auto it = sorted_runs_.begin();
  while (it != pick_result.first) {
    file_i += it->num_sorted_runs;
    assert(file_i < l0_files.size());
    ++it;
  }

  auto first_to_erase = pick_result.first;
  auto i = pick_result.first;
  uint64_t num = 0;
  uint64_t count = 0;
  uint64_t size = 0;
  for (;;) {
    num += 1;
    count += i->count;
    size += i->size;
    auto file = l0_files.at(file_i);
    assert(!file->being_compacted);
    inputs[0].files.push_back(file);
    if (i == pick_result.last) {
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

  it = sorted_runs_.erase(first_to_erase, pick_result.last);
  assert(it == pick_result.last);
  it->num_sorted_runs = num;
  it->count = count;
  it->size = size;
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
      BlobGarbageCollectionPolicy::kUseDefault, -1, &*it, Env::IO_LOW,
      pick_result.bloom_millibits_per_key);
}

}  // namespace ROCKSDB_NAMESPACE
