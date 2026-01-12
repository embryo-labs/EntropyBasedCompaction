#include "db/compaction/compaction_picker_by_size.h"

namespace ROCKSDB_NAMESPACE {

bool CompactionPickerBySize::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  assert(vstorage->num_levels() == 3);
  if (!vstorage->LevelFiles(1).empty()) {
    return true;
  }
  const auto& l0_files = vstorage->LevelFiles(0);
  std::vector<CompactionPolicyBySize::SortedRunInfo> status;
  for (const auto* file : l0_files) {
    status.emplace_back(file->being_compacted, file->fd.file_size);
  }
  CompactionPolicyBySize* policy =
      std::get<1>(mutable_cf_options.compaction_policy.value()).get();
  return policy
      ->PickCompaction(mutable_cf_options.write_buffer_size, status,
                       vstorage->NumLevelBytes(vstorage->num_levels() - 1))
      .has_value();
}

bool CompactionPickerBySize::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

Compaction* CompactionPickerBySize::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  CompactionPolicyBySize* policy =
      std::get<1>(mutable_cf_options.compaction_policy.value()).get();
  assert(policy != nullptr);
  const auto& l0_files = vstorage->LevelFiles(0);
  std::vector<CompactionPolicyBySize::SortedRunInfo> status;
  for (const auto* file : l0_files) {
    status.emplace_back(file->being_compacted, file->fd.file_size);
  }
  auto ret = policy->PickCompaction(
      mutable_cf_options.write_buffer_size, status,
      vstorage->NumLevelBytes(vstorage->num_non_empty_levels() - 1));
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
  if (b == status.size()) {
    output_level = 1;
    --b;
    assert(vstorage->num_levels() >= 2);
    if (vstorage->NumLevelFiles(1) > 0) {
      // The last global compaction is not finished yet.
      RecordTick(cfd->ioptions().stats,
                 Tickers::WAIT_FOR_LAST_LEVEL_COMPACTION);
      return nullptr;
    }
  } else {
    output_level = 0;
  }
  inputs[0].level = 0;
  for (; a <= b; ++a) {
    assert(!status[a].merging);
    auto file = l0_files[a];
    assert(!file->being_compacted);
    inputs[0].files.push_back(file);
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
      /* snapshot_checker */ nullptr, std::move(limiter_handle),
      CompactionReason::kPolicyBySize, /* trim_ts */ "", /*score*/ 2333,
      /* l0_files_might_overlap */ true,
      BlobGarbageCollectionPolicy::kUseDefault, -1, nullptr, io_priority);
}

}  // namespace ROCKSDB_NAMESPACE
