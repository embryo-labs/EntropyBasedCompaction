#include "db/compaction/compaction_picker_tiering.h"

#include <cassert>

namespace ROCKSDB_NAMESPACE {

bool CompactionPickerTiering::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  if (vstorage_ != vstorage) {
    // sorted_runs_ not synced yet. Will try again shortly.
    return false;
  }
  assert(vstorage->num_levels() == 1);
  return PickCompaction(mutable_cf_options).has_value();
}
bool CompactionPickerTiering::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

void CompactionPickerTiering::CompactionFinished(
    const VersionStorageInfo* vstorage, void* info, uint64_t size) {
  vstorage_ = vstorage;
  auto sr = static_cast<SortedRunInfo*>(info);
  sr->num_sorted_runs = 1;
  print_status();
}

void CompactionPickerTiering::Flushed(const VersionStorageInfo* vstorage,
                                      const FileMetaData& file) {
  vstorage_ = vstorage;
  sorted_runs_.emplace_front(1, 0, 1);
  print_status();
}

Compaction* CompactionPickerTiering::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  auto ret = PickCompaction(mutable_cf_options);
  if (!ret.has_value()) {
    return nullptr;
  }
  std::optional<BackgroundJobLimiter::CompactionHandle> limiter_handle;
  if (ioptions_.background_job_limiter) {
    limiter_handle = ioptions_.background_job_limiter->RegisterCompaction();
    if (!limiter_handle.has_value()) {
      return nullptr;
    }
  }

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
    assert(i->compaction_times == first_to_erase->compaction_times);
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
  it = sorted_runs_.erase(first_to_erase, j);
  assert(it == j);
  it->num_sorted_runs = num;
  it->compaction_times = first_to_erase->compaction_times + 1;
  it->count = count;
  auto c = new Compaction(
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
  print_status();
  return c;
}

void CompactionPickerTiering::print_status() const {
  {
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
}

std::optional<std::pair<
    std::list<CompactionPickerTiering::SortedRunInfo>::const_iterator,
    std::list<CompactionPickerTiering::SortedRunInfo>::const_iterator>>
CompactionPickerTiering::PickCompaction(
    const MutableCFOptions& mutable_cf_options) const {
  std::unique_lock<std::mutex> lock(lock_, std::try_to_lock);
  assert(lock.owns_lock());
  if (!mutable_cf_options.autotune_size_ratio || T_ == 0) {
    T_ = mutable_cf_options.max_bytes_for_level_multiplier;
  }
  for (;;) {
    if (sorted_runs_.size() == 1) {
      auto& sr = *sorted_runs_.begin();
      sr.compaction_times = std::log(sr.count) / std::log(T_) + 0.5;
      printf("Only one sorted run. Reset its compaction times to %" PRIu64 "\n",
             sr.compaction_times);
    }
    auto i = sorted_runs_.begin();
    if (i == sorted_runs_.end()) {
      return std::nullopt;
    }
    bool T_increased = false;
    auto j = i;
    size_t num = 0;
    while (j != sorted_runs_.end()) {
      if (j->is_merging()) {
        assert(num <= T_);
        if (num == T_) {
          if (j->compaction_times > i->compaction_times + 1) {
            --j;
            return std::make_pair(i, j);
          }
          assert((j->compaction_times == i->compaction_times + 1));
          if (mutable_cf_options.autotune_size_ratio) {
            T_ += 1;
            printf("Congested. Increase T to %zu\n", T_);
          }
        }
        return std::nullopt;
      }
      if (j->compaction_times > i->compaction_times) {
        if (num == T_) {
          --j;
          return std::make_pair(i, j);
        }
        assert(num < T_);
        i = j;
        ++j;
        num = 1;
        continue;
      }
      assert(j->compaction_times == i->compaction_times);
      assert(num <= T_);
      if (num == T_) {
        if (mutable_cf_options.autotune_size_ratio) {
          T_ += 1;
          T_increased = true;
          printf("Increase T to %zu\n", T_);
        } else {
          ++i;
          num -= 1;
        }
      }
      num += 1;
      ++j;
    }
    if (num == T_) {
      --j;
      return std::make_pair(i, j);
    }
    assert(num < T_);
    if (!mutable_cf_options.autotune_size_ratio) {
      return std::nullopt;
    }
    assert(!T_increased);
    if (T_ == 2) {
      return std::nullopt;
    }
    assert(T_ > 2);
    T_ -= 1;
    printf("Decrease T to %zu\n", T_);
  }
}

}  // namespace ROCKSDB_NAMESPACE
