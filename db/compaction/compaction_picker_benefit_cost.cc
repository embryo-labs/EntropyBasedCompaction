#include "db/compaction/compaction_picker_benefit_cost.h"

#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

void CompactionPickerBenefitCost::print_status() const {
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
        if (it->c) {
          printf("pri %d ",
                 it->c->io_priority().load(std::memory_order_relaxed));
        }
      }
      printf("%" PRIu64, it->size);
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
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator,
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator>>
CompactionPickerBenefitCost::PickCompaction(
    const MutableCFOptions& mutable_cf_options) const {
  const double entropy_threshold = mutable_cf_options.entropy_threshold;
  constexpr double k_n = 1.7;
  struct BestInfo {
    double r_e;
    double r_n;
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator first;
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator last;
  };
  std::optional<BestInfo> best;
  std::vector<
      std::reference_wrapper<const CompactionPickerBenefitCost::SortedRunInfo>>
      existing_compactions;
  auto last_sorted_run = sorted_runs_.end();
  if (last_sorted_run == sorted_runs_.begin()) {
    return std::nullopt;
  }
  --last_sorted_run;
  for (auto i = sorted_runs_.begin(); i != sorted_runs_.end();) {
    if (i->is_merging()) {
      existing_compactions.push_back(std::ref(*i));
      ++i;
      continue;
    }
    uint64_t size = i->size;
    size_t num = 1;
    double sum_nlog2n = i->size * log2(i->size);
    auto j = i;
    ++j;
    for (; j != sorted_runs_.end() && !j->is_merging(); ++j) {
      size += j->size;
      num += 1;
      sum_nlog2n += j->size * log2(j->size);
      if (mutable_cf_options.sliding_window) {
        while (num > 2 &&
               i->size <= (double)(size - i->size) / (num - 1) / k_n) {
          size -= i->size;
          num -= 1;
          sum_nlog2n -= i->size * log2(i->size);
          ++i;
        }
      }
      double r_e = log2(size) - sum_nlog2n / size;
      if (r_e < entropy_threshold - 1e-6) {
        continue;
      }
      bool better;
      if (!best.has_value()) {
        better = true;
      } else {
        bool decided = false;
        if (mutable_cf_options.preempt_by_entropy) {
          if (best->r_e < r_e - 1e-6) {
            decided = true;
            better = true;
          } else if (best->r_e > r_e + 1e-6) {
            decided = true;
            better = false;
          }
        }
        if (!decided) {
          if (mutable_cf_options.preempt_by_num) {
            better = best->r_n < (double)(num - 1) / size;
          } else {
            better = false;
          }
        }
      }
      if (better) {
        best = BestInfo{
            .r_e = r_e,
            .r_n = (double)(num - 1) / size,
            .first = i,
            .last = j,
        };
      }
    }
    if (mutable_cf_options.sliding_window) {
      i = j;
    } else {
      ++i;
    }
  }
  if (!best.has_value()) {
    return std::nullopt;
  }
  if (best->last == last_sorted_run &&
      mutable_cf_options.prioritize_last_sorted_run_compaction) {
    return std::make_pair(best->first, best->last);
  }
  // Check preemption conditions
  for (const auto& existing_compaction : existing_compactions) {
    const auto& c = existing_compaction.get();
    if (mutable_cf_options.preempt_by_entropy &&
        best->r_e < c.r_e * mutable_cf_options.k_e) {
      return std::nullopt;
    }
    if (mutable_cf_options.preempt_by_num &&
        best->r_n < (double)(c.num_sorted_runs - 1) / c.size * k_n) {
      return std::nullopt;
    }
  }
  return std::make_pair(best->first, best->last);
}

bool CompactionPickerBenefitCost::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  if (vstorage_ != vstorage) {
    // sorted_runs_ not synced yet. Will try again shortly.
    return false;
  }
  assert(vstorage->num_levels() == 1);
  return PickCompaction(mutable_cf_options).has_value();
}
bool CompactionPickerBenefitCost::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

bool CompactionPickerBenefitCost::SetIOPriorityOfLastCompaction(
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator
        last_sorted_run_picked_for_compaction) {
  auto it = sorted_runs_.end();
  if (it == sorted_runs_.begin()) {
    return false;
  }
  --it;
  while (it != last_sorted_run_picked_for_compaction) {
    if (it->is_merging()) {
      if (it->c->io_priority().load(std::memory_order_relaxed) != Env::IO_SA) {
        it->c->io_priority().store(Env::IO_SA, std::memory_order_relaxed);
        it->c->release_limiter_handle();
      }
      return true;
    }
    if (it == sorted_runs_.begin()) {
      break;
    }
    --it;
  }
  return false;
}

Compaction* CompactionPickerBenefitCost::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  auto ret = PickCompaction(mutable_cf_options);
  if (!ret.has_value()) {
    if (mutable_cf_options.last_compaction_io_sa) {
      SetIOPriorityOfLastCompaction(sorted_runs_.end());
    }
    return nullptr;
  }

  std::optional<BackgroundJobLimiter::CompactionHandle> limiter_handle;
  auto [i, j] = ret.value();
  Env::IOPriority io_pri;
  if (mutable_cf_options.last_compaction_io_sa) {
    if (SetIOPriorityOfLastCompaction(j)) {
      io_pri = Env::IO_LOW;
    } else {
      // The picked compaction will be the last compaction, which always runs.
      // Its write I/O is controlled by max_bytes_per_second_io_sa
      io_pri = Env::IO_SA;
    }
  } else {
    io_pri = Env::IO_LOW;
  }
  if (io_pri == Env::IO_LOW) {
    if (ioptions_.background_job_limiter) {
      limiter_handle = ioptions_.background_job_limiter->RegisterCompaction();
      if (!limiter_handle.has_value()) {
        return nullptr;
      }
    }
  }

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
  double sum_nlog2n = 0;
  uint64_t size = 0;
  size_t num = 0;
  for (;;) {
    sum_nlog2n += i->size * log2(i->size);
    size += i->size;
    num += 1;
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
  it->size = size;
  it->r_e = log2(size) - sum_nlog2n / size;
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
      BlobGarbageCollectionPolicy::kUseDefault, -1, &*it, io_pri);
  it->c = c;
  print_status();
  return c;
}

void CompactionPickerBenefitCost::CompactionFinished(
    const VersionStorageInfo* vstorage, void* info, uint64_t size) {
  vstorage_ = vstorage;
  auto sr = static_cast<SortedRunInfo*>(info);
  sr->num_sorted_runs = 1;
  sr->size = size;
  sr->r_e = 0;
  sr->c = nullptr;
  print_status();
}

void CompactionPickerBenefitCost::Flushed(const VersionStorageInfo* vstorage,
                                          const FileMetaData& file) {
  vstorage_ = vstorage;
  // raw_key_size and raw_value_size seem to be zero
  sorted_runs_.emplace_front(1, file.fd.GetFileSize(), 0);
  print_status();
}

}  // namespace ROCKSDB_NAMESPACE
