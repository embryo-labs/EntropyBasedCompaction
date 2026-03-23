#include "db/compaction/compaction_picker_entropy_based.h"

#include "utilities/entropy.h"

namespace ROCKSDB_NAMESPACE {

void CompactionPickerEntropyBased::print_status() const {
  {
    size_t num_sorted_runs = 0;
    for (const auto& sr : sorted_runs_) {
      num_sorted_runs += sr.sizes.size();
    }
    printf("%zu sorted runs: [", num_sorted_runs);
    for (auto it = sorted_runs_.begin();; ++it) {
      if (it == sorted_runs_.end()) {
        printf("]\n");
        fflush(stdout);
        break;
      }
      if (it != sorted_runs_.begin()) {
        printf(", ");
      }
      if (it->is_merging()) {
        printf("merging %zu Re %f [", it->sizes.size(), Reff(it->sizes));
        for (auto it_sizes = it->sizes.begin();; ++it_sizes) {
          if (it_sizes == it->sizes.end()) {
            printf("]");
            break;
          }
          if (it_sizes != it->sizes.begin()) {
            printf(", ");
          }
          printf("%" PRIu64, *it_sizes);
        }
      } else {
        printf("%" PRIu64, it->sizes[0]);
      }
    }
  }
}

bool CompactionPickerEntropyBased::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  assert(vstorage->num_levels() == 1);
  size_t num_sr_not_compacted = 0;
  for (const auto file : vstorage->LevelFiles(0)) {
    if (!file->being_compacted) {
      num_sr_not_compacted += 1;
    }
  }
  return (int)num_sr_not_compacted >=
         mutable_cf_options.level0_file_num_compaction_trigger;
}
bool CompactionPickerEntropyBased::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

struct CandidateInfo {
  size_t first, last;
  double reff;
  uint64_t size_sum;
};

class CandidatePicker {
 public:
  virtual ~CandidatePicker() = default;
  virtual std::optional<CandidateInfo> Pick() = 0;
};

class CandidatePickerEnumerate : public CandidatePicker {
 public:
  CandidatePickerEnumerate(const MutableCFOptions& mutable_cf_options,
                           const std::vector<FileMetaData*>& l0_files)
      : mutable_cf_options_(mutable_cf_options),
        l0_files_(l0_files),
        i_(0),
        j_(1) {
    if (l0_files_.size() > 0) {
      calculator_.add_sorted_run(l0_files_[0]->fd.file_size);
    }
  }
  std::optional<CandidateInfo> Pick() override {
    if (i_ == l0_files_.size()) {
      return std::nullopt;
    }
    for (;;) {
      auto first_file = l0_files_[i_];
      if (!first_file->being_compacted) {
        uint64_t first_file_size = first_file->fd.file_size;
        for (; j_ < l0_files_.size(); j_ += 1) {
          auto file = l0_files_[j_];
          if (file->being_compacted) {
            break;
          }
          calculator_.add_sorted_run(file->fd.file_size);
          if (j_ - i_ + 1 > 2 &&
              mutable_cf_options_.exclude_small_sorted_runs &&
              first_file_size <=
                  (double)(calculator_.size_sum() - first_file_size) /
                      (j_ - i_) / mutable_cf_options_.k_n) {
            continue;
          }
          assert(j_ - i_ + 1 >= 2);
          auto candidate = CandidateInfo{
              .first = i_,
              .last = j_,
              .reff = calculator_.reff(),
              .size_sum = calculator_.size_sum(),
          };
          j_ += 1;
          return candidate;
        }
      }
      i_ += 1;
      if (i_ == l0_files_.size()) {
        break;
      }
      j_ = i_ + 1;
      calculator_ = ReffCalculator();
      calculator_.add_sorted_run(l0_files_[i_]->fd.file_size);
    }
    return std::nullopt;
  }

 private:
  const MutableCFOptions& mutable_cf_options_;
  const std::vector<FileMetaData*>& l0_files_;
  size_t i_;
  size_t j_;
  ReffCalculator calculator_;
};

class CandidatePickerUniversal : public CandidatePicker {
 public:
  CandidatePickerUniversal(const MutableCFOptions& mutable_cf_options,
                           const std::vector<FileMetaData*>& l0_files)
      : mutable_cf_options_(mutable_cf_options), l0_files_(l0_files), i_(0) {}

  std::optional<CandidateInfo> Pick() override {
    while (i_ < l0_files_.size()) {
      auto file = l0_files_[i_];
      if (file->being_compacted) {
        i_ += 1;
        continue;
      }
      uint64_t first_file_size = file->fd.file_size;
      ReffCalculator calculator;
      calculator.add_sorted_run(first_file_size);
      size_t j = i_ + 1;
      for (; j < l0_files_.size(); ++j) {
        file = l0_files_[j];
        if (file->being_compacted ||
            file->fd.file_size >
                calculator.size_sum() *
                    (100 + mutable_cf_options_.compaction_options_universal
                               .size_ratio) /
                    100) {
          break;
        }
        if (j - i_ + 1 > 2 && mutable_cf_options_.exclude_small_sorted_runs &&
            first_file_size <= (double)(calculator.size_sum() -
                                        first_file_size + file->fd.file_size) /
                                   (j - i_) / mutable_cf_options_.k_n) {
          break;
        }
        calculator.add_sorted_run(file->fd.file_size);
      }
      j -= 1;
      auto i = i_;
      i_ += 1;
      if (j < l0_files_.size() && (j - i + 1) >= 2) {
        return CandidateInfo{
            .first = i,
            .last = j,
            .reff = calculator.reff(),
            .size_sum = calculator.size_sum(),
        };
      }
    }
    return std::nullopt;
  }

 private:
  const MutableCFOptions& mutable_cf_options_;
  const std::vector<FileMetaData*>& l0_files_;
  size_t i_;
};

std::optional<std::pair<size_t, size_t>>
CompactionPickerEntropyBased::PickCompaction(
    const MutableCFOptions& mutable_cf_options,
    VersionStorageInfo* vstorage) const {
  assert(vstorage->num_levels() == 1);

  std::optional<CandidateInfo> best;
  const auto& l0_files = vstorage->LevelFiles(0);
  std::unique_ptr<CandidatePicker> picker;
  if (mutable_cf_options.pick_candidates_with_universal) {
    picker = std::make_unique<CandidatePickerUniversal>(mutable_cf_options,
                                                        l0_files);
  } else {
    picker = std::make_unique<CandidatePickerEnumerate>(mutable_cf_options,
                                                        l0_files);
  }
  for (;;) {
    auto picked = picker->Pick();
    if (!picked.has_value()) {
      break;
    }
    auto candidate = picked.value();
    if (!mutable_cf_options.pick_best_reff) {
      assert(!best.has_value());
      best = candidate;
      break;
    }
    if (best.has_value() && best->reff >= candidate.reff) {
      continue;
    }
    best = candidate;
  }
  if (!best.has_value()) {
    return std::nullopt;
  }
  if (best->reff < mutable_cf_options.entropy_threshold - 1e-6) {
    return std::nullopt;
  }
  // Check preemption conditions
  for (const auto& sr : sorted_runs_) {
    if (sr.is_merging()) {
      ReffCalculator calculator;
      for (uint64_t size : sr.sizes) {
        calculator.add_sorted_run(size);
      }
      if (mutable_cf_options.preempt_by_entropy) {
        double reff = calculator.reff();
        if (best->reff <
            reff * mutable_cf_options.k_e + mutable_cf_options.b_e - 1e-6) {
          return std::nullopt;
        }
      }
      if (mutable_cf_options.preempt_by_num) {
        double r_num = (double)(sr.sizes.size() - 1) / calculator.size_sum();
        if ((double)(best->last - best->first) / best->size_sum <
            r_num * mutable_cf_options.k_n) {
          return std::nullopt;
        }
      }
    }
  }
  return std::make_pair(best->first, best->last);
}

bool CompactionPickerEntropyBased::SetIOPriorityOfLastCompaction(
    std::list<CompactionPickerEntropyBased::SortedRunInfo>::const_iterator
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

Compaction* CompactionPickerEntropyBased::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  auto best = PickCompaction(mutable_cf_options, vstorage);
  if (!best.has_value()) {
    if (mutable_cf_options.last_compaction_io_sa) {
      SetIOPriorityOfLastCompaction(sorted_runs_.end());
    }
    return nullptr;
  }
  const auto [first, last] = best.value();

  std::vector<CompactionInputFiles> inputs(1);
  inputs[0].level = 0;
  for (size_t i = first; i <= last; ++i) {
    inputs[0].files.push_back(vstorage->LevelFiles(0)[i]);
  }

  auto it_first = sorted_runs_.begin();
  auto i = best->first;
  while (i > 0) {
    assert(i >= it_first->sizes.size());
    i -= it_first->sizes.size();
    ++it_first;
  }
  auto it_last = it_first;
  for (const auto file : inputs[0].files) {
    assert(!it_last->is_merging());
    assert(it_last->sizes[0] == file->fd.file_size);
    ++it_last;
  }
  --it_last;

  Env::IOPriority io_pri;
  if (mutable_cf_options.last_compaction_io_sa) {
    if (SetIOPriorityOfLastCompaction(it_last)) {
      io_pri = Env::IO_LOW;
    } else {
      // The picked compaction will be the last compaction, which always runs.
      // Its write I/O is controlled by max_bytes_per_second_io_sa
      io_pri = Env::IO_SA;
    }
  } else {
    io_pri = Env::IO_LOW;
  }

  std::optional<BackgroundJobLimiter::CompactionHandle> limiter_handle;
  if (io_pri == Env::IO_LOW) {
    if (ioptions_.background_job_limiter) {
      limiter_handle =
          ioptions_.background_job_limiter->RegisterCompaction(Reff(inputs));
      if (!limiter_handle.has_value()) {
        return nullptr;
      }
    }
  }

  auto it = sorted_runs_.erase(it_first, it_last);
  it->sizes.clear();
  for (const auto file : inputs[0].files) {
    it->sizes.push_back(file->fd.file_size);
  }
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
      BlobGarbageCollectionPolicy::kUseDefault, -1, &*it,
      io_pri);
  it->c = c;
  print_status();
  RegisterCompaction(c);
  return c;
}

void CompactionPickerEntropyBased::CompactionFinished(
    const VersionStorageInfo* vstorage, void* info, uint64_t size) {
  auto sr = static_cast<SortedRunInfo*>(info);
  sr->sizes.clear();
  sr->sizes.push_back(size);
  sr->sizes.shrink_to_fit();
  sr->c = nullptr;
  print_status();
}
void CompactionPickerEntropyBased::Flushed(const VersionStorageInfo* vstorage,
                                           const FileMetaData& file) {
  // raw_key_size and raw_value_size seem to be zero
  sorted_runs_.emplace_front(std::vector<uint64_t>({file.fd.GetFileSize()}));
  print_status();
}

}  // namespace ROCKSDB_NAMESPACE
