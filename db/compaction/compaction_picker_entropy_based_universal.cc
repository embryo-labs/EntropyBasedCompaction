#include "db/compaction/compaction_picker_entropy_based_universal.h"

#include "utilities/entropy.h"

namespace ROCKSDB_NAMESPACE {

void CompactionPickerEntropyBasedUniversal::print_status() const {
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

bool CompactionPickerEntropyBasedUniversal::CalcNeedsCompaction(
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
bool CompactionPickerEntropyBasedUniversal::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

class CandidatePicker {
 public:
  CandidatePicker(const MutableCFOptions& mutable_cf_options,
                  VersionStorageInfo* vstorage)
      : mutable_cf_options_(mutable_cf_options),
        l0_files_(vstorage->LevelFiles(0)),
        i_(0) {}

  std::optional<std::pair<size_t, size_t>> Pick() {
    while (i_ < l0_files_.size()) {
      auto file = l0_files_[i_];
      if (file->being_compacted) {
        i_ += 1;
        continue;
      }
      uint64_t size_sum = file->fd.file_size;
      size_t j = i_ + 1;
      for (; j < l0_files_.size(); ++j) {
        file = l0_files_[j];
        if (file->being_compacted ||
            file->fd.file_size >
                size_sum *
                    (100 + mutable_cf_options_.compaction_options_universal
                               .size_ratio) /
                    100) {
          break;
        }
        size_sum += file->fd.file_size;
      }
      j -= 1;
      auto i = i_;
      i_ += 1;
      if (j < l0_files_.size() && (j - i + 1) >= 2) {
        return std::make_pair(i, j);
      }
    }
    return std::nullopt;
  }

 private:
  const MutableCFOptions& mutable_cf_options_;
  const std::vector<FileMetaData*>& l0_files_;
  size_t i_;
};

Compaction* CompactionPickerEntropyBasedUniversal::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  assert(vstorage->num_levels() == 1);

  struct BestInfo {
    size_t first;
    size_t last;
    double reff;
  };
  std::optional<BestInfo> best;
  CandidatePicker picker(mutable_cf_options, vstorage);
  for (;;) {
    auto picked = picker.Pick();
    if (!picked.has_value()) {
      break;
    }
    ReffCalculator calculator;
    auto [i, j] = picked.value();
    for (; i <= j; ++i) {
      calculator.add_sorted_run(vstorage->LevelFiles(0)[i]->fd.file_size);
    }
    double reff = calculator.calc();
    if (best.has_value() && best->reff >= reff) {
      continue;
    }
    best = BestInfo{
        .first = picked->first,
        .last = picked->second,
        .reff = reff,
    };
  }
  if (!best.has_value()) {
    return nullptr;
  }
  // Check preemption conditions
  for (const auto& sr : sorted_runs_) {
    if (sr.is_merging()) {
      if (mutable_cf_options.preempt_by_entropy) {
        ReffCalculator calculator;
        for (uint64_t size : sr.sizes) {
          calculator.add_sorted_run(size);
        }
        double reff = calculator.calc();
        if (best->reff <
            reff * mutable_cf_options.k_e + mutable_cf_options.b_e - 1e-6) {
          return nullptr;
        }
      }
    }
  }

  std::vector<CompactionInputFiles> inputs(1);
  inputs[0].level = 0;
  for (size_t i = best->first; i <= best->last; ++i) {
    inputs[0].files.push_back(vstorage->LevelFiles(0)[i]);
  }

  std::optional<BackgroundJobLimiter::CompactionHandle> limiter_handle;
  if (ioptions_.background_job_limiter) {
    limiter_handle =
        ioptions_.background_job_limiter->RegisterCompaction(Reff(inputs));
    if (!limiter_handle.has_value()) {
      return nullptr;
    }
  }

  auto first = sorted_runs_.begin();
  auto i = best->first;
  while (i > 0) {
    assert(i >= first->sizes.size());
    i -= first->sizes.size();
    ++first;
  }
  auto last = first;
  for (const auto file : inputs[0].files) {
    assert(!last->is_merging());
    assert(last->sizes[0] == file->fd.file_size);
    ++last;
  }
  --last;
  auto it = sorted_runs_.erase(first, last);
  it->sizes.clear();
  for (const auto file : inputs[0].files) {
    it->sizes.push_back(file->fd.file_size);
  }
  print_status();

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
      BlobGarbageCollectionPolicy::kUseDefault, -1, &*it,
      Env::IOPriority::IO_LOW);
}

void CompactionPickerEntropyBasedUniversal::CompactionFinished(
    const VersionStorageInfo* vstorage, void* info, uint64_t size) {
  vstorage_ = vstorage;
  auto sr = static_cast<SortedRunInfo*>(info);
  sr->sizes.clear();
  sr->sizes.push_back(size);
  sr->sizes.shrink_to_fit();
  print_status();
}
void CompactionPickerEntropyBasedUniversal::Flushed(
    const VersionStorageInfo* vstorage, const FileMetaData& file) {
  vstorage_ = vstorage;
  // raw_key_size and raw_value_size seem to be zero
  sorted_runs_.emplace_front(std::vector<uint64_t>({file.fd.GetFileSize()}));
  print_status();
}

}  // namespace ROCKSDB_NAMESPACE
