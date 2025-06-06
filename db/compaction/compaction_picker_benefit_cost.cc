#include "db/compaction/compaction_picker_benefit_cost.h"

#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

// Compaction from class i to class i+1 has a priority of
// IO_LOW + num_classes - i
std::optional<std::pair<
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator,
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator>>
CompactionPickerBenefitCost::PickCompaction() const {
  printf("[");
  auto it = sorted_runs_.begin();
  if (it == sorted_runs_.end()) {
    return std::nullopt;
  }
  for (;;) {
    if (it->is_merging()) {
      printf("merging %zu ", it->num_sorted_runs);
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
  auto k = sorted_runs_.begin();
  while (k != sorted_runs_.end() && !k->is_merging()) {
    ++k;
  }
  double best_ratio_benefit;
  double best_ratio_num;
  if (k != sorted_runs_.end()) {
    assert(k->is_merging());
    best_ratio_benefit = k->benefit_cost_ratio;
    best_ratio_num = (double)k->num_sorted_runs / k->size;
  } else {
    best_ratio_benefit = threshold_;
    best_ratio_num = 0;
  }
  std::optional<std::pair<
      std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator,
      std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator>>
      best_range;
  for (;;) {
    if (k == sorted_runs_.begin()) {
      break;
    }
    --k;
    if (k == sorted_runs_.begin()) {
      break;
    }
    auto i = k;
    --i;
    auto j = k;
    double sum_nlog2n = k->size * log2(k->size);
    uint64_t size = k->size;
    size_t num = 1;
    for (;;) {
      sum_nlog2n += i->size * log2(i->size);
      size += i->size;
      num += 1;
      for (;;) {
        auto next = j;
        ++next;
        if (next == sorted_runs_.end() || next->is_merging() ||
            next->size >= size) {
          break;
        }
        j = next;
        sum_nlog2n += j->size * log2(j->size);
        size += j->size;
        num += 1;
      }
      double ratio_benefit = log2(size) - sum_nlog2n / size;
      double ratio_num = (double)num / size;
      if (ratio_benefit >= best_ratio_benefit) {
        if (ratio_benefit >= best_ratio_benefit + 1 ||
            ratio_num >= best_ratio_num * 2) {
          best_ratio_benefit = ratio_benefit;
          best_ratio_num = ratio_num;
          best_range = std::make_pair(i, j);
        }
      }
      if (i == sorted_runs_.begin()) {
        break;
      }
      auto prev = i;
      --prev;
      if ((std::pow(2, std::max(best_ratio_benefit, ratio_benefit)) + 1) *
              prev->size <=
          size * 0.5) {
        break;
      }
      --i;
    }
  }
  return best_range;
}

bool CompactionPickerBenefitCost::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  assert(vstorage->num_levels() == 1);
  return PickCompaction().has_value();
}
bool CompactionPickerBenefitCost::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

Compaction* CompactionPickerBenefitCost::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer) {
  auto ret = PickCompaction();
  if (!ret.has_value()) {
    return nullptr;
  }
  auto [i, j] = ret.value();
  int num_compactions = 0;
  for (const auto& sr : sorted_runs_) {
    if (sr.is_merging()) {
      num_compactions += 1;
    }
  }
  Env::IOPriority io_priority = static_cast<Env::IOPriority>(num_compactions);

  std::vector<CompactionInputFiles> inputs(1);
  inputs[0].level = 0;
  size_t file_i = 0;
  auto it = sorted_runs_.begin();
  while (it != i) {
    ++it;
    ++file_i;
  }

  const auto& l0_files = vstorage->LevelFiles(0);
  auto first_to_erase = i;
  double sum_nlog2n = 0;
  uint64_t size = 0;
  size_t num = 0;
  for (;;) {
    sum_nlog2n += i->size * log2(i->size);
    size += i->size;
    num += 1;
    auto file = l0_files[file_i];
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
  it->benefit_cost_ratio = log2(size) - sum_nlog2n / size;
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
      /* snapshot_checker */ nullptr,
      /* is manual */ false, /* trim_ts */ "", /*score*/ 2333,
      false /* deletion_compaction */,
      /* l0_files_might_overlap */ true, CompactionReason::kPolicyByCount,
      BlobGarbageCollectionPolicy::kUseDefault, -1, &*it, io_priority);
}

void CompactionPickerBenefitCost::CompactionFinished(void* info) {
  auto sr = static_cast<SortedRunInfo*>(info);
  sr->num_sorted_runs = 1;
  sr->benefit_cost_ratio = 0;
}

void CompactionPickerBenefitCost::Flushed(ReadOnlyMemTable* memtable) {
  auto& new_files = memtable->GetEdits()->GetNewFiles();
  assert(new_files.size() == 1);
  auto& new_file = new_files[0].second;
  // raw_key_size and raw_value_size seem to be zero
  sorted_runs_.emplace_front(new_file.fd.GetFileSize(), 1, 0);
}

}  // namespace ROCKSDB_NAMESPACE
