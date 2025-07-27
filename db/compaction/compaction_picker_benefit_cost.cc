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

// Compaction from class i to class i+1 has a priority of
// IO_LOW + num_classes - i
std::optional<std::pair<
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator,
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator>>
CompactionPickerBenefitCost::PickCompaction() const {
  constexpr double k_c = 1.7;
  struct BestInfo {
    double ratio_benefit;
    double ratio_num;
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator first;
    std::list<CompactionPickerBenefitCost::SortedRunInfo>::const_iterator last;
  };
  std::optional<BestInfo> best;
  auto i = sorted_runs_.begin();
  if (i == sorted_runs_.end() || i->is_merging()) {
    return std::nullopt;
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
    while (i->size < j->size && num > 2 &&
           i->size < (double)(size - i->size) / (num - 1) / k_c) {
      size -= i->size;
      num -= 1;
      sum_nlog2n -= i->size * log2(i->size);
      ++i;
    }
    double ratio_benefit = log2(size) - sum_nlog2n / size;
    if (!best.has_value() || best->ratio_benefit < ratio_benefit) {
      best = BestInfo{
          .ratio_benefit = ratio_benefit,
          .ratio_num = (double)num / size,
          .first = i,
          .last = j,
      };
    }
  }
  if (!best.has_value()) {
    return std::nullopt;
  }
  if (j != sorted_runs_.end()) {
    assert(j->is_merging());
    if (best->ratio_benefit < j->benefit_cost_ratio ||
        best->ratio_num < (double)j->num_sorted_runs / j->size * k_c) {
      return std::nullopt;
    }
  }
  return std::make_pair(best->first, best->last);
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
      /* snapshot_checker */ nullptr,
      /* is manual */ false, /* trim_ts */ "", /*score*/ 2333,
      false /* deletion_compaction */,
      /* l0_files_might_overlap */ true, CompactionReason::kPolicyByCount,
      BlobGarbageCollectionPolicy::kUseDefault, -1, &*it, io_priority);
  it->c = c;
  print_status();
  return c;
}

void CompactionPickerBenefitCost::CompactionFinished(void* info,
                                                     uint64_t size) {
  auto sr = static_cast<SortedRunInfo*>(info);
  sr->num_sorted_runs = 1;
  sr->size = size;
  sr->benefit_cost_ratio = 0;
  sr->c = nullptr;
  for (const auto& i : sorted_runs_) {
    if (&i == sr) {
      break;
    }
    if (i.c != nullptr) {
      i.c->io_priority().fetch_sub(1, std::memory_order_relaxed);
    }
  }
  print_status();
}

void CompactionPickerBenefitCost::Flushed(const FileMetaData& file) {
  // raw_key_size and raw_value_size seem to be zero
  sorted_runs_.emplace_front(1, file.fd.GetFileSize(), 0);
  print_status();
}

}  // namespace ROCKSDB_NAMESPACE
