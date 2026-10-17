#include "db/compaction/compaction_picker_vertiorizon.h"

#include <cinttypes>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

uint64_t CompactionPickerVertiorizon::level1_capacity(
    const MutableCFOptions& mutable_cf_options) const {
  const uint64_t horizontal_size = mutable_cf_options.write_buffer_size * n_;
  const uint64_t last_level_size = vstorage_->NumLevelBytes(2);
  return horizontal_size *
         std::sqrt((double)last_level_size / horizontal_size / 2);
}
void CompactionPickerVertiorizon::print_status() {
  printf("[");
  for (const auto& level : horizontal_levels_) {
    printf("%" PRIi64 " [", level.compaction_counter);
    auto it = level.runs.begin();
    if (it != level.runs.end()) {
      for (;;) {
        if (it->is_merging()) {
          printf("merging %zu ", it->num_sorted_runs);
        }
        printf("%" PRIu64, it->size);
        ++it;
        if (it == level.runs.end()) {
          break;
        }
        printf(", ");
      }
    }
    printf("], ");
  }
  printf("L1 %" PRIu64 ", L2 %" PRIu64 "]\n", vstorage_->NumLevelBytes(1),
         vstorage_->NumLevelBytes(2));
  fflush(stdout);
}

bool CompactionPickerVertiorizon::CalcNeedsCompaction(
    const ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options,
    const VersionStorageInfo* vstorage) const {
  if (vstorage_ != vstorage) {
    // Status not synced yet. Will try again shortly.
    return false;
  }
  assert(vstorage->num_levels() == 3);
  for (const auto& level : horizontal_levels_) {
    if (level.compaction_counter <= 0) {
      assert(!level.runs.empty());
      return !level.runs[0].is_merging();
    }
  }
  if (vstorage->NumLevelBytes(1) > level1_capacity(mutable_cf_options)) {
    return true;
  }
  return false;
}
bool CompactionPickerVertiorizon::NeedsCompaction(
    const ColumnFamilyData* cfd, const VersionStorageInfo* vstorage) const {
  return vstorage->CompactionScore(0) >= 1;
}

void CompactionPickerVertiorizon::CompactionFinished(
    const VersionStorageInfo* vstorage, void* merging_info, uint64_t size) {
  vstorage_ = vstorage;
  if (merging_info == &compaction_to_l1_in_progress_) {
    assert(compaction_to_l1_in_progress_);
    compaction_to_l1_in_progress_ = false;
  } else if (merging_info != nullptr) {
    auto info = static_cast<SortedRunInfo*>(merging_info);
    assert(info->is_merging());
    info->num_sorted_runs = 1;
    info->size = size;
  }
  print_status();
}

void CompactionPickerVertiorizon::Flushed(const VersionStorageInfo* vstorage,
                                          const FileMetaData& file) {
  vstorage_ = vstorage;
  auto& level = horizontal_levels_[0];
  level.compaction_counter -= 1;
  level.runs.push_front(
      SortedRunInfo{.num_sorted_runs = 1, .size = file.fd.file_size});
  print_status();
}

std::vector<CompactionInputFiles>
CompactionPickerVertiorizon::PickCompactionToL2(
    const MutableCFOptions& mutable_cf_options, VersionStorageInfo* vstorage) {
  std::vector<CompactionInputFiles> inputs;
  if (vstorage->NumLevelBytes(1) <= level1_capacity(mutable_cf_options)) {
    return inputs;
  }
  for (int level = 1; level <= 2; ++level) {
    for (const auto& file : vstorage->LevelFiles(level)) {
      if (file->being_compacted) {
        return inputs;
      }
    }
  }
  const auto l1_files = vstorage->LevelFiles(1);
  const auto l2_files = vstorage->LevelFiles(2);
  assert(!l1_files.empty());
  struct Best {
    size_t l1, l2_left, l2_right;
    double score;
  };
  std::optional<Best> best;
  // [l2_left, l2_right)
  size_t l2_left = 0;
  size_t l2_right = 0;
  int64_t size = 0;
  for (size_t l1 = 0; l1 < l1_files.size(); ++l1) {
    // Shouldn't compare with internal key. For example, l1_file->smallest >
    // l2_left->largest, but they may share the same user key and should be
    // considered overlaping.
    while (l2_left < l2_files.size() &&
           ioptions_.user_comparator->Compare(
               l2_files[l2_left]->largest.user_key(),
               l1_files[l1]->smallest.user_key()) < 0) {
      size -= l2_files[l2_left]->fd.file_size;
      ++l2_left;
    }
    while (l2_right < l2_files.size() &&
           ioptions_.user_comparator->Compare(
               l2_files[l2_right]->smallest.user_key(),
               l1_files[l1]->largest.user_key()) <= 0) {
      size += l2_files[l2_right]->fd.file_size;
      ++l2_right;
    }
    assert(size >= 0);
    uint64_t file_size = l1_files[l1]->fd.file_size;
    uint64_t overlapping_bytes = file_size + size;
    double score = (double)file_size / overlapping_bytes;
    if (!best.has_value() || score > best->score) {
      best = Best{
          .l1 = l1,
          .l2_left = l2_left,
          .l2_right = l2_right,
          .score = score,
      };
    }
  }
  assert(best.has_value());
  inputs.resize(1);
  inputs[0].level = 1;
  inputs[0].files.push_back(l1_files[best->l1]);
  if (best->l2_left != best->l2_right) {
    inputs.resize(2);
    inputs[1].level = 2;
    for (size_t i = best->l2_left; i < best->l2_right; ++i) {
      inputs[1].files.push_back(l2_files[i]);
    }
  }
  return inputs;
}

std::optional<CompactionPickerVertiorizon::PickResult>
CompactionPickerVertiorizon::PickFilesToCompact(
    const MutableCFOptions& mutable_cf_options, VersionStorageInfo* vstorage) {
  auto it_level = horizontal_levels_.begin();
  while (it_level != horizontal_levels_.end()) {
    if (!it_level->runs.empty() && it_level->runs[0].is_merging()) {
      return std::nullopt;
    }
    if (it_level->compaction_counter <= 0) {
      break;
    }
    ++it_level;
  }
  if (it_level == horizontal_levels_.end()) {
    auto inputs = PickCompactionToL2(mutable_cf_options, vstorage);
    if (inputs.empty()) {
      return std::nullopt;
    }
    return PickResult{
        .it_start_level = horizontal_levels_.end(),
        .it_next_level = horizontal_levels_.end(),
        .inputs = std::move(inputs),
    };
  }
  const auto it_start_level = it_level;
  auto it_next_level = it_start_level;
  ++it_next_level;
  if (it_next_level != horizontal_levels_.end()) {
    if (!it_next_level->runs.empty() && it_next_level->runs[0].is_merging()) {
      return std::nullopt;
    }
  } else {
    const uint64_t horizontal_size = mutable_cf_options.write_buffer_size * n_;
    const auto l1_size = vstorage->NumLevelBytes(1);
    const auto l2_size = vstorage->NumLevelBytes(2);
    if (l1_size > 0 &&
        (double)l2_size / l1_size < (double)l1_size / horizontal_size) {
      auto inputs = PickCompactionToL2(mutable_cf_options, vstorage);
      if (inputs.empty()) {
        return std::nullopt;
      }
      return PickResult{
          .it_start_level = horizontal_levels_.end(),
          .it_next_level = horizontal_levels_.end(),
          .inputs = std::move(inputs),
      };
    }
    const auto& l1_files = vstorage->LevelFiles(1);
    for (const auto& file : l1_files) {
      if (file->being_compacted) {
        return std::nullopt;
      }
    }
  }
  const auto& l0_files = vstorage->LevelFiles(0);
  size_t file_i = 0;

  it_level = horizontal_levels_.begin();
  while (it_level != it_start_level) {
    for (const auto& run : it_level->runs) {
      assert(!run.is_merging());
      assert(!l0_files[file_i]->being_compacted);
      file_i += 1;
    }
    ++it_level;
  }
  auto it_run = it_start_level->runs.begin();
  for (int64_t i = 0; i < -it_start_level->compaction_counter; ++i) {
    assert(!l0_files.at(file_i)->being_compacted);
    ++file_i;
    assert(!it_run->is_merging());
    ++it_run;
  }

  CompactionInputFiles inputs0;
  inputs0.level = 0;
  while (it_run != it_start_level->runs.end()) {
    assert(!it_run->is_merging());
    assert(it_run->size == l0_files.at(file_i)->fd.file_size);
    inputs0.files.push_back(l0_files.at(file_i));
    ++it_run;
    ++file_i;
  }

  std::vector<CompactionInputFiles> inputs;
  if (it_next_level != horizontal_levels_.end()) {
    inputs.resize(1);
    inputs[0] = std::move(inputs0);
  } else {
    if (compaction_to_l1_in_progress_) {
      return std::nullopt;
    }
    inputs.resize(2);
    inputs[0] = std::move(inputs0);
    inputs[1].level = 1;
    inputs[1].files = vstorage->LevelFiles(1);
  }
  return PickResult{
      .it_start_level = it_start_level,
      .it_next_level = it_next_level,
      .inputs = std::move(inputs),
  };
}

Compaction* CompactionPickerVertiorizon::PickCompaction(
    ColumnFamilyData* cfd, const std::string& cf_name,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& existing_snapshots,
    const SnapshotChecker* snapshot_checker, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, bool require_max_output_level) {
  assert(vstorage->num_levels() == 3);
  std::vector<CompactionInputFiles> inputs;
  int output_level;
  std::optional<BackgroundJobLimiter::CompactionHandle> limiter_handle;
  void* merging_info;
  for (;;) {
    auto pick_result = PickFilesToCompact(mutable_cf_options, vstorage);
    if (!pick_result.has_value()) {
      return nullptr;
    }
    auto it_start_level = pick_result->it_start_level;
    auto it_next_level = pick_result->it_next_level;
    inputs = std::move(pick_result->inputs);
    assert(!inputs.empty());

    if (it_start_level == horizontal_levels_.end()) {
      assert(it_next_level == horizontal_levels_.end());
      output_level = 2;
    } else if (it_next_level == horizontal_levels_.end()) {
      output_level = 1;
    } else {
      output_level = 0;
    }

    bool compaction_needed;
    if (output_level == 0 && inputs.size() == 1) {
      assert(inputs[0].level == 0);
      compaction_needed = inputs[0].files.size() > 1;
    } else {
      compaction_needed = true;
    }

    if (compaction_needed) {
      if (ioptions_.background_job_limiter) {
        limiter_handle =
            ioptions_.background_job_limiter->RegisterCompaction(Reff(inputs));
        if (!limiter_handle.has_value()) {
          return nullptr;
        }
      }
    }

    if (output_level < 2) {
      it_start_level->runs.resize(-it_start_level->compaction_counter);
      if (output_level == 0) {
        assert(inputs.size() == 1);
        assert(inputs[0].level == 0);
        uint64_t size = 0;
        for (const auto& file : inputs[0].files) {
          size += file->fd.file_size;
        }

        it_next_level->compaction_counter -= 1;
        it_next_level->runs.push_front(SortedRunInfo{
            .num_sorted_runs = inputs[0].files.size(), .size = size});

        auto it_level = it_next_level;
        std::vector<int64_t> compaction_counters(
            1, it_next_level->compaction_counter);
        auto it_counter = compaction_counters.begin();
        for (;;) {
          if (*it_counter <= 0) {
            ++it_level;
            ++it_counter;
            if (it_level == horizontal_levels_.end()) {
              --it_level;
              --it_counter;
              *it_counter += ioptions_.k;
              continue;
            }
            if (it_counter == compaction_counters.end()) {
              compaction_counters.push_back(it_level->compaction_counter);
              it_counter = compaction_counters.end();
              --it_counter;
            }
            *it_counter -= 1;
          } else {
            if (it_counter == compaction_counters.begin()) {
              break;
            }
            int64_t new_counter = *it_counter;
            --it_counter;
            --it_level;
            *it_counter += new_counter;
          }
        }
        it_start_level->compaction_counter += compaction_counters[0];

        merging_info = &it_next_level->runs.front();
      } else {
        it_start_level->compaction_counter += (int64_t)ioptions_.k;
        assert(compaction_needed);
        assert(!compaction_to_l1_in_progress_);
        compaction_to_l1_in_progress_ = true;
        merging_info = &compaction_to_l1_in_progress_;
      }
      print_status();
    } else {
      assert(output_level == 2);
      assert(compaction_needed);
      merging_info = nullptr;
    }

    if (compaction_needed) {
      break;
    }
  }

  return new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          kCompactionStyleVertiorizon),
      std::numeric_limits<uint64_t>::max(), 0,
      GetCompressionType(vstorage, mutable_cf_options, output_level,
                         vstorage->base_level()),
      GetCompressionOptions(mutable_cf_options, vstorage, output_level,
                            /* compression not supported yet */ false),
      mutable_cf_options.default_write_temperature,
      /* max_subcompactions */ 0, /* grandparents not supported yet */ {},
      /* earliest_snapshot */ std::nullopt,
      /* snapshot_checker */ nullptr, std::move(limiter_handle),
      CompactionReason::kPolicyByCount, /* trim_ts */ "", /*score*/ 2333,
      /* l0_files_might_overlap */ true,
      BlobGarbageCollectionPolicy::kUseDefault, -1, merging_info, Env::IO_LOW);
}

}  // namespace ROCKSDB_NAMESPACE
