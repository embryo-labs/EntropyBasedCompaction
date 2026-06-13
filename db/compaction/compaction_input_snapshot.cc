#include "db/compaction/compaction_input_snapshot.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "db/column_family.h"
#include "db/compaction/compaction.h"
#include "db/dbformat.h"
#include "db/version_set.h"
#include "file/filename.h"
#include "table/table_builder.h"

namespace ROCKSDB_NAMESPACE {

CompactionInputSnapshot::CompactionInputSnapshot(Version* version,
                                                 const Compaction* compaction)
    : num_levels_(version->storage_info()->num_levels()),
      num_non_empty_levels_(version->storage_info()->num_non_empty_levels()),
      base_level_(version->storage_info()->base_level()),
      compaction_style_(version->cfd()->ioptions().compaction_style),
      version_number_(version->GetVersionNumber()),
      next_compact_cursors_(num_levels_),
      level0_non_overlapping_(
          version->storage_info()->level0_non_overlapping()),
      vset_(version->version_set()),
      cfd_(version->cfd()),
      has_blob_files_(!version->storage_info()->GetBlobFiles().empty()) {
  // This prototype does not support BlobDB.
  assert(!has_blob_files_);

  // Collect all files we need to reference:
  // 1. Input files
  // 2. Higher levels for tombstone optimization
  input_level_ranges_.resize(num_levels_);
  higher_level_ranges_.resize(num_levels_);

  for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
    int level = compaction->level(i);
    for (FileMetaData* f : *compaction->inputs(i)) {
      assert(f);
      f->refs++;
      referenced_files_.push_back(f);
      FileRange fr{
          .file_number = f->fd.GetNumber(),
          .smallest_seqno = f->fd.smallest_seqno,
          .largest_seqno = f->fd.largest_seqno,
          .smallest_key = f->smallest.Encode().ToString(),
          .largest_key = f->largest.Encode().ToString(),
          .file_size = f->fd.GetFileSize(),
      };
      input_level_ranges_[level].push_back(std::move(fr));
    }
  }

  // For tombstone optimization, we need output level and above.
  // We snapshot their key ranges (deep copy) so we don't need to pin them.
  int output_level = compaction->output_level();
  for (int level = output_level + 1; level < num_levels_; ++level) {
    const auto& level_files = version->storage_info()->LevelFiles(level);
    for (FileMetaData* f : level_files) {
      FileRange fr{
          .file_number = f->fd.GetNumber(),
          .smallest_seqno = f->fd.smallest_seqno,
          .largest_seqno = f->fd.largest_seqno,
          .smallest_key = f->smallest.Encode().ToString(),
          .largest_key = f->largest.Encode().ToString(),
          .file_size = f->fd.GetFileSize(),
      };
      higher_level_ranges_[level].push_back(std::move(fr));
    }
  }

  // Cache next compact cursor for round-robin compaction
  if (compaction->immutable_options().compaction_pri == kRoundRobin) {
    int start_level = compaction->start_level();
    if (start_level > 0 && start_level < num_levels_) {
      next_compact_cursors_[start_level] =
          version->storage_info()->GetNextCompactCursor(
              start_level, compaction->num_input_files(0));
    }
  }

  // Pre-load table properties for input files
  const ReadOptions read_options(Env::IOActivity::kCompaction);
  for (size_t i = 0; i < compaction->num_input_levels(); ++i) {
    for (const FileMetaData* fmd : *compaction->inputs(i)) {
      std::shared_ptr<const TableProperties> tp;
      std::string file_name =
          TableFileName(compaction->immutable_options().cf_paths,
                        fmd->fd.GetNumber(), fmd->fd.GetPathId());
      Status s =
          version->GetTableProperties(read_options, &tp, fmd, &file_name);
      if (s.ok()) {
        input_table_properties_[file_name] = tp;
      }
    }
  }
}

std::shared_ptr<const TableProperties>
CompactionInputSnapshot::GetTableProperties(const FileMetaData* file_meta,
                                            const std::string* fname) const {
  std::string file_name =
      fname ? *fname
            : TableFileName(cfd_->ioptions().cf_paths,
                            file_meta->fd.GetNumber(),
                            file_meta->fd.GetPathId());
  auto it = input_table_properties_.find(file_name);
  if (it == input_table_properties_.end()) {
    fprintf(stderr, "FATAL: Table properties not found for input file %s\n",
            file_name.c_str());
    std::abort();
  }
  return it->second;
}

uint64_t CompactionInputSnapshot::ApproximateSize(const Slice& start,
                                                  const Slice& end,
                                                  int start_level) const {
  uint64_t total = 0;
  const InternalKeyComparator& icmp = cfd_->internal_comparator();
  for (int level = start_level; level < num_levels_; ++level) {
    const auto& ranges = higher_level_ranges_[level];
    for (const auto& fr : ranges) {
      Slice fr_smallest(fr.smallest_key);
      Slice fr_largest(fr.largest_key);
      if (icmp.Compare(fr_largest, start) <= 0 ||
          icmp.Compare(fr_smallest, end) > 0) {
        continue;
      }
      total += fr.file_size;
    }
  }
  return total;
}

uint64_t CompactionInputSnapshot::GetOverlappingSize(
    int level, const Slice& smallest_user_key,
    const Slice& largest_user_key) const {
  if (level >= num_non_empty_levels_) {
    return 0;
  }
  const Comparator* ucmp = cfd_->user_comparator();
  uint64_t total = 0;
  const auto& ranges = higher_level_ranges_[level];
  for (const auto& fr : ranges) {
    Slice fr_smallest_user_key = ExtractUserKey(fr.smallest_key);
    Slice fr_largest_user_key = ExtractUserKey(fr.largest_key);
    if (ucmp->Compare(fr_largest_user_key, smallest_user_key) < 0 ||
        ucmp->Compare(fr_smallest_user_key, largest_user_key) > 0) {
      continue;
    }
    total += fr.file_size;
  }
  return total;
}

Env::WriteLifeTimeHint CompactionInputSnapshot::CalculateSSTWriteHint(
    int level, CompactionStyleSet compaction_style_set) const {
  return VersionStorageInfo::CalculateSSTWriteHint(
      compaction_style_, base_level_, level, compaction_style_set);
}

std::string CompactionInputSnapshot::DebugString(bool hex) const {
  std::string r;
  for (int level = 0; level < num_levels_; level++) {
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" --- version# ");
    AppendNumberTo(&r, version_number_);
    r.append(" ---\n");

    const auto& input_ranges = input_level_ranges_[level];
    const auto& higher_ranges = higher_level_ranges_[level];

    auto append_file = [&](const FileRange& fr) {
      r.push_back(' ');
      AppendNumberTo(&r, fr.file_number);
      r.push_back(':');
      AppendNumberTo(&r, fr.file_size);
      r.append("[");
      AppendNumberTo(&r, fr.smallest_seqno);
      r.append(" .. ");
      AppendNumberTo(&r, fr.largest_seqno);
      r.append("]");
      r.append("[");
      InternalKey smallest;
      smallest.DecodeFrom(fr.smallest_key);
      r.append(smallest.DebugString(hex));
      r.append(" .. ");
      InternalKey largest;
      largest.DecodeFrom(fr.largest_key);
      r.append(largest.DebugString(hex));
      r.append("]");
      r.append("\n");
    };

    for (const auto& fr : input_ranges) {
      append_file(fr);
    }
    for (const auto& fr : higher_ranges) {
      append_file(fr);
    }
  }
  return r;
}

CompactionInputSnapshot::~CompactionInputSnapshot() {
  for (FileMetaData* f : referenced_files_) {
    vset_->UnrefAndMaybeMakeObsolete(f, cfd_);
  }
}

}  // namespace ROCKSDB_NAMESPACE
