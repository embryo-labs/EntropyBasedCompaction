#pragma once

#include <memory>
#include <string>
#include <vector>

#include "db/version_edit.h"
#include "db/version_set.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class Compaction;
class InternalKey;
class VersionSet;
class ColumnFamilyData;

// A lightweight snapshot of the Version information needed by a Compaction
// after the Version reference has been released. Only holds references to
// input files. Key ranges for higher levels (used by tombstone optimization)
// are deep-copied so the underlying files do not need to be pinned.
//
// This is used to solve the problem where a paused (e.g. by BackgroundJobLimiter)
// compaction holds a Version reference for a long time, preventing files that
// have already been compacted away by newer compactions from being deleted.
class CompactionInputSnapshot {
 public:
  // Cached file metadata for a file (used by tombstone optimization and
  // debug string generation).
  struct FileRange {
    uint64_t file_number = 0;
    uint64_t smallest_seqno = 0;
    uint64_t largest_seqno = 0;
    std::string smallest_key;
    std::string largest_key;
    uint64_t file_size = 0;
  };

  // Constructs a snapshot from a Version. Takes ownership of references to
  // all input files and any files needed for tombstone optimization.
  // Must be called while holding the DB mutex.
  CompactionInputSnapshot(Version* version, const Compaction* compaction);
  ~CompactionInputSnapshot();

  // Disable copy/move
  CompactionInputSnapshot(const CompactionInputSnapshot&) = delete;
  CompactionInputSnapshot& operator=(const CompactionInputSnapshot&) = delete;

  // Basic info
  uint64_t version_number() const { return version_number_; }
  int base_level() const { return base_level_; }
  int num_levels() const { return num_levels_; }
  int num_non_empty_levels() const { return num_non_empty_levels_; }
  size_t NumLevelFiles(int level) const { return input_level_ranges_[level].size(); }

  // Cached table properties for input files (pre-loaded at construction)
  const TablePropertiesCollection& input_table_properties() const {
    return input_table_properties_;
  }

  // Returns the pre-loaded table properties for an input file.
  // Aborts if not found (input files are always expected to be cached).
  // If fname is non-null, it is used as the lookup key; otherwise the file
  // name is constructed from file_meta (similar to Version::GetTableProperties).
  std::shared_ptr<const TableProperties> GetTableProperties(
      const FileMetaData* file_meta,
      const std::string* fname = nullptr) const;

  // Next compact cursor (cached at construction)
  const InternalKey& next_compact_cursor(int level) const {
    return next_compact_cursors_[level];
  }

  // Snapshot of key ranges for higher levels (for tombstone optimization).
  const std::vector<FileRange>& GetLevelRanges(int level) const {
    return higher_level_ranges_[level];
  }

  // Approximate the size of data covered by [start, end] in levels >=
  // start_level. Conservative upper bound based on snapshot ranges.
  uint64_t ApproximateSize(const Slice& start, const Slice& end,
                           int start_level) const;

  // BlobDB is not supported in this prototype.
  bool has_blob_files() const { return has_blob_files_; }

  uint64_t GetVersionNumber() const { return version_number_; }

  // Human-readable debug string generated from the snapshot data on demand.
  // Similar to Version::DebugString(), but uses cached metadata instead of
  // accessing the current Version.
  std::string DebugString(bool hex = true) const;

  bool level0_non_overlapping() const { return level0_non_overlapping_; }

  // Returns the total file size in `level` that overlaps [smallest_user_key,
  // largest_user_key]. Used by trivial-move checks.
  uint64_t GetOverlappingSize(int level, const Slice& smallest_user_key,
                              const Slice& largest_user_key) const;

  // Calculate write lifetime hint for the output level, based on the input
  // version's compaction style and base level.
  Env::WriteLifeTimeHint CalculateSSTWriteHint(
      int level, CompactionStyleSet compaction_style_set) const;

 private:
  // Files we hold refs to (refs++ on construction, refs-- on destruction)
  std::vector<FileMetaData*> referenced_files_;

  // Snapshot of file metadata for input levels (for debug string generation).
  std::vector<std::vector<FileRange>> input_level_ranges_;

  // Snapshot of key ranges for higher levels (for tombstone optimization).
  // We deep-copy the keys so we don't need to keep FileMetaData alive.
  std::vector<std::vector<FileRange>> higher_level_ranges_;

  int num_levels_;
  int num_non_empty_levels_;
  int base_level_;
  CompactionStyle compaction_style_;
  uint64_t version_number_;
  std::vector<InternalKey> next_compact_cursors_;
  bool level0_non_overlapping_;

  TablePropertiesCollection input_table_properties_;

  // For obsolete file handling
  VersionSet* vset_;
  ColumnFamilyData* cfd_;

  bool has_blob_files_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
