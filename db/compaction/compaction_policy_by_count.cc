#include "rocksdb/compaction_policy_by_count.h"

#include <cinttypes>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

void CompactionPolicyByCount::print(const std::list<SortedRunInfo> &status) {
  printf("[");
  auto it = status.begin();
  if (it != status.end()) {
    for (;;) {
      if (it->merging) {
        printf("merging ");
      }
      printf("%" PRIu64, it->count);
      ++it;
      if (it == status.end()) {
        break;
      }
      printf(", ");
    }
  }
  printf("]\n");
}

}  // namespace ROCKSDB_NAMESPACE
