#include "rocksdb/compaction_policy_by_count.h"

#include <cinttypes>
#include <cstdio>

namespace ROCKSDB_NAMESPACE {

void CompactionPolicyByCount::print(const std::list<SortedRunInfo> &status) {
  printf("[");
  auto it = status.begin();
  if (it == status.end()) {
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
    if (it == status.end()) {
      printf("]\n");
      fflush(stdout);
      break;
    }
    printf(", ");
  }
}

}  // namespace ROCKSDB_NAMESPACE
