#pragma once

#include <cmath>
#include <cstdint>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class ReffCalculator {
 public:
  ReffCalculator() : sum_nlog2n_(0), sum_(0) {}
  void add_sorted_run(uint64_t size) {
    if (size == 0) {
      return;
    }
    sum_nlog2n_ += size * std::log2((double)size);
    sum_ += size;
  }
  double reff() const {
    return sum_ == 0 ? 0 : std::log2((double)sum_) - sum_nlog2n_ / sum_;
  }
  uint64_t size_sum() const { return sum_; }

 private:
  double sum_nlog2n_;
  uint64_t sum_;
};

inline double Reff(std::vector<uint64_t> sizes) {
  ReffCalculator calculator;
  for (uint64_t size : sizes) {
    calculator.add_sorted_run(size);
  }
  return calculator.reff();
}

}  // namespace ROCKSDB_NAMESPACE
