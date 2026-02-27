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
    sum_nlog2n_ += size * std::log2((double)size);
    sum_ += size;
  }
  double calc() { return std::log2((double)sum_) - sum_nlog2n_ / sum_; }

 private:
  double sum_nlog2n_;
  uint64_t sum_;
};

static inline double Reff(std::vector<uint64_t> sizes) {
  ReffCalculator calculator;
  for (uint64_t size : sizes) {
    calculator.add_sorted_run(size);
  }
  return calculator.calc();
}

}  // namespace ROCKSDB_NAMESPACE
