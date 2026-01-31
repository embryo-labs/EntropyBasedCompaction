#pragma once

#include <filesystem>
#include <fstream>
#include <mutex>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct JsonLogger {
  JsonLogger(std::filesystem::path path) : out(path) {}

  std::mutex mutex;
  std::ofstream out;
};

}  // namespace ROCKSDB_NAMESPACE
