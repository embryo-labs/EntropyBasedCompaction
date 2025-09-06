#pragma once

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <deque>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stack>
#include <variant>

#include "rocksdb/rate_limiter.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// Priority:
// Flush > Compaction
// Earier flush > later flush
// Earier compaction < later compaction
class BackgroundJobLimiter
    : public std::enable_shared_from_this<BackgroundJobLimiter> {
 private:
  struct CompactionStatus {
    std::mutex lock;
    std::condition_variable cv;
    std::atomic<bool> permit_to_run = true;
  };

 public:
  class FlushHandle {
   public:
    FlushHandle(const FlushHandle&) = delete;
    FlushHandle& operator=(const FlushHandle&) = delete;
    FlushHandle(FlushHandle&& rhs) : limiter_(std::move(rhs.limiter_)) {}
    FlushHandle& operator=(FlushHandle&& rhs) {
      this->~FlushHandle();
      limiter_ = std::move(rhs.limiter_);
      rhs.limiter_ = nullptr;
      return *this;
    }

    ~FlushHandle() {
      if (limiter_ == nullptr) {
        return;
      }
      limiter_->FlushFinishes();
    }

   private:
    explicit FlushHandle(std::shared_ptr<BackgroundJobLimiter> limiter)
        : limiter_(std::move(limiter)) {}
    std::shared_ptr<BackgroundJobLimiter> limiter_;
    friend class BackgroundJobLimiter;
  };
  class CompactionHandle {
   public:
    CompactionHandle(const CompactionHandle&) = delete;
    CompactionHandle& operator=(const CompactionHandle&) = delete;
    CompactionHandle(CompactionHandle&& rhs) noexcept
        : limiter_(std::move(rhs.limiter_)), status_(std::move(rhs.status_)) {}
    CompactionHandle& operator=(CompactionHandle&& rhs) {
      limiter_ = std::move(rhs.limiter_);
      status_ = std::move(rhs.status_);
      return *this;
    }

    ~CompactionHandle() {
      if (limiter_ == nullptr) {
        assert(status_ == nullptr);
        return;
      }
      assert(status_ != nullptr);
      std::shared_ptr<CompactionStatus> compaction_to_wake_up;
      std::unique_lock<std::mutex> lock(limiter_->lock_);
      auto it = limiter_->compactions_.end();
      do {
        assert(it != limiter_->compactions_.begin());
        --it;
      } while (*it != status_);
      limiter_->compactions_.erase(it);
      if (status_->permit_to_run.load(std::memory_order_relaxed)) {
        limiter_->num_running_compactions_.fetch_sub(1,
                                                     std::memory_order_relaxed);
      }
      limiter_->MaybeResumeCompactionLocked(std::move(lock));
    }

    void RequestToRun() const {
      // Most of time we don't need to request a lock.
      if (status_->permit_to_run.load(std::memory_order_relaxed)) {
        return;
      }
      {
        std::unique_lock<std::mutex> lock(status_->lock);
        status_->cv.wait(lock, [this]() {
          return status_->permit_to_run.load(std::memory_order_relaxed);
        });
      }
    }

   private:
    CompactionHandle(std::shared_ptr<BackgroundJobLimiter> limiter,
                     std::shared_ptr<CompactionStatus> status)
        : limiter_(std::move(limiter)), status_(std::move(status)) {}
    std::shared_ptr<BackgroundJobLimiter> limiter_;
    std::shared_ptr<CompactionStatus> status_;
    friend class BackgroundJobLimiter;
  };

  FlushHandle RegisterFlush() {
    std::unique_lock<std::mutex> lock(lock_);
    num_running_flushes_.fetch_add(1, std::memory_order_relaxed);
    MaybeStopEarliestRunningCompactionsLocked();
    return FlushHandle(shared_from_this());
  }

  // Return std::nullopt means that the LSM-tree is busy with flushing and
  // shouldn't start any new background jobs.
  std::optional<CompactionHandle> RegisterCompaction() {
    if (num_running_flushes_.load(std::memory_order_relaxed) >=
        max_running_background_jobs_.load(std::memory_order_relaxed)) {
      printf("RegisterCompaction: Reject due to too many background jobs\n");
      return std::nullopt;
    }
    auto compaction_status = std::make_shared<CompactionStatus>();

    std::unique_lock<std::mutex> lock(lock_);
    compactions_.emplace_back(compaction_status);
    num_running_compactions_.fetch_add(1, std::memory_order_relaxed);
    MaybeStopEarliestRunningCompactionsLocked();
    return CompactionHandle(shared_from_this(), std::move(compaction_status));
  }

  size_t num_running_bg_jobs() const {
    return num_running_flushes_.load(std::memory_order_relaxed) +
           num_running_compactions_.load(std::memory_order_relaxed);
  }

 private:
  explicit BackgroundJobLimiter(size_t max_running_background_jobs)
      : max_running_background_jobs_(max_running_background_jobs) {}

  void MaybeStopEarliestRunningCompactionsLocked() {
    assert(lock_.try_lock() == false);
    auto num_running_flushes =
        num_running_flushes_.load(std::memory_order_relaxed);
    auto max_running_background_jobs =
        max_running_background_jobs_.load(std::memory_order_relaxed);
    for (;;) {
      auto num_running_compactions =
          num_running_compactions_.load(std::memory_order_relaxed);
      if (num_running_flushes + num_running_compactions <=
          max_running_background_jobs) {
        return;
      }
      if (num_running_compactions == 0) {
        return;
      }
      assert(compactions_.size() >= num_running_compactions);
      bool ori_permit_to_run =
          compactions_[compactions_.size() - num_running_compactions]
              ->permit_to_run.exchange(false, std::memory_order_relaxed);
      assert(ori_permit_to_run);
      num_running_compactions_.fetch_sub(1, std::memory_order_relaxed);
    }
  }

  void MaybeResumeCompactionLocked(std::unique_lock<std::mutex> lock) {
    assert(lock_.try_lock() == false);
    auto num_running_compactions =
        num_running_compactions_.load(std::memory_order_relaxed);
    auto num_running_flushes =
        num_running_flushes_.load(std::memory_order_relaxed);
    auto max_running_background_jobs =
        max_running_background_jobs_.load(std::memory_order_relaxed);
    if (num_running_flushes + num_running_compactions >=
        max_running_background_jobs) {
      return;
    }
    if (num_running_compactions == compactions_.size()) {
      return;
    }
    assert(num_running_flushes + num_running_compactions ==
           max_running_background_jobs - 1);
    auto compaction_to_wake_up =
        compactions_[compactions_.size() - num_running_compactions - 1];
    bool ori_permit_to_run = compaction_to_wake_up->permit_to_run.exchange(
        true, std::memory_order_relaxed);
    assert(ori_permit_to_run == false);
    num_running_compactions_.fetch_add(1, std::memory_order_relaxed);
    lock.unlock();
    compaction_to_wake_up->cv.notify_one();
  }

  void FlushFinishes() {
    std::unique_lock<std::mutex> lock(lock_);
    num_running_flushes_.fetch_sub(1, std::memory_order_relaxed);
    MaybeResumeCompactionLocked(std::move(lock));
  }

  mutable std::mutex lock_;
  // atomic to suppress data race warnning
  std::atomic<size_t> max_running_background_jobs_;
  std::atomic<size_t> num_running_flushes_ = 0;
  std::atomic<size_t> num_running_compactions_ = 0;
  std::vector<std::shared_ptr<CompactionStatus>> compactions_;
  friend class CompactionHandle;
  friend std::shared_ptr<BackgroundJobLimiter> NewGenericBackgroundJobLimiter(
      size_t max_running_background_jobs);
};

inline std::shared_ptr<BackgroundJobLimiter> NewGenericBackgroundJobLimiter(
    size_t max_running_background_jobs) {
  return std::shared_ptr<BackgroundJobLimiter>(
      new BackgroundJobLimiter(max_running_background_jobs));
}

}  // namespace ROCKSDB_NAMESPACE
