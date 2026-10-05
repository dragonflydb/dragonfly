// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <source_location>
#include <thread>
#include <vector>

namespace util {
class ProactorPool;
}

namespace dfly {

class ShutdownProgress {
 public:
  void Update(const char* updater,
              std::source_location location = std::source_location::current()) {
    file_name_.store(location.file_name(), std::memory_order_relaxed);
    line_.store(location.line(), std::memory_order_relaxed);
    updater_.store(updater, std::memory_order_relaxed);
    ticks_.fetch_add(1, std::memory_order_relaxed);
  }

  const char* updater() const {
    return updater_.load(std::memory_order_relaxed);
  }

  uint64_t ticks() const {
    return ticks_.load(std::memory_order_relaxed);
  }

  const char* file_name() const {
    return file_name_.load(std::memory_order_relaxed);
  }

  uint_least32_t line() const {
    return line_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<const char*> file_name_{""};
  std::atomic<uint_least32_t> line_{0};
  std::atomic<const char*> updater_{nullptr};
  std::atomic<uint64_t> ticks_{0};
};

struct DetectStall {
  using Clock = std::chrono::steady_clock;

  const char* Check(const ShutdownProgress& progress, Clock::time_point now);

  uint64_t last_seen_ticks = 0;
  Clock::time_point last_prgress_at = Clock::now();
  bool stall_reported = false;
};

class ShutdownWatchdog {
 public:
  explicit ShutdownWatchdog(util::ProactorPool& pool);
  ~ShutdownWatchdog();

  ShutdownWatchdog(const ShutdownWatchdog&) = delete;
  ShutdownWatchdog& operator=(const ShutdownWatchdog&) = delete;

 private:
  void Run();

  const std::chrono::steady_clock::time_point started_at_ = std::chrono::steady_clock::now();
  std::vector<ShutdownProgress*> records_;
  std::mutex stop_mutex_;
  std::condition_variable stop_cond_;
  bool stopped_ = false;
  std::thread thread_;
};

// Reports some progress on the shard on which it is called. When called with nullptr, the shard
// progress is marked as inactive ie there is no active task running on the shard.
void ReportShutdownProgress(const char* updater,
                            std::source_location location = std::source_location::current());

// Using RAII marks the start and then end of a task (identified simply by string)
struct ShutdownProgressScope {
  explicit ShutdownProgressScope(const char* updater,
                                 std::source_location location = std::source_location::current()) {
    ReportShutdownProgress(updater, location);
  }
  ~ShutdownProgressScope() {
    ReportShutdownProgress(nullptr);
  }
  ShutdownProgressScope(const ShutdownProgressScope&) = delete;
  ShutdownProgressScope& operator=(const ShutdownProgressScope&) = delete;
};

}  // namespace dfly
