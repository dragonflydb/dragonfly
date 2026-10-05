// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/shutdown_watchdog.h"

#include <pthread.h>

#ifdef __FreeBSD__
#include <pthread_np.h>
#endif

#include <vector>

#include "base/logging.h"
#include "util/proactor_pool.h"

namespace dfly {
namespace {

using namespace std::chrono;
using Clock = steady_clock;

constexpr auto kPollPeriod = seconds(1);
constexpr auto kStallPeriod = seconds(20);
constexpr auto kStatusPeriod = seconds(10);

thread_local ShutdownProgress tl_progress;

}  // namespace

const char* DetectStall::Check(const ShutdownProgress& progress, Clock::time_point now) {
  const char* updater = progress.updater();
  const auto curr_ticks = progress.ticks();

  const bool is_idle = updater == nullptr;
  const bool made_progress = curr_ticks != last_seen_ticks;

  if (is_idle || made_progress) {
    last_seen_ticks = curr_ticks;
    last_prgress_at = now;
    stall_reported = false;
    return nullptr;
  }

  if (stall_reported)
    return nullptr;

  if (now - last_prgress_at < kStallPeriod)
    return nullptr;

  stall_reported = true;
  return updater;
}

void ReportShutdownProgress(const char* updater, std::source_location location) {
  tl_progress.Update(updater, location);
}

ShutdownWatchdog::ShutdownWatchdog(util::ProactorPool& pool) : records_(pool.size()) {
  pool.AwaitBrief([this](unsigned index, auto*) {
    tl_progress.Update(nullptr);
    records_[index] = &tl_progress;
  });
  thread_ = std::thread([this] { Run(); });
}

ShutdownWatchdog::~ShutdownWatchdog() {
  {
    std::lock_guard lk(stop_mutex_);
    stopped_ = true;
  }
  stop_cond_.notify_one();
  thread_.join();
}

void ShutdownWatchdog::Run() {
  pthread_setname_np(pthread_self(), "shutdown_watch");
  std::vector<DetectStall> detectors(records_.size());
  auto next_status_at = started_at_ + kStallPeriod;

  std::unique_lock lk(stop_mutex_);
  while (!stop_cond_.wait_for(lk, kPollPeriod, [this] { return stopped_; })) {
    const auto now = Clock::now();
    for (size_t i = 0; i < records_.size(); ++i) {
      if (const char* updater = detectors[i].Check(*records_[i], now)) {
        const auto lag = duration_cast<seconds>(now - detectors[i].last_prgress_at).count();
        LOG(ERROR) << "Shutdown proactor{" << i << "}, no progress in " << lag
                   << " seconds. Last action was: " << updater << " (" << records_[i]->file_name()
                   << ":" << records_[i]->line() << ")";
      }
    }

    if (now >= next_status_at) {
      LOG(INFO) << "Shutdown still in progress after "
                << duration_cast<seconds>(now - started_at_).count() << " seconds";
      for (size_t i = 0; i < records_.size(); ++i) {
        if (const char* updater = records_[i]->updater()) {
          const auto lag = duration_cast<seconds>(now - detectors[i].last_prgress_at).count();
          LOG(INFO) << "Shutdown proactor{" << i << "}, last reported action: " << updater << " ("
                    << records_[i]->file_name() << ":" << records_[i]->line() << ")"
                    << ", last observed progress " << lag << " seconds ago";
        } else {
          LOG(INFO) << "Shutdown proactor{" << i << "}, no active reported task";
        }
      }
      next_status_at = now + kStatusPeriod;
    }
  }
}

}  // namespace dfly
