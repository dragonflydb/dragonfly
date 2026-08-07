// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_slice.h"

#include <absl/flags/flag.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>

#include <algorithm>
#include <filesystem>

#include "base/function2.hpp"
#include "base/logging.h"
#include "server/common.h"
#include "server/engine_shard_set.h"
#include "server/journal/serializer.h"
#include "strings/human_readable.h"
#include "util/fibers/fibers.h"

ABSL_RETIRED_FLAG(uint32_t, shard_repl_backlog_len, 0,
                  "Deprecated. Use --shard_repl_backlog_time_ms and "
                  "--shard_repl_backlog_max_bytes instead.");
ABSL_FLAG(uint32_t, shard_repl_backlog_time_ms, 5000,
          "Target retention age in milliseconds of entries in the per-shard replication backlog. "
          "Entries are evicted in one-second buckets and can be retained for up to one extra "
          "second. 0 disables time-based eviction.");
ABSL_FLAG(strings::MemoryBytesFlag, shard_repl_backlog_max_bytes, 0,
          "Total bytes retained by replication backlog. 0 (the default) uses 0.5% of maxmemory.");

namespace dfly {
namespace journal {
using namespace std;
using namespace util;

namespace {
constexpr uint64_t kTimeBucketMs = 1000;

size_t GetPerShardBacklogMaxBytes() {
  size_t total_max_bytes = absl::GetFlag(FLAGS_shard_repl_backlog_max_bytes).value;
  if (total_max_bytes == 0) {
    constexpr size_t kBacklogMemoryFraction = 200;
    total_max_bytes = max_memory_limit.load(memory_order_relaxed) / kBacklogMemoryFraction;
  }

  const size_t shard_count = shard_set ? max<size_t>(1, shard_set->size()) : 1;
  return total_max_bytes / shard_count;
}

}  // namespace

JournalSlice::JournalSlice() {
}

JournalSlice::~JournalSlice() {
}

void JournalSlice::Init() {
  // calling this function multiple times is allowed and it's a no-op.
  if (ring_buffer_.capacity() > 0)
    return;

  constexpr size_t kDefaultBacklogCapacity = 8192;
  ring_buffer_.set_capacity(kDefaultBacklogCapacity);
  max_age_ms_ = absl::GetFlag(FLAGS_shard_repl_backlog_time_ms);
  max_bytes_ = GetPerShardBacklogMaxBytes();
}

bool JournalSlice::IsLSNInBuffer(LSN lsn) const {
  DCHECK(ring_buffer_.capacity() > 0);

  if (ring_buffer_.empty()) {
    return false;
  }

  if (ring_buffer_.size() == 1) {
    return ring_buffer_.front().lsn == lsn;
  }

  return ring_buffer_.front().lsn <= lsn && lsn <= ring_buffer_.back().lsn;
}

std::string_view JournalSlice::GetEntry(LSN lsn) const {
  DCHECK(ring_buffer_.capacity() > 0 && IsLSNInBuffer(lsn));

  auto start = ring_buffer_.front().lsn;
  DCHECK(ring_buffer_[lsn - start].lsn == lsn);
  return ring_buffer_[lsn - start].data;
}

void JournalSlice::SetFlushMode(bool allow_flush) {
  DCHECK(allow_flush != enable_journal_flush_);
  enable_journal_flush_ = allow_flush;
  if (allow_flush) {
    // This lock is never blocking because it contends with UnregisterOnChange, which is cpu only.
    // Hence this lock prevents the UnregisterOnChange to start running in the middle of
    // SetFlushMode.
    std::shared_lock lk(cb_mu_);
    for (auto k_v : journal_consumers_arr_) {
      k_v.second->ThrottleIfNeeded();
    }
  }
}

void JournalSlice::AddLogRecord(const Entry& entry) {
  DCHECK(ring_buffer_.capacity() > 0);

  JournalChangeItem item;

  {
    FiberAtomicGuard fg;
    item.journal_item.lsn = lsn_++;

    // only used by RestoreStreamer
    item.cmd = entry.payload.cmd;
    item.slot = entry.slot;

    io::StringSink sink;
    JournalWriter writer{&sink};
    writer.Write(entry);

    std::move(sink).str().swap(item.journal_item.data);

    if (item.journal_item.data.size() > 32) {
      // for non-SSO strings capacity should not be much higher than size.
      DCHECK_LE(item.journal_item.data.capacity(), item.journal_item.data.size() * 2);
    }
    VLOG(2) << "Writing item [" << item.journal_item.lsn << "]: " << entry.ToString();
  }

  CallOnChange(&item);
}

void JournalSlice::CallOnChange(JournalChangeItem* change_item) {
  // This lock is never blocking because it contends with UnregisterOnChange, which is cpu only.
  // Hence this lock prevents the UnregisterOnChange to start running in the middle of CallOnChange.
  // CallOnChange is atomic if JournalSlice::SetFlushMode(false) is called before.
  std::shared_lock lk(cb_mu_);
  for (auto k_v : journal_consumers_arr_) {
    k_v.second->ConsumeJournalChange(*change_item);
  }
  auto& item = change_item->journal_item;

  // We preserve order here. After ConsumeJournalChange there can be reordering.
  if (!ring_buffer_.empty()) {
    DCHECK(item.lsn == ring_buffer_.back().lsn + 1);
  }
  auto& data = item.data;

  // Small strings assignment keep the existing capacity intact due to SSO.
  // Shrink strings in this case to prevent excessive memory usage.
  if (data.size() < 32 && data.capacity() > 64) {
    data.shrink_to_fit();
  }
  const size_t item_bytes = ItemBytes(item);
  const uint64_t now_ms = max_age_ms_ != 0 ? GetCurrentTimeMs() : 0;

  Prune(item_bytes, now_ms);

  if (ring_buffer_.full()) {
    const size_t capacity = ring_buffer_.capacity();
    const size_t avg_item_bytes = ring_buffer_bytes_ / capacity;
    DCHECK_GT(avg_item_bytes, 0u);

    const size_t available_bytes =
        max_bytes_ > ring_buffer_bytes_ ? max_bytes_ - ring_buffer_bytes_ : 0;
    const size_t growth = available_bytes / avg_item_bytes;
    if (growth == 0) {
      // Do not grow the metadata buffer once the byte budget is exhausted.
      // Pop explicitly so boost::circular_buffer does not overwrite without accounting for it.
      PopFront();
    } else {
      ring_buffer_.set_capacity(capacity + growth);
    }
  }
  ring_buffer_.push_back(std::move(item));
  ring_buffer_bytes_ += item_bytes;

  if (max_age_ms_ != 0) {
    AddTimeBucket(now_ms);
  }

  if (enable_journal_flush_) {
    for (auto k_v : journal_consumers_arr_) {
      k_v.second->ThrottleIfNeeded();
    }
  }
}

void JournalSlice::AddTimeBucket(uint64_t now_ms) {
  DCHECK_NE(max_age_ms_, 0u);

  const uint64_t bucket_start_ms = now_ms - now_ms % kTimeBucketMs;
  if (!time_buckets_.empty() && bucket_start_ms <= time_buckets_.back().start_time_ms) {
    return;
  }

  time_buckets_.push_back(TimeBucket{bucket_start_ms, ring_buffer_.back().lsn});
}

size_t JournalSlice::ItemBytes(const JournalItem& item) {
  return sizeof(item) + item.data.capacity();
}

void JournalSlice::Prune(size_t next_item_bytes, uint64_t now_ms) {
  if (max_age_ms_ != 0) {
    const uint64_t max_bucket_age_ms = uint64_t{max_age_ms_} + kTimeBucketMs;
    auto first_retained = time_buckets_.begin();
    while (first_retained != time_buckets_.end() && now_ms >= first_retained->start_time_ms &&
           now_ms - first_retained->start_time_ms >= max_bucket_age_ms) {
      ++first_retained;
    }
    if (first_retained != time_buckets_.begin()) {
      const LSN first_retained_lsn =
          first_retained == time_buckets_.end() ? lsn_ : first_retained->first_lsn;
      while (!ring_buffer_.empty() && ring_buffer_.front().lsn < first_retained_lsn) {
        PopFront();
      }
      time_buckets_.erase(time_buckets_.begin(), first_retained);
    }
  }

  while (!ring_buffer_.empty() &&
         (ring_buffer_bytes_ > max_bytes_ || next_item_bytes > max_bytes_ - ring_buffer_bytes_)) {
    PopFront();
  }
}

void JournalSlice::PopFront() {
  const size_t item_bytes = ItemBytes(ring_buffer_.front());
  DCHECK_GE(ring_buffer_bytes_, item_bytes);
  ring_buffer_bytes_ -= item_bytes;
  ring_buffer_.pop_front();
}

uint32_t JournalSlice::RegisterOnChange(JournalConsumerInterface* consumer) {
  // mutex lock isn't needed due to iterators are not invalidated
  uint32_t id = next_cb_id_++;
  journal_consumers_arr_.emplace_back(id, consumer);
  return id;
}

void JournalSlice::UnregisterOnChange(uint32_t id) {
  // we need to wait until callback is finished before remove it
  lock_guard lk(cb_mu_);
  auto it = find_if(journal_consumers_arr_.begin(), journal_consumers_arr_.end(),
                    [id](const auto& e) { return e.first == id; });
  CHECK(it != journal_consumers_arr_.end());
  journal_consumers_arr_.erase(it);
}

}  // namespace journal
}  // namespace dfly
