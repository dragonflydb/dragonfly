// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_slice.h"

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/flags/reflection.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>

#include "base/function2.hpp"
#include "base/logging.h"
#include "server/common.h"
#include "server/engine_shard_set.h"
#include "server/journal/serializer.h"
#include "strings/human_readable.h"
#include "util/fibers/fibers.h"

ABSL_FLAG(uint32_t, shard_repl_backlog_len, 0,
          "Legacy maximum number of entries retained by each shard's replication backlog. "
          "A nonzero value disables time- and byte-based eviction unless either new backlog "
          "limit flag is explicitly configured.");
ABSL_FLAG(uint32_t, shard_repl_backlog_time_ms, 5000,
          "Target retention age in milliseconds of entries in the per-shard replication backlog. "
          "Entries older than this are evicted on later journal writes. 0 disables time-based "
          "eviction.");
ABSL_FLAG(strings::MemoryBytesFlag, shard_repl_backlog_max_bytes, 0,
          "Maximum bytes retained by each shard's replication backlog. 0 (the default) uses "
          "maxmemory / shard count / 200.");

namespace dfly {
namespace journal {
using namespace std;
using namespace util;

namespace {
constexpr size_t kMaxTimeEvictionsPerCall = 100;

bool IsFlagConfigured(const absl::CommandLineFlag& handle) {
  // DFLY_ environment flags are parsed programmatically (see ParseFlagsFromEnv) and do not set
  // Abseil's command-line marker. Check their presence separately so an explicitly configured
  // flag selects the new mode even when its value equals the default one.
  return absl::flags_internal::WasPresentOnCommandLine(handle.Name()) ||
         handle.CurrentValue() != handle.DefaultValue() ||
         std::getenv(absl::StrCat("DFLY_", handle.Name()).c_str()) != nullptr;
}

bool AreNewBacklogLimitsConfigured() {
  return IsFlagConfigured(absl::GetFlagReflectionHandle(FLAGS_shard_repl_backlog_time_ms)) ||
         IsFlagConfigured(absl::GetFlagReflectionHandle(FLAGS_shard_repl_backlog_max_bytes));
}

size_t GetPerShardBacklogMaxBytes() {
  const size_t per_shard_max_bytes = absl::GetFlag(FLAGS_shard_repl_backlog_max_bytes).value;
  if (per_shard_max_bytes != 0) {
    return per_shard_max_bytes;
  }

  constexpr size_t kBacklogMemoryFraction = 200;
  const size_t shard_count = shard_set ? max<size_t>(1, shard_set->size()) : 1;
  return max_memory_limit.load(memory_order_relaxed) / shard_count / kBacklogMemoryFraction;
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

  const uint32_t legacy_entry_limit = absl::GetFlag(FLAGS_shard_repl_backlog_len);
  if (legacy_entry_limit != 0 && !AreNewBacklogLimitsConfigured()) {
    LOG_FIRST_N(WARNING, 1) << "Using deprecated --shard_repl_backlog_len=" << legacy_entry_limit
                            << ". Time- and byte-based backlog eviction is disabled. Prefer "
                               "--shard_repl_backlog_time_ms/--shard_repl_backlog_max_bytes.";
    use_legacy_entry_limit_ = true;
    ring_buffer_.set_capacity(legacy_entry_limit);
    return;
  }

  ring_buffer_.set_capacity(8192);
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
  auto& item = change_item->journal_item;
  const uint64_t now_ms = GetCurrentTimeMs();
  item.time_ms = now_ms;

  std::shared_lock lk(cb_mu_);
  for (auto k_v : journal_consumers_arr_) {
    k_v.second->ConsumeJournalChange(*change_item);
  }

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
  CleanEntries(item_bytes, now_ms);
  ring_buffer_.push_back(std::move(item));
  ring_buffer_bytes_ += item_bytes;

  if (enable_journal_flush_) {
    for (auto k_v : journal_consumers_arr_) {
      k_v.second->ThrottleIfNeeded();
    }
  }
}

size_t JournalSlice::ItemBytes(const JournalItem& item) {
  return sizeof(item) + item.data.capacity();
}

void JournalSlice::CleanEntries(size_t next_item_bytes, uint64_t now_ms) {
  if (use_legacy_entry_limit_) {
    if (ring_buffer_.full()) {
      const size_t evicted_item_bytes = ItemBytes(ring_buffer_.front());
      DCHECK_GE(ring_buffer_bytes_, evicted_item_bytes);
      ring_buffer_bytes_ -= evicted_item_bytes;
    }
    return;
  }

  size_t retained_bytes = ring_buffer_bytes_;
  size_t time_evictions = 0;
  size_t bytes_to_free = 0;

  auto first_retained = ring_buffer_.begin();
  while (first_retained != ring_buffer_.end()) {
    const bool exceeds_byte_limit =
        retained_bytes > max_bytes_ || next_item_bytes > max_bytes_ - retained_bytes;
    const bool expired = max_age_ms_ != 0 && now_ms >= first_retained->time_ms &&
                         now_ms - first_retained->time_ms >= max_age_ms_;
    const bool evict_for_time = expired && time_evictions < kMaxTimeEvictionsPerCall;
    bool evict_for_capacity = bytes_to_free != 0;

    if (!exceeds_byte_limit && !evict_for_time && !evict_for_capacity &&
        first_retained == ring_buffer_.begin() && ring_buffer_.full()) {
      const size_t capacity = ring_buffer_.capacity();
      const size_t avg_item_bytes = retained_bytes / capacity;
      DCHECK_GT(avg_item_bytes, 0u);

      const size_t available_bytes = max_bytes_ > retained_bytes ? max_bytes_ - retained_bytes : 0;
      const size_t growth = available_bytes / avg_item_bytes;
      if (growth != 0) {
        ring_buffer_.set_capacity(capacity + growth);
        return;
      }
      bytes_to_free = next_item_bytes;
      evict_for_capacity = true;
    }

    if (!exceeds_byte_limit && !evict_for_time && !evict_for_capacity) {
      break;
    }

    const size_t item_bytes = ItemBytes(*first_retained);
    DCHECK_GE(retained_bytes, item_bytes);
    retained_bytes -= item_bytes;
    if (evict_for_capacity) {
      bytes_to_free = item_bytes >= bytes_to_free ? 0 : bytes_to_free - item_bytes;
    }
    if (evict_for_time && !exceeds_byte_limit && !evict_for_capacity) {
      ++time_evictions;
    }
    ++first_retained;
  }

  if (first_retained != ring_buffer_.begin()) {
    ring_buffer_bytes_ = retained_bytes;
    ring_buffer_.rerase(ring_buffer_.begin(), first_retained);
  }
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
