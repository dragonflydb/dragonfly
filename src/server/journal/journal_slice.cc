// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_slice.h"

#include <absl/flags/flag.h>
#include <absl/strings/escaping.h>
#include <fcntl.h>

#include "base/function2.hpp"
#include "base/logging.h"
#include "server/journal/serializer.h"
#include "util/fibers/fibers.h"

ABSL_FLAG(uint32_t, shard_repl_backlog_len, 8192,
          "The length of the circular replication log per shard");
ABSL_FLAG(uint64_t, shard_repl_backlog_max_bytes_soft, 0,
          "Soft limit for number of bytes held in replication log (0 is unlimited)");

namespace dfly {
namespace journal {
using namespace std;
using namespace util;

JournalSlice::JournalSlice() {
}

JournalSlice::~JournalSlice() {
}

void JournalSlice::Init() {
  // calling this function multiple times is allowed and it's a no-op.
  if (ring_buffer_.capacity() > 0)
    return;

  ring_buffer_.set_capacity(absl::GetFlag(FLAGS_shard_repl_backlog_len));
  ring_buffer_max_bytes_ = absl::GetFlag(FLAGS_shard_repl_backlog_max_bytes_soft);
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

    io::BufSink buf_sink{&ring_serialize_buf_};
    JournalWriter writer{&buf_sink};
    writer.Write(entry);

    // Deep copy here
    item.journal_item.data = io::View(ring_serialize_buf_.InputBuffer());
    ring_serialize_buf_.Clear();
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

  if (const auto new_item_size = sizeof(item) + item.data.size();
      ring_buffer_max_bytes_ && ring_buffer_bytes_ + new_item_size > ring_buffer_max_bytes_) {
    EvictEntries(new_item_size);
  }

  // We preserve order here. After ConsumeJournalChange there can reordering
  if (ring_buffer_.size() == ring_buffer_.capacity()) {
    const size_t bytes_removed = ring_buffer_.front().data.size() + sizeof(item);
    DCHECK_GE(ring_buffer_bytes_, bytes_removed);
    ring_buffer_bytes_ -= bytes_removed;
  }
  if (!ring_buffer_.empty()) {
    DCHECK(item.lsn == ring_buffer_.back().lsn + 1);
  }
  ring_buffer_.push_back(std::move(item));
  ring_buffer_bytes_ += sizeof(item) + ring_buffer_.back().data.size();

  if (enable_journal_flush_) {
    for (auto k_v : journal_consumers_arr_) {
      k_v.second->ThrottleIfNeeded();
    }
  }
}

void JournalSlice::EvictEntries(size_t new_item_size) {
  // Do not evict more than sqrt of current size. Using sqrt instead of a fixed percentage here
  // makes sure that the eviction target grows with size, but gently, so as not to spend too much
  // time on eviction. The idea is that repeated adding of records slowly brings the buffer size
  // closer to the target of ring_buffer_max_bytes_, rather than all at once. The limit can be
  // breached temporarily. The max(1) part ensures progress if the size of buffer is too small. In
  // pathological cases (stream of huge entries) there will be one or more evictions per addition,
  // in this cases the limits should be increased.
  size_t max_to_evict = static_cast<size_t>(std::max(1.0, sqrt(ring_buffer_.size())));
  VLOG(2) << "Evicting " << max_to_evict << " items to reach " << ring_buffer_max_bytes_
          << ", total size " << ring_buffer_.size() << ", total bytes " << ring_buffer_bytes_;
  size_t items_evicted = 0;
  size_t bytes_evicted = 0;
  while (!ring_buffer_.empty() && ring_buffer_bytes_ + new_item_size > ring_buffer_max_bytes_ &&
         max_to_evict-- > 0) {
    const size_t bytes_removed = ring_buffer_.front().data.size() + sizeof(JournalItem);
    DCHECK_GE(ring_buffer_bytes_, bytes_removed);
    ring_buffer_bytes_ -= bytes_removed;
    ring_buffer_.pop_front();

    items_evicted++;
    bytes_evicted += bytes_removed;
  }

  VLOG(2) << "Evicted " << items_evicted << " items to free up " << bytes_evicted << " bytes";
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

size_t JournalSlice::GetRingBufferSize() const {
  return ring_buffer_.size();
}

size_t JournalSlice::GetRingBufferBytes() const {
  return ring_buffer_bytes_;
}

void JournalSlice::ResetRingBuffer() {
  ring_buffer_.clear();
  ring_buffer_bytes_ = 0;
}

}  // namespace journal
}  // namespace dfly
