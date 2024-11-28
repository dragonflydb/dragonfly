// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

#include <absl/functional/bind_front.h>

#include "base/flags.h"
#include "base/logging.h"
#include "server/cluster/cluster_defs.h"
#include "server/journal/cmd_serializer.h"
#include "util/fibers/synchronization.h"

using namespace facade;

ABSL_FLAG(uint32_t, replication_timeout, 30000,
          "Time in milliseconds to wait for the replication writes being stuck.");

ABSL_FLAG(uint32_t, replication_stream_output_limit, 64_KB,
          "Time to wait for the replication output buffer go below the throttle limit");

namespace dfly {
using namespace util;
using namespace journal;

namespace {

iovec IoVec(io::Bytes src) {
  return iovec{const_cast<uint8_t*>(src.data()), src.size()};
}

uint32_t replication_stream_output_limit_cached = 64_KB;

}  // namespace

JournalStreamer::JournalStreamer(journal::Journal* journal, Context* cntx)
    : cntx_(cntx), journal_(journal) {
  // cache the flag to avoid accessing it later.
  replication_stream_output_limit_cached = absl::GetFlag(FLAGS_replication_stream_output_limit);
}

JournalStreamer::~JournalStreamer() {
  DCHECK_EQ(in_flight_bytes_, 0u);
  VLOG(1) << "~JournalStreamer";
}

void JournalStreamer::Start(util::FiberSocketBase* dest, bool send_lsn) {
  CHECK(dest_ == nullptr && dest != nullptr);
  dest_ = dest;
  journal_cb_id_ =
      journal_->RegisterOnChange([this, send_lsn](const JournalItem& item, bool allow_await) {
        if (allow_await) {
          ThrottleIfNeeded();
          // No record to write, just await if data was written so consumer will read the data.
          if (item.opcode == Op::NOOP)
            return;
        }

        if (!ShouldWrite(item)) {
          return;
        }

        Write(item.data);
        time_t now = time(nullptr);

        // TODO: to chain it to the previous Write call.
        if (send_lsn && now - last_lsn_time_ > 3) {
          last_lsn_time_ = now;
          io::StringSink sink;
          JournalWriter writer(&sink);
          writer.Write(Entry{journal::Op::LSN, item.lsn});
          Write(sink.str());
        }
      });
}

void JournalStreamer::Cancel() {
  VLOG(1) << "JournalStreamer::Cancel";
  waker_.notifyAll();
  journal_->UnregisterOnChange(journal_cb_id_);
  WaitForInflightToComplete();
}

size_t JournalStreamer::GetTotalBufferCapacities() const {
  return in_flight_bytes_ + pending_buf_.capacity();
}

void JournalStreamer::Write(std::string_view str) {
  DCHECK(!str.empty());
  DVLOG(3) << "Writing " << str.size() << " bytes";

  size_t total_pending = pending_buf_.size() + str.size();

  if (in_flight_bytes_ > 0) {
    // We can not flush data while there are in flight requests because AsyncWrite
    // is not atomic. Therefore, we just aggregate.
    size_t tail = pending_buf_.size();
    pending_buf_.resize(pending_buf_.size() + str.size());
    memcpy(pending_buf_.data() + tail, str.data(), str.size());
    return;
  }

  // If we do not have any in flight requests we send the string right a way.
  // We can not aggregate it since we do not know when the next update will follow.
  // because of potential SOO with strings, we allocate explicitly on heap.
  uint8_t* buf(new uint8_t[str.size()]);

  // TODO: it is possible to remove these redundant copies if we adjust high level
  // interfaces to pass reference-counted buffers.
  memcpy(buf, str.data(), str.size());
  in_flight_bytes_ += total_pending;
  total_sent_ += total_pending;

  iovec v[2];
  unsigned next_buf_id = 0;

  if (!pending_buf_.empty()) {
    v[0] = IoVec(pending_buf_);
    ++next_buf_id;
  }
  v[next_buf_id++] = IoVec(io::Bytes(buf, str.size()));

  dest_->AsyncWrite(
      v, next_buf_id,
      [buf0 = std::move(pending_buf_), buf, this, len = total_pending](std::error_code ec) {
        delete[] buf;
        OnCompletion(ec, len);
      });
}

void JournalStreamer::OnCompletion(std::error_code ec, size_t len) {
  DCHECK_GE(in_flight_bytes_, len);

  DVLOG(3) << "Completing from " << in_flight_bytes_ << " to " << in_flight_bytes_ - len;
  in_flight_bytes_ -= len;
  if (ec && !IsStopped()) {
    cntx_->ReportError(ec);
  } else if (in_flight_bytes_ == 0 && !pending_buf_.empty() && !IsStopped()) {
    // If everything was sent but we have a pending buf, flush it.
    io::Bytes src(pending_buf_);
    in_flight_bytes_ += src.size();
    dest_->AsyncWrite(src, [buf = std::move(pending_buf_), this](std::error_code ec) {
      OnCompletion(ec, buf.size());
    });
  }

  // notify ThrottleIfNeeded or WaitForInflightToComplete that waits
  // for all the completions to finish.
  // ThrottleIfNeeded can run from multiple fibers in the journal thread.
  // For example, from Heartbeat calling TriggerJournalWriteToSink to flush potential
  // expiration deletions and there are other cases as well.
  waker_.notifyAll();
}

void JournalStreamer::ThrottleIfNeeded() {
  if (IsStopped() || !IsStalled())
    return;

  auto next =
      chrono::steady_clock::now() + chrono::milliseconds(absl::GetFlag(FLAGS_replication_timeout));
  size_t inflight_start = in_flight_bytes_;
  size_t sent_start = total_sent_;

  std::cv_status status =
      waker_.await_until([this]() { return !IsStalled() || IsStopped(); }, next);
  if (status == std::cv_status::timeout) {
    LOG(WARNING) << "Stream timed out, inflight bytes/sent start: " << inflight_start << "/"
                 << sent_start << ", end: " << in_flight_bytes_ << "/" << total_sent_;
    cntx_->ReportError("JournalStreamer write operation timeout");
  }
}

void JournalStreamer::WaitForInflightToComplete() {
  while (in_flight_bytes_) {
    auto next = chrono::steady_clock::now() + 1s;
    std::cv_status status =
        waker_.await_until([this] { return this->in_flight_bytes_ == 0; }, next);
    LOG_IF(WARNING, status == std::cv_status::timeout)
        << "Waiting for inflight bytes " << in_flight_bytes_;
  }
}

bool JournalStreamer::IsStalled() const {
  return in_flight_bytes_ + pending_buf_.size() >= replication_stream_output_limit_cached;
}

RestoreStreamer::RestoreStreamer(DbSlice* slice, cluster::SlotSet slots, journal::Journal* journal,
                                 Context* cntx)
    : JournalStreamer(journal, cntx), db_slice_(slice), my_slots_(std::move(slots)) {
  DCHECK(slice != nullptr);
  db_array_ = slice->databases();  // Inc ref to make sure DB isn't deleted while we use it
}

void RestoreStreamer::Start(util::FiberSocketBase* dest, bool send_lsn) {
  if (fiber_cancelled_)
    return;

  VLOG(1) << "RestoreStreamer start";
  auto db_cb = absl::bind_front(&RestoreStreamer::OnDbChange, this);
  snapshot_version_ = db_slice_->RegisterOnChange(std::move(db_cb));

  JournalStreamer::Start(dest, send_lsn);
}

void RestoreStreamer::Run() {
  VLOG(1) << "RestoreStreamer run";

  PrimeTable::Cursor cursor;
  uint64_t last_yield = 0;
  PrimeTable* pt = &db_array_[0]->prime;

  do {
    if (fiber_cancelled_)
      return;
    cursor = pt->Traverse(cursor, [&](PrimeTable::bucket_iterator it) {
      db_slice_->FlushChangeToEarlierCallbacks(0 /*db_id always 0 for cluster*/,
                                               DbSlice::Iterator::FromPrime(it), snapshot_version_);
      WriteBucket(it);
    });

    if (++last_yield >= 100) {
      ThisFiber::Yield();
      last_yield = 0;
    }
  } while (cursor);
}

void RestoreStreamer::SendFinalize(long attempt) {
  VLOG(1) << "RestoreStreamer LSN opcode for : " << db_slice_->shard_id() << " attempt " << attempt;
  journal::Entry entry(journal::Op::LSN, attempt);

  io::StringSink sink;
  JournalWriter writer{&sink};
  writer.Write(entry);
  Write(sink.str());

  // TODO: is the intent here to flush everything?
  //
  ThrottleIfNeeded();
}

RestoreStreamer::~RestoreStreamer() {
}

void RestoreStreamer::Cancel() {
  auto sver = snapshot_version_;
  snapshot_version_ = 0;  // to prevent double cancel in another fiber
  fiber_cancelled_ = true;
  if (sver != 0) {
    db_slice_->UnregisterOnChange(sver);
    JournalStreamer::Cancel();
  }
}

bool RestoreStreamer::ShouldWrite(const journal::JournalItem& item) const {
  if (item.cmd == "FLUSHALL" || item.cmd == "FLUSHDB") {
    // On FLUSH* we restart the migration
    CHECK(dest_ != nullptr);
    cntx_->ReportError("FLUSH command during migration");
    std::ignore = dest_->Shutdown(SHUT_RDWR);
    return false;
  }

  if (!item.slot.has_value()) {
    return false;
  }

  return ShouldWrite(*item.slot);
}

bool RestoreStreamer::ShouldWrite(std::string_view key) const {
  return ShouldWrite(cluster::KeySlot(key));
}

bool RestoreStreamer::ShouldWrite(cluster::SlotId slot_id) const {
  return my_slots_.Contains(slot_id);
}

void RestoreStreamer::WriteBucket(PrimeTable::bucket_iterator it) {
  if (it.GetVersion() < snapshot_version_) {
    FiberAtomicGuard fg;
    it.SetVersion(snapshot_version_);
    string key_buffer;  // we can reuse it
    for (; !it.is_done(); ++it) {
      const auto& pv = it->second;
      string_view key = it->first.GetSlice(&key_buffer);
      if (ShouldWrite(key)) {
        uint64_t expire = 0;
        if (pv.HasExpire()) {
          auto eit = db_slice_->databases()[0]->expire.Find(it->first);
          expire = db_slice_->ExpireTime(eit);
        }

        WriteEntry(key, it->first, pv, expire);
      }
    }
  }
  ThrottleIfNeeded();
}

void RestoreStreamer::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  DCHECK_EQ(db_index, 0) << "Restore migration only allowed in cluster mode in db0";

  PrimeTable* table = db_slice_->GetTables(0).first;

  if (const PrimeTable::bucket_iterator* bit = req.update()) {
    WriteBucket(*bit);
  } else {
    string_view key = get<string_view>(req.change);
    table->CVCUponInsert(snapshot_version_, key, [this](PrimeTable::bucket_iterator it) {
      DCHECK_LT(it.GetVersion(), snapshot_version_);
      WriteBucket(it);
    });
  }
}

void RestoreStreamer::WriteEntry(string_view key, const PrimeValue& pk, const PrimeValue& pv,
                                 uint64_t expire_ms) {
  CmdSerializer serializer([&](std::string s) { Write(s); });
  serializer.SerializeEntry(key, pk, pv, expire_ms);
}

}  // namespace dfly
