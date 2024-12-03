// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <deque>

#include "server/common.h"
#include "server/db_slice.h"
#include "server/journal/journal.h"
#include "server/journal/serializer.h"
#include "server/rdb_save.h"

namespace dfly {

// Buffered single-shard journal streamer that listens for journal changes with a
// journal listener and writes them to a destination sink in a separate fiber.
class JournalStreamer {
 public:
  JournalStreamer(journal::Journal* journal, Context* cntx);
  virtual ~JournalStreamer();

  // Self referential.
  JournalStreamer(const JournalStreamer& other) = delete;
  JournalStreamer(JournalStreamer&& other) = delete;

  // Register journal listener and start writer in fiber.
  virtual void Start(util::FiberSocketBase* dest, bool send_lsn);

  // Must be called on context cancellation for unblocking
  // and manual cleanup.
  virtual void Cancel();

  size_t UsedBytes() const;

 protected:
  // TODO: we copy the string on each write because JournalItem may be passed to multiple
  // streamers so we can not move it. However, if we would either wrap JournalItem in shared_ptr
  // or wrap JournalItem::data in shared_ptr, we can avoid the cost of copying strings.
  // Also, for small strings it's more peformant to copy to the intermediate buffer than
  // to issue an io operation.
  void Write(std::string str);

  // Blocks the if the consumer if not keeping up.
  void ThrottleIfNeeded();

  virtual bool ShouldWrite(const journal::JournalItem& item) const {
    return !IsStopped();
  }

  void WaitForInflightToComplete();

  util::FiberSocketBase* dest_ = nullptr;
  Context* cntx_;

 private:
  void AsyncWrite();
  void OnCompletion(std::error_code ec, size_t len);

  bool IsStopped() const {
    return cntx_->IsCancelled();
  }

  bool IsStalled() const;

  journal::Journal* journal_;

  class PendingBuf {
   public:
    struct Buf {
      size_t mem_size = 0;
      std::vector<std::string> buf;  // TODO check inlinedVector option

      static constexpr size_t max_buf_size = 1024;  // depends on UIO_MAXIOV
    };

    PendingBuf() : bufs_(1) {
    }

    bool empty() const {
      return std::all_of(bufs_.begin(), bufs_.end(), [](const auto& b) { return b.buf.empty(); });
    }

    void push(std::string str) {
      DCHECK(!bufs_.empty());
      if (bufs_.back().buf.size() == Buf::max_buf_size) {
        bufs_.emplace_back();
      }
      auto& fron_buf = bufs_.back();

      fron_buf.mem_size += str.size();
      fron_buf.buf.push_back(std::move(str));
    }

    // should be called to get the next buffer for sending
    const Buf& PrepareNext() {
      if (bufs_.size() == 1) {
        bufs_.emplace_back();
      }
      return bufs_.front();
    }

    // should be called when the buf from PrepareNext() method was sent
    void Pop() {
      DCHECK(bufs_.size() >= 2);
      bufs_.pop_front();
    }

    size_t size() const {
      return std::accumulate(bufs_.begin(), bufs_.end(), 0,
                             [](size_t s, const auto& b) { return s + b.mem_size; });
    }

   private:
    std::deque<Buf> bufs_;
  };
  PendingBuf pending_buf_;

  size_t in_flight_bytes_ = 0, total_sent_ = 0;
  time_t last_lsn_time_ = 0;
  util::fb2::EventCount waker_;
  uint32_t journal_cb_id_{0};
};

// Serializes existing DB as RESTORE commands, and sends updates as regular commands.
// Only handles relevant slots, while ignoring all others.
class RestoreStreamer : public JournalStreamer {
 public:
  RestoreStreamer(DbSlice* slice, cluster::SlotSet slots, journal::Journal* journal, Context* cntx);
  ~RestoreStreamer() override;

  void Start(util::FiberSocketBase* dest, bool send_lsn = false) override;

  void Run();

  // Cancel() must be called if Start() is called
  void Cancel() override;

  void SendFinalize(long attempt);

  bool IsSnapshotFinished() const {
    return snapshot_finished_;
  }

 private:
  void OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req);
  bool ShouldWrite(const journal::JournalItem& item) const override;
  bool ShouldWrite(std::string_view key) const;
  bool ShouldWrite(cluster::SlotId slot_id) const;

  // Returns whether anything was written
  void WriteBucket(PrimeTable::bucket_iterator it);
  void WriteEntry(std::string_view key, const PrimeValue& pk, const PrimeValue& pv,
                  uint64_t expire_ms);

  DbSlice* db_slice_;
  DbTableArray db_array_;
  uint64_t snapshot_version_ = 0;
  cluster::SlotSet my_slots_;
  bool fiber_cancelled_ = false;
  bool snapshot_finished_ = false;
};

}  // namespace dfly
