// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/cycle_clock.h"
#include "server/cluster/slot_set.h"
#include "server/common.h"
#include "server/journal/journal.h"
#include "server/journal/pending_buf.h"

namespace dfly {

// Buffered single-shard journal streamer that listens for journal changes with a
// journal listener and writes them to a destination sink in a separate fiber.
class JournalStreamer : public journal::JournalConsumerInterface {
 public:
  enum class SendLsn : uint8_t { NO = 0, YES = 1 };
  JournalStreamer(journal::Journal* journal, ExecutionState* cntx, SendLsn send_lsn,
                  bool is_stable_sync, LSN partial_sync_lsn = 0);
  virtual ~JournalStreamer();

  // Self referential.
  JournalStreamer(const JournalStreamer& other) = delete;
  JournalStreamer(JournalStreamer&& other) = delete;

  // Register journal listener and start writer in fiber.
  virtual void Start(util::FiberSocketBase* dest);

  void ConsumeJournalChange(const journal::JournalChangeItem& item);

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
  void ThrottleIfNeeded() final;

  virtual bool ShouldWrite(const journal::JournalChangeItem& item) const {
    return cntx_->IsRunning();
  }

  void WaitForInflightToComplete();

  size_t inflight_bytes() const {
    return in_flight_bytes_;
  }

  util::FiberSocketBase* dest_ = nullptr;
  ExecutionState* cntx_;
  uint64_t throttle_count_ = 0;
  uint64_t total_throttle_wait_usec_ = 0;
  uint32_t throttle_waiters_ = 0;

 private:
  void AsyncWrite(bool force_send);
  void OnCompletion(std::error_code ec, size_t len);

  bool IsStalled() const;

  journal::Journal* journal_;

  util::fb2::Fiber stalled_data_writer_;
  util::fb2::Done stalled_data_writer_done_;
  void StartStalledDataWriterFiber();
  void StopStalledDataWriterFiber();
  void StalledDataWriterFiber(std::chrono::milliseconds period_ms, util::fb2::Done* waiter);

  PendingBuf pending_buf_;

  // If we are replication in stable sync we can aggregate data before sending
  bool is_stable_sync_;
  LSN partial_sync_lsn_ = 0;
  size_t in_flight_bytes_ = 0, total_sent_ = 0;
  // Last time that send data in milliseconds
  uint64_t last_async_write_time_ = 0;
  time_t last_lsn_time_ = 0;
  LSN last_lsn_writen_ = 0;
  util::fb2::EventCount waker_;
  uint32_t journal_cb_id_{0};
  SendLsn send_lsn_;
};

class CmdSerializer;

// Serializes existing DB as RESTORE commands, and sends updates as regular commands.
// Only handles relevant slots, while ignoring all others.
class RestoreStreamer : public JournalStreamer {
 public:
  RestoreStreamer(DbSlice* slice, cluster::SlotSet slots, journal::Journal* journal,
                  ExecutionState* cntx);
  ~RestoreStreamer() override;

  void Start(util::FiberSocketBase* dest) override;

  void Run();

  // Cancel() must be called if Start() is called
  void Cancel() override;

  void SendFinalize(long attempt);

 private:
  void OnDbChange(DbIndex db_index, const ChangeReq& req);
  bool ShouldWrite(const journal::JournalChangeItem& item) const override;
  bool ShouldWrite(std::string_view key) const;
  bool ShouldWrite(SlotId slot_id) const;

  // Returns true if any entry was actually written
  bool WriteBucket(PrimeTable::bucket_iterator it);

  void WriteEntry(std::string_view key, const PrimeValue& pk, const PrimeValue& pv,
                  uint64_t expire_ms);

  struct Stats {
    uint64_t buckets_skipped = 0;
    uint64_t buckets_written = 0;
    uint64_t buckets_loop = 0;
    uint64_t buckets_on_db_update = 0;
    uint64_t throttle_on_db_update = 0;
    uint64_t throttle_usec_on_db_update = 0;
    uint64_t keys_written = 0;
    uint64_t keys_skipped = 0;
    uint64_t commands = 0;
    uint64_t iter_skips = 0;
  };

  DbSlice* db_slice_;
  DbTableArray db_array_;
  uint64_t snapshot_version_ = 0;
  cluster::SlotSet my_slots_;

  std::unique_ptr<CmdSerializer> cmd_serializer_;

  ThreadLocalMutex big_value_mu_;
  Stats stats_;
  base::RealTimeAggregator cpu_aggregator_;
};

}  // namespace dfly
