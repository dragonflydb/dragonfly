// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "base/cycle_clock.h"
#include "server/cluster/slot_set.h"
#include "server/common_types.h"
#include "server/execution_state.h"
#include "server/journal/buffered_socket_writer.h"
#include "server/journal/journal.h"
#include "server/serializer_base.h"
#include "server/synchronization.h"
#include "util/fiber_socket_base.h"

namespace dfly {

// Streams a single shard's journal to a replica during stable sync.
// Optionally catches up from the partial sync buffer first, and periodically sends LSN markers.
class ReplicaStreamer : public journal::JournalConsumerInterface {
 public:
  // start_partial_sync_at: if not 0, first stream journal entries from this LSN from the
  // partial sync buffer, and only then register for new journal changes.
  ReplicaStreamer(ExecutionState* cntx, LSN start_partial_sync_at);
  ~ReplicaStreamer();

  // Register journal listener and start the periodic flush fiber.
  void Start(util::FiberSocketBase* dest);

  // Must be called on context cancellation for unblocking
  // and manual cleanup. If it unregistered a listener, returns true.
  bool Cancel();

  size_t UsedBytes() const {
    return writer_.pending_bytes();
  }

  // For debugging purposes. Return string with formatted internal state.
  std::string FormatInternalState() const;

 private:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) final;
  void ThrottleIfNeeded() final;

  // Return true if all lsn's from start_partial_sync_at_ were sent (or if started from 0).
  // Return false if not all lsn's were sent (stalled) in time. Cancels the context with error.
  bool MaybePartialStreamLSNs();

  void PeriodicFlushFiber(std::chrono::milliseconds period);

  ExecutionState* cntx_;
  const LSN start_partial_sync_at_;
  BufferedSocketWriter writer_;

  util::fb2::Fiber flush_fiber_;
  util::fb2::Done flush_fiber_done_;

  time_t last_lsn_time_ = 0;
  LSN last_lsn_writen_ = 0;
  uint32_t journal_cb_id_{0};
};

class CmdSerializer;

// Streams the migrated slots of a single shard to the target node during slot migration.
// Serializes existing DB as RESTORE commands, and sends updates as regular commands.
// Only handles relevant slots, while ignoring all others.
class SlotMigrationStreamer : public journal::JournalConsumerInterface, public SerializerBase {
 public:
  SlotMigrationStreamer(DbSlice* slice, cluster::SlotSet slots, ExecutionState* cntx);
  ~SlotMigrationStreamer() override;

  void Start(util::FiberSocketBase* dest);

  void Run();

  // Cancel() must be called if Start() is called
  bool Cancel();

  void SendFinalize(long attempt);

 private:
  void ConsumeJournalChange(const journal::JournalChangeItem& item) final;
  void ThrottleIfNeeded() final;

  unsigned SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                 bool on_update) override;

  void SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                            time_t expire, uint32_t mc_flags) override;

  bool ShouldWrite(const journal::JournalChangeItem& item) const;
  bool ShouldWrite(std::string_view key) const;
  bool ShouldWrite(SlotId slot_id) const;

  struct Stats {
    uint64_t buckets_loop = 0;
    uint64_t throttle_on_db_update = 0;
    uint64_t throttle_usec_on_db_update = 0;
    uint64_t keys_skipped = 0;
    uint64_t commands = 0;
    uint64_t iter_skips = 0;
  };

  ExecutionState* cntx_;
  BufferedSocketWriter writer_;
  cluster::SlotSet my_slots_;

  std::unique_ptr<CmdSerializer> cmd_serializer_;

  Stats stats_;
  base::RealTimeAggregator cpu_aggregator_;
  LSN last_lsn_writen_ = 0;
  uint32_t journal_cb_id_{0};
};

}  // namespace dfly
