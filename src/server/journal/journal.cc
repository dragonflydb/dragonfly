// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal.h"

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal_slice.h"

namespace dfly {
namespace journal {

using namespace std;
using namespace util;

namespace {

// Active only in shard threads.
thread_local JournalSlice journal_slice;

}  // namespace

void StartInThread() {
  journal_slice.Init();

  EngineShard* shard = EngineShard::tlocal();
  shard->set_journal(true);
}

void StartInThreadAtLsn(LSN lsn) {
  StartInThread();
  journal_slice.ResetRingBuffer();
  journal_slice.SetStartingLSN(lsn);
}

error_code Close() {
  VLOG(1) << "Journal::Close";

  auto close_cb = [&](auto* shard) {
    journal_slice.ResetRingBuffer();
    shard->set_journal(false);
  };

  shard_set->RunBriefInParallel(close_cb);

  return {};
}

bool HasRegisteredCallbacks() {
  return journal_slice.HasRegisteredCallbacks();
}

bool IsLSNInBuffer(LSN lsn) {
  return journal_slice.IsLSNInBuffer(lsn);
}

std::string_view GetEntry(LSN lsn) {
  return journal_slice.GetEntry(lsn);
}

uint32_t RegisterConsumer(JournalConsumerInterface* consumer) {
  return journal_slice.RegisterOnChange(consumer);
}

void UnregisterConsumer(uint32_t id) {
  journal_slice.UnregisterOnChange(id);
}

LSN GetLsn() {
  return journal_slice.cur_lsn();
}

void RecordEntry(TxId txid, Op opcode, DbIndex dbid, unsigned shard_cnt, std::optional<SlotId> slot,
                 Entry::Payload payload) {
  journal_slice.AddLogRecord(Entry{txid, opcode, dbid, shard_cnt, slot, std::move(payload)});
}

void SetFlushMode(bool allow_flush) {
  journal_slice.SetFlushMode(allow_flush);
}

size_t LsnBufferSize() {
  return journal_slice.GetRingBufferSize();
}

size_t LsnBufferBytes() {
  return journal_slice.GetRingBufferBytes();
}

void ResetBuffer() {
  journal_slice.ResetRingBuffer();
}

size_t thread_local DisableFlushGuard::counter_ = 0;

}  // namespace journal
}  // namespace dfly
