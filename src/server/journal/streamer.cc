// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/streamer.h"

#include <absl/functional/bind_front.h>

#include "base/logging.h"

namespace dfly {
using namespace util;

void JournalStreamer::Start(io::Sink* dest) {
  using namespace journal;
  write_fb_ = fb2::Fiber("journal_stream", &JournalStreamer::WriterFb, this, dest);
  journal_cb_id_ = journal_->RegisterOnChange([this](const JournalItem& item, bool allow_await) {
    if (!ShouldWrite(item)) {
      return;
    }

    if (item.opcode == Op::NOOP) {
      // No record to write, just await if data was written so consumer will read the data.
      return AwaitIfWritten();
    }

    Write(io::Buffer(item.data));
    NotifyWritten(allow_await);
  });
}

void JournalStreamer::Cancel() {
  Finalize();  // Finalize must be called before UnregisterOnChange because we first need to stop
               // writing to buffer and notify the all the producers.
               // Writing to journal holds mutex protecting change_cb_arr_, than the fiber can
               // preemt when calling NotifyWritten and it will not run again till notified.
               // UnregisterOnChange will try to lock the mutex therefor calling UnregisterOnChange
               // before Finalize may cause deadlock.
  journal_->UnregisterOnChange(journal_cb_id_);

  if (write_fb_.IsJoinable()) {
    write_fb_.Join();
  }
}

void JournalStreamer::WriterFb(io::Sink* dest) {
  if (auto ec = ConsumeIntoSink(dest); ec) {
    cntx_->ReportError(ec);
  }
}

RestoreStreamer::RestoreStreamer(DbSlice* slice, SlotSet slots, uint32_t sync_id,
                                 journal::Journal* journal, Context* cntx)
    : JournalStreamer(journal, cntx),
      db_slice_(slice),
      my_slots_(std::move(slots)),
      sync_id_(sync_id) {
  DCHECK(slice != nullptr);
}

void RestoreStreamer::Start(io::Sink* dest) {
  VLOG(2) << "RestoreStreamer start";
  auto db_cb = absl::bind_front(&RestoreStreamer::OnDbChange, this);
  snapshot_version_ = db_slice_->RegisterOnChange(std::move(db_cb));

  JournalStreamer::Start(dest);

  DCHECK(!snapshot_fb_.IsJoinable());
  snapshot_fb_ = fb2::Fiber("slot-snapshot", [this] {
    PrimeTable::Cursor cursor;
    uint64_t last_yield = 0;
    PrimeTable* pt = &db_slice_->databases()[0]->prime;

    do {
      if (fiber_cancellation_.IsCancelled())
        return;

      cursor = pt->Traverse(cursor, absl::bind_front(&RestoreStreamer::WriteBucket, this));
      ++last_yield;

      if (last_yield >= 100) {
        ThisFiber::Yield();
        last_yield = 0;
      }
    } while (cursor);

    VLOG(2) << "FULL-SYNC-CUT for " << sync_id_ << " : " << db_slice_->shard_id();
    WriteCommand(make_pair("DFLYMIGRATE", ArgSlice{"FULL-SYNC-CUT", absl::StrCat(sync_id_),
                                                   absl::StrCat(db_slice_->shard_id())}));
    NotifyWritten(true);
    snapshot_finished_ = true;
  });
}

void RestoreStreamer::SendFinalize() {
  VLOG(2) << "DFLYMIGRATE FINALIZE for " << sync_id_ << " : " << db_slice_->shard_id();
  journal::Entry entry(journal::Op::FIN, 0 /*db_id*/, 0 /*slot_id*/);

  JournalWriter writer{this};
  writer.Write(entry);
  NotifyWritten(true);
}

void RestoreStreamer::Cancel() {
  fiber_cancellation_.Cancel();
  snapshot_fb_.JoinIfNeeded();
  db_slice_->UnregisterOnChange(snapshot_version_);
  JournalStreamer::Cancel();
}

bool RestoreStreamer::ShouldWrite(const journal::JournalItem& item) const {
  if (!item.slot.has_value()) {
    return false;
  }

  return ShouldWrite(*item.slot);
}

bool RestoreStreamer::ShouldWrite(std::string_view key) const {
  return ShouldWrite(ClusterConfig::KeySlot(key));
}

bool RestoreStreamer::ShouldWrite(SlotId slot_id) const {
  return my_slots_.contains(slot_id);
}

void RestoreStreamer::WriteBucket(PrimeTable::bucket_iterator it) {
  bool is_data_present = false;

  if (it.GetVersion() < snapshot_version_) {
    it.SetVersion(snapshot_version_);
    FiberAtomicGuard fg;  // Can't switch fibers because that could invalidate iterator
    string key_buffer;    // we can reuse it
    for (; !it.is_done(); ++it) {
      const auto& pv = it->second;
      string_view key = it->first.GetSlice(&key_buffer);
      if (ShouldWrite(key)) {
        is_data_present = true;

        uint64_t expire = 0;
        if (pv.HasExpire()) {
          auto eit = db_slice_->databases()[0]->expire.Find(it->first);
          expire = db_slice_->ExpireTime(eit);
        }

        WriteEntry(key, pv, expire);
      }
    }
  }

  if (is_data_present)
    NotifyWritten(true);
}

void RestoreStreamer::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  DCHECK_EQ(db_index, 0) << "Restore migration only allowed in cluster mode in db0";

  FiberAtomicGuard fg;
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

void RestoreStreamer::WriteEntry(string_view key, const PrimeValue& pv, uint64_t expire_ms) {
  absl::InlinedVector<string_view, 4> args;
  args.push_back(key);

  string expire_str = absl::StrCat(expire_ms);
  args.push_back(expire_str);

  io::StringSink value_dump_sink;
  SerializerBase::DumpObject(pv, &value_dump_sink);
  args.push_back(value_dump_sink.str());

  args.push_back("ABSTTL");  // Means expire string is since epoch

  WriteCommand(make_pair("RESTORE", ArgSlice{args}));
}

void RestoreStreamer::WriteCommand(journal::Entry::Payload cmd_payload) {
  journal::Entry entry(0,                     // txid
                       journal::Op::COMMAND,  // single command
                       0,                     // db index
                       1,                     // shard count
                       0,                     // slot-id, but it is ignored at this level
                       cmd_payload);

  JournalWriter writer{this};
  writer.Write(entry);
}

}  // namespace dfly
