// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

extern "C" {
#include "redis/object.h"
}

#include <absl/functional/bind_front.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/rdb_extensions.h"
#include "server/rdb_save.h"
#include "util/fiber_sched_algo.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace chrono_literals;
namespace this_fiber = ::boost::this_fiber;
using boost::fibers::fiber;

SliceSnapshot::SliceSnapshot(DbSlice* slice, RecordChannel* dest, CompressionMode compression_mode)

    : db_slice_(slice), dest_(dest), compression_mode_(compression_mode) {
  db_array_ = slice->databases();
}

SliceSnapshot::~SliceSnapshot() {
}

void SliceSnapshot::Start(bool stream_journal, const Cancellation* cll) {
  DCHECK(!snapshot_fb_.joinable());

  auto db_cb = absl::bind_front(&SliceSnapshot::OnDbChange, this);
  snapshot_version_ = db_slice_->RegisterOnChange(move(db_cb));

  if (stream_journal) {
    auto* journal = db_slice_->shard_owner()->journal();
    DCHECK(journal);
    auto journal_cb = absl::bind_front(&SliceSnapshot::OnJournalEntry, this);
    journal_cb_id_ = journal->RegisterOnChange(move(journal_cb));
  }

  default_serializer_.reset(new RdbSerializer(compression_mode_));

  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  snapshot_fb_ = fiber([this, stream_journal, cll] {
    IterateBucketsFb(cll);
    if (cll->IsCancelled()) {
      Cancel();
    } else if (!stream_journal) {
      CloseRecordChannel();
    }
    db_slice_->UnregisterOnChange(snapshot_version_);
  });
}

void SliceSnapshot::Stop() {
  // Wait for serialization to finish in any case.
  Join();

  if (journal_cb_id_) {
    db_slice_->shard_owner()->journal()->UnregisterOnChange(journal_cb_id_);
  }

  FlushDefaultBuffer(true);
  CloseRecordChannel();
}

void SliceSnapshot::Cancel() {
  VLOG(1) << "SliceSnapshot::Cancel";

  CloseRecordChannel();
  if (journal_cb_id_) {
    db_slice_->shard_owner()->journal()->UnregisterOnChange(journal_cb_id_);
    journal_cb_id_ = 0;
  }
}

void SliceSnapshot::Join() {
  // Fiber could have already been joined by Stop.
  if (snapshot_fb_.joinable())
    snapshot_fb_.join();
}

// The algorithm is to go over all the buckets and serialize those with
// version < snapshot_version_. In order to serialize each physical bucket exactly once we update
// bucket version to snapshot_version_ once it has been serialized.
// We handle serialization at physical bucket granularity.
// To further complicate things, Table::Traverse covers a logical bucket that may comprise of
// several physical buckets in dash table. For example, items belonging to logical bucket 0
// can reside in buckets 0,1 and stash buckets 56-59.
// PrimeTable::Traverse guarantees an atomic traversal of a single logical bucket,
// it also guarantees 100% coverage of all items that exists when the traversal started
// and survived until it finished.

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::IterateBucketsFb(const Cancellation* cll) {
  {
    auto fiber_name = absl::StrCat("SliceSnapshot-", ProactorBase::GetIndex());
    FiberProps::SetName(std::move(fiber_name));
  }

  PrimeTable::Cursor cursor;
  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    if (cll->IsCancelled())
      return;

    if (!db_array_[db_indx])
      continue;

    uint64_t last_yield = 0;
    PrimeTable* pt = &db_array_[db_indx]->prime;
    {
      lock_guard lk(mu_);
      current_db_ = db_indx;
    }

    VLOG(1) << "Start traversing " << pt->size() << " items for index " << db_indx;
    do {
      if (cll->IsCancelled())
        return;

      PrimeTable::Cursor next =
          pt->Traverse(cursor, absl::bind_front(&SliceSnapshot::BucketSaveCb, this));
      cursor = next;
      FlushDefaultBuffer(false);

      if (stats_.loop_serialized >= last_yield + 100) {
        DVLOG(2) << "Before sleep " << this_fiber::properties<FiberProps>().name();
        fibers_ext::Yield();
        DVLOG(2) << "After sleep";

        last_yield = stats_.loop_serialized;
        // flush in case other fibers (writes commands that pushed previous values)
        // filled the buffer.
        FlushDefaultBuffer(false);
      }
    } while (cursor);

    DVLOG(2) << "after loop " << this_fiber::properties<FiberProps>().name();
    FlushDefaultBuffer(true);
  }  // for (dbindex)

  // Wait for SerializePhysicalBucket to finish.
  mu_.lock();
  mu_.unlock();

  // TODO: investigate why a single byte gets stuck and does not arrive to replica
  for (unsigned i = 10; i > 1; i--)
    CHECK(!default_serializer_->SendFullSyncCut());
  FlushDefaultBuffer(true);

  // serialized + side_saved must be equal to the total saved.
  VLOG(1) << "Exit SnapshotSerializer (loop_serialized/side_saved/cbcalls): "
          << stats_.loop_serialized << "/" << stats_.side_saved << "/" << stats_.savecb_calls;
}

bool SliceSnapshot::BucketSaveCb(PrimeIterator it) {
  ++stats_.savecb_calls;

  uint64_t v = it.GetVersion();
  if (v >= snapshot_version_) {
    // either has been already serialized or added after snapshotting started.
    DVLOG(3) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << ":" << it.slot_id()
             << " at " << v;
    ++stats_.skipped;
    return false;
  }
  db_slice_->CallChangeOnAllLessThanVersion(current_db_, it, snapshot_version_);

  stats_.loop_serialized += SerializeBucket(current_db_, it);
  return false;
}

unsigned SliceSnapshot::SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it) {
  DCHECK_LT(it.GetVersion(), snapshot_version_);

  // traverse physical bucket and write it into string file.
  it.SetVersion(snapshot_version_);
  unsigned result = 0;

  lock_guard lk(mu_);

  optional<RdbSerializer> tmp_serializer;
  RdbSerializer* serializer_ptr = default_serializer_.get();
  if (db_index != current_db_) {
    CompressionMode compression_mode = compression_mode_ == CompressionMode::NONE
                                           ? CompressionMode::NONE
                                           : CompressionMode::SINGLE_ENTRY;
    tmp_serializer.emplace(compression_mode);
    serializer_ptr = &*tmp_serializer;
  }

  while (!it.is_done()) {
    ++result;
    SerializeEntry(db_index, it->first, it->second, nullopt, serializer_ptr);
    ++it;
  }

  if (tmp_serializer) {
    PushBytesToChannel(db_index, &*tmp_serializer);
    VLOG(1) << "Pushed " << result << " entries via tmp_serializer";
  }

  return result;
}

// This function should not block and should not preempt because it's called
// from SerializePhysicalBucket which should execute atomically.
void SliceSnapshot::SerializeEntry(DbIndex db_indx, const PrimeKey& pk, const PrimeValue& pv,
                                   optional<uint64_t> expire, RdbSerializer* serializer) {
  time_t expire_time = expire.value_or(0);
  if (!expire && pv.HasExpire()) {
    auto eit = db_array_[db_indx]->expire.Find(pk);
    expire_time = db_slice_->ExpireTime(eit);
  }

  io::Result<uint8_t> res = serializer->SaveEntry(pk, pv, expire_time);
  CHECK(res);
  ++type_freq_map_[*res];
}

size_t SliceSnapshot::PushBytesToChannel(DbIndex db_index, RdbSerializer* serializer) {
  auto id = rec_id_++;
  DVLOG(2) << "Pushed " << id;

  io::StringFile sfile;
  serializer->FlushToSink(&sfile);

  size_t serialized = sfile.val.size();
  if (serialized == 0)
    return 0;
  stats_.channel_bytes += serialized;

  DbRecord db_rec{.db_index = db_index, .id = id, .value = std::move(sfile.val)};

  dest_->Push(std::move(db_rec));
  return serialized;
}

bool SliceSnapshot::FlushDefaultBuffer(bool force) {
  if (!force && default_serializer_->SerializedLen() < 4096)
    return false;

  size_t written = PushBytesToChannel(current_db_, default_serializer_.get());
  VLOG(2) << "FlushDefaultBuffer " << written << " bytes";
  return true;
}

void SliceSnapshot::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  PrimeTable* table = db_slice_->GetTables(db_index).first;

  if (const PrimeTable::bucket_iterator* bit = req.update()) {
    if (bit->GetVersion() < snapshot_version_) {
      stats_.side_saved += SerializeBucket(db_index, *bit);
    }
  } else {
    string_view key = get<string_view>(req.change);
    table->CVCUponInsert(snapshot_version_, key, [this, db_index](PrimeTable::bucket_iterator it) {
      DCHECK_LT(it.GetVersion(), snapshot_version_);
      stats_.side_saved += SerializeBucket(db_index, it);
    });
  }
}

// For any key any journal entry must arrive at the replica strictly after its first original rdb
// value. This is guaranteed by the fact that OnJournalEntry runs always after OnDbChange, and
// no database switch can be performed between those two calls, because they are part of one
// transaction.
// OnJournalEntry registers for changes in journal, the journal change function signature is
// (const journal::Entry& entry, bool await) In snapshot flow we dont use the await argument.
void SliceSnapshot::OnJournalEntry(const journal::Entry& entry, bool unused_await_arg) {
  // We ignore non payload entries like EXEC because we have no transactional ordering during
  // LOAD phase on replica.
  if (!entry.HasPayload()) {
    return;
  }

  optional<RdbSerializer> tmp_serializer;
  RdbSerializer* serializer_ptr = default_serializer_.get();
  if (entry.dbid != current_db_) {
    CompressionMode compression_mode = compression_mode_ == CompressionMode::NONE
                                           ? CompressionMode::NONE
                                           : CompressionMode::SINGLE_ENTRY;
    tmp_serializer.emplace(compression_mode);
    serializer_ptr = &*tmp_serializer;
  }

  serializer_ptr->WriteJournalEntries(absl::Span{&entry, 1});

  if (tmp_serializer) {
    PushBytesToChannel(entry.dbid, &*tmp_serializer);
  } else {
    // This is the only place that flushes in streaming mode
    // once the iterate buckets fiber finished.
    FlushDefaultBuffer(false);
  }
}

void SliceSnapshot::CloseRecordChannel() {
  // stupid barrier to make sure that SerializePhysicalBucket finished.
  // Can not think of anything more elegant.
  mu_.lock();
  mu_.unlock();

  // Make sure we close the channel only once with a CAS check.
  bool expected = false;
  if (closed_chan_.compare_exchange_strong(expected, true)) {
    dest_->StartClosing();
  }
}

}  // namespace dfly
