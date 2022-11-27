// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

extern "C" {
#include "redis/object.h"
}

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/rdb_save.h"
#include "util/fiber_sched_algo.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace chrono_literals;
namespace this_fiber = ::boost::this_fiber;
using boost::fibers::fiber;

SliceSnapshot::SliceSnapshot(DbSlice* slice, RecordChannel* dest) : db_slice_(slice), dest_(dest) {
  db_array_ = slice->databases();
}

SliceSnapshot::~SliceSnapshot() {
}

void SliceSnapshot::Start(bool include_journal_changes) {
  DCHECK(!fb_.joinable());

  auto on_change = [this](DbIndex db_index, const DbSlice::ChangeReq& req) {
    OnDbChange(db_index, req);
  };

  snapshot_version_ = db_slice_->RegisterOnChange(move(on_change));
  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  if (include_journal_changes) {
    auto* journal = db_slice_->shard_owner()->journal();
    DCHECK(journal);
    journal_cb_id_ = journal->RegisterOnChange(
        [this](const journal::Entry& e) { OnJournalEntry(e); });
  }

  sfile_.reset(new io::StringFile);

  rdb_serializer_.reset(new RdbSerializer(sfile_.get()));

  fb_ = fiber([this] {
    FiberFunc();
    db_slice_->UnregisterOnChange(snapshot_version_);
    if (journal_cb_id_)
      db_slice_->shard_owner()->journal()->Unregister(journal_cb_id_);
  });
}

void SliceSnapshot::Join() {
  fb_.join();
}

// This function should not block and should not preempt because it's called
// from SerializePhysicalBucket which should execute atomically.
void SliceSnapshot::SerializeSingleEntry(DbIndex db_indx, const PrimeKey& pk, const PrimeValue& pv,
                                         RdbSerializer* serializer) {
  time_t expire_time = 0;

  if (pv.HasExpire()) {
    auto eit = db_array_[db_indx]->expire.Find(pk);
    expire_time = db_slice_->ExpireTime(eit);
  }

  io::Result<uint8_t> res = serializer->SaveEntry(pk, pv, expire_time);
  CHECK(res);  // we write to StringFile.
  ++type_freq_map_[*res];
}

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::FiberFunc() {
  this_fiber::properties<FiberProps>().set_name(
      absl::StrCat("SliceSnapshot", ProactorBase::GetIndex()));
  PrimeTable::Cursor cursor;

  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    if (!db_array_[db_indx])
      continue;

    PrimeTable* pt = &db_array_[db_indx]->prime;
    VLOG(1) << "Start traversing " << pt->size() << " items";

    uint64_t last_yield = 0;
    mu_.lock();
    savecb_current_db_ = db_indx;
    mu_.unlock();

    do {
      PrimeTable::Cursor next = pt->Traverse(cursor, [this](auto it) { this->SaveCb(move(it)); });

      cursor = next;

      // Flush if needed.
      FlushSfile(false);
      if (serialized_ >= last_yield + 100) {
        DVLOG(2) << "Before sleep " << this_fiber::properties<FiberProps>().name();
        this_fiber::yield();
        last_yield = serialized_;
        DVLOG(2) << "After sleep";

        // flush in case other fibers (writes commands that pushed previous values)
        // filled the buffer.
        FlushSfile(false);
      }
    } while (cursor);

    DVLOG(2) << "after loop " << this_fiber::properties<FiberProps>().name();
    FlushSfile(true);
  }  // for (dbindex)

  // stupid barrier to make sure that SerializePhysicalBucket finished.
  // Can not think of anything more elegant.
  mu_.lock();
  mu_.unlock();
  dest_->StartClosing();

  VLOG(1) << "Exit SnapshotSerializer (serialized/side_saved/cbcalls): " << serialized_ << "/"
          << side_saved_ << "/" << savecb_calls_;
}

bool SliceSnapshot::FlushSfile(bool force) {
  if (force) {
    auto ec = rdb_serializer_->FlushMem();
    CHECK(!ec);
    if (sfile_->val.empty())
      return false;
  } else {
    if (sfile_->val.size() < 4096) {
      return false;
    }

    // Make sure we flush everything from membuffer in order to preserve the atomicity of keyvalue
    // serializations.
    auto ec = rdb_serializer_->FlushMem();
    CHECK(!ec);  // stringfile always succeeds.
  }
  VLOG(2) << "FlushSfile " << sfile_->val.size() << " bytes";

  DbRecord rec = GetDbRecord(savecb_current_db_, std::move(sfile_->val), num_records_in_blob_);
  num_records_in_blob_ = 0;  // We can not move this line after the push, because Push is blocking.
  dest_->Push(std::move(rec));

  return true;
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
bool SliceSnapshot::SaveCb(PrimeIterator it) {
  // if we touched that physical bucket - skip it.
  // We must to make sure we TraverseBucket exactly once for each physical bucket.
  // This test is the first one because it's likely to be the fastest one:
  // physical_mask_ is likely to be loaded in L1 and bucket_id() does not require accesing the
  // prime_table.
  /*if (physical_mask_.test(it.bucket_id())) {
    return false;
  }*/
  ++savecb_calls_;

  uint64_t v = it.GetVersion();
  if (v >= snapshot_version_) {
    // either has been already serialized or added after snapshotting started.
    DVLOG(3) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << ":" << it.slot_id()
             << " at " << v;
    ++skipped_;
    return false;
  }

  ++serialized_ += SerializePhysicalBucket(savecb_current_db_, it);

  return false;
}

void SliceSnapshot::OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req) {
  PrimeTable* table = db_slice_->GetTables(db_index).first;

  if (const PrimeTable::bucket_iterator* bit = req.update()) {
    if (bit->GetVersion() < snapshot_version_) {
      side_saved_ += SerializePhysicalBucket(db_index, *bit);
    }
  } else {
    string_view key = get<string_view>(req.change);
    table->CVCUponInsert(snapshot_version_, key, [this, db_index](PrimeTable::bucket_iterator it) {
      DCHECK_LT(it.GetVersion(), snapshot_version_);
      side_saved_ += SerializePhysicalBucket(db_index, it);
    });
  }
}

void SliceSnapshot::OnJournalEntry(const journal::Entry& entry) {
  CHECK(journal::Op::VAL == entry.opcode);

  PrimeKey pkey{entry.key};

  if (entry.db_ind == savecb_current_db_) {
    ++num_records_in_blob_;
    io::Result<uint8_t> res =
        rdb_serializer_->SaveEntry(pkey, *entry.pval_ptr, entry.expire_ms);
    CHECK(res);  // we write to StringFile.
  } else {
    io::StringFile sfile;
    RdbSerializer tmp_serializer(&sfile);

    io::Result<uint8_t> res =
        tmp_serializer.SaveEntry(pkey, *entry.pval_ptr, entry.expire_ms);
    CHECK(res);  // we write to StringFile.

    error_code ec = tmp_serializer.FlushMem();
    CHECK(!ec && !sfile.val.empty());

    DbRecord rec = GetDbRecord(entry.db_ind, std::move(sfile.val), 1);
    dest_->Push(std::move(rec));
  }
}

unsigned SliceSnapshot::SerializePhysicalBucket(DbIndex db_index, PrimeTable::bucket_iterator it) {
  DCHECK_LT(it.GetVersion(), snapshot_version_);

  // traverse physical bucket and write it into string file.
  it.SetVersion(snapshot_version_);
  unsigned result = 0;

  lock_guard lk(mu_);

  if (db_index == savecb_current_db_) {
    while (!it.is_done()) {
      ++result;
      SerializeSingleEntry(db_index, it->first, it->second, rdb_serializer_.get());
      ++it;
    }
    num_records_in_blob_ += result;
  } else {
    io::StringFile sfile;
    RdbSerializer tmp_serializer(&sfile);

    while (!it.is_done()) {
      ++result;
      SerializeSingleEntry(db_index, it->first, it->second, &tmp_serializer);
      ++it;
    }
    error_code ec = tmp_serializer.FlushMem();
    CHECK(!ec && !sfile.val.empty());

    dest_->Push(GetDbRecord(db_index, std::move(sfile.val), result));
  }
  return result;
}

auto SliceSnapshot::GetDbRecord(DbIndex db_index, std::string value, unsigned num_records)
    -> DbRecord {
  channel_bytes_ += value.size();
  auto id = rec_id_++;
  DVLOG(2) << "Pushed " << id;

  return DbRecord{
      .db_index = db_index, .id = id, .num_records = num_records, .value = std::move(value)};
}

}  // namespace dfly
