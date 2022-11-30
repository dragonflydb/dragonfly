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

  auto on_change = [this](DbIndex db_index, const DbSlice::ChangeReq& req) {
    OnDbChange(db_index, req);
  };

  snapshot_version_ = db_slice_->RegisterOnChange(move(on_change));
  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;

  if (stream_journal) {
    auto* journal = db_slice_->shard_owner()->journal();
    DCHECK(journal);
    journal_cb_id_ =
        journal->RegisterOnChange([this](const journal::Entry& e) { OnJournalEntry(e); });
  }

  sfile_.reset(new io::StringFile);

  bool do_compression = (compression_mode_ == CompressionMode::SINGLE_ENTRY);
  rdb_serializer_.reset(new RdbSerializer(do_compression));

  snapshot_fb_ = fiber([this, stream_journal, cll] {
    SerializeEntriesFb(cll);
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
    db_slice_->shard_owner()->journal()->Unregister(journal_cb_id_);
  }

  FlushSfile(true);
  CloseRecordChannel();
}

void SliceSnapshot::Cancel() {
  CloseRecordChannel();
  if (journal_cb_id_) {
    db_slice_->shard_owner()->journal()->Unregister(journal_cb_id_);
    journal_cb_id_ = 0;
  }
}

void SliceSnapshot::Join() {
  // Fiber could have already been joined by Stop.
  if (snapshot_fb_.joinable())
    snapshot_fb_.join();
}

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::SerializeEntriesFb(const Cancellation* cll) {
  this_fiber::properties<FiberProps>().set_name(
      absl::StrCat("SliceSnapshot", ProactorBase::GetIndex()));
  PrimeTable::Cursor cursor;

  for (DbIndex db_indx = 0; db_indx < db_array_.size(); ++db_indx) {
    if (cll->IsCancelled())
      return;

    if (!db_array_[db_indx])
      continue;

    PrimeTable* pt = &db_array_[db_indx]->prime;
    VLOG(1) << "Start traversing " << pt->size() << " items";

    uint64_t last_yield = 0;
    mu_.lock();
    savecb_current_db_ = db_indx;
    mu_.unlock();

    do {
      if (cll->IsCancelled())
        return;

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

  // Wait for SerializePhysicalBucket to finish.
  mu_.lock();
  mu_.unlock();

  for (unsigned i = 10; i > 1; i--)
    CHECK(!rdb_serializer_->SendFullSyncCut(sfile_.get()));
  FlushSfile(true);

  VLOG(1) << "Exit SnapshotSerializer (serialized/side_saved/cbcalls): " << serialized_ << "/"
          << side_saved_ << "/" << savecb_calls_;
}

void SliceSnapshot::CloseRecordChannel() {
  // stupid barrier to make sure that SerializePhysicalBucket finished.
  // Can not think of anything more elegant.
  mu_.lock();
  mu_.unlock();

  // Make sure we close the channel only once with a CAS check.
  bool actual = false;
  if (closed_chan_.compare_exchange_strong(actual, true)) {
    dest_->StartClosing();
  }
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

bool SliceSnapshot::FlushSfile(bool force) {
  if ((!force) && (rdb_serializer_->SerializedLen() < 4096)) {
    return false;
  }

  auto ec = rdb_serializer_->FlushToSink(sfile_.get());
  CHECK(!ec);

  if (sfile_->val.empty())
    return false;

  VLOG(2) << "FlushSfile " << sfile_->val.size() << " bytes";

  uint32_t record_num = num_records_in_blob_;
  num_records_in_blob_ = 0;  // We can not move this line after the push, because Push is blocking.
  bool multi_entries_compression = (compression_mode_ == CompressionMode::MULTY_ENTRY);
  PushFileToChannel(sfile_.get(), savecb_current_db_, record_num, multi_entries_compression);

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
    io::Result<uint8_t> res = rdb_serializer_->SaveEntry(pkey, *entry.pval_ptr, entry.expire_ms);
    CHECK(res);  // we write to StringFile.
  } else {
    bool serializer_compression = (compression_mode_ != CompressionMode::NONE);
    RdbSerializer tmp_serializer(serializer_compression);

    io::Result<uint8_t> res = tmp_serializer.SaveEntry(pkey, *entry.pval_ptr, entry.expire_ms);
    CHECK(res);  // we write to StringFile.

    io::StringFile sfile;
    error_code ec = tmp_serializer.FlushToSink(&sfile);
    CHECK(!ec && !sfile.val.empty());
    PushFileToChannel(&sfile, entry.db_ind, 1, false);
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
    bool serializer_compression = (compression_mode_ != CompressionMode::NONE);
    RdbSerializer tmp_serializer(serializer_compression);

    while (!it.is_done()) {
      ++result;
      SerializeSingleEntry(db_index, it->first, it->second, &tmp_serializer);
      ++it;
    }
    io::StringFile sfile;
    error_code ec = tmp_serializer.FlushToSink(&sfile);
    CHECK(!ec && !sfile.val.empty());
    PushFileToChannel(&sfile, db_index, result, false);
  }
  return result;
}

void SliceSnapshot::PushFileToChannel(io::StringFile* sfile, DbIndex db_index, unsigned num_records,
                                      bool should_compress) {
  string string_to_push = std::move(sfile->val);

  if (should_compress) {
    if (!zstd_serializer_) {
      zstd_serializer_.reset(new ZstdCompressSerializer());
    }
    auto comp_res = zstd_serializer_->Compress(string_to_push);
    if (comp_res.first) {
      string_to_push.swap(comp_res.second);
    }
  }
  dest_->Push(GetDbRecord(db_index, std::move(string_to_push), num_records));
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
