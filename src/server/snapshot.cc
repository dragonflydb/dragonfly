// Copyright 2022, Roman Gershman.  All rights reserved.
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
#include "server/rdb_save.h"
#include "util/fiber_sched_algo.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace chrono_literals;
namespace this_fiber = ::boost::this_fiber;
using boost::fibers::fiber;

SliceSnapshot::SliceSnapshot(PrimeTable* prime, ExpireTable* et, StringChannel* dest)
    : prime_table_(prime), expire_tbl_(et), dest_(dest) {
}

SliceSnapshot::~SliceSnapshot() {
}

void SliceSnapshot::Start(DbSlice* slice) {
  DCHECK(!fb_.joinable());
  db_slice_ = slice;

  auto on_change = [this, slice](DbIndex db_index, const DbSlice::ChangeReq& req) {
    PrimeTable* table = slice->GetTables(db_index).first;

    if (const auto* it = get_if<PrimeTable::bucket_iterator>(&req)) {
      if (it->GetVersion() < snapshot_version_) {
        side_saved_ += SerializePhysicalBucket(table, *it);
      }
    } else {
      string_view key = get<string_view>(req);
      table->CVCUponInsert(key, [this, table](PrimeTable::bucket_iterator it) {
        if (it.GetVersion() < snapshot_version_) {
          side_saved_ += SerializePhysicalBucket(table, it);
        }
      });
    }
  };

  snapshot_version_ = slice->RegisterOnChange(move(on_change));
  VLOG(1) << "DbSaver::Start - saving entries with version less than " << snapshot_version_;
  sfile_.reset(new io::StringFile);

  rdb_serializer_.reset(new RdbSerializer(sfile_.get()));

  fb_ = fiber([slice, this] {
    FiberFunc();
    slice->UnregisterOnChange(snapshot_version_);
  });
}

void SliceSnapshot::Join() {
  fb_.join();
}

void SliceSnapshot::SerializeSingleEntry(const PrimeKey& pk, const PrimeValue& pv) {
  time_t expire_time = 0;

  if (pv.HasExpire()) {
    auto eit = expire_tbl_->Find(pk);
    expire_time = db_slice_->ExpireTime(eit);
  }

  error_code ec = rdb_serializer_->SaveEntry(pk, pv, expire_time);
  CHECK(!ec);  // we write to StringFile.
  ++serialized_;
}

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::FiberFunc() {
  this_fiber::properties<FiberProps>().set_name(
      absl::StrCat("SliceSnapshot", ProactorBase::GetIndex()));
  PrimeTable::cursor cursor;

  VLOG(1) << "Start traversing " << prime_table_->size() << " items";

  uint64_t last_yield = 0;
  do {
    // Traverse a single logical bucket but do not update its versions.
    // we can not update a version because entries in the same bucket share part of the version.
    // Therefore we save first, and then update version in one atomic swipe.
    PrimeTable::cursor next =
        prime_table_->Traverse(cursor, [this](auto it) { this->SaveCb(move(it)); });

    cursor = next;

    // Flush if needed.
    FlushSfile(false);
    if (serialized_ >= last_yield + 100) {
      DVLOG(2) << "Before sleep " << this_fiber::properties<FiberProps>().name();
      this_fiber::yield();
      last_yield = serialized_;
      DVLOG(2) << "After sleep";
      // flush in case other fibers (writes commands that pushed previous values) filled the file.
      FlushSfile(false);
    }
  } while (cursor);

  DVLOG(2) << "after loop " << this_fiber::properties<FiberProps>().name();
  FlushSfile(true);
  dest_->StartClosing();

  VLOG(1) << "Exit SnapshotSerializer (serialized/cbcalls): " << serialized_ << "/"
          << savecb_calls_;
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

  string tmp = std::move(sfile_->val);  // important to move before pushing!
  dest_->Push(std::move(tmp));
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
    DVLOG(2) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << ":" << it.slot_id()
             << " at " << v;
    ++skipped_;
    return false;
  }

  SerializePhysicalBucket(prime_table_, it);

  return false;
}

unsigned SliceSnapshot::SerializePhysicalBucket(PrimeTable* table, PrimeTable::bucket_iterator it) {
  DCHECK_LT(it.GetVersion(), snapshot_version_);

  // traverse physical bucket and write it into string file.
  it.SetVersion(snapshot_version_);
  unsigned result = 0;
  string tmp;
  while (!it.is_done()) {
    ++result;
    SerializeSingleEntry(it->first, it->second);
    ++it;
  }

  return result;
}

}  // namespace dfly
