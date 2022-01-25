// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/snapshot.h"

extern "C" {
#include "redis/object.h"
}

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
    : prime_table_(prime), dest_(dest) {
}

SliceSnapshot::~SliceSnapshot() {
}

void SliceSnapshot::Start(DbSlice* slice) {
  DCHECK(!fb_.joinable());
  db_slice_ = slice;

  auto on_change = [this, slice](DbIndex db_index, const DbSlice::ChangeReq& req) {
    PrimeTable* table = slice->GetTables(db_index).first;

    if (const MainIterator* it = get_if<MainIterator>(&req)) {
      if (it->GetVersion() < snapshot_version_) {
        side_saved_ += SerializePhysicalBucket(table, *it);
      }
    } else {
      string_view key = get<string_view>(req);
      table->CVCUponInsert(key, [this, table](PrimeTable::const_iterator it) {
        if (it.MinVersion() < snapshot_version_) {
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

static_assert(sizeof(PrimeTable::const_iterator) == 16);

void SliceSnapshot::SerializeSingleEntry(MainIterator it) {
  error_code ec;

  string tmp;

  auto key = it->first.GetSlice(&tmp);

  // TODO: fetch expire.
  if (it->second.ObjType() == OBJ_STRING) {
    ec = rdb_serializer_->SaveKeyVal(key, it->second.ToString(), 0);
  } else {
    robj* obj = it->second.AsRObj();
    ec = rdb_serializer_->SaveKeyVal(key, obj, 0);
  }
  CHECK(!ec);  // we write to StringFile.
  ++serialized_;
}

// Serializes all the entries with version less than snapshot_version_.
void SliceSnapshot::FiberFunc() {
  this_fiber::properties<FiberProps>().set_name(
      absl::StrCat("SliceSnapshot", ProactorBase::GetIndex()));
  uint64_t cursor = 0;
  static_assert(PHYSICAL_LEN > PrimeTable::kPhysicalBucketNum);

  uint64_t last_yield = 0;
  do {
    // Traverse a single logical bucket but do not update its versions.
    // we can not update a version because entries in the same bucket share part of the version.
    // Therefore we save first, and then update version in one atomic swipe.
    uint64_t next = prime_table_->Traverse(cursor, [this](auto it) { this->SaveCb(move(it)); });

    cursor = next;
    physical_mask_.reset();

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
  } while (cursor > 0);

  DVLOG(1) << "after loop " << this_fiber::properties<FiberProps>().name();
  FlushSfile(true);
  dest_->StartClosing();

  VLOG(1) << "Exit RdbProducer fiber with " << serialized_ << " serialized";
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

// The algorithm is to go over all the buckets and serialize entries that
// have version < snapshot_version_. In order to serialize each entry exactly once we update its
// version to snapshot_version_ once it has been serialized.
// Due to how bucket versions work we can not update individual entries - they may affect their
// neighbours in the bucket. Instead we handle serialization at physical bucket granularity.
// To further complicate things, Table::Traverse covers a logical bucket that may comprise of
// several physical buckets. The reason for this complication is that we need to guarantee
// a stable traversal during prime table mutations. PrimeTable::Traverse guarantees an atomic
// traversal of a single logical bucket, it also guarantees 100% coverage of all items
// that existed when the traversal started and survived until it finished.
//
// It's important that cb will run atomically so we avoid anu I/O work inside it.
// Instead, we flush our string file to disk in the traverse loop below.
bool SliceSnapshot::SaveCb(MainIterator it) {
  // if we touched that physical bucket - skip it.
  // We must to make sure we TraverseBucket exactly once for each physical bucket.
  // This test is the first one because it's likely to be the fastest one:
  // physical_mask_ is likely to be loaded in L1 and bucket_id() does not require accesing the
  // prime_table.
  if (physical_mask_.test(it.bucket_id())) {
    return false;
  }

  uint64_t v = it.GetVersion();
  if (v >= snapshot_version_) {
    // either has been already serialized or added after snapshotting started.
    DVLOG(2) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << ":" << it.slot_id()
             << " at " << v;
    ++skipped_;
    return false;
  }

  physical_mask_.set(it.bucket_id());
  SerializePhysicalBucket(prime_table_, it);

  return false;
}

unsigned SliceSnapshot::SerializePhysicalBucket(PrimeTable* table, PrimeTable::const_iterator it) {
  // Both traversals below execute atomically.
  // traverse physical bucket and write into string file.
  unsigned result = 0;
  table->TraverseBucket(it, [&](auto entry_it) {
    ++result;
    SerializeSingleEntry(move(entry_it));
  });

  table->TraverseBucket(it, [this](MainIterator entry_it) {
    DCHECK_LE(entry_it.GetVersion(), snapshot_version_);
    DVLOG(3) << "Bumping up version " << entry_it.bucket_id() << ":" << entry_it.slot_id();

    entry_it.SetVersion(snapshot_version_);
  });

  return result;
}

}  // namespace dfly
