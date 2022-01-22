// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_snapshot.h"

extern "C" {
#include "redis/object.h"
}

#include <bitset>

#include "base/logging.h"
#include "server/rdb_save.h"
#include "util/fiber_sched_algo.h"

namespace dfly {
using namespace boost;
using namespace std;
using namespace util;
using namespace chrono_literals;

RdbSnapshot::RdbSnapshot(PrimeTable* prime, ExpireTable* et, StringChannel* dest)
    : prime_table_(prime), dest_(dest) {
}

RdbSnapshot::~RdbSnapshot() {
}

void RdbSnapshot::Start(uint64_t version) {
  DCHECK(!fb_.joinable());

  VLOG(1) << "DbSaver::Start";
  sfile_.reset(new io::StringFile);

  rdb_serializer_.reset(new RdbSerializer(sfile_.get()));
  snapshot_version_ = version;
  fb_ = fibers::fiber([this] { FiberFunc(); });
}

void RdbSnapshot::Join() {
  fb_.join();
}

static_assert(sizeof(PrimeTable::const_iterator) == 16);

void RdbSnapshot::PhysicalCb(MainIterator it) {
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
  ++processed_;
}

// Serializes all the entries with version less than snapshot_version_.
void RdbSnapshot::FiberFunc() {
  this_fiber::properties<FiberProps>().set_name("RdbSnapshot");
  VLOG(1) << "Saving entries with version less than " << snapshot_version_;

  uint64_t cursor = 0;
  uint64_t skipped = 0;
  bitset<128> physical;
  vector<MainIterator> physical_list;

  static_assert(physical.size() > PrimeTable::kPhysicalBucketNum);

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
  auto save_cb = [&](MainIterator it) {
    uint64_t v = it.GetVersion();

    if (v >= snapshot_version_) {
      // either has been already serialized or added after snapshotting started.
      DVLOG(2) << "Skipped " << it.segment_id() << ":" << it.bucket_id() << ":" << it.slot_id()
               << " at " << v;
      ++skipped;
      return false;
    }

    // if we touched that physical bucket - skip it.
    // If DashTable interface would introduce TraversePhysicalBuckets - where it
    // goes over each bucket once - we would not need to check for uniqueness here .
    // But right now we must to make sure we TraverseBucket exactly once for each physical
    // bucket.
    if (physical.test(it.bucket_id())) {
      return false;
    }

    physical.set(it.bucket_id());
    physical_list.push_back(it);

    // traverse physical bucket and write into string file.
    // TODO: I think we can avoid using physical_list by calling here
    // prime_table_->TraverseBucket(it, version_cb);
    prime_table_->TraverseBucket(it, [this](auto&& it) { this->PhysicalCb(it); });

    return false;
  };

  auto version_cb = [&](MainIterator it) {
    DCHECK_LE(it.GetVersion(), snapshot_version_);
    DVLOG(2) << "Bumping up version " << it.bucket_id() << ":" << it.slot_id();

    it.SetVersion(snapshot_version_);
  };

  uint64_t last_yield = 0;
  do {
    DVLOG(2) << "traverse cusrsor " << cursor;

    // Traverse a single logical bucket but do not update its versions.
    // we can not update a version because entries in the same bucket share part of the version.
    // Therefore we save first, and then update version in one atomic swipe.
    uint64_t next = prime_table_->Traverse(cursor, save_cb);

    // Traverse physical buckets that were touched and update their version.
    for (auto it : physical_list) {
      prime_table_->TraverseBucket(it, version_cb);
    }
    cursor = next;
    physical.reset();
    physical_list.clear();

    // Flush if needed.
    FlushSfile();
    if (processed_ >= last_yield + 200) {
      this_fiber::yield();
      last_yield = processed_;

      // flush in case other fibers (writes commands that pushed previous values) filled the file.
      FlushSfile();
    }
  } while (cursor > 0);
  auto ec = rdb_serializer_->FlushMem();
  CHECK(!ec);

  FlushSfile();
  dest_->StartClosing();

  VLOG(1) << "Exit RdbProducer fiber with " << processed_ << " processed";
}

void RdbSnapshot::FlushSfile() {
  // Flush the string file if needed.
  if (!sfile_->val.empty()) {
    // Make sure we flush everything from membuffer in order to preserve the atomicity of keyvalue
    // serializations.
    auto ec = rdb_serializer_->FlushMem();
    CHECK(!ec);                           // stringfile always succeeds.
    string tmp = std::move(sfile_->val);  // important to move before pushing!
    dest_->Push(std::move(tmp));
  }
}

}  // namespace dfly
