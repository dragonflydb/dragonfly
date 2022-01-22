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

void RdbSnapshot::Start() {
  DCHECK(!fb_.joinable());

  VLOG(1) << "DbSaver::Start";
  sfile_.reset(new io::StringFile);

  rdb_serializer_.reset(new RdbSerializer(sfile_.get()));
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

// Serializes all the entries with version less than top_version.
void RdbSnapshot::FiberFunc() {
  this_fiber::properties<FiberProps>().set_name("RdbSnapshot");

  uint64_t cursor = 0;

  // it's important that cb will run uninterrupted.
  // so no I/O work inside it.
  // We flush our string file to disk in the traverse loop below.
  auto save_cb = [&](const MainIterator& it) {
    this->PhysicalCb(it);

    return false;
  };

  uint64_t last_yield = 0;
  do {
    DVLOG(2) << "traverse cusrsor " << cursor;

    // Traverse a single logical bucket but do not update its versions.
    // we can not update a version because entries in the same bucket share part of the version.
    // Therefore we save first, and then update version in one atomic swipe.
    uint64_t next = prime_table_->Traverse(cursor, save_cb);

    cursor = next;

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
