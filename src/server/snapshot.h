// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <bitset>

#include "io/file.h"
#include "server/table.h"
#include "server/db_slice.h"
#include "util/fibers/simple_channel.h"

namespace dfly {

class RdbSerializer;

class SliceSnapshot {
 public:
  using StringChannel =
      ::util::fibers_ext::SimpleChannel<std::string, base::mpmc_bounded_queue<std::string>>;

  SliceSnapshot(DbTableArray db_array, DbSlice* slice, StringChannel* dest);
  ~SliceSnapshot();

  void Start();
  void Join();

  uint64_t snapshot_version() const {
    return snapshot_version_;
  }

  RdbSerializer* serializer() { return rdb_serializer_.get(); }
 private:
  void FiberFunc();
  bool FlushSfile(bool force);
  void SerializeSingleEntry(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv);

  bool SaveCb(PrimeIterator it);
  void OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req);

  // Returns number of entries serialized.
  // Updates the version of the bucket to snapshot version.
  unsigned SerializePhysicalBucket(DbIndex db_index, PrimeTable::bucket_iterator it);

  ::boost::fibers::fiber fb_;

  DbTableArray db_array_;

  std::unique_ptr<io::StringFile> sfile_;
  std::unique_ptr<RdbSerializer> rdb_serializer_;

  // version upper bound for entries that should be saved (not included).
  uint64_t snapshot_version_ = 0;
  DbSlice* db_slice_;
  DbIndex savecb_current_db_;  // used by SaveCb
  StringChannel* dest_;

  size_t serialized_ = 0, skipped_ = 0, side_saved_ = 0, savecb_calls_ = 0;
};

}  // namespace dfly
