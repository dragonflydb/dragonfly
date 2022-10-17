// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <bitset>
#include <boost/fiber/barrier.hpp>

#include "io/file.h"
#include "server/db_slice.h"
#include "server/table.h"
#include "util/fibers/simple_channel.h"

namespace dfly {

// TODO: Change to virtual interface?
struct SnapshotSyncBlock {
  explicit SnapshotSyncBlock(unsigned threads, std::atomic_uint16_t* counter)
      : barrier_(threads), counter_(counter) {
  }

  void wait() {
    counter_->fetch_add(-1, std::memory_order_relaxed);
    barrier_.wait();
  }

 private:
  // Barrier to sync consumer fibers on CONTINIOUS_JOURNAL mode.
  // TODO: No wait until. No fail behaviour.
  ::boost::fibers::barrier barrier_;
  std::atomic_uint16_t* counter_;
};

namespace journal {
struct Entry;
}  // namespace journal

class RdbSerializer;

class SliceSnapshot {
 public:
  // Each dbrecord should belong to exactly one db.
  // RdbSaver adds "select" opcodes when necessary in order to maintain consistency.
  struct DbRecord {
    DbIndex db_index;
    uint64_t id;
    uint32_t num_records;
    std::string value;
  };

  using RecordChannel =
      ::util::fibers_ext::SimpleChannel<DbRecord, base::mpmc_bounded_queue<DbRecord>>;

  SliceSnapshot(DbSlice* slice, RecordChannel* dest);
  ~SliceSnapshot();

  void Start(SnapshotSyncBlock* block);
  void Join();

  uint64_t snapshot_version() const {
    return snapshot_version_;
  }

  RdbSerializer* serializer() {
    return rdb_serializer_.get();
  }

  size_t channel_bytes() const {
    return channel_bytes_;
  }

  const RdbTypeFreqMap& freq_map() const {
    return type_freq_map_;
  }

 private:
  void FiberFunc(SnapshotSyncBlock* block);
  bool FlushSfile(bool force);
  void SerializeSingleEntry(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                            RdbSerializer* serializer);

  bool SaveCb(PrimeIterator it);
  void OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req);
  void OnJournalEntry(const journal::Entry& entry);

  // Returns number of entries serialized.
  // Updates the version of the bucket to snapshot version.
  unsigned SerializePhysicalBucket(DbIndex db_index, PrimeTable::bucket_iterator it);
  DbRecord GetDbRecord(DbIndex db_index, std::string value, unsigned num_records);

  ::boost::fibers::fiber fb_;  // fiber dispatched by Start().

  DbTableArray db_array_;
  RdbTypeFreqMap type_freq_map_;

  std::unique_ptr<io::StringFile> sfile_;
  std::unique_ptr<RdbSerializer> rdb_serializer_;
  boost::fibers::mutex mu_;  // guards multiple members.

  // version upper bound for entries that should be saved (not included).
  uint64_t snapshot_version_ = 0;
  DbSlice* db_slice_;
  DbIndex savecb_current_db_;  // used by SaveCb
  RecordChannel* dest_;
  ::size_t channel_bytes_ = 0;
  size_t serialized_ = 0, skipped_ = 0, side_saved_ = 0, savecb_calls_ = 0;
  uint64_t rec_id_ = 0;
  uint32_t num_records_in_blob_ = 0;
  uint32_t journal_cb_id_ = 0;
};

}  // namespace dfly
