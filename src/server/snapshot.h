// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <bitset>

#include "base/pod_array.h"
#include "io/file.h"
#include "server/db_slice.h"
#include "server/rdb_save.h"
#include "server/table.h"
#include "util/fibers/simple_channel.h"

namespace dfly {

namespace journal {
struct Entry;
}  // namespace journal

class RdbSerializer;
class ZstdCompressSerializer;

//┌────────────────┐   ┌─────────────┐
//│IterateBucketsFb│   │  OnDbChange │
//└──────┬─────────┘   └─┬───────────┘
//       │               │            OnDbChange forces whole bucket to be
//       ▼               ▼            serialized if iterate didn't reach it yet
//┌──────────────────────────┐
//│     SerializeBucket      │        Both might fall back to a temporary serializer
//└────────────┬─────────────┘        if default is used on another db index
//             │
//             |                      Channel is left open in journal streaming mode
//             ▼
//┌──────────────────────────┐          ┌─────────────────────────┐
//│     SerializeEntry       │ ◄────────┤     OnJournalEntry      │
//└─────────────┬────────────┘          └─────────────────────────┘
//              │
//        PushFileToChannel         Default buffer gets flushed on iteration,
//              │                   temporary on destruction
//              ▼
//┌──────────────────────────────┐
//│     dest->Push(buffer)       │
//└──────────────────────────────┘

// SliceSnapshot is used for iterating over a shard at a specified point-in-time
// and submitting all values to an output channel.
// In journal streaming mode, the snapshot continues submitting changes
// over the channel until explicitly stopped.
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

  SliceSnapshot(DbSlice* slice, RecordChannel* dest, CompressionMode compression_mode);
  ~SliceSnapshot();

  // Initialize snapshot, start bucket iteration fiber, register listeners.
  // In journal streaming mode it needs to be stopped by either Stop or Cancel.
  void Start(bool stream_journal, const Cancellation* cll);

  // Stop snapshot. Only needs to be called for journal streaming mode.
  void Stop();

  // Wait for iteration fiber to stop.
  void Join();

  // Force stop. Needs to be called together with cancelling the context.
  // Snapshot can't always react to cancellation in streaming mode becuase the
  // iteration fiber might have finished running by then.
  void Cancel();

 private:
  // Main fiber that iterates over all buckets in the db slice
  // and submits them to SerializeBucket.
  void IterateBucketsFb(const Cancellation* cll);

  // Called on traversing cursor by IterateBucketsFb.
  bool BucketSaveCb(PrimeIterator it);

  // Serialize single bucket.
  // Returns number of serialized entries, updates bucket version to snapshot version.
  unsigned SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator bucket_it);

  // Serialize entry into passed serializer.
  void SerializeEntry(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                      std::optional<uint64_t> expire, RdbSerializer* serializer);

  // Push StringFile buffer to channel.
  void PushFileToChannel(DbIndex db_index, unsigned num_records, bool should_compress,
                         io::StringFile* sfile);

  // Journal listener
  void OnDbChange(DbIndex db_index, const DbSlice::ChangeReq& req);

  // DbChange listener
  void OnJournalEntry(const journal::Entry& entry);

  // Close dest channel if not closed yet.
  void CloseRecordChannel();

  // Call PushFileToChannel on default buffer if needed.
  // Flush regradless of size if force is true.
  // Return if flushed.
  bool FlushDefaultBuffer(bool force);

  // Convert value into DbRecord.
  DbRecord GetDbRecord(DbIndex db_index, std::string value, unsigned num_records);

  // Allows getting a RdbSerializer pointer without taking care of it.
  // Initializes a temporary serializer if the default is used on another db index.
  // Else re-uses the default serializer.
  struct BorrowedSerializer {
    BorrowedSerializer(SliceSnapshot* snapshot, DbIndex, CompressionMode compression);
    ~BorrowedSerializer();  // Flush temporary buffer or update default's stats

    operator RdbSerializer*();

    unsigned num_records;

   private:
    DbIndex db_index;
    SliceSnapshot* snapshot;
    std::optional<RdbSerializer> serializer;
  };

  friend struct BorrowedSerializer;

 public:
  uint64_t snapshot_version() const {
    return snapshot_version_;
  }

  RdbSerializer* serializer() {
    return default_serializer_.get();
  }

  size_t channel_bytes() const {
    return stats_.channel_bytes;
  }

  const RdbTypeFreqMap& freq_map() const {
    return type_freq_map_;
  }

 private:
  DbSlice* db_slice_;
  DbTableArray db_array_;

  RecordChannel* dest_;
  std::atomic_bool closed_chan_{false};  // true if dest_->StartClosing was already called

  DbIndex current_db_;
  uint32_t default_buffer_records_ = 0;             // number of records in default buffer
  std::unique_ptr<io::StringFile> default_buffer_;  // filled by default_serializer_
  std::unique_ptr<RdbSerializer> default_serializer_;
  std::unique_ptr<ZstdCompressSerializer> zstd_serializer_;

  ::boost::fibers::mutex mu_;
  ::boost::fibers::fiber snapshot_fb_;  // IterateEntriesFb

  CompressionMode compression_mode_;
  RdbTypeFreqMap type_freq_map_;

  // version upper bound for entries that should be saved (not included).
  uint64_t snapshot_version_ = 0;
  uint32_t journal_cb_id_ = 0;
  uint64_t rec_id_ = 0;

  struct Stats {
    size_t channel_bytes = 0;
    size_t serialized = 0, skipped = 0, side_saved = 0;
    size_t savecb_calls = 0;
  } stats_;
};

}  // namespace dfly
