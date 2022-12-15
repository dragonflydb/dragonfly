// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include "base/ring_buffer.h"
#include "core/external_alloc.h"
#include "server/common.h"
#include "server/io_mgr.h"
#include "server/table.h"
#include "util/fibers/event_count.h"

namespace dfly {

class DbSlice;

class TieredStorage {
 public:
  enum : uint16_t { kMinBlobLen = 64 };

  explicit TieredStorage(DbSlice* db_slice);
  ~TieredStorage();

  std::error_code Open(const std::string& path);

  std::error_code Read(size_t offset, size_t len, char* dest);

  // Schedules unloading of the item, pointed by the iterator.
  std::error_code UnloadItem(DbIndex db_index, PrimeIterator it);

  static bool EligibleForOffload(std::string_view val) {
    return val.size() >= kMinBlobLen;
  }

  void Free(size_t offset, size_t len);

  void Shutdown();

  TieredStats GetStats() const;

 private:
  class ActiveIoRequest;

  struct Hasher {
    size_t operator()(const PrimeKey& o) const {
      return o.HashCode();
    }
  };

  struct PerDb {
    base::RingBuffer<uint64_t> bucket_cursors;  // buckets cursors pending for unloading.
    absl::flat_hash_map<PrimeKey, ActiveIoRequest*, Hasher> active_requests;

    PerDb(const PerDb&) = delete;
    PerDb& operator=(const PerDb&) = delete;

    PerDb() : bucket_cursors(256) {
    }

    bool ShouldFlush() const;
  };

  void WriteSingle(DbIndex db_index, PrimeIterator it, size_t blob_len);

  void FlushPending(DbIndex db_index);
  void InitiateGrow(size_t size);
  void SendIoRequest(ActiveIoRequest* req);
  void FinishIoRequest(int io_res, ActiveIoRequest* req);
  void SetExternal(DbIndex db_index, size_t item_offset, PrimeValue* dest);

  DbSlice& db_slice_;
  IoMgr io_mgr_;
  ExternalAllocator alloc_;

  size_t submitted_io_writes_ = 0;
  size_t submitted_io_write_size_ = 0;
  uint32_t num_active_requests_ = 0;
  util::fibers_ext::EventCount active_req_sem_;

  std::vector<PerDb*> db_arr_;

  /*struct PendingReq {
    uint64_t cursor;
    DbIndex db_indx = kInvalidDbId;
  };*/

  // map of cursor -> pending size
  // absl::flat_hash_map<uint64_t, size_t> pending_upload;

  // multi_cnt_ - counts how many unloaded items exists in the batch at specified page offset.
  // here multi_cnt_.first is (file_offset in 4k pages) and
  // multi_cnt_.second is MultiBatch object storing number of allocated records in the batch
  // and its capacity (/ 4k).
  struct MultiBatch {
    uint16_t used;      // number of used bytes
    uint16_t reserved;  // in 4k pages.

    MultiBatch(uint16_t mem_used) : used(mem_used) {
    }
  };
  absl::flat_hash_map<uint32_t, MultiBatch> multi_cnt_;

  TieredStats stats_;
};

}  // namespace dfly
