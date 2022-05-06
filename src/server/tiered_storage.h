// Copyright 2022, Roman Gershman.  All rights reserved.
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
  explicit TieredStorage(DbSlice* db_slice);
  ~TieredStorage();

  std::error_code Open(const std::string& path);

  std::error_code Read(size_t offset, size_t len, char* dest);

  std::error_code UnloadItem(DbIndex db_index, PrimeIterator it);

  void Shutdown();

  const TieredStats& stats() const {
    return stats_;
  }

 private:
  struct ActiveIoRequest;

  bool ShouldFlush();

  void FlushPending();
  void InitiateGrow(size_t size);
  void SendIoRequest(ActiveIoRequest* req);
  void FinishIoRequest(int io_res, ActiveIoRequest* req);

  DbSlice& db_slice_;
  IoMgr io_mgr_;
  ExternalAllocator alloc_;

  size_t pending_unload_bytes_ = 0;
  size_t submitted_io_writes_ = 0;
  size_t submitted_io_write_size_ = 0;
  uint32_t num_active_requests_ = 0;
  util::fibers_ext::EventCount active_req_sem_;

  struct Hasher {
    size_t operator()(const PrimeKey& o) const {
      return o.HashCode();
    }
  };

  struct PerDb {
    absl::flat_hash_map<PrimeKey, ActiveIoRequest*, Hasher> active_requests;
  };

  std::vector<PerDb*> db_arr_;

  struct PendingReq {
    uint64_t cursor;
    DbIndex db_indx = kInvalidDbId;
  };

  base::RingBuffer<PendingReq> pending_req_;

  // map of cursor -> pending size
  // absl::flat_hash_map<uint64_t, size_t> pending_upload;

  TieredStats stats_;
};

}  // namespace dfly
