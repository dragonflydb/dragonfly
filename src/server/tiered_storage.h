// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

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
  std::error_code ScheduleOffload(DbIndex db_index, PrimeIterator it);

  void CancelIo(DbIndex db_index, PrimeIterator it);

  static bool EligibleForOffload(std::string_view val) {
    return val.size() >= kMinBlobLen;
  }

  void Free(size_t offset, size_t len);

  void Shutdown();

  TieredStats GetStats() const;

 private:
  class InflightWriteRequest;

  void WriteSingle(DbIndex db_index, PrimeIterator it, size_t blob_len);

  bool FlushPending(DbIndex db_index, unsigned bin_index);

  void InitiateGrow(size_t size);

  void FinishIoRequest(int io_res, InflightWriteRequest* req);
  void SetExternal(DbIndex db_index, size_t item_offset, PrimeValue* dest);

  DbSlice& db_slice_;
  IoMgr io_mgr_;
  ExternalAllocator alloc_;

  size_t submitted_io_writes_ = 0;
  size_t submitted_io_write_size_ = 0;
  uint32_t num_active_requests_ = 0;

  EventCount active_req_sem_;

  struct PerDb;
  std::vector<PerDb*> db_arr_;

  absl::flat_hash_map<uint32_t, uint8_t> page_refcnt_;

  TieredStats stats_;
};

}  // namespace dfly
