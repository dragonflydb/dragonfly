// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include "core/external_alloc.h"
#include "server/common.h"
#include "server/io_mgr.h"
#include "server/table.h"

namespace dfly {

class DbSlice;

class TieredStorage {
 public:
  explicit TieredStorage(DbSlice* db_slice);
  ~TieredStorage();

  std::error_code Open(const std::string& path);

  void Shutdown();
  void UnloadItem(DbIndex db_index, PrimeIterator it);

 private:

  // return 0 if everything was sent.
  // if more storage is needed returns requested size in bytes.
  size_t SerializePendingItems();

  DbSlice& db_slice_;
  IoMgr io_mgr_;
  ExternalAllocator alloc_;

  size_t pending_unload_bytes_ = 0;
  size_t submitted_io_writes_ = 0;
  size_t submitted_io_write_size_ = 0;

  struct PerDb {
    // map of cursor -> pending size
    absl::flat_hash_map<uint64_t, size_t> pending_upload_;
  };
  std::vector<PerDb*> db_arr_;
};

}  // namespace dfly
