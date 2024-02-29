// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include "core/compact_object.h"
#include "core/external_alloc.h"
#include "core/fibers.h"
#include "server/common.h"
#include "server/io_mgr.h"
#include "server/table.h"
#include "server/db_slice.h"
#include "server/tiering/common.h"
#include "server/tiering/disk_storage.h"
#include "server/tiering/op_manager.h"

#include "base/logging.h"

namespace dfly {

class DbSlice;

class TieredStorage : protected tiering::KeyStorage {
  friend class tiering::OpManager;

  TieredStorage(TieredStorage&& other) = delete;

 public:
  TieredStorage(DbSlice* slice) : db_slice_{slice}, storage_{}, manager_{&storage_, this, 5} {
  }

  void ReportStored(std::string_view key, tiering::BlobLocator locator) {
    auto& co = db_slice_->FindMutable({}, key).it->second;
    co.SetIoPending(false);
    co.SetExternal(locator.offset, locator.len);
    VLOG(0) << "Externalized " << key << " to " << locator.offset << " " << locator.len;
  }

  bool ReportFetched(std::string_view key, std::string_view value) {
    return false;
  }

  tiering::Future<std::string> Fetch(std::string_view key, const CompactObj& co) {
    auto [offset, len] = co.GetExternalSlice();
    VLOG(0) << "Scheduling read " << key << " from " << offset << " " << len;
    return manager_.Read(key, {offset, len});
  }

  void Store(std::string_view key, std::string_view value) {
    manager_.Store(key, std::string{value});
  }

  void Delete(std::string_view key, CompactObj& co) {
    auto [offset, len] = co.GetExternalSlice();
    manager_.Delete(key, {offset, len});
    co.Reset();
  }

  void Dismiss(std::string_view key) {
    manager_.DismissStore(key);
  }

  std::error_code Open(std::string_view file) {
    storage_.Open(file);
    manager_.Start();
    return {};
  }

  void Shutdown() {
    manager_.Stop();
    storage_.Shutdown();
  }

 private:
  DbSlice* db_slice_;
  tiering::DiskStorage storage_;
  tiering::OpManager manager_;
};

}  // namespace dfly