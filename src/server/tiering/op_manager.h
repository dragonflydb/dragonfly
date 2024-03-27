// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <memory>

#include "server/table.h"
#include "server/tiering/common.h"
#include "server/tiering/disk_storage.h"

namespace dfly::tiering {

// Manages READ/MODIFY/DELETE/STASH operations on top of a DiskStorage.
class OpManager {
 public:
  OpManager(OpManager&& other) = delete;
  ~OpManager();

  // Schedule read for offloaded object that will resolve the future
  Future<std::string> Read(const CompactObj& co);

  // Modify currently stored value for offloaded object with callback
  void Modify(const CompactObj& co, std::function<std::string(std::string)>);

  // Delete currently offloaded object, happens only once all pending reads finished
  void Delete(std::string_view key, CompactObj& co);

  // Stash primve value to be offloaded
  void Stash(std::string_view key, CompactObj& co);

  // Open file
  std::error_code Open(std::string_view file);

 protected:
  virtual PrimeIterator FindByKey(std::string_view key) = 0;

 private:
  using PostReadAction = std::variant<Future<std::string>, std::function<std::string(std::string)>>;

  struct ReadInfo {
    DiskSegment segment;

    std::vector<PostReadAction> actions;
    bool delete_requested = false;
  };

  // Prepare read operation for segment or return pending if it exists
  ReadInfo* PrepareRead(DiskSegment segment);

  // Called once read finished
  void ProcessRead(size_t offset, std::string_view value);

  // Called once Stash finished
  void ProcessStashed(std::string_view key, unsigned version, DiskSegment segment);

 private:
  DiskStorage storage_;

  absl::node_hash_map<size_t /* offset */, ReadInfo> pending_reads_;
  absl::flat_hash_map<std::string, unsigned /* version */> pending_stashes_;
};

};  // namespace dfly::tiering
