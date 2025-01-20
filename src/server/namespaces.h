// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/node_hash_map.h>

#include <memory>
#include <string>
#include <vector>

#include "server/blocking_controller.h"
#include "server/db_slice.h"
#include "server/tx_base.h"
#include "util/fibers/synchronization.h"

namespace dfly {

// A Namespace is a way to separate and isolate different databases in a single instance.
// It can be used to allow multiple tenants to use the same server without hacks of using a common
// prefix, or SELECT-ing a different database.
// Each Namespace contains per-shard DbSlice, as well as a BlockingController.
class Namespace {
 public:
  Namespace();

  DbSlice& GetCurrentDbSlice();

  DbSlice& GetDbSlice(ShardId sid);
  BlockingController* GetOrAddBlockingController(EngineShard* shard);
  BlockingController* GetBlockingController(ShardId sid);

 private:
  std::vector<std::unique_ptr<DbSlice>> shard_db_slices_;
  std::vector<std::unique_ptr<BlockingController>> shard_blocking_controller_;

  friend class Namespaces;
};

// Namespaces is a registry and container for Namespace instances.
// Each Namespace has a unique string name, which identifies it in the store.
// Any attempt to access a non-existing Namespace will first create it, add it to the internal map
// and will then return it.
// It is currently impossible to remove a Namespace after it has been created.
// The default Namespace can be accessed via either GetDefaultNamespace() (which guarantees not to
// yield), or via the GetOrInsert() with an empty string.
// The initialization order of this class with the engine shards is slightly subtle, as they have
// mutual dependencies.
class Namespaces {
 public:
  Namespaces();
  ~Namespaces();

  void Clear() ABSL_LOCKS_EXCLUDED(mu_);  // Thread unsafe, use in tear-down or tests

  Namespace& GetDefaultNamespace() const;  // No locks
  Namespace& GetOrInsert(std::string_view ns) ABSL_LOCKS_EXCLUDED(mu_);

 private:
  util::fb2::SharedMutex mu_{};
  absl::node_hash_map<std::string, Namespace> namespaces_ ABSL_GUARDED_BY(mu_);
  Namespace* default_namespace_ = nullptr;
};

}  // namespace dfly
