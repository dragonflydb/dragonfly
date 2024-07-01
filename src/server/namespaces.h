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

class Namespace {
 public:
  explicit Namespace();

  DbSlice& GetCurrentDbSlice();

  DbSlice& GetDbSlice(ShardId sid);
  BlockingController* GetOrAddBlockingController(EngineShard* shard);
  BlockingController* GetBlockingController(ShardId sid);

 private:
  std::vector<std::unique_ptr<DbSlice>> shard_db_slices_;
  std::vector<std::unique_ptr<BlockingController>> shard_blocking_controller_;

  friend class Namespaces;
};

class Namespaces {
 public:
  Namespaces() = default;
  ~Namespaces();

  void Init();
  bool IsInitialized() const;

  Namespace& GetDefaultNamespace() const;  // No locks
  Namespace& GetOrInsert(std::string_view ns);

 private:
  util::fb2::Mutex mu_{};
  absl::node_hash_map<std::string, Namespace> namespaces_ ABSL_GUARDED_BY(mu_);
  Namespace* default_namespace_ = nullptr;
};

extern Namespaces* namespaces;

}  // namespace dfly
