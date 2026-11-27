// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "server/common_types.h"
#include "server/execution_state.h"
#include "server/journal/aof_manifest.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace util::fb2 {
class ProactorBase;
}  // namespace util::fb2

namespace dfly {

class MultiShardExecution;
class Service;

// Loads a snapshot, blocking until it finishes.
using AofLoadFn = std::function<GenericError(const std::string& path)>;

// How startup restores the AOF, decided before the server listens.
struct AofStartPlan {
  enum class Kind : uint8_t { kBootstrap, kResume, kRebasing };
  Kind kind = Kind::kBootstrap;

  // kResume and kRebasing.
  AofManifest manifest;

  // kBootstrap: the --dbfilename dump to adopt, empty if none.
  std::string adopt_path;
};

// The AOF base the server started from.
struct AofBase {
  // Summary file name, empty without a base.
  std::string file = {};

  // Bytes on disk of the base: the summary plus every shard file.
  size_t size = 0;

  // No base covers the data yet; appending is paused.
  bool rebasing = false;
};

// Where a shard's log resumes after replay.
struct AofResume {
  uint64_t seq = 0;
  uint64_t next_lsn = 1;

  // Size of the kept segments.
  size_t log_size = 0;
};

// Bytes of the DFS file set whose summary is base_path.
size_t AofBaseSize(std::string_view dir, std::string_view base_path);

// Restores the AOF at startup: loads the base, replays the log and starts the streamers.
class AofRestore {
 public:
  // Gets the restored base, or nullopt if a shutdown stopped the restore.
  using DoneCb = std::function<void(std::optional<AofBase>)>;

  // Runs the restore in a fiber on pb. The caller holds LOADING until on_done.
  void Start(util::fb2::ProactorBase* pb, std::string dir, AofStartPlan plan, Service* service,
             AofLoadFn load, DoneCb on_done);

  bool Running() const;

  // Stops the replay before its next record and waits until the restore ended.
  void Cancel();

  void Join();

 private:
  std::optional<AofBase> Run();
  std::optional<std::string> Bootstrap();
  std::optional<AofBase> Resume();

  // A full sync replaces the data: marks the manifest rebasing and pauses appending.
  void Abandon();

  std::error_code Replay(std::vector<AofResume>* resume);
  std::error_code ReplayChain(ShardId sid, std::shared_ptr<MultiShardExecution> mse,
                              AofResume* resume, TxId* max_txid);

  std::string dir_;
  AofStartPlan plan_;
  Service* service_ = nullptr;
  AofLoadFn load_;
  DoneCb on_done_;

  std::atomic_bool cancel_{false};
  util::fb2::EmbeddedBlockingCounter restore_done_;
  util::fb2::Fiber restore_fb_;
};

}  // namespace dfly
