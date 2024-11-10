// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard_set.h"

#include <sys/statvfs.h>

#include <filesystem>

#include "base/flags.h"
#include "base/logging.h"
#include "server/namespaces.h"
#include "server/tiered_storage.h"
#include "strings/human_readable.h"

using namespace std;

ABSL_FLAG(bool, cache_mode, false,
          "If true, the backend behaves like a cache, "
          "by evicting entries when getting close to maxmemory limit");

ABSL_FLAG(dfly::MemoryBytesFlag, tiered_max_file_size, dfly::MemoryBytesFlag{},
          "Limit on maximum file size that is used by the database for tiered storage. "
          "0 - means the program will automatically determine its maximum file size. "
          "default: 0");

ABSL_DECLARE_FLAG(string, tiered_prefix);

namespace dfly {

using namespace tiering::literals;

using namespace util;
using absl::GetFlag;
using strings::HumanReadableNumBytes;

namespace {

uint64_t GetFsLimit() {
  std::filesystem::path file_path(GetFlag(FLAGS_tiered_prefix));
  std::string dir_name_str = file_path.parent_path().string();

  if (dir_name_str.empty())
    dir_name_str = ".";

  struct statvfs stat;
  if (statvfs(dir_name_str.c_str(), &stat) == 0) {
    uint64_t limit = stat.f_frsize * stat.f_blocks;
    return limit;
  }
  LOG(WARNING) << "Error getting filesystem information " << errno;
  return 0;
}

size_t GetTieredFileLimit(size_t threads) {
  string file_prefix = GetFlag(FLAGS_tiered_prefix);
  if (file_prefix.empty())
    return 0;

  size_t max_shard_file_size = 0;

  size_t max_file_size = absl::GetFlag(FLAGS_tiered_max_file_size).value;
  size_t max_file_size_limit = GetFsLimit();
  if (max_file_size == 0) {
    LOG(INFO) << "max_file_size has not been specified. Deciding myself....";
    max_file_size = (max_file_size_limit * 0.8);
  } else {
    if (max_file_size_limit < max_file_size) {
      LOG(WARNING) << "Got max file size " << HumanReadableNumBytes(max_file_size)
                   << ", however only " << HumanReadableNumBytes(max_file_size_limit)
                   << " disk space was found.";
    }
  }

  max_shard_file_size = max_file_size / threads;
  if (max_shard_file_size < 256_MB) {
    LOG(ERROR) << "Max tiering file size is too small. Setting: "
               << HumanReadableNumBytes(max_file_size) << " Required at least "
               << HumanReadableNumBytes(256_MB * threads) << ". Exiting..";
    exit(1);
  }
  LOG(INFO) << "Max file size is: " << HumanReadableNumBytes(max_file_size);

  return max_shard_file_size;
}

}  // namespace

/**


  _____                _               ____   _                      _  ____         _
 | ____| _ __    __ _ (_) _ __    ___ / ___| | |__    __ _  _ __  __| |/ ___|   ___ | |_
 |  _|  | '_ \  / _` || || '_ \  / _ \\___ \ | '_ \  / _` || '__|/ _` |\___ \  / _ \| __|
 | |___ | | | || (_| || || | | ||  __/ ___) || | | || (_| || |  | (_| | ___) ||  __/| |_
 |_____||_| |_| \__, ||_||_| |_| \___||____/ |_| |_| \__,_||_|   \__,_||____/  \___| \__|
                |___/

 */

EngineShardSet* shard_set = nullptr;

void EngineShardSet::Init(uint32_t sz, std::function<void()> shard_handler) {
  CHECK_EQ(0u, size());
  CHECK(namespaces == nullptr);

  shards_.reset(new EngineShard*[sz]);

  size_ = sz;
  size_t max_shard_file_size = GetTieredFileLimit(sz);
  pp_->AwaitFiberOnAll([this](uint32_t index, ProactorBase* pb) {
    if (index < size_) {
      InitThreadLocal(pb);
    }
  });

  // The order is important here. We must initialize namespaces after shards_.
  namespaces = new Namespaces();

  pp_->AwaitFiberOnAll([&](uint32_t index, ProactorBase* pb) {
    if (index < size_) {
      auto* shard = EngineShard::tlocal();
      shard->InitTieredStorage(pb, max_shard_file_size);

      // Must be last, as it accesses objects initialized above.
      // We can not move shard_handler because this code is called multiple times.
      shard->StartPeriodicHeartbeatFiber(pb);
      shard->StartPeriodicShardHandlerFiber(pb, shard_handler);
    }
  });
}

void EngineShardSet::PreShutdown() {
  RunBlockingInParallel([](EngineShard* shard) {
    shard->StopPeriodicFiber();

    // We must close tiered_storage before we destroy namespaces that own db slices.
    if (shard->tiered_storage()) {
      shard->tiered_storage()->Close();
    }
  });
}

void EngineShardSet::Shutdown() {
  // Calling Namespaces::Clear before destroying engine shards, because it accesses them
  // internally.
  namespaces->Clear();
  RunBlockingInParallel([](EngineShard*) { EngineShard::DestroyThreadLocal(); });

  delete namespaces;
  namespaces = nullptr;
}

void EngineShardSet::InitThreadLocal(ProactorBase* pb) {
  EngineShard::InitThreadLocal(pb);
  EngineShard* es = EngineShard::tlocal();
  shards_[es->shard_id()] = es;
}

void EngineShardSet::TEST_EnableCacheMode() {
  RunBlockingInParallel([](EngineShard* shard) {
    namespaces->GetDefaultNamespace().GetCurrentDbSlice().TEST_EnableCacheMode();
  });
}

}  // namespace dfly
