// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/sharding.h"

#include <xxhash.h>

#include "absl/strings/match.h"
#include "base/flags.h"
#include "base/logging.h"
#include "server/cluster_support.h"
#include "server/common.h"
#include "util/fibers/synchronization.h"

using namespace std;

ABSL_FLAG(string, shard_round_robin_prefix, "", "Deprecated -- will be removed");

ABSL_FLAG(bool, shard_sidekiq_queues, false,
          "If true, Sidekiq queues (<prefix>queue:<q>) and Sidekiq Pro super_fetch private queues "
          "(<prefix>queue:sq|<identity>|<q>) are sharded by the queue name <q> only, so a queue "
          "and its private queues share a shard. Ignored with "
          "--experimental_cluster_shard_by_slot.");

namespace dfly {
namespace {

thread_local bool shard_sidekiq_queues = false;

// Sidekiq Pro super_fetch hardcodes private queue names as "queue:sq|<identity>|<q>" (since 2017).
// Returns <q> for "<prefix>queue:<q>" and "<prefix>queue:sq|<identity>|<q>", empty otherwise.
string_view SidekiqQueueName(string_view key) {
  constexpr string_view kQueue = "queue:";

  size_t pos = key.rfind(kQueue);
  if (pos == string_view::npos)
    return {};

  string_view q = key.substr(pos + kQueue.size());
  if (absl::StartsWith(q, "sq|"))
    q = q.substr(q.rfind('|') + 1);
  return q;
}

// RoundRobinSharder implements a way to distribute keys that begin with some prefix.
// Round-robin is disabled by default. It is not a general use-case optimization, but instead only
// reasonable when there are a few highly contended keys, which we'd like to spread between the
// shards evenly.
// When enabled, the distribution is done via hash table: the hash of the key is used to look into
// a pre-allocated vector. This means that collisions are possible, but are very unlikely if only
// a few keys are used.
// Thread safe.
class RoundRobinSharder {
 public:
  static void Init(uint32_t shard_set_size) {
    round_robin_prefix_ = absl::GetFlag(FLAGS_shard_round_robin_prefix);
    shard_set_size_ = shard_set_size;

    if (IsEnabled()) {
      LOG(WARNING) << "shard_round_robin_prefix is deprecated and will be removed in new versions";
      // ~100k entries will consume 200kb per thread, and will allow 100 keys with < 2.5% collision
      // probability. Since this has a considerable footprint, we only allocate when enabled. We're
      // using a prime number close to 100k for better utilization.
      constexpr size_t kRoundRobinSize = 100'003;
      round_robin_shards_tl_cache_.resize(kRoundRobinSize);
      std::fill(round_robin_shards_tl_cache_.begin(), round_robin_shards_tl_cache_.end(),
                kInvalidSid);

      util::fb2::LockGuard guard(mutex_);
      if (round_robin_shards_.empty()) {
        round_robin_shards_ = round_robin_shards_tl_cache_;
      }
    }
  }

  static bool IsEnabled() {
    return !round_robin_prefix_.empty();
  }

  static optional<ShardId> TryGetShardId(string_view key, XXH64_hash_t key_hash) {
    DCHECK(!round_robin_shards_tl_cache_.empty());

    if (!absl::StartsWith(key, round_robin_prefix_)) {
      return nullopt;
    }

    size_t index = key_hash % round_robin_shards_tl_cache_.size();
    ShardId sid = round_robin_shards_tl_cache_[index];

    if (sid == kInvalidSid) {
      util::fb2::LockGuard guard(mutex_);
      sid = round_robin_shards_[index];
      if (sid == kInvalidSid) {
        sid = next_shard_;
        round_robin_shards_[index] = sid;
        next_shard_ = (next_shard_ + 1) % shard_set_size_;
      }
      round_robin_shards_tl_cache_[index] = sid;
    }

    return sid;
  }

 private:
  static thread_local string round_robin_prefix_;
  static thread_local vector<ShardId> round_robin_shards_tl_cache_;
  static thread_local uint32_t shard_set_size_;
  static vector<ShardId> round_robin_shards_ ABSL_GUARDED_BY(mutex_);
  static ShardId next_shard_ ABSL_GUARDED_BY(mutex_);
  static util::fb2::Mutex mutex_;
};

}  // namespace

thread_local string RoundRobinSharder::round_robin_prefix_;
thread_local uint32_t RoundRobinSharder::shard_set_size_;
thread_local vector<ShardId> RoundRobinSharder::round_robin_shards_tl_cache_;
vector<ShardId> RoundRobinSharder::round_robin_shards_;
ShardId RoundRobinSharder::next_shard_;
util::fb2::Mutex RoundRobinSharder::mutex_;

ShardId Shard(string_view v, ShardId shard_num) {
  // This cluster sharding is not necessary and may degrade keys distribution among shard threads.
  // For example, if we have 3 shards, then no single-char keys will be assigned to shard 2 and
  // 32 single char keys in range ['_' - '~'] will be assigned to shard 0.
  // Yes, SlotId function does not have great distribution properties.
  // On the other side, slot based sharding may help with pipeline squashing optimizations,
  // because they rely on commands being single-sharded.
  // TODO: once we improve our squashing logic, we can remove this.
  if (IsClusterShardedBySlot()) {
    return KeySlot(v) % shard_num;
  }

  if (IsClusterShardedByTag()) {
    v = LockTagOptions::instance().Tag(v);
  }

  // Map the lock tag, not the key, so keys sharing a lock tag stay on the same shard.
  if (shard_sidekiq_queues) {
    if (string_view q = SidekiqQueueName(v); !q.empty()) {
      v = q;
    }
  }

  XXH64_hash_t hash = XXH64(v.data(), v.size(), 120577240643ULL);

  if (RoundRobinSharder::IsEnabled()) {
    auto round_robin = RoundRobinSharder::TryGetShardId(v, hash);
    if (round_robin.has_value()) {
      return *round_robin;
    }
  }

  return hash % shard_num;
}

namespace sharding {
void InitThreadLocals(uint32_t shard_set_size) {
  shard_sidekiq_queues = absl::GetFlag(FLAGS_shard_sidekiq_queues);
  RoundRobinSharder::Init(shard_set_size);
}
}  // namespace sharding

}  // namespace dfly
