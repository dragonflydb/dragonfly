// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

#include <array>
#include <functional>
#include <mutex>
#include <shared_mutex>

#include "base/logging.h"
#include "util/fibers/synchronization.h"

namespace dfly {

// Thread-safe hash map sharded into NUM_SHARDS independent shards.
//
// Each shard contains an absl::flat_hash_map protected by two fiber-aware locks:
//   - write_mu_ (Mutex): serializes writers. Only one writer can modify the shard at a time.
//   - read_mu_ (SharedMutex): guards readers. Acquired in shared mode for reads (FindIf,
//     ForEachShared, Size) and in exclusive mode when a writer needs to commit changes that
//     must be visible atomically to readers.
//
// The two-lock design allows multiple concurrent readers on a shard while a single writer
// prepares its mutation (holding only write_mu_). The writer then briefly acquires read_mu_
// exclusively to publish the change, minimizing the window during which readers are blocked.
//
// Shard selection is determined by hashing the key with Hash (default: absl::Hash<K>) and
// taking modulo NUM_SHARDS. Both Hash and Eq are forwarded to the underlying
// absl::flat_hash_map, so a custom Hash can be supplied as the fourth template argument and
// a custom equality as the fifth. To enable heterogeneous lookup (e.g. finding a std::string
// key via std::string_view), both Hash and Eq must be transparent. absl::Hash<K> is NOT
// transparent — its operator() only accepts const K&. Supply a custom hash that declares
// is_transparent and accepts all query types (e.g. std::string_view for string keys), paired
// with std::equal_to<> as Eq. Without both being transparent, heterogeneous lookups will
// not compile or will silently fall back to non-heterogeneous comparison.
//
// Thread safety guarantees:
//   - Concurrent reads on the same shard are safe (shared read_mu_).
//   - Concurrent writes to different shards are safe (independent locks).
//   - A write and a read on the same shard are safe (write_mu_ + exclusive read_mu_).
//   - Concurrent writes to the same shard are serialized by write_mu_.
//
// Re-entrancy: callbacks passed to FindIf, ForEachShared, ForEachExclusive, and
// WithReadExclusiveLock are invoked while one or more shard locks are held. Calling any
// ShardedHashMap method that would re-acquire the same lock on the same shard from within
// a callback will deadlock.
//
template <typename K, typename V, size_t NUM_SHARDS = 32, typename Hash = absl::Hash<K>,
          typename Eq = std::equal_to<K>>
class ShardedHashMap {
  static_assert(NUM_SHARDS > 0, "NUM_SHARDS must be greater than 0");
  using InternalMap = absl::flat_hash_map<K, V, Hash, Eq>;

 public:
  static constexpr size_t kNumShards = NUM_SHARDS;

  // Returned by the AcquireReaderLock callable passed to Mutate(). Holds an exclusive lock on
  // read_mu_ for the duration of its lifetime and exposes a mutable reference to the shard
  // map. Mutations must be performed through LockedMap::map to guarantee that no reader
  // observes a partial update.
  struct LockedMap {
    std::unique_lock<util::fb2::SharedMutex> lock;
    InternalMap& map;
  };

  // Looks up `key` under a shared read lock on its shard. If found, invokes f(const V&)
  // with the mapped value while still holding the lock, then returns true.
  // Returns false if the key is not present. The callback must not modify the value.
  //
  // The template parameter Q allows heterogeneous lookup — any type hashable via
  // absl::Hash<Q> and comparable against K can be used (e.g., std::string_view for
  // std::string keys).
  template <typename Q, typename F> bool FindIf(const Q& key, F&& f) const {
    const Shard& shard = shards_[ShardOf(key)];
    std::shared_lock read_lock(shard.read_mu_);
    auto it = shard.map_.find(key);
    if (it == shard.map_.end()) {
      return false;
    }
    std::forward<F>(f)(it->second);
    return true;
  }

  // Iterates over all entries across every shard, invoking f(const K&, const V&) for each.
  // Each shard's read_mu_ is acquired in shared mode independently — the iteration is NOT
  // a global snapshot, so entries may be added or removed in other shards concurrently.
  // Suitable for building approximate views or collecting statistics.
  template <typename F> void ForEachShared(F&& f) const {
    for (const Shard& shard : shards_) {
      std::shared_lock read_lock(shard.read_mu_);
      for (const auto& [k, v] : shard.map_) {
        f(k, v);
      }
    }
  }

  // Iterates over all entries with full exclusive access, invoking f(const K&, V&) for each.
  // Both write_mu_ and read_mu_ are held exclusively per shard, so no concurrent readers
  // or writers can access the shard during iteration. This is the heaviest locking mode —
  // use it only when entries must be mutated in-place or when a consistent per-shard view
  // is required. Note: like ForEachShared, this is still not a global snapshot across shards.
  template <typename F> void ForEachExclusive(F&& f) {
    for (Shard& shard : shards_) {
      std::unique_lock write_lock{shard.write_mu_};
      std::unique_lock reader_lock{shard.read_mu_};
      for (auto& [k, v] : shard.map_) {
        f(k, v);
      }
    }
  }

  // Primary mutation interface. Acquires write_mu_ exclusively on the shard that owns `key`,
  // then invokes f(const InternalMap& map, auto AcquireReaderLock).
  //
  // The callback receives:
  //   - map: a const reference to the shard's underlying absl::flat_hash_map. The caller
  //     may inspect data while only write_mu_ is held (readers still proceed).
  //   - AcquireReaderLock: a callable that returns LockedMap, which holds an exclusive lock
  //     on read_mu_ and a mutable InternalMap& reference. Mutations must go through LockedMap::map
  //     only — this ensures no reader observes a partial update.
  //
  // Do not hold multiple LockedMap instances simultaneously within the callback — read_mu_ is
  // non-recursive, so acquiring it twice will deadlock. Calling lock_readers() more than once
  // is safe only if the previous LockedMap has gone out of scope first.
  //
  // Typical usage pattern:
  //   map.Mutate(key, [&](const auto& m, auto lock_readers) {
  //       /* optionally inspect m (const) without blocking readers */
  //       auto lm = lock_readers();
  //       lm.map[key] = new_value;  // now no reader sees a partial update
  //   });
  template <typename F> void Mutate(const K& key, F&& f) {
    Shard& shard = shards_[ShardOf(key)];
    std::unique_lock write_lock{shard.write_mu_};
    std::forward<F>(f)(static_cast<const InternalMap&>(shard.map_), [&shard]() -> LockedMap {
      return {std::unique_lock<util::fb2::SharedMutex>{shard.read_mu_}, shard.map_};
    });
  }

  // Shard-index overload of Mutate. Same semantics as Mutate(key, f) but addresses the
  // shard directly by its index `sid` (0 <= sid < NUM_SHARDS). Useful when the caller has
  // already computed the shard via ShardOf() or needs to batch multiple keys that map to
  // the same shard under a single lock acquisition. The same lock_readers() re-entrancy
  // restriction applies: do not hold two LockedMap instances at the same time.
  template <typename F> void Mutate(size_t sid, F&& f) {
    DCHECK_LT(sid, NUM_SHARDS);
    Shard& shard = shards_[sid];
    std::unique_lock write_lock{shard.write_mu_};
    std::forward<F>(f)(static_cast<const InternalMap&>(shard.map_), [&shard]() -> LockedMap {
      return {std::unique_lock<util::fb2::SharedMutex>{shard.read_mu_}, shard.map_};
    });
  }

  // Returns the shard index (0 .. NUM_SHARDS-1) that `key` maps to. Can be used to
  // pre-compute the shard for later use with the shard-index overloads of Mutate() or
  // WithReadExclusiveLock(), or to group operations on keys that share a shard.
  template <typename Q> size_t ShardOf(const Q& key) const {
    return Hash{}(key) % NUM_SHARDS;
  }

  // Acquires read_mu_ exclusively on the shard that owns `key`, blocking all concurrent
  // readers (FindIf, ForEachShared, Size) on that shard, then invokes f(). The write_mu_
  // is NOT acquired, so this does not serialize against other writers. Use this when you
  // need to perform an external side-effect that must not race with readers of this shard
  // but the map itself is not being modified.
  template <typename F> void WithReadExclusiveLock(const K& key, F&& f) {
    Shard& shard = shards_[ShardOf(key)];
    std::unique_lock l{shard.read_mu_};
    std::forward<F>(f)();
  }

  // Shard-index overload of WithReadExclusiveLock. Same semantics but addresses the shard
  // directly by its index `sid` (0 <= sid < NUM_SHARDS).
  template <typename F> void WithReadExclusiveLock(size_t sid, F&& f) {
    DCHECK_LT(sid, NUM_SHARDS);
    std::unique_lock l{shards_[sid].read_mu_};
    std::forward<F>(f)();
  }

  // Returns the approximate total number of entries across all shards. Each shard's
  // read_mu_ is acquired in shared mode independently and its size accumulated.
  size_t SizeApproximate() const {
    size_t total = 0;
    for (const Shard& shard : shards_) {
      std::shared_lock read_lock{shard.read_mu_};
      total += shard.map_.size();
    }
    return total;
  }

 private:
  // Aligned to cache line.
  struct alignas(64) Shard {
    util::fb2::Mutex write_mu_;
    mutable util::fb2::SharedMutex read_mu_;
    InternalMap map_;
  };

  std::array<Shard, NUM_SHARDS> shards_;
};

}  // namespace dfly
