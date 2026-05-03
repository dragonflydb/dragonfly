// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

#include <array>
#include <atomic>
#include <string_view>

#include "core/sharded_hash_map.h"
#include "facade/connection_ref.h"
#include "facade/facade_types.h"

namespace dfly {

class ConnectionContext;

namespace cluster {
class SlotSet;
}

// ChannelStore manages PUB/SUB subscriptions.
//
// The store is backed by two ChannelMap instances (ShardedHashMap<string, UpdatablePointer, 16>)
// — one for exact-channel subscriptions and one for pattern subscriptions.
// Each of the 16 shards carries two independent fiber-aware mutexes:
//
//   write_mu_  — serializes all writers within the shard; readers never acquire it.
//   read_mu_   — taken shared by readers; taken exclusively only for structural map changes
//                and for safe deletion of old SubscribeMaps (draining in-flight readers).
//
// UpdatablePointer wraps an atomic<SubscribeMap*> enabling RCU-style pointer swap:
// a writer copies the old map, modifies the copy, then atomically stores the new pointer
// via UpdatablePointer::Set — without holding read_mu_, so readers run concurrently.
//
// Add / Remove subscriber flow (via ChannelStoreUpdater::Apply)
// -------------------------------------------------------------
//   Key already exists (add/remove subscriber from a non-empty channel):
//     1. Acquire write_mu_ exclusively (Mutate) — serializes writers on this shard.
//     2. Copy the SubscribeMap, modify the copy, atomically swap via UpdatablePointer::Set.
//        Readers are NOT blocked; they may still read the old pointer.
//     3. Release write_mu_ (Mutate returns).
//     4. Acquire read_mu_ exclusively (WithReadExclusiveLock, separate call) — this drains
//        any reader that loaded the old SubscribeMap pointer, then deletes it.
//
//   Key does not exist (first subscriber) / last subscriber removed (channel erased):
//     Inside the Mutate callback, acquire read_mu_ exclusively via the
//     AcquireReaderExclusiveLock callable — blocks all readers in the shard while
//     inserting or erasing the key. Writers on other shards are unaffected.
//
class ChannelStore {
  friend class ChannelStoreUpdater;

 public:
  struct Subscriber : public facade::ConnectionRef {
    Subscriber(ConnectionRef ref, const std::string& pattern)
        : facade::ConnectionRef(std::move(ref)), pattern(pattern) {
    }

    // Sort by thread-id. Subscriber without owner comes first.
    static bool ByThread(const Subscriber& lhs, const Subscriber& rhs);
    static bool ByThreadId(const Subscriber& lhs, const unsigned thread);

    std::string pattern;  // non-empty if registered via psubscribe
  };

  ChannelStore() = default;
  ~ChannelStore();

  ChannelStore(const ChannelStore&) = delete;
  ChannelStore& operator=(const ChannelStore&) = delete;

  // Send messages to channel, block on connection backpressure.
  unsigned SendMessages(std::string_view channel, facade::ArgRange messages, bool sharded) const;

  // Fetch all subscribers for channel, including matching patterns.
  // Note: not a global snapshot — each shard is locked independently.
  std::vector<Subscriber> FetchSubscribers(std::string_view channel) const;

  std::vector<std::string> ListChannels(const std::string_view pattern) const;

  size_t PatternCount() const;

  void UnsubscribeAfterClusterSlotMigration(const cluster::SlotSet& deleted_slots);

 private:
  using ThreadId = unsigned;

  using ChannelsSubMap =
      absl::flat_hash_map<std::string_view, std::vector<ChannelStore::Subscriber>>;

  // Subscribers for a single channel/pattern: connection context → owning thread-id.
  using SubscribeMap = absl::flat_hash_map<ConnectionContext*, ThreadId>;

  // Atomic wrapper around a raw SubscribeMap pointer enabling RCU-style pointer swap.
  // Updated under write_mu_ exclusive (does NOT need read_mu_), so readers can be
  // concurrently active. The old SubscribeMap is deleted via WithReadExclusiveLock
  // after write_mu_ is released — that acquires read_mu_ exclusive, draining any
  // in-flight readers before calling delete.
  struct UpdatablePointer {
    explicit UpdatablePointer(SubscribeMap* sm) : ptr{sm} {
    }

    UpdatablePointer(const UpdatablePointer& other);
    UpdatablePointer(UpdatablePointer&& other) noexcept;

    UpdatablePointer& operator=(const UpdatablePointer&) = delete;
    UpdatablePointer& operator=(UpdatablePointer&&) = delete;

    SubscribeMap* Get() const;
    void Set(SubscribeMap* sm) const;

    SubscribeMap* operator->() const;
    const SubscribeMap& operator*() const;

   private:
    mutable std::atomic<SubscribeMap*> ptr;
  };

  // Transparent hash: accepts both std::string and std::string_view for heterogeneous lookup.
  struct StringViewHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const {
      return absl::Hash<std::string_view>{}(sv);
    }
  };

  using ChannelMap =
      ShardedHashMap<std::string, UpdatablePointer, 16, StringViewHash, std::equal_to<>>;

  // Remove all subscribers from a channel, erasing it from the map.
  void RemoveAllSubscribers(bool pattern, std::string_view channel);

  static void Fill(const SubscribeMap& src, const std::string& pattern,
                   std::vector<Subscriber>* out);

  void UnsubscribeConnectionsFromDeletedSlots(const ChannelsSubMap& sub_map, uint32_t idx);

  ChannelMap channels_;
  ChannelMap patterns_;
};

extern ChannelStore* channel_store;

// ChannelStoreUpdater batches multiple subscribe/unsubscribe operations for a single
// connection, groups them by shard index, and processes each shard in one Mutate call
// to minimise lock acquisitions.
class ChannelStoreUpdater {
 public:
  ChannelStoreUpdater(bool pattern, bool to_add, ConnectionContext* cntx, uint32_t thread_id);

  void Record(std::string_view channel);
  void Apply();

 private:
  bool pattern_;
  bool to_add_;
  ConnectionContext* cntx_;
  uint32_t thread_id_;

  // Pending operations grouped by shard index.
  std::array<std::vector<std::string_view>, ChannelStore::ChannelMap::kNumShards> ops_;
  // Replaced SubscribeMaps that need to be deleted safely, grouped by shard index.
  std::array<std::vector<ChannelStore::SubscribeMap*>, ChannelStore::ChannelMap::kNumShards>
      freelist_;
};

}  // namespace dfly
