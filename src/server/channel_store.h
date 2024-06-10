// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <string_view>

#include "facade/dragonfly_connection.h"
#include "server/conn_context.h"

namespace dfly {

class ChannelStoreUpdater;

// ChannelStore manages PUB/SUB subscriptions.
//
// Updates are carried out via RCU (read-copy-update). Each thread stores a pointer to ChannelStore
// in its local ServerState and uses it for reads. Whenever an update needs to be performed,
// a new ChannelStore is constructed with the requested modifications and broadcasted to all
// threads.
//
// ServerState ChannelStore* -> ChannelMap* -> atomic<SubscribeMap*> (cntx -> thread)
//
// Specifically, whenever a new channel is registered or a channel is removed fully,
// a new ChannelMap for the specified type (channel/pattern) needs to be constructed. However, if
// only a single SubscribeMap is modified (no map ChannelMap slots are added or removed),
// we can update only it with a simpler version of RCU, as SubscribeMap is stored as an atomic
// pointer inside ChannelMap.
//
// To prevent parallel (and thus overlapping) updates, a centralized ControlBlock is used.
// Update operations are carried out by the ChannelStoreUpdater.
//
// A centralized ChannelStore, contrary to sharded storage, avoids contention on a single shard
// thread for heavy throughput on a single channel and thus seamlessly scales on multiple threads
// even with a small number of channels. In general, it has a slightly lower latency, due to the
// fact that no hop is required to fetch the subscribers.
class ChannelStore {
  friend class ChannelStoreUpdater;

 public:
  struct Subscriber : public facade::Connection::WeakRef {
    Subscriber(WeakRef ref, const std::string& pattern)
        : facade::Connection::WeakRef(std::move(ref)), pattern(pattern) {
    }

    // Sort by thread-id. Subscriber without owner comes first.
    static bool ByThread(const Subscriber& lhs, const Subscriber& rhs);
    static bool ByThreadId(const Subscriber& lhs, const unsigned thread);

    std::string pattern;  // non-empty if registered via psubscribe
  };

  ChannelStore();

  // Send messages to channel, block on connection backpressure
  unsigned SendMessages(std::string_view channel, facade::ArgRange messages) const;

  // Fetch all subscribers for channel, including matching patterns.
  std::vector<Subscriber> FetchSubscribers(std::string_view channel) const;

  std::vector<std::string> ListChannels(const std::string_view pattern) const;
  size_t PatternCount() const;

  // Destroy current instance and delete it.
  static void Destroy();

 private:
  using ThreadId = unsigned;

  // Subscribers for a single channel/pattern.
  using SubscribeMap = absl::flat_hash_map<ConnectionContext*, ThreadId>;

  // Wrapper around atomic pointer that allows copying and moving.
  // Made to overcome restrictions of absl::flat_hash_map.
  // Copy/Move don't need to be atomic with RCU.
  struct UpdatablePointer {
    UpdatablePointer(SubscribeMap* sm) : ptr{sm} {
    }

    UpdatablePointer(const UpdatablePointer& other);

    SubscribeMap* Get() const;
    void Set(SubscribeMap* sm);

    SubscribeMap* operator->() const;
    const SubscribeMap& operator*() const;

   private:
    std::atomic<SubscribeMap*> ptr;
  };

  // SubscriberMaps for channels/patterns.
  struct ChannelMap : absl::flat_hash_map<std::string, UpdatablePointer> {
    void Add(std::string_view key, ConnectionContext* me, uint32_t thread_id);
    void Remove(std::string_view key, ConnectionContext* me);

    // Delete all stored SubscribeMap pointers.
    void DeleteAll();
  };

  // Centralized controller to prevent overlaping updates.
  struct ControlBlock {
    std::atomic<ChannelStore*> most_recent;
    util::fb2::Mutex update_mu;  // locked during updates.
  };

 private:
  static ControlBlock control_block;

  ChannelStore(ChannelMap* channels, ChannelMap* patterns);

  static void Fill(const SubscribeMap& src, const std::string& pattern,
                   std::vector<Subscriber>* out);

  ChannelMap* channels_;
  ChannelMap* patterns_;
};

// Performs RCU (read-copy-update) updates to the channel store.
// See ChannelStore header top for design details.
// Queues operations and performs them with Apply().
class ChannelStoreUpdater {
 public:
  ChannelStoreUpdater(bool pattern, bool to_add, ConnectionContext* cntx, uint32_t thread_id);

  void Record(std::string_view key);
  void Apply();

 private:
  using ChannelMap = ChannelStore::ChannelMap;

  // Get target map and flag whether it was copied.
  // Must be called with locked control block.
  std::pair<ChannelMap*, bool> GetTargetMap(ChannelStore* store);

  // Apply modify operation to target map.
  void Modify(ChannelMap* target, std::string_view key);

 private:
  bool pattern_;
  bool to_add_;
  ConnectionContext* cntx_;
  uint32_t thread_id_;

  // Pending operations.
  std::vector<std::string_view> ops_;

  // Replaced SubscribeMaps that need to be deleted safely.
  std::vector<ChannelStore::SubscribeMap*> freelist_;
};

}  // namespace dfly
