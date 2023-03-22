// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <boost/fiber/mutex.hpp>
#include <string_view>

#include "server/conn_context.h"

namespace dfly {

struct ChannelStoreUpdater;

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
class ChannelStore {
  friend struct ChannelStoreUpdater;

 public:
  struct Subscriber {
    Subscriber(ConnectionContext* cntx, uint32_t tid);
    Subscriber(uint32_t tid);

    Subscriber(Subscriber&&) noexcept = default;
    Subscriber& operator=(Subscriber&&) noexcept = default;

    Subscriber(const Subscriber&) = delete;
    void operator=(const Subscriber&) = delete;

    // Sort by thread-id. Subscriber without owner comes first.
    static bool ByThread(const Subscriber& lhs, const Subscriber& rhs);

    ConnectionContext* conn_cntx;
    util::fibers_ext::BlockingCounter borrow_token;  // to keep connection alive
    uint32_t thread_id;
    std::string pattern;  // non-empty if registered via psubscribe
  };

  // Centralized controller to prevent overlaping updates.
  struct ControlBlock {
    void Destroy();

    ChannelStore* most_recent;
    ::boost::fibers::mutex update_mu;  // locked during updates.
  };

  // Initialize ChannelStore bound to specific control block.
  ChannelStore(ControlBlock* cb);

  // Fetch all subscribers for channel, including matching patterns.
  std::vector<Subscriber> FetchSubscribers(std::string_view channel) const;

  std::vector<std::string> ListChannels(const std::string_view pattern) const;
  size_t PatternCount() const;

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

    SubscribeMap* operator->();
    const SubscribeMap& operator*() const;

   private:
    std::atomic<SubscribeMap*> ptr;
  };

  // Subscriber maps for channels/pointers.
  struct ChannelMap : absl::flat_hash_map<std::string, UpdatablePointer> {
    void Add(std::string_view key, ConnectionContext* me, uint32_t thread_id);
    void Remove(std::string_view key, ConnectionContext* me);

    // Delete stored all SubscribeMap pointers.
    void DeleteAll();
  };

 private:
  ChannelStore(ChannelMap* channels, ChannelMap* patterns, ControlBlock*);

  // Size of underlying SubscribeMapf or key, or 0 if not present.
  unsigned SlotSize(bool pattern, std::string_view key);

  static void Fill(const SubscribeMap& src, const std::string& pattern,
                   std::vector<Subscriber>* out);

  ChannelMap* channels_;
  ChannelMap* patterns_;
  ControlBlock* control_block_;
};

// Performs RCU (read-copy-update) updates to the channel store.
// See ChannelStore header top for design details.
// Queues operations and performs them with Apply().
struct ChannelStoreUpdater {
  ChannelStoreUpdater(ChannelStore* store, bool pattern, ConnectionContext* cntx,
                      uint32_t thread_id);

  void Add(std::string_view key);
  void Remove(std::string_view key);
  void Apply();

 private:
  ChannelStore* store_;

  bool pattern_;
  ConnectionContext* cntx_;
  uint32_t thread_id_;

  bool needs_copy_ = false;
  std::vector<std::pair<std::string_view, bool>> ops_;
  std::vector<ChannelStore::SubscribeMap*> freelist_;
};

}  // namespace dfly
