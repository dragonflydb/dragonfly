// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>
#include <base/RWSpinLock.h>

#include <string_view>

#include "server/conn_context.h"

namespace dfly {

// Centralized store holding pubsub subscribers. All public functions are thread safe.
class ChannelStore {
 public:
  struct Subscriber {
    ConnectionContext* conn_cntx;
    util::fibers_ext::BlockingCounter borrow_token;
    uint32_t thread_id;

    // non-empty if was registered via psubscribe
    std::string pattern;

    Subscriber(ConnectionContext* cntx, uint32_t tid);
    Subscriber(uint32_t tid);
    // Subscriber() : borrow_token(0) {}

    Subscriber(Subscriber&&) noexcept = default;
    Subscriber& operator=(Subscriber&&) noexcept = default;

    Subscriber(const Subscriber&) = delete;
    void operator=(const Subscriber&) = delete;

    // Sort by thread-id. Subscriber without owner comes first.
    static bool ByThread(const Subscriber& lhs, const Subscriber& rhs) {
      if (lhs.thread_id == rhs.thread_id)
        return (lhs.conn_cntx != nullptr) < (rhs.conn_cntx != nullptr);
      return lhs.thread_id < rhs.thread_id;
    }
  };

  void AddSub(std::string_view channel, ConnectionContext* me, uint32_t thread_id);
  void RemoveSub(std::string_view channel, ConnectionContext* me);

  void AddPatternSub(std::string_view pattern, ConnectionContext* me, uint32_t thread_id);
  void RemovePatternSub(std::string_view pattern, ConnectionContext* me);

  std::vector<Subscriber> FetchSubscribers(std::string_view channel);

  std::vector<std::string> ListChannels(const std::string_view pattern) const;
  size_t PatternCount() const;

 private:
  using ThreadId = unsigned;
  using SubscribeMap = absl::flat_hash_map<ConnectionContext*, ThreadId>;

  struct ChannelMap : absl::flat_hash_map<std::string, std::unique_ptr<SubscribeMap>> {
    void Add(std::string_view key, ConnectionContext* me, uint32_t thread_id);
    void Remove(std::string_view key, ConnectionContext* me);
  };

  static void Fill(const SubscribeMap& src, const std::string& pattern,
                   std::vector<Subscriber>* out);

  mutable folly::RWSpinLock lock_;
  ChannelMap channels_;
  ChannelMap patterns_;
};

}  // namespace dfly
