#include "server/channel_store.h"

// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/fixed_array.h>

#include "base/logging.h"
#include "core/glob_matcher.h"
#include "facade/dragonfly_connection.h"
#include "server/cluster/slot_set.h"
#include "server/cluster_support.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

namespace dfly {
using namespace std;

namespace {

// Build functor for sending messages to connection
auto BuildSender(string_view channel, facade::ArgRange messages, bool sharded = false,
                 bool unsubscribe = false) {
  absl::FixedArray<string_view, 1> views(messages.Size());
  size_t messages_size = accumulate(messages.begin(), messages.end(), 0,
                                    [](int sum, string_view str) { return sum + str.size(); });
  auto buf = shared_ptr<char[]>{new char[channel.size() + messages_size]};
  {
    memcpy(buf.get(), channel.data(), channel.size());
    char* ptr = buf.get() + channel.size();

    size_t i = 0;
    for (string_view message : messages) {
      memcpy(ptr, message.data(), message.size());
      views[i++] = {ptr, message.size()};
      ptr += message.size();
    }
  }

  return [channel, buf = std::move(buf), views = std::move(views), sharded, unsubscribe](
             facade::Connection* conn, string pattern) {
    string_view channel_view{buf.get(), channel.size()};
    for (std::string_view message_view : views) {
      conn->SendPubMessageAsync(
          {std::move(pattern), buf, channel_view, message_view, sharded, unsubscribe});
    }
  };
}

}  // namespace

ChannelStore* channel_store = nullptr;

bool ChannelStore::Subscriber::ByThread(const Subscriber& lhs, const Subscriber& rhs) {
  return ByThreadId(lhs, rhs.LastKnownThreadId());
}

bool ChannelStore::Subscriber::ByThreadId(const Subscriber& lhs, const unsigned thread) {
  return lhs.LastKnownThreadId() < thread;
}

void ChannelStore::Add(string_view channel, ConnectionContext* cntx, uint32_t thread_id,
                       bool pattern) {
  auto& map = pattern ? patterns_ : channels_;
  // try_emplace_l holds the write lock for the duration of the lambda ensuring atomicity
  map.try_emplace_l(
      channel, [&](auto& kv) { kv.second.emplace(cntx, thread_id); },
      SubscribeMap{{cntx, thread_id}});
}

void ChannelStore::Remove(string_view channel, ConnectionContext* cntx, bool pattern) {
  auto& map = pattern ? patterns_ : channels_;
  // erase_if holds the write lock for its duration, removing cntx from the SubscribeMap
  // and erasing the channel entry atomically if no subscribers remain.
  map.erase_if(string{channel}, [&](auto& kv) {
    kv.second.erase(cntx);
    return kv.second.empty();
  });
}

unsigned ChannelStore::SendMessages(std::string_view channel, facade::ArgRange messages,
                                    bool sharded) const {
  vector<Subscriber> subscribers = FetchSubscribers(channel);
  if (subscribers.empty())
    return 0;

  // Make sure none of the threads publish buffer limits is reached. We don't reserve memory ahead
  // and don't prevent the buffer from possibly filling, but the approach is good enough for
  // limiting fast producers. Most importantly, we can use DispatchBrief below as we block here
  int32_t last_thread = -1;

  for (auto& sub : subscribers) {
    int sub_thread = sub.LastKnownThreadId();
    DCHECK_LE(last_thread, sub_thread);
    if (last_thread == sub_thread)  // skip same thread
      continue;

    if (sub.IsExpired())
      continue;

    // Make sure the connection thread has enough memory budget to accept the message.
    // This is a heuristic and not entirely hermetic since the connection memory might
    // get filled again.
    facade::Connection::EnsureMemoryBudget(sub_thread);
    last_thread = sub_thread;
  }

  auto subscribers_ptr = make_shared<decltype(subscribers)>(std::move(subscribers));
  auto cb = [subscribers_ptr, send = BuildSender(channel, messages, sharded)](unsigned idx, auto*) {
    auto it = lower_bound(subscribers_ptr->begin(), subscribers_ptr->end(), idx,
                          ChannelStore::Subscriber::ByThreadId);
    while (it != subscribers_ptr->end() && it->LastKnownThreadId() == idx) {
      if (auto* ptr = it->Get(); ptr && ptr->cntx() != nullptr)
        send(ptr, it->pattern);
      it++;
    }
  };
  shard_set->pool()->DispatchBrief(std::move(cb));

  return subscribers_ptr->size();
}

vector<ChannelStore::Subscriber> ChannelStore::FetchSubscribers(string_view channel) const {
  vector<Subscriber> res;

  // if_contains holds the per-submap read lock for the duration of the lambda.
  channels_.if_contains(channel, [&](const auto& kv) { Fill(kv.second, string{}, &res); });

  // for_each holds each per-submap read lock while iterating it.
  patterns_.for_each([&](const auto& kv) {
    GlobMatcher matcher{kv.first, true};
    if (matcher.Matches(channel))
      Fill(kv.second, kv.first, &res);
  });

  sort(res.begin(), res.end(), Subscriber::ByThread);
  return res;
}

void ChannelStore::Fill(const SubscribeMap& src, const string& pattern, vector<Subscriber>* out) {
  out->reserve(out->size() + src.size());
  for (const auto [cntx, thread_id] : src) {
    // `cntx` is expected to be valid as it unregisters itself from the channel_store before
    // closing.
    CHECK(cntx->conn_state.subscribe_info);
    Subscriber sub{cntx->conn()->Borrow(), pattern};
    out->push_back(std::move(sub));
  }
}

std::vector<string> ChannelStore::ListChannels(const string_view pattern) const {
  vector<string> res;
  GlobMatcher matcher{pattern, true};
  // for_each holds each per-submap read lock while iterating it.
  channels_.for_each([&](const auto& kv) {
    if (pattern.empty() || matcher.Matches(kv.first))
      res.push_back(kv.first);
  });
  return res;
}

size_t ChannelStore::PatternCount() const {
  return patterns_.size();
}

void ChannelStore::UnsubscribeAfterClusterSlotMigration(const cluster::SlotSet& deleted_slots) {
  if (deleted_slots.Empty()) {
    return;
  }

  // channels_.for_each holds one channels_ submap lock at a time.
  // patterns_.for_each inside acquires patterns_ submap locks while a channels_
  // lock is held — safe because no other code path holds a patterns_ lock while
  // acquiring a channels_ lock (see locking-order invariant in channel_store.h).
  ChannelsSubMap channel_subs_map;
  channels_.for_each([&](const auto& kv) {
    if (!deleted_slots.Contains(KeySlot(kv.first)))
      return;
    vector<Subscriber> subs;
    Fill(kv.second, string{}, &subs);
    patterns_.for_each([&](const auto& pkv) {
      GlobMatcher matcher{pkv.first, true};
      if (matcher.Matches(kv.first))
        Fill(pkv.second, pkv.first, &subs);
    });
    if (!subs.empty()) {
      sort(subs.begin(), subs.end(), Subscriber::ByThread);
      channel_subs_map.emplace(kv.first, std::move(subs));
    }
  });

  shard_set->pool()->AwaitFiberOnAll([&channel_subs_map](unsigned idx, util::ProactorBase*) {
    channel_store->UnsubscribeConnectionsFromDeletedSlots(channel_subs_map, idx);
  });
}

// TODO: Reuse common code with Send function
// TODO: Find proper solution to hacky `force_unsubscribe` flag or at least move logic out of io
void ChannelStore::UnsubscribeConnectionsFromDeletedSlots(const ChannelsSubMap& sub_map,
                                                          uint32_t idx) {
  for (const auto& [channel, subscribers] : sub_map) {
    // ignored by pub sub handler because should_unsubscribe is true
    std::string msg = "__ignore__";
    auto send = BuildSender(channel, {facade::ArgSlice{msg}}, false, true);

    auto it = lower_bound(subscribers.begin(), subscribers.end(), idx,
                          ChannelStore::Subscriber::ByThreadId);
    while (it != subscribers.end() && it->LastKnownThreadId() == idx) {
      // if ptr->cntx() is null, a connection might have closed or be in the process of closing
      if (auto* ptr = it->Get(); ptr && ptr->cntx() != nullptr) {
        DCHECK(it->pattern.empty());
        send(ptr, it->pattern);
      }
      ++it;
    }
  }
}

}  // namespace dfly
