// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/channel_store.h"

#include <absl/container/fixed_array.h>
#include <absl/container/inlined_vector.h>

#include "base/logging.h"
#include "core/glob_matcher.h"
#include "facade/dragonfly_connection.h"
#include "server/cluster/slot_set.h"
#include "server/cluster_support.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"

namespace dfly {
using namespace std;

namespace {

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

ChannelStore::UpdatablePointer::UpdatablePointer(const UpdatablePointer& other) {
  ptr.store(other.ptr.load(memory_order_acquire), memory_order_release);
}

ChannelStore::UpdatablePointer::UpdatablePointer(UpdatablePointer&& other) noexcept {
  ptr.store(other.ptr.load(memory_order_acquire), memory_order_release);
  other.ptr.store(nullptr, memory_order_release);
}

ChannelStore::SubscribeMap* ChannelStore::UpdatablePointer::Get() const {
  return ptr.load(memory_order_acquire);
}

void ChannelStore::UpdatablePointer::Set(ChannelStore::SubscribeMap* sm) const {
  ptr.store(sm, memory_order_release);
}

ChannelStore::SubscribeMap* ChannelStore::UpdatablePointer::operator->() const {
  return Get();
}

const ChannelStore::SubscribeMap& ChannelStore::UpdatablePointer::operator*() const {
  return *Get();
}

bool ChannelStore::Subscriber::ByThread(const Subscriber& lhs, const Subscriber& rhs) {
  return ByThreadId(lhs, rhs.LastKnownThreadId());
}

bool ChannelStore::Subscriber::ByThreadId(const Subscriber& lhs, const unsigned thread) {
  return lhs.LastKnownThreadId() < thread;
}

ChannelStore::~ChannelStore() {
  auto del_cb = [](const string&, UpdatablePointer& up) { delete up.Get(); };
  channels_.ForEachExclusive(del_cb);
  patterns_.ForEachExclusive(del_cb);
}

unsigned ChannelStore::SendMessages(string_view channel, facade::ArgRange messages,
                                    bool sharded) const {
  vector<Subscriber> subscribers = FetchSubscribers(channel);
  if (subscribers.empty())
    return 0;

  int32_t last_thread = -1;
  for (auto& sub : subscribers) {
    int sub_thread = sub.LastKnownThreadId();
    DCHECK_LE(last_thread, sub_thread);
    if (last_thread == sub_thread)
      continue;
    if (sub.IsExpired())
      continue;
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

  channels_.FindIf(channel, [&](const UpdatablePointer& up) { Fill(*up, string{}, &res); });

  patterns_.ForEachShared([&](const string& pat, const UpdatablePointer& up) {
    GlobMatcher matcher{pat, true};
    if (matcher.Matches(channel))
      Fill(*up, pat, &res);
  });

  sort(res.begin(), res.end(), Subscriber::ByThread);
  return res;
}

void ChannelStore::Fill(const SubscribeMap& src, const string& pattern, vector<Subscriber>* out) {
  out->reserve(out->size() + src.size());
  for (const auto [cntx, thread_id] : src) {
    CHECK(cntx->conn_state.subscribe_info);
    Subscriber sub{cntx->conn()->Borrow(), pattern};
    out->push_back(std::move(sub));
  }
}

vector<string> ChannelStore::ListChannels(const string_view pattern) const {
  vector<string> res;
  GlobMatcher matcher{pattern, true};
  channels_.ForEachShared([&](const string& channel, const UpdatablePointer&) {
    if (pattern.empty() || matcher.Matches(channel))
      res.push_back(channel);
  });
  return res;
}

size_t ChannelStore::PatternCount() const {
  return patterns_.SizeApproximate();
}

void ChannelStore::UnsubscribeAfterClusterSlotMigration(const cluster::SlotSet& deleted_slots) {
  if (deleted_slots.Empty())
    return;

  // Single pass: collect matching channels and their subscribers.
  absl::flat_hash_map<string, vector<Subscriber>> owned_subs;
  channels_.ForEachShared([&](const string& channel, const UpdatablePointer& up) {
    if (!deleted_slots.Contains(KeySlot(channel)))
      return;
    vector<Subscriber> subs;
    Fill(*up, string{}, &subs);
    if (!subs.empty()) {
      sort(subs.begin(), subs.end(), Subscriber::ByThread);
      owned_subs.emplace(channel, std::move(subs));
    }
  });

  if (owned_subs.empty())
    return;

  for (const auto& [channel, _] : owned_subs)
    RemoveAllSubscribers(false, channel);

  ChannelsSubMap channel_subs_map;
  channel_subs_map.reserve(owned_subs.size());
  for (auto& [channel, subs] : owned_subs)
    channel_subs_map.emplace(channel, std::move(subs));

  shard_set->pool()->AwaitFiberOnAll([&channel_subs_map](unsigned idx, util::ProactorBase*) {
    channel_store->UnsubscribeConnectionsFromDeletedSlots(channel_subs_map, idx);
  });
}

void ChannelStore::RemoveAllSubscribers(bool pattern, string_view channel) {
  ChannelMap& map = pattern ? patterns_ : channels_;
  string key{channel};
  map.Mutate(key, [&](const auto& m, auto AcquireReadExclusiveLock) {
    auto it = m.find(key);
    if (it == m.end())
      return;
    auto locked_map = AcquireReadExclusiveLock();
    delete it->second.Get();
    locked_map.map.erase(it);
  });
}

void ChannelStore::UnsubscribeConnectionsFromDeletedSlots(const ChannelsSubMap& sub_map,
                                                          uint32_t idx) {
  for (const auto& [channel, subscribers] : sub_map) {
    std::string msg = "__ignore__";
    auto send = BuildSender(channel, {facade::ArgSlice{msg}}, false, true);

    auto it = lower_bound(subscribers.begin(), subscribers.end(), idx,
                          ChannelStore::Subscriber::ByThreadId);
    while (it != subscribers.end() && it->LastKnownThreadId() == idx) {
      if (auto* ptr = it->Get(); ptr && ptr->cntx() != nullptr) {
        DCHECK(it->pattern.empty());
        send(ptr, it->pattern);
      }
      ++it;
    }
  }
}

ChannelStoreUpdater::ChannelStoreUpdater(bool pattern, bool to_add, ConnectionContext* cntx,
                                         uint32_t thread_id)
    : pattern_{pattern}, to_add_{to_add}, cntx_{cntx}, thread_id_{thread_id} {
}

void ChannelStoreUpdater::Record(string_view channel) {
  ChannelStore::ChannelMap& map = pattern_ ? channel_store->patterns_ : channel_store->channels_;
  size_t sid = map.ShardOf(channel);
  ops_[sid].push_back(channel);
}

void ChannelStoreUpdater::Apply() {
  ChannelStore::ChannelMap& map = pattern_ ? channel_store->patterns_ : channel_store->channels_;

  for (size_t sid = 0; sid < ChannelStore::ChannelMap::kNumShards; ++sid) {
    const auto& shard_keys = ops_[sid];

    if (shard_keys.empty()) {
      continue;
    }

    map.Mutate(ChannelStore::ChannelMap::ShardId{sid}, [&](const auto& m, auto LockReaders) {
      // Track which keys require map changes - new insert or last-subscriber erase.
      absl::InlinedVector<bool, 8> needs_map_change(shard_keys.size(), false);
      bool has_map_change = false;

      // Apply RCU update if possible, track if map change is needed.
      for (size_t i = 0; i < shard_keys.size(); ++i) {
        std::string_view key = shard_keys[i];
        auto it = m.find(key);
        if (to_add_) {
          if (it == m.end()) {
            needs_map_change[i] = true;
            has_map_change = true;
          } else {
            auto* old_sm = it->second.Get();
            auto* new_sm = new ChannelStore::SubscribeMap{*old_sm};
            new_sm->emplace(cntx_, thread_id_);
            it->second.Set(new_sm);
            freelist_[sid].push_back(old_sm);
          }
        } else {
          if (it == m.end())
            continue;
          if (it->second->size() == 1) {
            needs_map_change[i] = true;
            has_map_change = true;
          } else {
            auto* old_sm = it->second.Get();
            auto* new_sm = new ChannelStore::SubscribeMap{*old_sm};
            new_sm->erase(cntx_);
            it->second.Set(new_sm);
            freelist_[sid].push_back(old_sm);
          }
        }
      }

      // Apply map changes under exclusive if needed.
      if (has_map_change) {
        auto locked_map = LockReaders();
        for (size_t i = 0; i < shard_keys.size(); ++i) {
          if (!needs_map_change[i]) {
            continue;
          }
          std::string_view key = shard_keys[i];
          if (to_add_) {
            locked_map.map.emplace(std::string{key},
                                   ChannelStore::UpdatablePointer{
                                       new ChannelStore::SubscribeMap{{cntx_, thread_id_}}});
          } else {
            auto it = locked_map.map.find(key);
            if (it == locked_map.map.end()) {
              continue;
            }
            delete it->second.Get();
            locked_map.map.erase(it);
          }
        }
      }
    });

    // Delete old SubscribeMaps after taking exclusive read lock.
    if (!freelist_[sid].empty()) {
      map.WithReadExclusiveLock(ChannelStore::ChannelMap::ShardId{sid},
                                [&old_sms = freelist_[sid]]() {
                                  for (auto* sm : old_sms)
                                    delete sm;
                                });
    }
  }
}

}  // namespace dfly
