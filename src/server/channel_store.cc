#include "server/channel_store.h"

// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <shared_mutex>

extern "C" {
#include "redis/util.h"
}

#include "base/logging.h"

namespace dfly {
using namespace std;

ChannelStore::Subscriber::Subscriber(ConnectionContext* cntx, uint32_t tid)
    : conn_cntx(cntx), borrow_token(cntx->conn_state.subscribe_info->borrow_token), thread_id(tid) {
}

ChannelStore::Subscriber::Subscriber(uint32_t tid)
    : conn_cntx(nullptr), borrow_token(0), thread_id(tid) {
}

void ChannelStore::ChannelMap::Add(string_view key, ConnectionContext* me, uint32_t thread_id) {
  (*this)[key].emplace(me, thread_id);
}

void ChannelStore::ChannelMap::Remove(string_view key, ConnectionContext* me) {
  if (auto it = find(key); it != end()) {
    it->second.erase(me);
    if (it->second.empty())
      erase(it);
  }
}

void ChannelStore::AddSubscription(string_view channel, ConnectionContext* me, uint32_t thread_id) {
  unique_lock lk{lock_};
  channels_.Add(channel, me, thread_id);
}

void ChannelStore::AddGlobPattern(string_view pattern, ConnectionContext* me, uint32_t thread_id) {
  unique_lock lk{lock_};
  patterns_.Add(pattern, me, thread_id);
}

void ChannelStore::RemoveSubscription(string_view channel, ConnectionContext* me) {
  unique_lock lk{lock_};
  channels_.Remove(channel, me);
}

void ChannelStore::RemoveGlobPattern(string_view pattern, ConnectionContext* me) {
  unique_lock lk{lock_};
  patterns_.Remove(pattern, me);
}

vector<ChannelStore::Subscriber> ChannelStore::FetchSubscribers(string_view channel) {
  shared_lock lk{lock_};
  vector<Subscriber> res;

  if (auto it = channels_.find(channel); it != channels_.end()) {
    Fill(it->second, string{}, &res);
  }

  for (const auto& [pat, subs] : patterns_) {
    if (stringmatchlen(pat.data(), pat.size(), channel.data(), channel.size(), 0) == 1) {
      Fill(subs, pat, &res);
    }
  }

  sort(res.begin(), res.end(), Subscriber::ByThread);
  return res;
}

void ChannelStore::Fill(const SubscribeMap& src, const string& pattern, vector<Subscriber>* out) {
  out->reserve(out->size() + src.size());
  for (const auto [cntx, thread_id] : src) {
    CHECK(cntx->conn_state.subscribe_info);

    Subscriber s(cntx, thread_id);
    s.pattern = pattern;
    s.borrow_token.Inc();

    out->push_back(std::move(s));
  }
}

std::vector<string> ChannelStore::ListChannels(const string_view pattern) const {
  shared_lock lk{lock_};
  vector<string> res;
  for (const auto& [channel, _] : channels_) {
    if (pattern.empty() ||
        stringmatchlen(pattern.data(), pattern.size(), channel.data(), channel.size(), 0) == 1) {
      res.push_back(channel);
    }
  }
  return res;
}

size_t ChannelStore::PatternCount() const {
  shared_lock lk{lock_};
  return patterns_.size();
}

}  // namespace dfly
