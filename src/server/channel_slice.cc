// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/channel_slice.h"

extern "C" {
#include "redis/util.h"
}

#include "base/logging.h"

namespace dfly {
using namespace std;

ChannelSlice::Subscriber::Subscriber(ConnectionContext* cntx, uint32_t tid)
    : conn_cntx(cntx), borrow_token(cntx->conn_state.subscribe_info->borrow_token), thread_id(tid) {
}

void ChannelSlice::AddSubscription(string_view channel, ConnectionContext* me, uint32_t thread_id) {
  auto [it, added] = channels_.emplace(channel, nullptr);
  if (added) {
    it->second.reset(new Channel);
  }
  it->second->subscribers.emplace(me, SubscriberInternal{thread_id});
}

void ChannelSlice::RemoveSubscription(string_view channel, ConnectionContext* me) {
  auto it = channels_.find(channel);
  if (it != channels_.end()) {
    it->second->subscribers.erase(me);
    if (it->second->subscribers.empty())
      channels_.erase(it);
  }
}

void ChannelSlice::AddGlobPattern(string_view pattern, ConnectionContext* me, uint32_t thread_id) {
  auto [it, added] = patterns_.emplace(pattern, nullptr);
  if (added) {
    it->second.reset(new Channel);
  }
  it->second->subscribers.emplace(me, SubscriberInternal{thread_id});
}

void ChannelSlice::RemoveGlobPattern(string_view pattern, ConnectionContext* me) {
  auto it = patterns_.find(pattern);
  if (it != patterns_.end()) {
    it->second->subscribers.erase(me);
    if (it->second->subscribers.empty())
      patterns_.erase(it);
  }
}

auto ChannelSlice::FetchSubscribers(string_view channel) -> vector<Subscriber> {
  vector<Subscriber> res;

  auto it = channels_.find(channel);
  if (it != channels_.end()) {
    res.reserve(it->second->subscribers.size());
    CopySubscribers(it->second->subscribers, string{}, &res);
  }

  for (const auto& k_v : patterns_) {
    const string& pat = k_v.first;
    // 1 - match
    if (stringmatchlen(pat.data(), pat.size(), channel.data(), channel.size(), 0) == 1) {
      CopySubscribers(k_v.second->subscribers, pat, &res);
    }
  }

  return res;
}

void ChannelSlice::CopySubscribers(const SubscribeMap& src, const std::string& pattern,
                                   vector<Subscriber>* dest) {
  for (const auto& sub : src) {
    ConnectionContext* cntx = sub.first;
    CHECK(cntx->conn_state.subscribe_info);

    Subscriber s(cntx, sub.second.thread_id);
    s.pattern = pattern;
    s.borrow_token.Inc();

    dest->push_back(std::move(s));
  }
}

vector<string> ChannelSlice::ListChannels(const string_view pattern) {
  vector<string> res;
  for (const auto& k_v : channels_) {
    const string& channel = k_v.first;

    if (pattern.empty() || stringmatchlen(pattern.data(), pattern.size(), channel.data(), channel.size(), 0) == 1) {
      res.push_back(channel);
    }
  }

  return res;
}

size_t ChannelSlice::PatternCount() {
  return patterns_.size();
}

}  // namespace dfly
