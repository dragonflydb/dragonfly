// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/channel_slice.h"

extern "C" {
#include "redis/util.h"
}

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
    CopySubsribers(it->second->subscribers, string{}, &res);
  }

  for (const auto& k_v : patterns_) {
    const string& pat = k_v.first;
    // 1 - match
    if (stringmatchlen(pat.data(), pat.size(), channel.data(), channel.size(), 0) == 1) {
      CopySubsribers(k_v.second->subscribers, pat, &res);
    }
  }

  return res;
}

void ChannelSlice::CopySubsribers(const SubscribeMap& src, const std::string& pattern,
                                  vector<Subscriber>* dest) {
  for (const auto& sub : src) {
    Subscriber s(sub.first, sub.second.thread_id);
    s.pattern = pattern;
    s.borrow_token.Inc();

    dest->push_back(std::move(s));
  }
}

}  // namespace dfly
