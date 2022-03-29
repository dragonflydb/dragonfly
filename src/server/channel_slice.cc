// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/channel_slice.h"

namespace dfly {
using namespace std;

ChannelSlice::Subscriber::Subscriber(ConnectionContext* cntx, uint32_t tid)
    : conn_cntx(cntx), borrow_token(cntx->conn_state.subscribe_info->borrow_token), thread_id(tid) {
}

void ChannelSlice::RemoveSubscription(string_view channel, ConnectionContext* me) {
  auto it = channels_.find(channel);
  if (it != channels_.end()) {
    it->second->subscribers.erase(me);
    if (it->second->subscribers.empty())
      channels_.erase(it);
  }
}

void ChannelSlice::AddSubscription(string_view channel, ConnectionContext* me, uint32_t thread_id) {
  auto [it, added] = channels_.emplace(channel, nullptr);
  if (added) {
    it->second.reset(new Channel);
  }
  it->second->subscribers.emplace(me, SubscriberInternal{thread_id});
}

auto ChannelSlice::FetchSubscribers(string_view channel) -> vector<Subscriber> {
  vector<Subscriber> res;

  auto it = channels_.find(channel);
  if (it != channels_.end()) {
    res.reserve(it->second->subscribers.size());
    for (const auto& k_v : it->second->subscribers) {
      Subscriber s(k_v.first, k_v.second.thread_id);
      s.borrow_token.Inc();

      res.push_back(std::move(s));
    }
  }

  return res;
}

}  // namespace dfly
