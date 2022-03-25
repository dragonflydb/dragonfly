// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/channel_slice.h"

namespace dfly {
using namespace std;

size_t ChannelSlice::Publish(std::string_view channel, std::string_view message) {
  auto it = channels_.find(channel);
  if (it == channels_.end())
    return 0;

  // TODO: to implement message passing.
  return it->second->subscribers.size();
}

void ChannelSlice::ChangeSubscription(std::string_view channel, bool to_add,
                                      facade::Connection* me) {
  if (to_add) {
    auto [it, added] = channels_.emplace(channel, nullptr);
    if (added) {
      it->second.reset(new Channel);
    }
    it->second->subscribers.emplace(me, Subscriber{});
  } else {
    auto it = channels_.find(channel);
    if (it != channels_.end()) {
      it->second->subscribers.erase(me);
      if (it->second->subscribers.empty())
        channels_.erase(it);
    }
  }
}

}  // namespace dfly
