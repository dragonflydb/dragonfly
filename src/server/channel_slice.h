// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <string_view>

#include "facade/dragonfly_connection.h"

namespace dfly {

// Database holding pubsub subscribers.
class ChannelSlice {
 public:
  size_t Publish(std::string_view channel, std::string_view message);

  void ChangeSubscription(std::string_view channel, bool to_add, facade::Connection* me);

 private:
  using Message = std::shared_ptr<std::string>;

  struct Subscriber {
  };

  struct Channel {
    absl::flat_hash_map<facade::Connection*, Subscriber> subscribers;
  };

  absl::flat_hash_map<std::string, std::unique_ptr<Channel>> channels_;
  absl::flat_hash_map<std::string, std::unique_ptr<Channel>> patterns_;
};

}  // namespace dfly
