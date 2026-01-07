// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include <memory>
#include <string_view>

#include "facade/acl_commands_def.h"

namespace facade {

class Connection;

class ConnectionContext {
 public:
  explicit ConnectionContext(Connection* owner) : owner_(owner) {
    conn_closing = false;
    req_auth = false;
    replica_conn = false;
    authenticated = false;
    async_dispatch = false;
    sync_dispatch = false;
    paused = false;
    blocked = false;

    subscriptions = 0;
  }

  virtual ~ConnectionContext() {
  }

  Connection* conn() {
    return owner_;
  }

  const Connection* conn() const {
    return owner_;
  }

  virtual size_t UsedMemory() const {
    return 0;
  }

  // Noop.
  virtual void Unsubscribe(std::string_view channel) {
  }

  // connection state / properties.
  bool conn_closing : 1;
  bool req_auth : 1;
  bool replica_conn : 1;  // whether it's a replica connection on the master side.
  bool authenticated : 1;
  bool async_dispatch : 1;  // whether this connection is amid an async dispatch
  bool sync_dispatch : 1;   // whether this connection is amid a sync dispatch

  bool paused = false;  // whether this connection is paused due to CLIENT PAUSE
  // whether it's blocked on blocking commands like BLPOP, needs to be addressable
  bool blocked = false;

  // Skip ACL validation, used by internal commands and commands run on admin port
  bool skip_acl_validation = false;

  // How many async subscription sources are active: monitor and/or pubsub - at most 2.
  uint8_t subscriptions;

 private:
  Connection* owner_;
};

}  // namespace facade
