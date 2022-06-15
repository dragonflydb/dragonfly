// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include "facade/facade_types.h"
#include "facade/reply_builder.h"

namespace facade {

class Connection;

class ConnectionContext {
 public:
  ConnectionContext(::io::Sink* stream, Connection* owner);

  // We won't have any virtual methods, probably. However, since we allocate derived class,
  // we need to declare a virtual d-tor so we could delete them inside Connection.
  virtual ~ConnectionContext() {}

  Connection* owner() {
    return owner_;
  }

  Protocol protocol() const {
    return protocol_;
  }

  // A convenient proxy for redis interface.
  RedisReplyBuilder* operator->();

  SinkReplyBuilder* reply_builder() {
    return rbuilder_.get();
  }

  // Allows receiving the output data from the commands called from scripts.
  SinkReplyBuilder* Inject(SinkReplyBuilder* new_i) {
    SinkReplyBuilder* res = rbuilder_.release();
    rbuilder_.reset(new_i);
    return res;
  }

  // connection state / properties.
  bool async_dispatch: 1;  // whether this connection is currently handled by dispatch fiber.
  bool conn_closing: 1;
  bool req_auth: 1;
  bool replica_conn: 1;
  bool authenticated: 1;
  bool force_dispatch: 1;   // whether we should route all requests to the dispatch fiber.

  virtual void OnClose() {}

  virtual std::string GetContextInfo() const { return std::string{}; }

 private:
  Connection* owner_;
  Protocol protocol_ = Protocol::REDIS;
  std::unique_ptr<SinkReplyBuilder> rbuilder_;
};

}  // namespace facade
