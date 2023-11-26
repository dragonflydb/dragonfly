// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_set.h>

#include <memory>

#include "core/heap_size.h"
#include "facade/acl_commands_def.h"
#include "facade/facade_types.h"
#include "facade/reply_builder.h"

namespace facade {

class Connection;

class ConnectionContext {
 public:
  ConnectionContext(::io::Sink* stream, Connection* owner);

  virtual ~ConnectionContext() {
  }

  Connection* conn() {
    return owner_;
  }

  const Connection* conn() const {
    return owner_;
  }

  Protocol protocol() const {
    return protocol_;
  }

  // A convenient proxy for redis interface.
  // Use with caution -- should only be used only
  // in execution paths that are Redis *only*
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

  void SendError(std::string_view str, std::string_view type = std::string_view{}) {
    rbuilder_->SendError(str, type);
  }

  void SendError(ErrorReply&& error) {
    rbuilder_->SendError(std::move(error));
  }

  void SendSimpleString(std::string_view str) {
    rbuilder_->SendSimpleString(str);
  }

  void SendOk() {
    rbuilder_->SendOk();
  }

  virtual size_t UsedMemory() const {
    return dfly::HeapSize(rbuilder_);
  }

  // connection state / properties.
  bool conn_closing : 1;
  bool req_auth : 1;
  bool replica_conn : 1;
  bool authenticated : 1;
  bool async_dispatch : 1;    // whether this connection is amid an async dispatch
  bool sync_dispatch : 1;     // whether this connection is amid a sync dispatch
  bool journal_emulated : 1;  // whether it is used to dispatch journal commands

  // How many async subscription sources are active: monitor and/or pubsub - at most 2.
  uint8_t subscriptions;

  std::string authed_username{"default"};
  uint32_t acl_categories{dfly::acl::ALL};
  std::vector<uint64_t> acl_commands;
  // Skip ACL validation, used by internal commands and commands run on admin port
  bool skip_acl_validation = false;

 private:
  Connection* owner_;
  Protocol protocol_ = Protocol::REDIS;
  std::unique_ptr<SinkReplyBuilder> rbuilder_;
};

}  // namespace facade
