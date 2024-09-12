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

  SinkReplyBuilder* reply_builder() {
    return rbuilder_.get();
  }

  // Allows receiving the output data from the commands called from scripts.
  SinkReplyBuilder* Inject(SinkReplyBuilder* new_i) {
    SinkReplyBuilder* res = rbuilder_.release();
    rbuilder_.reset(new_i);
    return res;
  }

  virtual void SendError(std::string_view str, std::string_view type = std::string_view{});

  virtual void SendError(ErrorReply error);

  virtual void SendError(OpStatus status);

  void SendStored() {
    rbuilder_->SendStored();
  }

  void SendSetSkipped() {
    rbuilder_->SendSetSkipped();
  }

  void SendMGetResponse(SinkReplyBuilder::MGetResponse resp) {
    rbuilder_->SendMGetResponse(std::move(resp));
  }

  void SendLong(long val) {
    rbuilder_->SendLong(val);
  }

  void SendSimpleString(std::string_view str) {
    rbuilder_->SendSimpleString(str);
  }

  void SendOk() {
    rbuilder_->SendOk();
  }

  void SendProtocolError(std::string_view str) {
    rbuilder_->SendProtocolError(str);
  }

  virtual size_t UsedMemory() const;

  // connection state / properties.
  bool conn_closing : 1;
  bool req_auth : 1;
  bool replica_conn : 1;
  bool authenticated : 1;
  bool async_dispatch : 1;    // whether this connection is amid an async dispatch
  bool sync_dispatch : 1;     // whether this connection is amid a sync dispatch
  bool journal_emulated : 1;  // whether it is used to dispatch journal commands

  bool paused = false;  // whether this connection is paused due to CLIENT PAUSE
  // whether it's blocked on blocking commands like BLPOP, needs to be addressable
  bool blocked = false;

  // Skip ACL validation, used by internal commands and commands run on admin port
  bool skip_acl_validation = false;

  // How many async subscription sources are active: monitor and/or pubsub - at most 2.
  uint8_t subscriptions;

  // TODO fix inherit actual values from default
  std::string authed_username{"default"};
  std::vector<uint64_t> acl_commands;
  // keys
  dfly::acl::AclKeys keys{{}, true};
  // pub/sub
  dfly::acl::AclPubSub pub_sub{{}, true};

 private:
  Connection* owner_;
  Protocol protocol_ = Protocol::REDIS;
  std::unique_ptr<SinkReplyBuilder> rbuilder_;
};

}  // namespace facade
