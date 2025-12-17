// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "common/backed_args.h"
#include "facade/memcache_parser.h"
#include "facade/reply_payload.h"

namespace facade {

class ConnectionContext;
class SinkReplyBuilder;

// ParsedCommand is a protocol-agnostic holder for parsed request state.
// It wraps cmn::BackedArguments so the facade can populate RESP arguments and
// optionally attach a MemcacheParser::Command, complementing the arguments
// with memcache-specific data.
// The purpose of ParsedCommand is to hold the entire state of a parsed request
// during its lifetime, from parsing to dispatching and reply building including
// any async dispatching.
class ParsedCommand : public cmn::BackedArguments {
  friend class ServiceInterface;

 protected:
  SinkReplyBuilder* rb_ = nullptr;  // either RedisReplyBuilder or MCReplyBuilder
  ConnectionContext* conn_cntx_ = nullptr;

  std::unique_ptr<MemcacheParser::Command> mc_cmd_;  // only for memcache protocol

  ParsedCommand() = default;

 public:
  payload::Payload reply_payload;  // captured reply payload for async dispatches
  bool dispatch_async = false;     // whether the command can be dispatched asynchronously
  ParsedCommand* next = nullptr;

  // time when the message was parsed as reported by CycleClock::Now()
  uint64_t parsed_cycle = 0;

  void Init(SinkReplyBuilder* rb, ConnectionContext* conn_cntx) {
    rb_ = rb;
    conn_cntx_ = conn_cntx;
  }

  void CreateMemcacheCommand() {
    mc_cmd_ = std::make_unique<MemcacheParser::Command>();
    mc_cmd_->backed_args = this;
  }

  SinkReplyBuilder* rb() const {
    return rb_;
  }

  ConnectionContext* conn_cntx() const {
    return conn_cntx_;
  }
  MemcacheParser::Command* mc_command() const {
    return mc_cmd_.get();
  }

  size_t UsedMemory() const {
    // TODO: sizeof(*this) is inaccurate here, as service can allocate extra space for
    // its derived class. Seems that we will have to make ParsedCommand polymorphic and use
    // virtual function here.
    size_t sz = HeapMemory() + sizeof(*this);
    if (mc_cmd_) {
      sz += sizeof(*mc_cmd_);
    }
    return sz;
  }

  void SendError(std::string_view str, std::string_view type = std::string_view{}) const;
  void SendError(facade::OpStatus status) const;
  void SendError(const facade::ErrorReply& error) const;
};

static_assert(sizeof(ParsedCommand) == 216);

}  // namespace facade
