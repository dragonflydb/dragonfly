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
  // time when the message was parsed as reported by CycleClock::Now()
  uint64_t parsed_cycle = 0;
  ParsedCommand* next = nullptr;

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

  void set_reply_direct(bool direct) {
    reply_direct_ = direct;
  }

  // Returns whether the reply is sent directly to the client
  bool reply_direct() const {
    return reply_direct_;
  }

  void set_dispatch_async(bool async) {
    dispatch_async_ = async;
  }

  bool dispatch_async() const {
    return dispatch_async_;
  }

  void ResetForReuse();

  void SendError(std::string_view str, std::string_view type = std::string_view{});
  void SendError(facade::OpStatus status);
  void SendError(const facade::ErrorReply& error);
  void SendStored(bool ok /* true - ok, false - skipped*/);
  void SendSimpleString(std::string_view str);

  // If payload exists, sends it to reply builder, resets it and returns true.
  // Otherwise, returns false.
  bool SendPayload();

  // Polls whether the command can be finalized for execution.
  // If it was executed synchronously, returns true.
  // If it was dispatched asynchronously, marks it as head command and returns
  // true if it finished executing and its reply is ready, false otherwise.
  bool PollHeadForCompletion() {
    if (!dispatch_async_ || reply_direct_)
      return true;  // assert(holds_alternative<monostate>(cmd->TakeReplyPayload()));

    return CheckDoneAndMarkHead();
  }

 private:
  bool IsReplyCached() const {
    return !reply_direct_ && !std::holds_alternative<std::monostate>(reply_payload_);
  }

  bool CheckDoneAndMarkHead();
  void NotifyReplied();

  // Synchronization state bits. The reply callback in a shard thread sets ASYNC_REPLY_DONE
  // when payload is filled. It also notifies the connection if the command is marked as HEAD_REPLY.
  // The connection fiber checks for ASYNC_REPLY_DONE and sets HEAD_REPLY via
  // CheckDoneAndMarkHead().
  enum StateBits : uint8_t {
    ASYNC_REPLY_DONE = 1 << 0,
    HEAD_REPLY = 1 << 1,  // it's the first command in the reply chain.
  };
  std::atomic_uint8_t state_{0};

  // whether the reply should be sent directly, or captured for later sending
  bool reply_direct_ = true;

  // whether the command can be dispatched asynchronously.
  bool dispatch_async_ = false;

  payload::Payload reply_payload_;  // captured reply payload for async dispatches
};

#ifdef __APPLE__
static_assert(sizeof(ParsedCommand) == 208);
#else
static_assert(sizeof(ParsedCommand) == 216);
#endif

}  // namespace facade
