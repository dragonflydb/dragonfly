// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "common/backed_args.h"
#include "facade/memcache_parser.h"
#include "facade/reply_payload.h"

namespace util::fb2 {
class EmbeddedBlockingCounter;
}  // namespace util::fb2

namespace facade {

class ConnectionContext;
class SinkReplyBuilder;

// Renders simple string responses based on flags.
// Returns empty string if no response is to be sent.
class MCRender {
 public:
  explicit MCRender(MemcacheCmdFlags flags) : flags_(flags) {
  }

  std::string RenderNotFound() const;
  std::string RenderMiss() const;
  std::string RenderDeleted() const;
  std::string RenderGetEnd() const;
  std::string RenderStored(bool ok) const;

 private:
  MemcacheCmdFlags flags_;
};

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
  virtual ~ParsedCommand() = default;

  virtual size_t GetSize() const {
    return sizeof(ParsedCommand);
  }

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
    size_t sz = HeapMemory() + GetSize();
    if (mc_cmd_) {
      sz += sizeof(*mc_cmd_);
    }
    return sz;
  }

  // Allows the possibility for asynchronous execution of this command.
  void AllowAsyncExecution() {
    allow_async_execution_ = true;
  }

  bool AsyncExecutionAllowed() const {
    return allow_async_execution_;
  }

  // Marks this command as having reply stored in its payload instead of being sent directly.
  void SetDeferredReply() {
    is_deferred_reply_ = true;
  }

  bool IsDeferredReply() const {
    return is_deferred_reply_;
  }

  void ResetForReuse();

  void SendError(std::string_view str, std::string_view type = std::string_view{});
  void SendError(facade::OpStatus status);
  void SendError(const facade::ErrorReply& error);

  void SendSimpleString(std::string_view str);
  void SendOk() {
    SendSimpleString("OK");
  }

  void SendNotFound() {  // For MC only.
    SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderNotFound());
  }

  void SendLong(long val);
  void SendNull();

  // If payload exists, sends it to reply builder, resets it and returns true.
  // Otherwise, returns false.
  bool SendPayload();
  bool CanReply() const;

  util::fb2::EmbeddedBlockingCounter* task_blocker;
  std::function<void(facade::SinkReplyBuilder*)> replier;

  payload::Payload reply_payload_;  // captured reply payload for async dispatches

 private:
  // whether the command can be dispatched asynchronously.
  bool allow_async_execution_ = false;

  // if false then the reply was sent directly to reply builder,
  // otherwise, moved asynchronously into reply_payload_
  bool is_deferred_reply_ = false;
};

static_assert(sizeof(ParsedCommand) == 264);

}  // namespace facade
