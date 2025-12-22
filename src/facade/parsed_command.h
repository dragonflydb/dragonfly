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

  // Marks this command as executed asynchronously with deferred reply.
  // Precondition: AsyncExecutionAllowed() must be true.
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

  void SendStored(bool ok /* true - ok, false - skipped*/);

  void SendNotFound() {  // For MC only.
    SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderNotFound());
  }

  void SendMiss() {  // For MC only.
    SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderMiss());
  }

  void SendGetEnd() {  // For MC only.
    SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderGetEnd());
  }

  void SendDeleted() {  // For MC only.
    SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderDeleted());
  }

  // If payload exists, sends it to reply builder, resets it and returns true.
  // Otherwise, returns false.
  bool SendPayload();

  // Polls whether the command can be finalized for execution.
  // If it was executed synchronously, returns true.
  // If it was dispatched asynchronously, marks it as head command and returns
  // true if it finished executing and its reply is ready, false otherwise.
  bool PollHeadForCompletion() {
    if (!is_deferred_reply_)
      return true;  // assert(holds_alternative<monostate>(cmd->TakeReplyPayload()));

    return CheckDoneAndMarkHead();
  }

  // Returns true if the caller can destroy this ParsedCommand.
  // false, if the command will be destroyed by its asynchronous callback.
  bool MarkForDestruction() {
    if (!is_deferred_reply_)
      return true;
    uint8_t prev_state = state_.fetch_or(DELETE_INTENT, std::memory_order_acq_rel);

    // If the reply is already done, we can destroy it now.
    return (prev_state & ASYNC_REPLY_DONE) != 0;
  }

 private:
  bool CheckDoneAndMarkHead();
  void NotifyReplied();

  // Synchronization state bits. The reply callback in a shard thread sets ASYNC_REPLY_DONE
  // when payload is filled. It also notifies the connection if the command is marked as HEAD_REPLY.
  // The connection fiber checks for ASYNC_REPLY_DONE and sets HEAD_REPLY via
  // CheckDoneAndMarkHead().
  enum StateBits : uint8_t {
    ASYNC_REPLY_DONE = 1 << 0,
    HEAD_REPLY = 1 << 1,  // it's the first command in the reply chain.
    DELETE_INTENT = 1 << 2,
  };
  std::atomic_uint8_t state_{0};

  // whether the command can be dispatched asynchronously.
  bool allow_async_execution_ = false;

  // if false then the reply was sent directly to reply builder,
  // otherwise, moved asynchronously into reply_payload_
  bool is_deferred_reply_ = false;

  payload::Payload reply_payload_;  // captured reply payload for async dispatches
};

#ifdef __APPLE__
static_assert(sizeof(ParsedCommand) == 216);
#else
static_assert(sizeof(ParsedCommand) == 224);
#endif

}  // namespace facade
