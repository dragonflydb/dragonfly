// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <variant>

#include "base/function2.hpp"
#include "common/backed_args.h"
#include "facade/memcache_parser.h"
#include "facade/reply_payload.h"
#include "util/fibers/synchronization.h"

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

  template <typename T> struct ArgumentExtractor;

  // limited to lambdas with one argument as this is what we need here
  template <typename C, typename Arg> struct ArgumentExtractor<void (C::*)(Arg) const> {
    using type = Arg;
  };

  // extracts the type of the first argument of a callable lambda F
  template <typename F>
  using first_arg_t = typename ArgumentExtractor<decltype(&std::decay_t<F>::operator())>::type;

 public:
  using ReplyFunc = fu2::function_base<true, false, fu2::capacity_fixed<16, 8>, false, false,
                                       void(SinkReplyBuilder*)>;

  virtual ~ParsedCommand() = default;

  virtual size_t GetSize() const {
    return sizeof(ParsedCommand);
  }

  // time when the message was parsed as reported by CycleClock::Now()
  // Also serves as the enqueue timestamp for calculating pipeline wait latency.
  uint64_t parsed_cycle = 0;
  ParsedCommand* next = nullptr;

  void Init(SinkReplyBuilder* rb, ConnectionContext* conn_cntx) {
    rb_ = rb;
    conn_cntx_ = conn_cntx;
  }

  // If true, creates mc specific fields, false - destroys them.
  void ConfigureMCExtension(bool is_mc) {
    if (is_mc && !mc_cmd_) {
      mc_cmd_ = std::make_unique<MemcacheParser::Command>();
      mc_cmd_->backed_args = this;
    } else if (!is_mc) {
      mc_cmd_.reset();
    }
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

  // TODO: remove
  void SendLong(long val);
  void SendNull();
  void SendEmptyArray();

  // TODO: remove
  template <typename F> void ReplyWith(F&& func) {
    if (is_deferred_reply_) {
      reply_ = [func = std::forward<F>(func)](SinkReplyBuilder* builder) {
        auto* rb = static_cast<first_arg_t<F>>(builder);
        func(rb);
      };
    } else {
      auto* rb = static_cast<first_arg_t<F>>(rb_);
      func(rb);
    }
  }

  // Below are main commands for the async api and all assume that the command defers replies

  // Whether SendReply() can be called. If not, it must be waited via Blocker()
  bool CanReply() const;

  // Reaching zero on blocker means CanReply() turns true
  util::fb2::EmbeddedBlockingCounter* Blocker() const {
    return std::get<AsyncTask>(reply_).blocker;
  }

  // Assumes CanReply() is true. Sends reply
  void SendReply();

  // Resolve deferred command with reply
  void Resolve(const facade::ErrorReply& error) {
    SendError(error);
  }

  // Resolve deferred command with async task
  void Resolve(util::fb2::EmbeddedBlockingCounter* blocker, ReplyFunc replier) {
    reply_ = AsyncTask{blocker, std::move(replier)};
  }

 protected:
  virtual void ReuseInternal() = 0;

 private:
  // Pending async command. Once blocker is ready, replier can be called
  struct AsyncTask {
    util::fb2::EmbeddedBlockingCounter* blocker;
    ReplyFunc replier;
  };

  // if false then the reply was sent directly to reply builder,
  // otherwise, moved asynchronously into reply_payload_
  bool is_deferred_reply_ = false;

  std::variant<payload::Payload, AsyncTask> reply_;
};

#ifdef __linux__
static_assert(sizeof(ParsedCommand) == 232);
#endif

}  // namespace facade
