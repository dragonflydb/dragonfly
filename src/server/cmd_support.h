// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <coroutine>
#include <variant>

#include "facade/error.h"
#include "server/conn_context.h"
#include "server/engine_shard.h"
#include "server/transaction.h"
#include "util/fibers/synchronization.h"

namespace dfly::cmd {

// Pointer to blocker to wait for before replying
using BlockResult = util::fb2::EmbeddedBlockingCounter*;

// No execution was performed, the command is ready to reply
struct JustReplySentinel {};

// Request to perform a single hop
using SingleHopSentinel = Transaction::RunnableType;
SingleHopSentinel SingleHop(auto&& f) {
  return f;
}

// Handler for dispatching hops, must be part of a context
struct HopCoordinator {
  // Perform single hop. Callback must be kept alive until end!
  BlockResult SingleHop(CommandContext* cntx, Transaction::RunnableType cb);

  boost::intrusive_ptr<Transaction> tx_keepalive_;
};

// Base interface for async context
struct AsyncContextInterface {
  virtual ~AsyncContextInterface() = default;
  using PrepareResult = std::variant<facade::ErrorReply, JustReplySentinel, BlockResult>;

  // Prepare command. Must return either an error, JustReplySentinel (immediate reply),
  // or BlockResult (scheduled operation)
  virtual PrepareResult Prepare(ArgSlice args, CommandContext* cntx) = 0;

  // Reply after scheduled operation was performed
  virtual void Reply(facade::SinkReplyBuilder*) = 0;

  static void RunSync(AsyncContextInterface* async_cntx, ArgSlice, CommandContext*);
  static void RunAsync(std::unique_ptr<AsyncContextInterface> async_cntx, ArgSlice,
                       CommandContext*);
};

// Basic implementation of AsyncContext providing limited interface for single hop commands.
// Uses CRTP with `Derived` template to provide type-dependent helper functions
template <typename Derived>
struct SimpleContext : public AsyncContextInterface, private HopCoordinator {
  // Automatic runner function that is async agnostic
  static void Run(ArgSlice args, CommandContext* cmd_cntx) {
    using ACI = AsyncContextInterface;
    static_assert(std::is_base_of_v<ACI, Derived>);

    if (cmd_cntx->IsDeferredReply()) {
      auto* async_cntx = new Derived{};
      async_cntx->Init(cmd_cntx);
      ACI::RunAsync(std::unique_ptr<AsyncContextInterface>{async_cntx}, args, cmd_cntx);
    } else {
      Derived async_cntx{};
      async_cntx.Init(cmd_cntx);
      ACI::RunSync(&async_cntx, args, cmd_cntx);
    }
  }

  // Wrapper function to shard callback to call different signatures
  OpStatus operator()(Transaction* t, EngineShard* es) const {
    const auto& c = *static_cast<const Derived*>(this);
    return c(t->GetShardArgs(es->shard_id()), t->GetOpArgs(es));
  }

 private:
  void Init(CommandContext* cmd_cntx) {
    this->cmd_cntx = cmd_cntx;
  }

 protected:
  // Default prepare implementation that schedules a single hop
  PrepareResult Prepare(ArgSlice args, CommandContext* cntx) override {
    return SingleHop();
  }

  // Run single hop. Restricted to member operator call to ensure lifetime safety
  BlockResult SingleHop() {
    return HopCoordinator::SingleHop(cmd_cntx, *this);
  }

  // Don't run transaction, just Reply()
  JustReplySentinel JustReply() {
    return JustReplySentinel{};
  }

  CommandContext* cmd_cntx;
};

// Use for standard commands that inherit directly from SimpleContext.
// This macro includes the 'struct' keyword automatically.
// Example: ASYNC_CMD(Get) { ... };
#define ASYNC_CMD(Name) struct Cmd##Name : public ::dfly::cmd::SimpleContext<Cmd##Name>

// Return type of async command
struct CmdR {
  struct Coro;
  using promise_type = Coro;
};

struct SingleHopWaiter : HopCoordinator {
  bool await_ready() noexcept;
  void await_suspend(std::coroutine_handle<> handle) const noexcept;
  facade::OpStatus await_resume() const noexcept;

  CommandContext* cmd_cntx;
  BlockResult blocker;
  Transaction::RunnableType callback;
};

// Underlying driver (promise) of async
struct CmdR::Coro {
  Coro(facade::CmdArgList arg, CommandContext* cmd_cntx) : cmd_cntx{cmd_cntx} {
  }

  auto await_transform(SingleHopSentinel callback) {
    return SingleHopWaiter{{}, cmd_cntx, nullptr, callback};
  }

  // Return error
  void return_value(facade::ErrorReply&& err) const noexcept;

  // Conclude command without any error
  void return_value(std::nullopt_t) const noexcept {
  }

  // Blank default implmenetations
  CmdR get_return_object() {
    return {};
  }
  void unhandled_exception() noexcept {
  }
  std::suspend_never initial_suspend() noexcept {
    return {};
  }
  std::suspend_never final_suspend() noexcept {
    return {};
  }

  CommandContext* cmd_cntx;
};

}  // namespace dfly::cmd
