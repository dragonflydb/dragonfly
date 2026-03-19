// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <concepts>
#include <coroutine>
#include <variant>

#include "facade/error.h"
#include "facade/op_status.h"
#include "server/conn_context.h"
#include "server/engine_shard.h"
#include "server/transaction.h"
#include "util/fibers/synchronization.h"

namespace dfly::cmd {

// Awaitable sentinel for the single hop of a transaction. Used instead of the
// actual awaitable to allow Promise to inject context implicitly and make command code simple.
using SingleHopSentinel = Transaction::RunnableType;

// Awaitable in command context for the single hop of a transaction with return value
template <typename RT> using SingleHopSentinelT = absl::FunctionRef<RT(Transaction*, EngineShard*)>;

// Perform single hop. Returns awaitable that resolves to resulting OpStatus
SingleHopSentinel SingleHop(const auto& f) {
  return f;
}

// Perform single hop. Returns awaitable that resolves to return value.
auto SingleHopT(const auto& f) -> SingleHopSentinelT<decltype(f(nullptr, nullptr))> {
  return f;
}

// Awaitable object for waiting for the single hop of a transaction to finish.
// Avoids coroutine suspending in synchronous mode, doing a fiber suspend instead.
// In asynchronous mode it registers the promise / blocker on the context.
struct SingleHopWaiter {
  bool await_ready() noexcept;
  void await_suspend(std::coroutine_handle<> handle) const noexcept;
  facade::OpStatus await_resume() const noexcept;

  CommandContext* cmd_cntx;
  Transaction::RunnableType callback;
  boost::intrusive_ptr<Transaction> tx_keepalive_ = nullptr;
};

// Extension of SingleHopWaiter capturing the return value of the callback
template <typename RT> struct SingleHopWaiterT : public SingleHopWaiter {
  static_assert(std::is_base_of_v<facade::OpResultBase, RT>);

  SingleHopWaiterT(CommandContext* cmd_cntx,
                   absl::FunctionRef<RT(Transaction*, EngineShard*)> callback)
      : SingleHopWaiter{cmd_cntx, *this}, callback{callback} {
  }

  OpStatus operator()(Transaction* tx, EngineShard* es) const {
    result = callback(tx, es);
    return result.status();
  }

  RT&& await_resume() noexcept {
    return std::move(result);
  }

  absl::FunctionRef<RT(Transaction*, EngineShard*)> callback;
  mutable RT result;
};

// Return type of async command. No actual use as of now
struct CmdR {
  struct Coro;
  using promise_type = Coro;
};

constexpr CmdR kAborted = {};

// Underlying driver (promise) of coroutine that defines its context
struct CmdR::Coro {
  // Coroutine created of a top level command
  Coro(facade::CmdArgList arg, CommandContext* cmd_cntx) : cmd_cntx{cmd_cntx} {
  }

  // Coroutine created of a internal function with arguments
  template <typename... Ts> Coro(CommandContext* cmd_cntx, const Ts&... ts) : cmd_cntx{cmd_cntx} {
  }

  // Use it waiter directly cases when it needs to stay in scope to keep the transaction alive
  auto& await_transform(SingleHopWaiter& waiter) const {
    return waiter;
  }

  auto await_transform(SingleHopSentinel callback) const {
    return SingleHopWaiter{cmd_cntx, callback};
  }

  template <typename RT> auto await_transform(SingleHopSentinelT<RT> callback) const {
    return SingleHopWaiterT<RT>{cmd_cntx, callback};
  }

  // Return error
  void return_value(const facade::ErrorReply& err) const noexcept;

  // Conclude command without any error
  void return_value(std::nullopt_t) const noexcept {
  }

  // Blank default implementations
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
