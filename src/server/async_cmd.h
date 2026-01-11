#pragma once

#include <absl/functional/function_ref.h>

#include <concepts>
#include <coroutine>

#include "facade/error.h"
#include "facade/facade_types.h"

namespace facade {
class SinkReplyBuilder;
}

namespace dfly {

class CommandContext;
class Transaction;  // TODO: acl_commands_def breaks with include
class EngineShard;
class ShardArgs;
class OpArgs;

namespace cmd {

namespace detail {

// No-op waiter used to hide reply builder access behind an await interface for future changes.
// For now the only awaitable is woken up when the coroutine is already allowed to reply directly.
struct ReplyEventWaiter {
  struct Event {};

  bool await_ready() const noexcept {
    return true;
  }
  void await_suspend(std::coroutine_handle<> handle) const noexcept {
  }

  facade::SinkReplyBuilder* await_resume() const noexcept;  // return cmnd_cntx->rb()

  CommandContext* cmnd_cntx;
};

// Wait for a last transaction hop to finish. Driven by io-event loop
// For async, it will register the transaction barrier as the context blocker and suspend.
// For sync, it will fiber-block on transaction and continue without coroutine-suspension.
struct LastHopWaiter {
  using Event = absl::FunctionRef<facade::OpStatus(::dfly::Transaction*, ::dfly::EngineShard*)>;

  bool await_ready() const noexcept;
  void await_suspend(std::coroutine_handle<> handle) const noexcept;
  facade::OpStatus await_resume() const noexcept;

  Event callback;
  CommandContext* cmnd_cntx;
};

};  // namespace detail

// Acquire reply builder for direct replies
inline auto direct_reply() {
  return detail::ReplyEventWaiter::Event{};
}

// Run single hop
inline auto single_hop(detail::LastHopWaiter::Event runnable) {
  return detail::LastHopWaiter::Event(runnable);
}

// Initial handle of async command. No functional methods.
// The coroutine manages itself via direct access to CommandContext*
struct Task {
  // Main coroutine driver
  // 1. co_return - return error / no error
  // 2. co_await - only specific awaitables are supported and provided with await_transform
  struct Promise {
    Promise(facade::CmdArgList arg, CommandContext* cmnd_cntx) : cmnd_cntx{cmnd_cntx} {
    }

    // Awaitable for acquiring reply builder. Currently no-op
    auto await_transform(detail::ReplyEventWaiter::Event) {
      return detail::ReplyEventWaiter{cmnd_cntx};
    }

    // Awaitable for performing a single hop
    auto await_transform(detail::LastHopWaiter::Event cb) {
      return detail::LastHopWaiter{cb, cmnd_cntx};
    }

    // Conclude command with an error
    void return_value(facade::ErrorReply&& err) noexcept;

    // Conclude command without any error
    void return_value(std::nullopt_t) const noexcept {
    }

    // == Blank default implementations below ==
    Task get_return_object() {
      return Task{};
    }
    void unhandled_exception() noexcept {
    }
    std::suspend_never initial_suspend() noexcept {
      return {};
    }
    std::suspend_never final_suspend() noexcept {
      return {};
    }

   private:
    CommandContext* cmnd_cntx;
  };
  using promise_type = Promise;
};

}  // namespace cmd

};  // namespace dfly
