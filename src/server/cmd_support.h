#pragma once

#include <variant>

#include "facade/error.h"
#include "server/conn_context.h"
#include "server/transaction.h"

namespace dfly::cmd {
// Base class of async context
struct AsyncContextBase {
  struct TxBlockSentiel {};
  using PrepareResult = std::variant<facade::ErrorReply, TxBlockSentiel>;

 protected:
  void Init(CommandContext* cntx) {
    cntx_ = cntx;
  }

  // Perform single hop. Callback must live as long as context
  TxBlockSentiel SingleHop(Transaction::RunnableType cb);

  CommandContext* cntx_ = nullptr;
  boost::intrusive_ptr<Transaction> tx_keepalive_;
};

template <typename C>
concept IsAsyncContext = requires(C c) {
  {c.Reply(nullptr)};
};

// Extension of async context with transactional methods
template <typename C> struct AsyncContext : private AsyncContextBase {
 protected:
  using PrepareResult = AsyncContextBase::PrepareResult;

  // Default prepare implementation that schedules a single hop
  PrepareResult Prepare(ArgSlice args, CommandContext* cntx) {
    return SingleHop();
  }

  // Wrap SingleHop to call member operator
  TxBlockSentiel SingleHop() {
    return AsyncContextBase::SingleHop(static_cast<C&>(*this));
  }

  CommandContext* cntx() const {
    return cntx_;
  }

  // static_assert(IsAsyncContext<C>, "Provided command does not satisfy async interface");

  // Run async context in sync mode
  static void RunSync(ArgSlice args, CommandContext* cmd_cntx) {
    C async_cntx{};
    async_cntx.Init(cmd_cntx);

    auto result = async_cntx.Prepare(args, cmd_cntx);
    if (std::holds_alternative<facade::ErrorReply>(result))
      return cmd_cntx->SendError(std::get<facade::ErrorReply>(result));

    DCHECK(cmd_cntx->tx()->Blocker()->IsCompleted());  // Must be ready
    async_cntx.Reply(cmd_cntx->rb());
  }

  // Run async context in async mode
  static void RunAsync(ArgSlice args, CommandContext* cmd_cntx) {
    auto async_cntx = std::make_unique<C>();
    async_cntx->Init(cmd_cntx);

    auto result = async_cntx->Prepare(args, cmd_cntx);
    if (std::holds_alternative<facade::ErrorReply>(result))
      return cmd_cntx->Resolve(std::get<facade::ErrorReply>(result));

    auto replier = [async_cntx = std::move(async_cntx)](facade::SinkReplyBuilder* rb) {
      async_cntx->Reply(rb);
    };
    cmd_cntx->Resolve(cmd_cntx->tx()->Blocker(), std::move(replier));
  }

 public:
  static void Run(ArgSlice args, CommandContext* cntx) {
    if (cntx->IsDeferredReply())
      RunAsync(args, cntx);
    else
      RunSync(args, cntx);
  }
};

#define ASYNC_CMD(Name) struct Cmd##Name : public ::dfly::cmd::AsyncContext<Cmd##Name>

}  // namespace dfly::cmd
