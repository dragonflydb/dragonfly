#pragma once

#include <variant>

#include "facade/error.h"
#include "server/conn_context.h"
#include "server/transaction.h"

namespace dfly::cmd {

// Base class of async context. Provides virtual async command interface
struct AsyncContextBase {
  struct BlockerSentinel {};
  using PrepareResult = std::variant<facade::ErrorReply, BlockerSentinel>;

  virtual ~AsyncContextBase() = default;

  // Prepare command. Must end with error or scheduled operation
  virtual PrepareResult Prepare(ArgSlice args, CommandContext* cntx) = 0;

  // Reply after scheduled operation was performed
  virtual void Reply(facade::SinkReplyBuilder*) = 0;

  template <typename C> static void Run(ArgSlice args, CommandContext* cmd_cntx) {
    static_assert(std::is_base_of_v<AsyncContextBase, C>);
    if (cmd_cntx->IsDeferredReply()) {
      RunAsync(std::make_unique<C>(), args, cmd_cntx);
    } else {
      C async_cntx{};
      RunSync(&async_cntx, args, cmd_cntx);
    }
  }

 protected:
  // Perform single hop. Callback must live as long as context
  BlockerSentinel SingleHop(Transaction::RunnableType cb);

  CommandContext* cntx() const {
    return cntx_;
  }

 private:
  // Initializer instead of constructor to simplify inheritance
  void Init(CommandContext* cntx) {
    cntx_ = cntx;
  }

  // Init and Prepare. Return true if successful
  static bool InitAndPrepare(AsyncContextBase* async_cntx, ArgSlice, CommandContext*);

  static void RunSync(AsyncContextBase* async_cntx, ArgSlice, CommandContext*);
  static void RunAsync(std::unique_ptr<AsyncContextBase> async_cntx, ArgSlice, CommandContext*);

  CommandContext* cntx_ = nullptr;
  boost::intrusive_ptr<Transaction> tx_keepalive_;
};

// Extension of async context with limited interface for most transactional commands
template <typename C> struct AsyncContext : public AsyncContextBase {
  static constexpr auto Run = AsyncContextBase::Run<C>;

 protected:
  // Default prepare implementation that schedules a single hop
  PrepareResult Prepare(ArgSlice args, CommandContext* cntx) override {
    return SingleHop();
  }

  // Run single hop. Restricted to member operator call to ensure lifetime safety
  BlockerSentinel SingleHop() {
    return AsyncContextBase::SingleHop(static_cast<C&>(*this));
  }
};

#define ASYNC_CMD(Name) struct Cmd##Name : public ::dfly::cmd::AsyncContext<Cmd##Name>

}  // namespace dfly::cmd
