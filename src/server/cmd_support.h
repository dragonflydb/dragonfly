#pragma once

#include <variant>

#include "facade/error.h"
#include "server/conn_context.h"
#include "server/transaction.h"

namespace dfly::cmd {

// Base class of async context
struct AsyncContextBase {
  struct BlockerSentinel {};
  using PrepareResult = std::variant<facade::ErrorReply, BlockerSentinel>;

  virtual ~AsyncContextBase() = default;
  virtual PrepareResult Prepare(ArgSlice args, CommandContext* cntx) = 0;
  virtual void Reply(facade::SinkReplyBuilder*) = 0;

 protected:
  // Initializer instead of constructor to simplify inheritance
  void Init(CommandContext* cntx) {
    cntx_ = cntx;
  }

  // Perform single hop. Callback must live as long as context
  BlockerSentinel SingleHop(Transaction::RunnableType cb);

  // Init and Prepare. Return true if successful
  static bool InitAndPrepare(AsyncContextBase* async_cntx, ArgSlice, CommandContext*);

  static void RunSync(AsyncContextBase* async_cntx, ArgSlice, CommandContext*);
  static void RunAsync(std::unique_ptr<AsyncContextBase> async_cntx, ArgSlice, CommandContext*);

  CommandContext* cntx_ = nullptr;
  boost::intrusive_ptr<Transaction> tx_keepalive_;
};

// Extension of async context with limited interface for most commands
template <typename C> struct AsyncContext : private AsyncContextBase {
 protected:
  using PrepareResult = AsyncContextBase::PrepareResult;

  // Default prepare implementation that schedules a single hop
  PrepareResult Prepare(ArgSlice args, CommandContext* cntx) override {
    return SingleHop();
  }

  // Run single hop.
  // Restricted to member call to ensure lifetime and allocation efficiency
  BlockerSentinel SingleHop() {
    return AsyncContextBase::SingleHop(static_cast<C&>(*this));
  }

  CommandContext* cntx() const {
    return cntx_;
  }

 public:
  static void Run(ArgSlice args, CommandContext* cmd_cntx) {
    if (cmd_cntx->IsDeferredReply()) {
      AsyncContextBase::RunAsync(std::unique_ptr<AsyncContextBase>{new C{}}, args, cmd_cntx);
    } else {
      C async_cntx{};
      AsyncContextBase::RunSync(&async_cntx, args, cmd_cntx);
    }
  }
};

#define ASYNC_CMD(Name) struct Cmd##Name : public ::dfly::cmd::AsyncContext<Cmd##Name>

}  // namespace dfly::cmd
