#pragma once

#include <variant>

#include "facade/error.h"
#include "server/conn_context.h"
#include "server/engine_shard.h"
#include "server/transaction.h"
#include "util/fibers/synchronization.h"

namespace dfly::cmd {

// Pointer to blocker to wait for before replying
using BlockResult = util::fb2::EmbeddedBlockingCounter*;

// Handler for dispatching hops, must be part of a context
struct HopCoordinator {
  // Perform single hop. Callback must be kept alive until end!
  BlockResult SingleHop(CommandContext* cntx, Transaction::RunnableType cb);

  boost::intrusive_ptr<Transaction> tx_keepalive_;
};

// Base interface for async context
struct AsyncContextInterface {
  virtual ~AsyncContextInterface() = default;
  using PrepareResult = std::variant<facade::ErrorReply, BlockResult>;

  // Prepare command. Must end with error or scheduled operation
  virtual PrepareResult Prepare(ArgSlice args, CommandContext* cntx) = 0;

  // Reply after scheduled operation was performed
  virtual void Reply(facade::SinkReplyBuilder*) = 0;

  static void RunSync(AsyncContextInterface* async_cntx, ArgSlice, CommandContext*);
  static void RunAsync(std::unique_ptr<AsyncContextInterface> async_cntx, ArgSlice,
                       CommandContext*);
};

// Basic implementation of AsyncContext providing limited interface for single hop commands
template <typename C> struct SimpleContext : public AsyncContextInterface, private HopCoordinator {
  // Automatic runner function that is async agnostic
  static void Run(ArgSlice args, CommandContext* cmd_cntx) {
    using ACI = AsyncContextInterface;
    static_assert(std::is_base_of_v<ACI, C>);

    if (cmd_cntx->IsDeferredReply()) {
      auto* async_cntx = new C{};
      async_cntx->Init(cmd_cntx);
      ACI::RunAsync(std::unique_ptr<AsyncContextInterface>{async_cntx}, args, cmd_cntx);
    } else {
      C async_cntx{};
      async_cntx.Init(cmd_cntx);
      ACI::RunSync(&async_cntx, args, cmd_cntx);
    }
  }

  // Wrapper function to shard callback to call different signatures
  OpStatus operator()(Transaction* t, EngineShard* es) const {
    const auto& c = *static_cast<const C*>(this);
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

  CommandContext* cmd_cntx;
};

#define ASYNC_CMD(Name) struct Cmd##Name : public ::dfly::cmd::SimpleContext<Cmd##Name>

}  // namespace dfly::cmd
