// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/transaction.h"

#include <gmock/gmock.h>

#include "base/logging.h"
#include "facade/facade_stats.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/engine_shard.h"
#include "server/engine_shard_set.h"
#include "server/namespaces.h"
#include "server/server_state.h"
#include "util/fibers/pool.h"
#include "util/fibers/synchronization.h"

namespace dfly {

using namespace std;
using namespace std::chrono_literals;
using namespace util;
using namespace testing;

constexpr size_t kNumThreads = 3;

class TransactionTest : public Test {
 protected:
  void SetUp() override;
  void TearDown() override;

  static void SetUpTestSuite() {
    ServerState::Init(kNumThreads, kNumThreads, nullptr, nullptr);
    facade::tl_facade_stats = new facade::FacadeStats;
  }

  boost::intrusive_ptr<Transaction> MakeTx(const CommandId* cid, StringVec keys) {
    boost::intrusive_ptr<Transaction> tx(new Transaction{cid});
    auto argv = std::make_shared<CmdArgVec>();
    for (auto& k : keys)
      argv->emplace_back(k);
    arg_holders_.push_back({std::move(keys), argv});
    CmdArgList args{argv->data(), argv->size()};
    CHECK_EQ(OpStatus::OK, tx->InitByArgs(&namespaces->GetDefaultNamespace(), 0, args));
    return tx;
  }

  std::unique_ptr<ProactorPool> pp_;
  // Keep argument backing storage alive for the duration of the test.
  std::vector<std::pair<StringVec, std::shared_ptr<CmdArgVec>>> arg_holders_;
};

void TransactionTest::SetUp() {
  pp_.reset(fb2::Pool::Epoll(kNumThreads));
  pp_->Run();
  pp_->AwaitBrief([](unsigned index, ProactorBase* p) {
    ServerState::Init(index, kNumThreads, nullptr, nullptr);
    if (facade::tl_facade_stats == nullptr) {
      facade::tl_facade_stats = new facade::FacadeStats;
    }
  });

  shard_set = new EngineShardSet(pp_.get());
  // Pass a no-op shard handler (not nullptr): the periodic shard-handler fiber would
  // otherwise invoke an empty std::function and crash if it fires during the test.
  shard_set->Init(kNumThreads, [] {});
}

void TransactionTest::TearDown() {
  shard_set->PreShutdown();
  shard_set->Shutdown();
  delete shard_set;
  shard_set = nullptr;

  pp_->Stop();
  pp_.reset();
}

// Reproduces a deadlock where the next hop of a continuation transaction (A) is
// silently dropped because an *inline* transaction (B) holds running_tx_ while
// its concluding callback preempts. Nothing re-drives the dropped poll, so A is
// stranded.
//
// Roles:
//   A: 2-hop, multi-shard (keys on shard 0 and shard 2). After hop1 it becomes
//      continuation_trans_ on each shard (removed from the txq).
//   B: 2-hop, single-shard on shard 0 (distinct key), coordinated from shard 0's
//      own thread -> runs inline. Its concluding hop parks while holding
//      running_tx_, simulating a snapshot/journal preemption inside RunCallback.
TEST_F(TransactionTest, ContinuationPollDroppedByInlineTx) {
  // Sanity check the key->shard mapping the scenario relies on.
  ASSERT_EQ(0u, Shard("x", shard_set->size()));
  ASSERT_EQ(2u, Shard("z", shard_set->size()));
  ASSERT_EQ(0u, Shard("a", shard_set->size()));

  static CommandId cid_a{"tx_test_a", 0, -1, 1, -1, acl::NONE};
  static CommandId cid_b{"tx_test_b", 0, -1, 1, -1, acl::NONE};

  auto tx_a = MakeTx(&cid_a, {"x", "z"});  // multi-shard: shard 0 + shard 2
  auto tx_b = MakeTx(&cid_b, {"a"});       // single shard: shard 0

  auto noop = [](Transaction*, EngineShard*) { return OpStatus::OK; };

  // Phase 1: A's first (non-concluding) hop -> A becomes continuation on 0 and 2.
  pp_->at(1)->Await([&] { tx_a->Execute(noop, false); });

  // Phase 2: drive B inline on shard 0's thread. hop1 schedules B OOO into the
  // txq; hop2 (concluding) parks while holding running_tx_ on shard 0.
  fb2::Done b_parked, b_release;
  auto fb_b = pp_->at(0)->LaunchFiber([&] {
    tx_b->Execute(noop, false);  // hop1: no preemption, stays in txq as OOO
    tx_b->Execute(
        [&](Transaction*, EngineShard*) {
          b_parked.Notify();
          b_release.Wait();  // running_tx_ == tx_b while parked here
          return OpStatus::OK;
        },
        true);  // hop2: concluding
  });

  ASSERT_TRUE(b_parked.WaitFor(5s)) << "B's concluding hop never parked";

  // Phase 3: A's second (concluding) hop. Arms A on shards 0 and 2.
  //   shard 2: continuation poll runs A's hop2 -> ok.
  //   shard 0: PollExecution sees running_tx_=B and drops A's poll.
  // A's run_barrier never completes on shard 0, so this fiber blocks.
  fb2::Done a_done;
  auto fb_a = pp_->at(1)->LaunchFiber([&] {
    tx_a->Execute(noop, true);
    a_done.Notify();
  });

  // Wait until A's hop2 has actually been armed on shard 0 (i.e. dispatched), then
  // give the shard fiber a moment to run -> and drop -> that poll.
  bool armed = false;
  for (int i = 0; i < 400 && !armed; ++i) {
    armed = pp_->at(0)->Await([&] { return tx_a->DEBUG_IsArmedInShard(0); });
    if (!armed)
      pp_->at(2)->Await([] { ThisFiber::SleepFor(5ms); });
  }
  ASSERT_TRUE(armed) << "A's hop2 was never armed on shard 0";
  pp_->at(2)->Await([] { ThisFiber::SleepFor(50ms); });

  // Phase 4: release B. Its hop2 concludes and clears running_tx_ on shard 0.
  // Because B dropped A's poll while it was running, B must re-drive the poll loop
  // (needs_repoll_) on conclusion and pick up A's stranded continuation.
  b_release.Notify();
  fb_b.Join();

  // Phase 5: A's hop2 completes once B re-drives the dropped poll. Without the fix
  // nothing re-drives the continuation on shard 0 and A stays stranded forever.
  ASSERT_TRUE(a_done.WaitFor(5s))
      << "A's continuation hop was stranded: the dropped PollExecution on shard 0 "
         "was never replayed after running_tx_ cleared";

  fb_a.Join();
}

}  // namespace dfly
