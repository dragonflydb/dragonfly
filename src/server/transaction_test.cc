// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/transaction.h"

#include <gmock/gmock.h>

#include "base/logging.h"
#include "facade/facade_stats.h"
#include "server/acl/acl_commands_def.h"
#include "server/blocking_controller.h"
#include "server/cmd_memory_scope.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/db_slice.h"
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

  static OpStatus Noop(Transaction*, EngineShard*) {
    return OpStatus::OK;
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

  // Runs `f` on proactor `index` and returns its result.
  template <typename F> auto OnShard(unsigned index, F&& f) {
    return pp_->at(index)->Await(std::forward<F>(f));
  }

  // Polls `pred` on proactor `index` until it holds or `timeout` elapses, yielding between
  // checks so the shard fiber can make progress. Returns whether the condition was met.
  bool AwaitOnShard(unsigned index, std::function<bool()> pred,
                    std::chrono::milliseconds timeout = 5s) {
    return OnShard(index, [&]() -> bool {
      auto deadline = std::chrono::steady_clock::now() + timeout;
      do {
        if (pred())
          return true;
        ThisFiber::SleepFor(5ms);
      } while (std::chrono::steady_clock::now() < deadline);
      return false;
    });
  }

  // Blocks the test for `d` while the shard proactors keep running (used to let a dropped
  // poll be processed). Paced from an otherwise idle proactor.
  void Quiesce(std::chrono::milliseconds d) {
    OnShard(kNumThreads - 1, [d] { ThisFiber::SleepFor(d); });
  }

  // Handle for a transaction parked mid-callback while holding running_tx_.
  struct InlineHold {
    fb2::Done parked, release;
    fb2::Fiber fiber;

    bool WaitParked(std::chrono::milliseconds t = 5s) {
      return parked.WaitFor(t);
    }
    void ReleaseAndJoin() {
      release.Notify();
      fiber.Join();
    }
  };

  // Drives `tx` on proactor `index` so that a concluding hop parks while still holding
  // running_tx_ (mimicking an in-callback snapshot/journal preemption). The hop may run inline
  // or on the shard queue fiber depending on CanRunInlined; either way running_tx_ is held while
  // parked. `prelude`, if set, runs earlier hops on the same thread before the parking hop.
  std::unique_ptr<InlineHold> ParkHoldingRunningTx(unsigned index, Transaction* tx,
                                                   std::function<void()> prelude = {}) {
    auto hold = std::make_unique<InlineHold>();
    InlineHold* h = hold.get();
    h->fiber = pp_->at(index)->LaunchFiber([tx, prelude = std::move(prelude), h] {
      if (prelude)
        prelude();
      tx->Execute(
          [h](Transaction*, EngineShard*) {
            h->parked.Notify();
            h->release.Wait();  // running_tx_ == tx while parked here
            return OpStatus::OK;
          },
          true);  // concluding hop
    });
    return hold;
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

  // Phase 1: A's first (non-concluding) hop -> A becomes continuation on 0 and 2.
  OnShard(1, [&] { tx_a->Execute(Noop, false); });

  // Phase 2: drive B inline on shard 0. hop1 schedules B OOO into the txq; hop2 (concluding)
  // parks while holding running_tx_ on shard 0.
  auto b = ParkHoldingRunningTx(0, tx_b.get(), [&] { tx_b->Execute(Noop, false); });
  ASSERT_TRUE(b->WaitParked()) << "B's concluding hop never parked";

  // Phase 3: A's second (concluding) hop. Arms A on shards 0 and 2.
  //   shard 2: continuation poll runs A's hop2 -> ok.
  //   shard 0: PollExecution sees running_tx_=B and drops A's poll.
  // A's run_barrier never completes on shard 0, so this fiber blocks.
  fb2::Done a_done;
  auto fb_a = pp_->at(1)->LaunchFiber([&] {
    tx_a->Execute(Noop, true);
    a_done.Notify();
  });

  // Wait until A's hop2 has actually been armed (dispatched) on shard 0, then let the shard
  // fiber run and drop that poll.
  ASSERT_TRUE(AwaitOnShard(0, [&] { return tx_a->DEBUG_IsArmedInShard(0); }))
      << "A's hop2 was never armed on shard 0";
  Quiesce(50ms);

  // Phase 4: release B. Its hop2 concludes and clears running_tx_ on shard 0. Because B dropped
  // A's poll while it was running, B must re-drive the poll loop (needs_repoll_) on conclusion
  // and pick up A's stranded continuation.
  b->ReleaseAndJoin();

  // Phase 5: A's hop2 completes once B re-drives the dropped poll. Without the fix nothing
  // re-drives the continuation on shard 0 and A stays stranded forever.
  ASSERT_TRUE(a_done.WaitFor(5s))
      << "A's continuation hop was stranded: the dropped PollExecution on shard 0 "
         "was never replayed after running_tx_ cleared";

  fb_a.Join();
}

// Companion to ContinuationPollDroppedByInlineTx, but for an *awakened* blocking
// transaction (AWAKED_Q) rather than a continuation.
//
// A suspended blocking tx is removed from the txq (transaction.cc RunInShard). When it is
// later awakened, its action hop is polled *with itself as `trans`* and runs via the AWAKED_Q
// branch of PollExecutionInternal (step 1), which only executes when `trans != nullptr`.
//
// If an *inlined* tx holds running_tx_ across a mid-callback preemption when the awaken lands,
// PollExecution drops the poll. The deferred re-drive runs with trans == nullptr, so step 1 is
// skipped and the awakened tx is never re-run - it would be stranded (unlike a continuation, it
// is not recoverable by a generic re-drive).
//
// The fix (Transaction::CanRunInlined): a transaction refuses to inline while the shard has any
// blocked transactions. The tx that could preempt then runs on the shard's own queue fiber, so a
// concurrent awaken's poll queues behind it instead of racing it and being dropped.
//
// Roles:
//   BL: single-shard blocking tx on key "x" (shard 0). Schedules, then suspends via
//       WaitOnWatch; after being awakened it runs an action hop.
//   Z:  single-shard tx on key "a" (shard 0). Its concluding hop parks while holding
//       running_tx_, simulating a snapshot/journal preemption. Without the fix it would run
//       inline and drop BL's awaken poll; with the fix it is forced non-inline.
TEST_F(TransactionTest, AwakenedPollNotDroppedWhenBlockedTxPresent) {
  ASSERT_EQ(0u, Shard("x", shard_set->size()));
  ASSERT_EQ(0u, Shard("a", shard_set->size()));

  static CommandId cid_bl{"tx_test_bl", 0, -1, 1, -1, acl::NONE};
  static CommandId cid_z{"tx_test_z", 0, -1, 1, -1, acl::NONE};

  auto tx_bl = MakeTx(&cid_bl, {"x"});
  auto tx_z = MakeTx(&cid_z, {"a"});

  auto ready_checker = [](EngineShard*, const DbContext&, std::string_view) {
    return KeyReadyResult::kReady;
  };

  // Phase 1: BL schedules a first hop, then suspends watching key "x".
  bool bl_blocked = false, bl_paused = false;
  fb2::Done bl_done;
  auto fb_bl = pp_->at(1)->LaunchFiber([&] {
    tx_bl->Execute(Noop, false);  // schedule + first (non-concluding) hop -> stays scheduled
    std::string key = "x";
    auto tp = Transaction::time_point::max();
    OpStatus st =
        tx_bl->WaitOnWatch(tp, std::string_view{key}, ready_checker, &bl_blocked, &bl_paused);
    ASSERT_EQ(OpStatus::OK, st);
    tx_bl->Execute(Noop, true);  // action hop after wakeup
    bl_done.Notify();
  });

  // Wait until BL is parked inside the blocking barrier (suspended, off the txq).
  ASSERT_TRUE(AwaitOnShard(0, [&] {
    return (tx_bl->DEBUG_GetLocalMask(0) & Transaction::WAS_SUSPENDED) != 0;
  })) << "BL never suspended";

  // Phase 2: try to drive Z inline on shard 0. Because BL is blocked on the shard, the fix
  // forces Z non-inline, so Z runs on the shard's queue fiber; its single concluding hop parks
  // there while holding running_tx_ (Z is not a continuation, so it doesn't block the awaken).
  auto z = ParkHoldingRunningTx(0, tx_z.get());
  ASSERT_TRUE(z->WaitParked()) << "Z's concluding hop never parked";

  // Phase 3: awaken BL on shard 0 while Z holds running_tx_. NotifyPending closes BL's
  // blocking barrier -> BL's coordinator wakes and dispatches its action hop poll to shard 0.
  // Since Z runs on the shard's own queue fiber (not inline), BL's poll queues behind it
  // instead of racing it; without the fix Z would inline and the poll would be dropped.
  OnShard(0, [&] {
    auto* bc = namespaces->GetDefaultNamespace().GetBlockingController(0);
    ASSERT_TRUE(bc != nullptr);
    bc->Awaken(0, "x");
    bc->NotifyPending();
  });

  // Give BL's coordinator time to wake and dispatch its action poll.
  Quiesce(50ms);

  // Phase 4: release Z. Its hop concludes and clears running_tx_ on shard 0; the shard fiber
  // then processes BL's queued awaken poll.
  z->ReleaseAndJoin();

  // Phase 5: BL's action hop completes. Without the fix, an inlined Z would have dropped the
  // awaken poll and BL would be stranded (a trans==nullptr re-drive cannot re-run an AWAKED_Q).
  ASSERT_TRUE(bl_done.WaitFor(5s))
      << "awakened BL was stranded: its PollExecution on shard 0 was never run";

  fb_bl.Join();
}

// Verifies Task 2.1 of issue #7458: an uncontended, concluding single-shard transaction with an
// empty tx-queue and no in-flight callback on the shard runs inline without ever registering an
// intent lock. If the old (always-Acquire) behavior were still in place, the key would appear
// locked (CheckLock would return false) from inside the callback itself.
TEST_F(TransactionTest, OptimisticFastPathSkipsLockTable) {
  ASSERT_EQ(0u, Shard("a", shard_set->size()));

  static CommandId cid{"tx_test_fastpath", 0, -1, 1, -1, acl::NONE};
  auto tx = MakeTx(&cid, {"a"});

  bool lock_free_during_cb = false;
  OnShard(1, [&] {
    tx->Execute(
        [&](Transaction* t, EngineShard* shard) {
          lock_free_during_cb =
              t->GetDbSlice(shard->shard_id()).CheckLock(IntentLock::EXCLUSIVE, 0, "a");
          return OpStatus::OK;
        },
        true);
  });

  EXPECT_TRUE(lock_free_during_cb)
      << "fast path should run inline without ever registering an intent lock";
}

// Verifies Tasks 2.2/2.3/2.4 of issue #7458: while a transaction (Z) is mid-callback (running_tx_
// set, simulating a preemption) and never registered a real lock for its own key (fast path), a
// new *conflicting* transaction (W) scheduling on the same shard must lazily register Z's lock on
// its behalf, see the conflict, and queue behind it (not run out of order). Once Z concludes, it
// must release the lazily-registered lock (no leak), and W must then run.
TEST_F(TransactionTest, LazyLockConflictingKey) {
  ASSERT_EQ(0u, Shard("a", shard_set->size()));

  static CommandId cid_z{"tx_test_lz_z", 0, -1, 1, -1, acl::NONE};
  static CommandId cid_w{"tx_test_lz_w", 0, -1, 1, -1, acl::NONE};

  auto tx_z = MakeTx(&cid_z, {"a"});
  auto tx_w = MakeTx(&cid_w, {"a"});  // same key -> conflicts with Z

  // Phase 1: park Z mid-callback on shard 0, holding running_tx_. Z is the very first scheduling
  // attempt on an empty, uncontended shard, so it takes the fast path and never registers a lock.
  auto z = ParkHoldingRunningTx(0, tx_z.get());
  ASSERT_TRUE(z->WaitParked()) << "Z's concluding hop never parked";

  ASSERT_TRUE(OnShard(0, [&] {
    return tx_z->GetDbSlice(0).CheckLock(IntentLock::EXCLUSIVE, 0, "a");
  })) << "Z should not have registered a lock while merely parked, absent a rival scheduler";

  // Phase 2: schedule W on the same key from another thread. Because running_tx_ == Z on shard 0,
  // W must lazily register Z's lock before checking its own -> sees a conflict -> queues instead
  // of racing Z's still in-flight callback.
  std::atomic_bool w_ran{false};
  fb2::Done w_done;
  auto fb_w = pp_->at(1)->LaunchFiber([&] {
    tx_w->Execute(
        [&](Transaction*, EngineShard*) {
          w_ran.store(true, memory_order_relaxed);
          return OpStatus::OK;
        },
        true);
    w_done.Notify();
  });

  // Wait until W's scheduling attempt has landed on shard 0 and queued.
  ASSERT_TRUE(AwaitOnShard(0, [&] { return tx_w->DEBUG_GetTxqPosInShard(0) != TxQueue::kEnd; }))
      << "W was never scheduled (queued) on shard 0";

  EXPECT_FALSE(w_ran.load(memory_order_relaxed))
      << "W ran out of order while Z was still mid-callback";
  EXPECT_FALSE(tx_w->DEBUG_GetLocalMask(0) & Transaction::OUT_OF_ORDER)
      << "W should not be marked OUT_OF_ORDER: it conflicts with Z's (lazily locked) key";

  // Z's lock must now be visible: W's scheduling lazily registered it on Z's behalf.
  ASSERT_FALSE(OnShard(0, [&] {
    return tx_z->GetDbSlice(0).CheckLock(IntentLock::EXCLUSIVE, 0, "a");
  })) << "Z's key lock was not lazily registered while it was mid-callback";

  // Phase 3: release Z. Its callback concludes, and because its lock was lazily registered by W,
  // it must release it (Task 2.4) instead of leaking it. This also drains the deferred poll so W
  // finally runs.
  z->ReleaseAndJoin();

  ASSERT_TRUE(w_done.WaitFor(5s)) << "W was never run after Z concluded";
  EXPECT_TRUE(w_ran.load(memory_order_relaxed));

  ASSERT_TRUE(OnShard(0, [&] {
    return tx_z->GetDbSlice(0).CheckLock(IntentLock::EXCLUSIVE, 0, "a");
  })) << "lock leaked: still registered after both Z and W concluded";

  fb_w.Join();
}

// Companion to LazyLockConflictingKey: a transaction (V) scheduling on a *disjoint* key while Z is
// mid-callback must still see Z's lazily-registered lock (so the table stays accurate), but since
// its own key doesn't overlap, it must be marked OUT_OF_ORDER rather than forced into strict
// queue order. It only actually executes once Z concludes and releases running_tx_, since at most
// one callback may run per shard at a time.
TEST_F(TransactionTest, LazyLockDisjointKeyMarkedOutOfOrder) {
  ASSERT_EQ(0u, Shard("a", shard_set->size()));
  ASSERT_EQ(0u, Shard("x", shard_set->size()));

  static CommandId cid_z{"tx_test_lz2_z", 0, -1, 1, -1, acl::NONE};
  static CommandId cid_v{"tx_test_lz2_v", 0, -1, 1, -1, acl::NONE};

  auto tx_z = MakeTx(&cid_z, {"a"});
  auto tx_v = MakeTx(&cid_v, {"x"});  // disjoint key, same shard

  auto z = ParkHoldingRunningTx(0, tx_z.get());
  ASSERT_TRUE(z->WaitParked()) << "Z's concluding hop never parked";

  fb2::Done v_done;
  auto fb_v = pp_->at(1)->LaunchFiber([&] {
    tx_v->Execute(Noop, true);
    v_done.Notify();
  });

  ASSERT_TRUE(AwaitOnShard(0, [&] { return tx_v->DEBUG_GetTxqPosInShard(0) != TxQueue::kEnd; }))
      << "V was never scheduled (queued) on shard 0";

  EXPECT_TRUE(tx_v->DEBUG_GetLocalMask(0) & Transaction::OUT_OF_ORDER)
      << "V should be marked OUT_OF_ORDER: its key is disjoint from Z's";

  z->ReleaseAndJoin();

  ASSERT_TRUE(v_done.WaitFor(5s)) << "V was never run after Z concluded";
  fb_v.Join();
}

namespace {

std::vector<int64_t> DeltaDiff(const std::vector<int64_t>& before,
                               const std::vector<int64_t>& after) {
  std::vector<int64_t> diffs = after;
  for (size_t i = 0; i < before.size(); ++i)
    diffs[i] -= before[i];
  return diffs;
}

void AllButOneDeltaIs(const std::vector<int64_t>& deltas, size_t pos, int64_t expected) {
  ASSERT_GT(deltas.size(), pos);
  for (size_t i = 0; i < deltas.size(); ++i)
    if (i == pos)
      ASSERT_EQ(deltas[i], expected);
    else
      ASSERT_EQ(deltas[i], 0);
}

}  // namespace

TEST_F(TransactionTest, DeltaChanges) {
  OnShard(0, [] {
    const auto shard = EngineShard::tlocal();
    const auto mr = shard->memory_resource();
    void* p = nullptr;
    const auto before = shard->cmd_type_mem_delta();
    WithMemTrack(OBJ_STRING, [&] { p = mr->allocate(1024); });
    AllButOneDeltaIs(DeltaDiff(before, shard->cmd_type_mem_delta()), OBJ_STRING, 1024);
    mr->deallocate(p, 1024);
  });
}

TEST_F(TransactionTest, DeltaNoObjTypeDiscarded) {
  OnShard(0, [] {
    const auto shard = EngineShard::tlocal();
    const auto mr = shard->memory_resource();
    void* p = nullptr;
    const auto before = shard->cmd_type_mem_delta();
    WithMemTrack(-1, [&] { p = mr->allocate(1024); });
    ASSERT_THAT(DeltaDiff(before, shard->cmd_type_mem_delta()), Each(0));
    mr->deallocate(p, 1024);
  });
}

TEST_F(TransactionTest, DeltaAllocAndFree) {
  OnShard(0, [] {
    const auto shard = EngineShard::tlocal();
    const auto mr = shard->memory_resource();
    void* p = mr->allocate(1024);
    const auto before = shard->cmd_type_mem_delta();
    WithMemTrack(OBJ_LIST, [&] { mr->deallocate(p, 1024); });
    AllButOneDeltaIs(DeltaDiff(before, shard->cmd_type_mem_delta()), OBJ_LIST, -1024);
  });
}

TEST_F(TransactionTest, DeltaNested) {
  OnShard(0, [] {
    const auto shard = EngineShard::tlocal();
    const auto mr = shard->memory_resource();
    std::vector<std::pair<void*, size_t>> tracked;
    auto track = [&](size_t s) { tracked.emplace_back(mr->allocate(s), s); };

    auto hash_cb = [&] {
      void* p = mr->allocate(256);
      track(128);
      mr->deallocate(p, 256);
    };

    auto list_cb = [&] {
      track(512);
      WithMemTrack(OBJ_HASH, hash_cb);
    };

    auto str_cb = [&] {
      track(1024);
      WithMemTrack(OBJ_LIST, list_cb);
    };

    const auto before = shard->cmd_type_mem_delta();
    WithMemTrack(OBJ_STRING, str_cb);
    auto deltas = DeltaDiff(before, shard->cmd_type_mem_delta());

    ASSERT_EQ(deltas[OBJ_STRING], 1024);
    ASSERT_EQ(deltas[OBJ_LIST], 512);
    ASSERT_EQ(deltas[OBJ_HASH], 128);

    deltas[OBJ_HASH] = 0;
    deltas[OBJ_LIST] = 0;
    deltas[OBJ_STRING] = 0;

    ASSERT_THAT(deltas, Each(0));

    for (const auto& [ptr, size] : tracked)
      mr->deallocate(ptr, size);
  });
}

TEST_F(TransactionTest, DeltaSuspendResume) {
  OnShard(0, [] {
    const auto shard = EngineShard::tlocal();
    const auto mr = shard->memory_resource();

    const auto before = shard->cmd_type_mem_delta();
    void* p = nullptr;
    void* q = nullptr;

    {
      CmdMemoryScope scope_for_string{OBJ_STRING};
      p = mr->allocate(1024);

      scope_for_string.Suspend();

      {
        CmdMemoryScope scope_for_list{OBJ_LIST};
        q = mr->allocate(128);
      }

      scope_for_string.Resume();
    }

    const auto diff = DeltaDiff(before, shard->cmd_type_mem_delta());
    ASSERT_EQ(diff[OBJ_LIST], 128);
    ASSERT_EQ(diff[OBJ_STRING], 1024);

    mr->deallocate(p, 1024);
    mr->deallocate(q, 128);
  });
}

}  // namespace dfly
