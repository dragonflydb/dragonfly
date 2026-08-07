// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/blocking_controller.h"

#include <gmock/gmock.h>

#include <atomic>

#include "base/logging.h"
#include "facade/facade_stats.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/namespaces.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "util/fibers/pool.h"

namespace dfly {

using namespace util;
using namespace std;
using namespace std::chrono;
using namespace testing;

constexpr size_t kNumThreads = 3;

class BlockingControllerTest : public Test {
 protected:
  BlockingControllerTest() : cid_("blpop", 0, -3, 1, -2, acl::NONE) {
  }
  void SetUp() override;
  void TearDown() override;

  static void SetUpTestSuite() {
    ServerState::Init(kNumThreads, kNumThreads, nullptr, nullptr);
    facade::tl_facade_stats = new facade::FacadeStats;
  }

  std::unique_ptr<ProactorPool> pp_;
  boost::intrusive_ptr<Transaction> trans_;
  CommandId cid_;
  StringVec str_vec_;
  CmdArgVec arg_vec_;
};

void BlockingControllerTest::SetUp() {
  pp_.reset(fb2::Pool::Epoll(kNumThreads));
  pp_->Run();
  pp_->AwaitBrief([](unsigned index, ProactorBase* p) {
    ServerState::Init(index, kNumThreads, nullptr, nullptr);
    if (facade::tl_facade_stats == nullptr) {
      facade::tl_facade_stats = new facade::FacadeStats;
    }
  });

  shard_set = new EngineShardSet(pp_.get());
  shard_set->Init(kNumThreads, nullptr);

  trans_.reset(new Transaction{&cid_});

  str_vec_.assign({"x", "z", "0"});
  for (auto& s : str_vec_) {
    arg_vec_.emplace_back(s);
  }

  trans_->InitByArgs(&namespaces->GetDefaultNamespace(), 0,
                     CmdArgList{arg_vec_.data(), arg_vec_.size()});
  CHECK_EQ(0u, Shard("x", shard_set->size()));
  CHECK_EQ(2u, Shard("z", shard_set->size()));

  const TestInfo* const test_info = UnitTest::GetInstance()->current_test_info();
  LOG(INFO) << "Starting " << test_info->name();
}

void BlockingControllerTest::TearDown() {
  shard_set->PreShutdown();
  shard_set->Shutdown();
  delete shard_set;

  pp_->Stop();
  pp_.reset();
}

TEST_F(BlockingControllerTest, Basic) {
  trans_->ScheduleSingleHop([&](Transaction* t, EngineShard* shard) {
    BlockingController bc(shard, &namespaces->GetDefaultNamespace());
    auto keys = t->GetShardArgs(shard->shard_id());
    bc.AddWatched(
        keys, [](auto...) { return KeyReadyResult::kReady; }, t);
    EXPECT_EQ(1, bc.NumWatched(0));

    bc.RemovedWatched(keys, t);
    EXPECT_EQ(0, bc.NumWatched(0));
    return OpStatus::OK;
  });
}

// Regression for https://github.com/dragonflydb/dragonfly/pull/7225:
// NotifyWatchQueue used to walk every queued waiter (O(N) per notify) when
// the key was absent. The fast path now short-circuits via FindReadOnly. We
// assert the per-waiter checker is never invoked.
TEST_F(BlockingControllerTest, NotifyWatchQueueFastPathOnAbsentKey) {
  constexpr size_t kWaiters = 64;
  const std::string_view key = str_vec_[0];  // "x", hashes to shard 0 (verified in SetUp)

  std::vector<boost::intrusive_ptr<Transaction>> txs;
  txs.reserve(kWaiters);
  for (size_t i = 0; i < kWaiters; ++i) {
    auto t = boost::intrusive_ptr<Transaction>(new Transaction{&cid_});
    t->InitByArgs(&namespaces->GetDefaultNamespace(), 0,
                  CmdArgList{arg_vec_.data(), arg_vec_.size()});
    txs.push_back(std::move(t));
  }

  size_t checker_calls = 0;

  shard_set->Await(0, [&] {
    EngineShard* shard = EngineShard::tlocal();
    BlockingController bc(shard, &namespaces->GetDefaultNamespace());

    auto checker = [&checker_calls](EngineShard*, const DbContext&, std::string_view) {
      ++checker_calls;
      return KeyReadyResult::kKeyNotFound;
    };

    for (auto& t : txs) {
      bc.AddWatched(t->GetShardArgs(shard->shard_id()), checker, t.get());
    }
    ASSERT_EQ(1u, bc.NumWatched(0));  // 1 watched key, kWaiters items in its queue

    bc.Awaken(0, key);
    bc.NotifyPending();
  });

  // With the enum-based fast path, the first item's checker is called once and returns
  // kKeyNotFound, aborting the scan without visiting the remaining kWaiters-1 items.
  EXPECT_EQ(1u, checker_calls) << "fast path did not short-circuit";
}

struct ThrottlingJournalConsumer final : journal::JournalConsumerInterface {
  fb2::Done throttle_entered;
  fb2::Done release_throttle;

  void ConsumeJournalChange(const journal::JournalChangeItem&) final {
  }

  void ThrottleIfNeeded() final {
    throttle_entered.Notify();
    release_throttle.Wait();
  }
};

// FindReadOnly can lazily expire the watched key while NotifyPending holds WatchQueue references.
// That expiry journals a deletion, which must not be allowed to block until the scan has finished:
// a blocked timeout cleanup could otherwise erase those references while the scan is suspended.
TEST_F(BlockingControllerTest, NotifyPendingDoesNotYieldDuringLazyExpiry) {
  constexpr ShardId kShard = 0;
  constexpr string_view kKey = "x";
  ASSERT_EQ(kShard, Shard(kKey, shard_set->size()));

  static CommandId cid{"lazy_expiry_notify", 0, -1, 1, -1, acl::NONE};
  auto args = std::make_shared<CmdArgVec>();
  args->emplace_back(kKey);
  auto tx = boost::intrusive_ptr<Transaction>(new Transaction{&cid});
  ASSERT_EQ(OpStatus::OK, tx->InitByArgs(&namespaces->GetDefaultNamespace(), 0,
                                         CmdArgList{args->data(), args->size()}));

  ThrottlingJournalConsumer consumer;
  std::atomic_bool checker_returned{false};
  fb2::Done notify_finished;
  BlockingController* bc = nullptr;
  uint32_t consumer_id = 0;

  shard_set->Await(kShard, [&] {
    auto* shard = EngineShard::tlocal();
    auto& ns = namespaces->GetDefaultNamespace();
    auto& db = ns.GetDbSlice(kShard);
    DbContext cntx{&ns, 0, GetCurrentTimeMs()};
    auto added = db.AddOrUpdate(cntx, kKey, PrimeValue{"payload"}, GetCurrentTimeMs() - 1);
    ASSERT_TRUE(added);
    added->post_updater.Run();

    journal::StartInThread();
    consumer_id = journal::RegisterConsumer(&consumer);
    bc = ns.GetOrAddBlockingController(shard);
    bc->AddWatched(
        tx->GetShardArgs(kShard),
        [&](EngineShard* owner, const DbContext& context, string_view watched_key) {
          auto result =
              context.GetDbSlice(owner->shard_id()).FindReadOnly(context, watched_key, OBJ_STRING);
          checker_returned.store(true, memory_order_relaxed);
          return result.ok() ? KeyReadyResult::kReady : KeyReadyResult::kKeyNotFound;
        },
        tx.get());
    bc->Awaken(0, kKey);
  });

  auto notifier = pp_->at(kShard)->LaunchFiber([&] {
    bc->NotifyPending();
    notify_finished.Notify();
  });

  const bool throttle_seen = consumer.throttle_entered.WaitFor(2s);
  const bool checker_finished_before_throttle =
      throttle_seen && checker_returned.load(memory_order_relaxed);
  consumer.release_throttle.Notify();

  EXPECT_TRUE(throttle_seen) << "lazy expiry did not issue a journal write";
  EXPECT_TRUE(notify_finished.WaitFor(2s));
  notifier.Join();

  shard_set->Await(kShard, [&] {
    bc->RemovedWatched(tx->GetShardArgs(kShard), tx.get());
    journal::UnregisterConsumer(consumer_id);
  });
  journal::Close();

  EXPECT_TRUE(checker_finished_before_throttle)
      << "NotifyPending yielded on a journal write while holding WatchQueue references";
}

TEST_F(BlockingControllerTest, Timeout) {
  time_point tp = steady_clock::now() + chrono::milliseconds(10);
  bool blocked;
  bool paused;

  facade::OpStatus status = trans_->WaitOnWatch(
      tp, Transaction::kShardArgs, [](auto...) { return KeyReadyResult::kReady; }, &blocked,
      &paused);

  EXPECT_EQ(status, facade::OpStatus::TIMED_OUT);
  unsigned num_watched = shard_set->Await(

      0, [&] {
        return namespaces->GetDefaultNamespace()
            .GetBlockingController(EngineShard::tlocal()->shard_id())
            ->NumWatched(0);
      });

  EXPECT_EQ(0, num_watched);
  trans_.reset();
}

}  // namespace dfly
