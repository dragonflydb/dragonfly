// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/blocking_controller.h"

#include <gmock/gmock.h>

#include "base/logging.h"
#include "facade/facade_stats.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
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

  trans_->InitByArgs(&namespaces->GetDefaultNamespace(), 0, {arg_vec_.data(), arg_vec_.size()});
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
        keys, [](auto...) { return true; }, t);
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
    t->InitByArgs(&namespaces->GetDefaultNamespace(), 0, {arg_vec_.data(), arg_vec_.size()});
    txs.push_back(std::move(t));
  }

  size_t checker_calls = 0;

  shard_set->Await(0, [&] {
    EngineShard* shard = EngineShard::tlocal();
    BlockingController bc(shard, &namespaces->GetDefaultNamespace());

    auto checker = [&checker_calls](EngineShard*, const DbContext&, std::string_view) {
      ++checker_calls;
      return false;
    };

    for (auto& t : txs) {
      bc.AddWatched(t->GetShardArgs(shard->shard_id()), checker, t.get());
    }
    ASSERT_EQ(1u, bc.NumWatched(0));  // 1 watched key, kWaiters items in its queue

    bc.Awaken(0, key);
    bc.NotifyPending();
  });

  EXPECT_EQ(0u, checker_calls) << "fast path did not short-circuit";
}

TEST_F(BlockingControllerTest, Timeout) {
  time_point tp = steady_clock::now() + chrono::milliseconds(10);
  bool blocked;
  bool paused;

  facade::OpStatus status = trans_->WaitOnWatch(
      tp, Transaction::kShardArgs, [](auto...) { return true; }, &blocked, &paused);

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
