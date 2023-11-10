// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/blocking_controller.h"

#include <gmock/gmock.h>

#include "base/logging.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "util/fibers/pool.h"

namespace dfly {

using namespace util;
using namespace std;
using namespace std::chrono;
using namespace testing;

class BlockingControllerTest : public Test {
 protected:
  BlockingControllerTest() : cid_("blpop", 0, -3, 1, -2, acl::NONE) {
  }
  void SetUp() override;
  void TearDown() override;

  std::unique_ptr<ProactorPool> pp_;
  boost::intrusive_ptr<Transaction> trans_;
  CommandId cid_;
  StringVec str_vec_;
  CmdArgVec arg_vec_;
};

constexpr size_t kNumThreads = 3;

void BlockingControllerTest::SetUp() {
  pp_.reset(fb2::Pool::Epoll(kNumThreads));
  pp_->Run();
  pp_->Await([](unsigned index, ProactorBase* p) { ServerState::Init(index, nullptr); });
  ServerState::Init(kNumThreads, nullptr);

  shard_set = new EngineShardSet(pp_.get());
  shard_set->Init(kNumThreads, false);

  trans_.reset(new Transaction{&cid_});

  str_vec_.assign({"blpop", "x", "z", "0"});
  for (auto& s : str_vec_) {
    arg_vec_.emplace_back(s);
  }

  trans_->InitByArgs(0, {arg_vec_.data(), arg_vec_.size()});
  CHECK_EQ(0u, Shard("x", shard_set->size()));
  CHECK_EQ(2u, Shard("z", shard_set->size()));

  const TestInfo* const test_info = UnitTest::GetInstance()->current_test_info();
  LOG(INFO) << "Starting " << test_info->name();
}

void BlockingControllerTest::TearDown() {
  shard_set->Shutdown();
  delete shard_set;

  pp_->Stop();
  pp_.reset();
}

TEST_F(BlockingControllerTest, Basic) {
  shard_set->Await(0, [&] {
    EngineShard* shard = EngineShard::tlocal();
    BlockingController bc(shard);
    auto keys = trans_->GetShardArgs(shard->shard_id());
    bc.AddWatched(keys, trans_.get());
    EXPECT_EQ(1, bc.NumWatched(0));

    bc.FinalizeWatched(keys, trans_.get());
    EXPECT_EQ(0, bc.NumWatched(0));
  });
}

TEST_F(BlockingControllerTest, Timeout) {
  time_point tp = steady_clock::now() + chrono::milliseconds(10);

  trans_->Schedule();
  auto cb = [&](Transaction* t, EngineShard* shard) { return trans_->GetShardArgs(0); };

  bool res = trans_->WaitOnWatch(tp, cb);

  EXPECT_FALSE(res);
  unsigned num_watched = shard_set->Await(
      0, [&] { return EngineShard::tlocal()->blocking_controller()->NumWatched(0); });

  EXPECT_EQ(0, num_watched);
  trans_.reset();
}

}  // namespace dfly
