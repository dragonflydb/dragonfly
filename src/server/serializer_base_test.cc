// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/serializer_base.h"

#include <absl/container/flat_hash_map.h>
#include <absl/random/distributions.h>
#include <absl/random/random.h>

#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "base/gtest.h"
#include "base/logging.h"
#include "io/io.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/execution_state.h"
#include "server/journal/journal.h"
#include "server/journal/serializer.h"
#include "server/journal/types.h"
#include "server/test_utils.h"
#include "server/transaction.h"
#include "util/fibers/fibers.h"

namespace dfly {

struct TestDriver : public SerializerBase, journal::JournalConsumerInterface {
  TestDriver(DbSlice* slice, ExecutionState* cntx, CommandRegistry* reg)
      : SerializerBase(slice, cntx), reg_{reg} {
  }

  unsigned SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                 bool on_update) override;

  void SerializeFetchedEntry(const TieredDelayedEntry& tde, const PrimeValue& pv) override {
    Serialize(tde.key.ToString());
  }

  void ConsumeJournalChange(const journal::JournalChangeItem& item) override;

  void ThrottleIfNeeded() override {
  }

  // TODO: possibly replace with unified loop if we decide on this?
  void Loop();

  void Serialize(std::string key) {
    EXPECT_FALSE(seen_journal_keys_.contains(key));  // No journal entries must exist for this key
    emitted_baselines_.emplace(std::move(key));
  }

  void Start() {
    SerializerBase::RegisterChangeListener();
    journal::StartInThread();
    journal_id_ = journal::RegisterConsumer(this);

    snapshot_fb_ = util::fb2::Fiber{[this] {
      Loop();
      journal::UnregisterConsumer(journal_id_);
      UnregisterChangeListener();
    }};
  }

  auto Finish() {
    snapshot_fb_.JoinIfNeeded();
    return std::make_pair(GetStats(), std::move(emitted_baselines_));
  }

  CommandRegistry* reg_;

  absl::InsecureBitGen bg_;

  util::fb2::Fiber snapshot_fb_;
  PrimeTable::Cursor snapshot_cursor_;
  uint32_t journal_id_;

  absl::flat_hash_set<std::string> emitted_baselines_;
  absl::flat_hash_set<std::string> seen_journal_keys_;
};

void TestDriver::Loop() {
  for (DbIndex snapshot_db_indx = 0; snapshot_db_indx < db_array_.size(); ++snapshot_db_indx) {
    if (!base_cntx_->IsRunning())
      return;

    if (!db_array_[snapshot_db_indx])
      continue;

    PrimeTable* pt = &db_array_[snapshot_db_indx]->prime;
    do {
      if (!base_cntx_->IsRunning()) {
        return;
      }

      snapshot_cursor_ = pt->TraverseBuckets(snapshot_cursor_, [this, snapshot_db_indx](auto it) {
        ProcessBucket(snapshot_db_indx, it, false);
      });

      util::ThisFiber::Yield();
    } while (snapshot_cursor_);

    {
      std::lock_guard guard(big_value_mu_);
      ProcessDelayedEntries(true, 0, base_cntx_);
    }

    util::ThisFiber::Yield();
  }  // for (dbindex)
}

void TestDriver::ConsumeJournalChange(const journal::JournalChangeItem& item) {
  io::BytesSource bytes{item.journal_item.data};
  JournalReader reader{&bytes, 0};

  // Check entry is parsable
  journal::ParsedEntry entry;
  auto ec = reader.ReadEntry(&entry);
  CHECK(!ec) << ec;

  // Check entry is a command trace
  if (entry.opcode != journal::Op::COMMAND)
    return;

  // Extract cid + original args
  auto cid = reg_->Find(entry.cmd.Front());
  std::vector<std::string_view> str_vec;
  for (auto v : entry.cmd.view())
    str_vec.push_back(v);
  str_vec.erase(str_vec.begin());

  // Check all keys were baseline emitted before
  auto keys = DetermineKeys(cid, str_vec);
  CHECK(keys);
  for (auto key : keys->Range(str_vec))
    seen_journal_keys_.emplace(key);
}

unsigned TestDriver::SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                           bool on_update) {
  unsigned serialized = 0;
  for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
    DCHECK_EQ(it.GetVersion(), snapshot_version_);

    Serialize(it->first.ToString());
    ++serialized;

    while (absl::Bernoulli(bg_, 0.3)) {
      for (unsigned it = absl::Uniform(bg_, 1, 10); it > 0; it--)
        util::ThisFiber::Yield();
    }
  }
  return serialized;
}

class SerializerBaseTest : public BaseFamilyTest {
 public:
  void SetUp() {
    num_threads_ = 1;
    BaseFamilyTest::SetUp();
  }

 protected:
  void Start() {
    pp_->at(0)->Await([this] { StartOnThread(); });
  }

  auto Finish() {
    std::decay_t<decltype(driver_->Finish())> res;
    pp_->at(0)->Await([this, &res] {
      res = driver_->Finish();
      driver_.reset();  // must be destroyed in this thread
    });

    return res;
  }

 private:
  void StartOnThread() {
    auto* reg = service_->mutable_registry();

    boost::intrusive_ptr<Transaction> tx = new Transaction{reg->Find("SAVE")};
    tx->InitByArgs(&namespaces->GetDefaultNamespace(), 0, {});

    tx->ScheduleSingleHop([this, reg](Transaction* t, EngineShard* es) {
      driver_.emplace(&t->GetDbSlice(es->shard_id()), &cntx_, reg);
      driver_->Start();
      return OpStatus::OK;
    });
  }

  ExecutionState cntx_;
  std::optional<TestDriver> driver_;
};

// Check that serialization of debug populate is successful
TEST_F(SerializerBaseTest, StaticDebugPopulate) {
  // Fill databse with keys
  const size_t kKeys = 10000;
  Run({"DEBUG", "POPULATE", std::to_string(kKeys)});

  // Start snapshot
  Start();

  // Issue appends at the same time
  std::atomic_bool running = true;
  auto worker = pp_->at(0)->LaunchFiber([&] {
    for (unsigned i = 0; running.load(std::memory_order_relaxed) && i < kKeys; i++) {
      Run("W1", {"APPEND", absl::StrCat("key:", i), "D"});
      util::ThisFiber::Yield();
    }
  });

  // Finish and join worker
  auto stats = Finish();
  running = false;
  worker.Join();

  // Expect serialized keys
  EXPECT_EQ(stats.first.keys_serialized, kKeys);
  for (unsigned i = 0; i < kKeys; i++)
    EXPECT_TRUE(stats.second.contains(absl::StrCat("key:", i)));
}

// Check that serialization of debug populate is successful
TEST_F(SerializerBaseTest, NewValues) {
  // Fill databse with keys
  const size_t kKeys = 5000;
  Run({"DEBUG", "POPULATE", std::to_string(kKeys), "key", "100", "TYPE", "LIST", "ELEMENTS", "11"});

  // Start snapshot
  Start();

  // Issue appends at the same time in range [0, 2 * kKeys] to have a balance of new keys
  std::atomic_bool running = true;
  std::vector<util::fb2::Fiber> workers;
  for (size_t w = 0; w < 3; w++) {
    auto worker = pp_->at(w % pp_->size())->LaunchFiber([&, w] {
      std::string id = absl::StrCat("w", w);
      absl::InsecureBitGen bg;

      while (running.load(std::memory_order_relaxed)) {
        size_t i = absl::Uniform(bg, 0u, 2 * kKeys);
        size_t j = absl::Uniform(bg, 0u, 1000u);

        Run(id, {"LPUSH", absl::StrCat("key:", i), absl::StrCat("v", j)});
        util::ThisFiber::Yield();
      }
    });
    workers.push_back(std::move(worker));
  }

  // Finish and join worker
  auto stats = Finish();
  running = false;
  for (auto& w : workers)
    w.Join();
}

}  // namespace dfly
