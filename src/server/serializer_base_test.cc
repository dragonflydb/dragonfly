// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/serializer_base.h"

#include <absl/container/flat_hash_map.h>
#include <absl/random/distributions.h>
#include <absl/random/random.h>
#include <gtest/gtest.h>

#include <atomic>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <chrono>
#include <queue>

#include "base/logging.h"
#include "facade/facade_test.h"
#include "facade/resp_expr.h"
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
#include "server/table.h"
#include "server/test_utils.h"
#include "server/transaction.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

using namespace std::chrono_literals;

namespace dfly {

// Driver for "artificially" resolving delayed entries with some delay
// driven by a fiber in the background
struct TestDelayDriver {
  using Fut = util::fb2::Future<io::Result<std::string>>;
  using OrdEntry = std::pair<std::chrono::steady_clock::time_point, Fut>;

  struct Comp {
    bool operator()(const OrdEntry& e1, const OrdEntry& e2) const {
      return e1.first > e2.first;
    }
  };

  Fut Enqeue(unsigned delay_us) {
    auto tp = std::chrono::steady_clock::now() + std::chrono::microseconds(delay_us);
    Fut future{};
    q_.emplace(tp, future);
    var_.notify_one();
    return future;
  }

  void Loop() {
    while (!done) {
      util::fb2::NoOpLock lock;
      var_.wait(lock, [this]() { return done || (!paused && !q_.empty()); });

      while (!paused && !done && !q_.empty()) {
        auto entry = q_.top();
        q_.pop();

        util::ThisFiber::SleepUntil(entry.first);
        entry.second.Resolve(std::string{});
      }
    }
  }

  void Pause() {
    paused = true;
    var_.notify_all();
  }

  void Resume() {
    paused = false;
    var_.notify_all();
  }

  void Start() {
    resolver_fb_ = {std::bind_front(&TestDelayDriver::Loop, this)};
  }

  void Stop() {
    done = true;
    var_.notify_all();
    resolver_fb_.JoinIfNeeded();
  }

  bool done = false;
  bool paused = false;
  util::fb2::CondVarAny var_;

  std::priority_queue<OrdEntry, std::vector<OrdEntry>, Comp> q_;
  util::fb2::Fiber resolver_fb_;
};

struct TestDriver : public SerializerBase, journal::JournalConsumerInterface {
  struct Params {
    float delay_prob = 0.0;
    std::pair<unsigned, unsigned> delay_lat_us = {0, 100};
  };

  TestDriver(Params params, DbSlice* slice, ExecutionState* cntx, CommandRegistry* reg)
      : SerializerBase(slice, cntx), params_{params}, reg_{reg} {
  }

  unsigned SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                 bool on_update) override;

  void SerializeFetchedEntry(const TieredDelayedEntry& tde, const PrimeValue& pv) override {
    RecordSerialized(tde.key.ToString());
  }

  void ConsumeJournalChange(const journal::JournalChangeItem& item) override;

  void ThrottleIfNeeded() override {
  }

  // TODO: possibly replace with unified loop if we decide on this?
  void Loop();

  void Serialize(BucketIdentity bucket, std::string key) {
    if (absl::Bernoulli(bg_, params_.delay_prob)) {
      DelayedEntryHandler::deps_.Increment(bucket);
      unsigned delay = absl::Uniform(bg_, params_.delay_lat_us.first, params_.delay_lat_us.second);
      auto de = std::make_unique<TieredDelayedEntry>(0, CompactKey{key},
                                                     delay_driver_.Enqeue(delay), 0, 0);
      DelayedEntryHandler::delayed_entries_.emplace(bucket, std::move(de));
    } else {
      RecordSerialized(std::move(key));
    }
  }

  void RecordSerialized(std::string key) {
    CHECK(!emitted_baselines_.contains(key));
    CHECK(!journal_writes_.contains(key));  // No journal entries must exist for this key
    emitted_baselines_.emplace(std::move(key));
  }

  void Start() {
    SerializerBase::RegisterChangeListener();
    journal::StartInThread();
    journal_id_ = journal::RegisterConsumer(this);

    snapshot_fb_ = util::fb2::Fiber{[this] {
      Loop();
      UnregisterChangeListener();
    }};

    delay_driver_.Start();
  }

  void Wait() {
    snapshot_fb_.JoinIfNeeded();
  }

  auto Finish() {
    Wait();
    delay_driver_.Stop();
    journal::UnregisterConsumer(journal_id_);
    return std::tuple(GetStats(), std::move(emitted_baselines_), std::move(journal_writes_));
  }

  Params params_;
  CommandRegistry* reg_;

  absl::InsecureBitGen bg_;

  util::fb2::Fiber snapshot_fb_;
  PrimeTable::Cursor snapshot_cursor_;
  uint32_t journal_id_;

  // subdriver for delayed entries
  TestDelayDriver delay_driver_;

  absl::flat_hash_set<std::string> emitted_baselines_;
  absl::flat_hash_map<std::string, unsigned> journal_writes_;
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
      std::lock_guard guard(stream_mu_);
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
    journal_writes_[key]++;
}

unsigned TestDriver::SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                           bool on_update) {
  unsigned serialized = 0;
  for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
    DCHECK_EQ(it.GetVersion(), snapshot_version_);

    std::lock_guard lk{stream_mu_};
    Serialize(it.bucket_address(), it->first.ToString());
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

  void Change(auto cb) {
    pp_->at(0)->Await([this, cb] { cb(*driver_); });
  }

  TestDriver::Params driver_params;

 private:
  void StartOnThread() {
    auto* reg = service_->mutable_registry();

    boost::intrusive_ptr<Transaction> tx = new Transaction{reg->Find("SAVE")};
    tx->InitByArgs(&namespaces->GetDefaultNamespace(), 0, {});

    tx->ScheduleSingleHop([this, reg](Transaction* t, EngineShard* es) {
      driver_.emplace(driver_params, &t->GetDbSlice(es->shard_id()), &cntx_, reg);
      driver_->Start();
      return OpStatus::OK;
    });
  }

  ExecutionState cntx_;
  std::optional<TestDriver> driver_;
};

// Check that basic serialization of debug populate is successful and fullfils all driver asserts
TEST_F(SerializerBaseTest, StaticDebugPopulate) {
  const size_t kKeys = 10000;
  Run({"DEBUG", "POPULATE", std::to_string(kKeys)});
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
  auto [stats, baselines, _] = Finish();
  running = false;
  worker.Join();

  // Expect serialized keys
  EXPECT_EQ(stats.keys_serialized, kKeys);
  for (unsigned i = 0; i < kKeys; i++)
    EXPECT_TRUE(baselines.contains(absl::StrCat("key:", i)));
}

// Check serialization of lists is successful with parallel additions to list.
// Each operation (including creation) adds one item to the list
// and each operation causes either serialization or a journal write.
// So at the end the number of writes (baseline and journal) must be equal to the list length
// TODO: Add multiple drivers
// TODO: Will be wrong with journal omits
TEST_F(SerializerBaseTest, IncreasingLists) {
  const size_t kKeys = 5000;
  Run({"DEBUG", "POPULATE", std::to_string(kKeys), "key", "100", "TYPE", "LIST", "ELEMENTS", "1"});
  Start();

  // Issue single value appends at the same time
  // Select keys in range [0, 2 * kKeys] to have a balance of new and existing keys
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

  // Wait for main loop
  Change([](TestDriver& d) { d.Wait(); });

  // Stop all writers and finish
  running = false;
  for (auto& w : workers)
    w.Join();
  auto [stats, baselines, journal_writes] = Finish();

  // Verify invariants (see comment at function top)
  unsigned seen = 0;
  for (size_t i = 0; i < kKeys * 2; i++) {
    auto key = absl::StrCat("key:", i);
    if (Run({"exists", key}).GetInt() == 0)
      continue;

    seen++;
    unsigned len = Run({"LLEN", key}).GetInt().value_or(0);
    unsigned base_written = baselines.contains(key);
    unsigned journal_written = journal_writes.contains(key) ? journal_writes.at(key) : 0u;

    EXPECT_EQ(len, base_written + journal_written);
  }

  EXPECT_THAT(Run({"dbsize"}), IntArg(seen));
}

// During delayed read of a tiered value, it can be come expired.
// Mass expity of items can cause previously occupied buckets to become empty.
// Serialization code has many paths that omit empty bucket checks at all -
// assert those "lost" delayed reads are correctly flushed before new changes
TEST_F(SerializerBaseTest, DelayedAllDeleted) {
  GTEST_SKIP() << "To be fixed";

  // 1-2 ms
  driver_params = {.delay_prob = 0.9, .delay_lat_us = {1000, 2000}};

  // Fill database with some keys
  const size_t kKeys = 10000;
  Run({"DEBUG", "POPULATE", std::to_string(kKeys)});

  // Set short expiry (10ms)
  for (unsigned i = 0; i < kKeys; i++)
    Run({"PEXPIRE", absl::StrCat("key:", i), "10"});

  // Start and pause reolution of delayed entries
  Start();
  Change([](TestDriver& d) { d.delay_driver_.Pause(); });

  // Let all values to be expire deleted
  TEST_current_time_ms = TEST_current_time_ms + 100;
  for (unsigned i = 0; i < kKeys; i++)
    EXPECT_THAT(Run({"GET", absl::StrCat("key:", i)}), ArgType(RespExpr::NIL));

  // Reallow delayed entry resolution
  Change([](TestDriver& d) { d.delay_driver_.Resume(); });

  // Trigger changes with dels
  for (unsigned i = 0; i < kKeys; i++)
    Run({"SET", absl::StrCat("key:", i), "V"});

  Finish();
}
}  // namespace dfly
