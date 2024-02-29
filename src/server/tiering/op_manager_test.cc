// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/pool.h"

namespace dfly::tiering {

using namespace std;
using namespace std::string_literals;

class PoolTestBase : public testing::Test {
 protected:
  static void SetUpTestSuite();
  static void TearDownTestSuite();

  static unique_ptr<util::ProactorPool> pp_;
};

unique_ptr<util::ProactorPool> PoolTestBase::pp_ = nullptr;

void PoolTestBase::SetUpTestSuite() {
  pp_.reset(util::fb2::Pool::Epoll(2));
  pp_->Run();
}

void PoolTestBase::TearDownTestSuite() {
  pp_->Stop();
  pp_.reset();
}

struct TestStorage : public Storage, public KeyStorage {
  string Read(BlobLocator locator) override {
    util::ThisFiber::Yield();
    reads++;
    return values[locator.offset];
  }

  BlobLocator Store(string_view value) override {
    util::ThisFiber::Yield();
    size_t offset = ++offset_counter;
    values[offset] = value;
    writes++;
    return {offset, value.size()};
  }

  void Delete(BlobLocator locator) {
    deletes++;
    values.erase(locator.offset);
  }

  void ReportStored(std::string_view key, BlobLocator locator) override {
    reports++;
    offsets[key] = locator.offset;
  }

  bool ReportFetched(std::string_view key, std::string_view value) override {
    fetches++;
    return false;
  }

  BlobLocator TEST_Insert(string key, string value) {
    size_t offset = ++offset_counter;
    values[offset] = value;
    offsets[key] = offset;
    return {offset, value.size()};
  }

  size_t reads = 0;
  size_t writes = 0;
  size_t deletes = 0;
  size_t fetches = 0;
  size_t reports = 0;
  size_t offset_counter = 0;

  absl::flat_hash_map<size_t, string> values;
  absl::flat_hash_map<string, size_t> offsets;
};

struct OpQueueTest : public PoolTestBase {};

struct OpManagerTest : public PoolTestBase {};

TEST_F(OpQueueTest, SimpleReads) {
  TestStorage storage;
  OpQueue queue{&storage, &storage};

  auto loc_a = storage.TEST_Insert("a", "a-value");
  auto loc_b = storage.TEST_Insert("a", "b-value");
  auto loc_c = storage.TEST_Insert("a", "c-value");

  for (int i = 0; i < 10; i++) {
    queue.Enqueue("a", Op{Op::READ, loc_a, [](string_view value) { EXPECT_EQ(value, "a-value"); }});
    queue.Enqueue("b", Op{Op::READ, loc_b, [](string_view value) { EXPECT_EQ(value, "b-value"); }});
    queue.Enqueue("c", Op{Op::READ, loc_c, [](string_view value) { EXPECT_EQ(value, "c-value"); }});
  }

  pp_->at(0)->Await([&queue] {
    queue.Start();
    queue.TEST_Drain();
  });

  EXPECT_EQ(storage.reads, 3u);
  EXPECT_EQ(storage.fetches, 3u);
  EXPECT_EQ(storage.writes + storage.deletes, 0u);
}

TEST_F(OpQueueTest, ReadDelStore) {
  TestStorage storage;
  OpQueue queue{&storage, &storage};

  auto loc = storage.TEST_Insert("key", "value-1");
  queue.Enqueue("key", Op{Op::READ, loc, [](auto value) { EXPECT_EQ(value, "value-1"); }});
  queue.Enqueue("key", Op{Op::DEL, loc});
  queue.Enqueue("key", Op{Op::STORE, "value-2"});

  pp_->at(0)->Await([&storage, &queue] {
    queue.Start();
    queue.TEST_Drain();
  });

  EXPECT_EQ(storage.reads, 1);
  EXPECT_EQ(storage.deletes, 1);
  EXPECT_EQ(storage.writes, 1);
  EXPECT_EQ(storage.fetches, 0);
  EXPECT_EQ(storage.reports, 1);
}

TEST_F(OpQueueTest, TestDismiss) {
  TestStorage storage;
  OpQueue queue{&storage, &storage};

  queue.Enqueue("key", Op{Op::STORE, "value"});
  queue.DismissStore("key");

  pp_->at(0)->Await([&storage, &queue] {
    queue.Start();
    queue.TEST_Drain();
  });

  EXPECT_EQ(storage.writes, 0);
  EXPECT_EQ(storage.reports, 0);
}

TEST_F(OpManagerTest, SimpleReads) {
  TestStorage storage;
  OpManager manager{&storage, &storage, 5};

  std::vector<Future<std::string>> futures;
  for (size_t i = 0; i < 10; i++) {
    string key = to_string(i);
    auto loc = storage.TEST_Insert(key, to_string(i));
    futures.emplace_back(manager.Read(key, loc));
    futures.emplace_back(manager.Read(key, loc));  // no-op read
  }

  pp_->at(0)->Await([&manager, &futures] {
    manager.Start();
    for (size_t i = 0; i < futures.size(); i++)
      EXPECT_EQ(futures[i].Get(), to_string(i / 2));
    manager.Stop();
  });

  EXPECT_EQ(storage.reads, 10);
  EXPECT_EQ(storage.fetches, 10);
}

}  // namespace dfly::tiering
