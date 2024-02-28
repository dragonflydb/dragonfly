// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include "base/gtest.h"
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

struct TestStorage : public Storage {
  string Read(string_view key) override {
    util::ThisFiber::Yield();
    reads++;
    return values[key];
  }

  void Write(string_view key, string value) override {
    util::ThisFiber::Yield();
    writes++;
    values[key] = value;
  }

  void Delete(std::string_view key) override {
    util::ThisFiber::Yield();
    deletes++;
    values.erase(key);
  }

  size_t reads = 0;
  size_t writes = 0;
  size_t deletes = 0;
  absl::flat_hash_map<string, string> values;
};

struct OpQueueTest : public PoolTestBase {};

struct OpManagerTest : public PoolTestBase {};

TEST_F(OpQueueTest, SimpleReads) {
  TestStorage storage;
  OpQueue queue{&storage};

  storage.values["a"] = "a-value";
  storage.values["b"] = "b-value";
  storage.values["c"] = "c-value";

  for (int i = 0; i < 10; i++) {
    queue.Enqueue("a", Op{Op::GET, "", [](string_view value) { EXPECT_EQ(value, "a-value"); }});
    queue.Enqueue("b", Op{Op::GET, "", [](string_view value) { EXPECT_EQ(value, "b-value"); }});
    queue.Enqueue("c", Op{Op::GET, "", [](string_view value) { EXPECT_EQ(value, "c-value"); }});
  }

  pp_->at(0)->Await([&queue] {
    queue.Start();
    queue.TEST_Drain();
  });

  EXPECT_EQ(storage.reads, 3u);
  EXPECT_EQ(storage.writes + storage.deletes, 0u);
}

TEST_F(OpQueueTest, MixedReadWrite) {
  TestStorage storage;
  OpQueue queue{&storage};

  storage.values["key"] = "1";
  for (int i = 1; i < 10; i++) {
    queue.Enqueue("key", Op{Op::GET, "", [i](auto value) { EXPECT_EQ(value, to_string(i)); }});
    queue.Enqueue("key", Op{Op::SET, to_string(i + 1)});
  }

  pp_->at(0)->Await([&queue] {
    queue.Start();
    queue.TEST_Drain();
  });

  EXPECT_EQ(storage.reads, 1);
  EXPECT_EQ(storage.writes, 1);
}

TEST_F(OpQueueTest, SimpleDels) {
  TestStorage storage;
  OpQueue queue{&storage};

  storage.values["key"] = "old-value";

  queue.Enqueue("key", Op{Op::GET, "", [](string_view value) { EXPECT_EQ(value, "old-value"); }});
  queue.Enqueue("key", Op{Op::SET, "value"});
  queue.Enqueue("key", Op{Op::DEL});

  pp_->at(0)->Await([&queue] {
    queue.Start();
    queue.TEST_Drain();
  });

  EXPECT_EQ(storage.reads, 1u);
  EXPECT_EQ(storage.writes, 0u);  // SET was ignored because DEL followed
  EXPECT_EQ(storage.deletes, 1u);
}

TEST_F(OpManagerTest, SimpleReads) {
  TestStorage storage;
  OpManager manager{&storage, 5};

  std::vector<Future<std::string>> futures;
  for (size_t i = 0; i < 10; i++) {
    string key = to_string(i);
    storage.values[key] = to_string(i + 1);
    futures.emplace_back(manager.Read(key));
    futures.emplace_back(manager.Read(key));  // no-op read
  }

  pp_->at(0)->Await([&manager, &futures] {
    manager.Start();
    for (size_t i = 0; i < futures.size(); i++)
      EXPECT_EQ(futures[i].Get(), to_string(i / 2 + 1));
    manager.Stop();
  });

  EXPECT_EQ(storage.reads, 10);
}

TEST_F(OpManagerTest, MixedReadWrite) {
  TestStorage storage;
  OpManager manager{&storage, 1};

  pp_->at(0)->Await([&manager, &storage] {
    manager.Start();

    for (size_t i = 0; i < 10; i++) {
      string key = to_string(i);
      string value = "v1" + to_string(i);
      manager.Write(key, value);
      EXPECT_EQ(manager.Read(key).Get(), value);
    }

    EXPECT_EQ(storage.reads, 0u);

    manager.Stop();
  });
}

}  // namespace dfly::tiering
