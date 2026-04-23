// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/sharded_hash_map.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

using namespace std;

// Transparent hash for string-like types. absl::Hash<string> is not transparent (its
// operator() only accepts const string&), so heterogeneous lookup requires a custom hash
// that declares is_transparent and accepts string_view (which string and const char* both
// convert to). absl guarantees that hashing equal string contents produces the same value
// regardless of the concrete string type, so this is consistent with the stored keys.
struct TransparentStringHash {
  using is_transparent = void;
  size_t operator()(std::string_view sv) const {
    return absl::Hash<std::string_view>{}(sv);
  }
};

class ShardedHashMapTest : public testing::Test {
 protected:
  ShardedHashMap<string, int> map_;
};

TEST_F(ShardedHashMapTest, EmptyMap) {
  EXPECT_EQ(map_.SizeApproximate(), 0u);

  bool found = map_.FindIf(string("missing"), [](const int&) {});
  EXPECT_FALSE(found);
}

TEST_F(ShardedHashMapTest, MutateInsertAndFind) {
  map_.Mutate(string("key1"), [](const auto& m, auto lock_readers) {
    auto lm = lock_readers();
    lm.map["key1"] = 42;
  });

  EXPECT_EQ(map_.SizeApproximate(), 1u);

  bool found = map_.FindIf(string("key1"), [](const int& v) { EXPECT_EQ(v, 42); });
  EXPECT_TRUE(found);
}

TEST_F(ShardedHashMapTest, MutateOverwrite) {
  map_.Mutate(string("key1"), [](const auto& m, auto lock_readers) {
    auto lm = lock_readers();
    lm.map["key1"] = 10;
  });

  map_.Mutate(string("key1"), [](const auto& m, auto lock_readers) {
    auto lm = lock_readers();
    lm.map["key1"] = 20;
  });

  EXPECT_TRUE(map_.FindIf(string("key1"), [](const int& v) { EXPECT_EQ(v, 20); }));
  EXPECT_EQ(map_.SizeApproximate(), 1u);
}

TEST_F(ShardedHashMapTest, MutateErase) {
  map_.Mutate(string("key1"), [](const auto& m, auto lock_readers) {
    auto lm = lock_readers();
    lm.map["key1"] = 1;
  });
  EXPECT_EQ(map_.SizeApproximate(), 1u);

  map_.Mutate(string("key1"), [](const auto& m, auto lock_readers) {
    auto lm = lock_readers();
    lm.map.erase("key1");
  });
  EXPECT_EQ(map_.SizeApproximate(), 0u);

  EXPECT_FALSE(map_.FindIf(string("key1"), [](const int&) {}));
}

TEST_F(ShardedHashMapTest, FindIfReturnsFalseForMissing) {
  map_.Mutate(string("a"), [](const auto& m, auto lock_readers) {
    auto lm = lock_readers();
    lm.map["a"] = 1;
  });

  EXPECT_FALSE(map_.FindIf(string("b"), [](const int&) {}));
}

TEST_F(ShardedHashMapTest, MultipleKeys) {
  for (int i = 0; i < 100; ++i) {
    string key = "key" + to_string(i);
    map_.Mutate(key, [&key, i](const auto& m, auto lock_readers) {
      auto lm = lock_readers();
      lm.map[key] = i;
    });
  }

  EXPECT_EQ(map_.SizeApproximate(), 100u);

  for (int i = 0; i < 100; ++i) {
    string key = "key" + to_string(i);
    bool found = map_.FindIf(key, [i](const int& v) { EXPECT_EQ(v, i); });
    EXPECT_TRUE(found);
  }
}

TEST_F(ShardedHashMapTest, HeterogeneousLookup) {
  // Use transparent Eq so that string_view / C-string queries compile and match correctly.
  ShardedHashMap<string, int, 32, TransparentStringHash, std::equal_to<>> hmap;

  hmap.Mutate(string("hello"), [](const auto& m, auto lr) {
    auto lm = lr();
    lm.map["hello"] = 7;
  });

  string_view sv = "hello";
  bool found = hmap.FindIf(sv, [](const int& v) { EXPECT_EQ(v, 7); });
  EXPECT_TRUE(found);

  const char* cstr = "hello";
  found = hmap.FindIf(cstr, [](const int& v) { EXPECT_EQ(v, 7); });
  EXPECT_TRUE(found);

  EXPECT_FALSE(hmap.FindIf(string_view{"missing"}, [](const int&) {}));
}

TEST_F(ShardedHashMapTest, ShardOf) {
  // ShardOf should be deterministic and within range.
  string key = "test_key";
  size_t shard = map_.ShardOf(key);
  EXPECT_LT(shard, map_.kNumShards);
  // Same key always maps to same shard.
  EXPECT_EQ(shard, map_.ShardOf(key));
}

TEST_F(ShardedHashMapTest, MutateByShard) {
  string key = "key1";
  size_t sid = map_.ShardOf(key);

  map_.Mutate(sid, [&key](const auto& m, auto lock_readers) {
    auto lm = lock_readers();
    lm.map[key] = 99;
  });

  bool found = map_.FindIf(key, [](const int& v) { EXPECT_EQ(v, 99); });
  EXPECT_TRUE(found);
}

TEST_F(ShardedHashMapTest, ForEachShared) {
  map_.Mutate(string("a"), [](const auto& m, auto lr) {
    auto lm = lr();
    lm.map["a"] = 1;
  });
  map_.Mutate(string("b"), [](const auto& m, auto lr) {
    auto lm = lr();
    lm.map["b"] = 2;
  });

  int sum = 0;
  map_.ForEachShared([&sum](const string&, const int& v) { sum += v; });
  EXPECT_EQ(sum, 3);
}

TEST_F(ShardedHashMapTest, ForEachExclusive) {
  map_.Mutate(string("x"), [](const auto& m, auto lr) {
    auto lm = lr();
    lm.map["x"] = 10;
  });
  map_.Mutate(string("y"), [](const auto& m, auto lr) {
    auto lm = lr();
    lm.map["y"] = 20;
  });

  // Double all values via exclusive iteration.
  map_.ForEachExclusive([](const string&, int& v) { v *= 2; });

  EXPECT_TRUE(map_.FindIf(string("x"), [](const int& v) { EXPECT_EQ(v, 20); }));
  EXPECT_TRUE(map_.FindIf(string("y"), [](const int& v) { EXPECT_EQ(v, 40); }));
}

TEST_F(ShardedHashMapTest, WithReadExclusiveLockByKey) {
  map_.Mutate(string("k"), [](const auto& m, auto lr) {
    auto lm = lr();
    lm.map["k"] = 5;
  });

  bool executed = false;
  map_.WithReadExclusiveLock(string("k"), [&executed]() { executed = true; });
  EXPECT_TRUE(executed);
}

TEST_F(ShardedHashMapTest, WithReadExclusiveLockByShard) {
  bool executed = false;
  map_.WithReadExclusiveLock(size_t{0}, [&executed]() { executed = true; });
  EXPECT_TRUE(executed);
}

TEST_F(ShardedHashMapTest, ConcurrentReadersAndWriter) {
  // Insert initial data.
  for (int i = 0; i < 50; ++i) {
    string key = "key" + to_string(i);
    map_.Mutate(key, [&key, i](const auto& m, auto lr) {
      auto lm = lr();
      lm.map[key] = i;
    });
  }

  constexpr int kReaders = 4;
  constexpr int kReadsPerFiber = 200;

  util::fb2::Barrier barrier(kReaders + 1);  // +1 for writer fiber
  vector<util::fb2::Fiber> fibers;

  // Launch reader fibers.
  for (int r = 0; r < kReaders; ++r) {
    fibers.emplace_back("reader", [&] {
      barrier.Wait();
      for (int j = 0; j < kReadsPerFiber; ++j) {
        string key = "key" + to_string(j % 50);
        map_.FindIf(key, [](const int&) {});
      }
    });
  }

  // Launch writer fiber.
  fibers.emplace_back("writer", [&] {
    barrier.Wait();
    for (int i = 50; i < 100; ++i) {
      string key = "key" + to_string(i);
      map_.Mutate(key, [&key, i](const auto& m, auto lr) {
        auto lm = lr();
        lm.map[key] = i;
      });
    }
  });

  for (auto& fb : fibers) {
    fb.Join();
  }

  EXPECT_EQ(map_.SizeApproximate(), 100u);
}

TEST_F(ShardedHashMapTest, ConcurrentWriters) {
  constexpr int kWriters = 4;
  constexpr int kKeysPerWriter = 50;

  vector<util::fb2::Fiber> fibers;
  util::fb2::Barrier barrier(kWriters);

  for (int w = 0; w < kWriters; ++w) {
    fibers.emplace_back("writer", [&, w] {
      barrier.Wait();
      for (int i = 0; i < kKeysPerWriter; ++i) {
        // Each writer writes to its own key space to avoid contention on values.
        string key = "w" + to_string(w) + "_k" + to_string(i);
        map_.Mutate(key, [&key, val = w * 1000 + i](const auto& m, auto lr) {
          auto lm = lr();
          lm.map[key] = val;
        });
      }
    });
  }

  for (auto& fb : fibers) {
    fb.Join();
  }

  EXPECT_EQ(map_.SizeApproximate(), kWriters * kKeysPerWriter);

  // Verify all values.
  for (int w = 0; w < kWriters; ++w) {
    for (int i = 0; i < kKeysPerWriter; ++i) {
      string key = "w" + to_string(w) + "_k" + to_string(i);
      int expected = w * 1000 + i;
      bool found = map_.FindIf(key, [expected](const int& v) { EXPECT_EQ(v, expected); });
      EXPECT_TRUE(found) << "missing key: " << key;
    }
  }
}

}  // namespace dfly
