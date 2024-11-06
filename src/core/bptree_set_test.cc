// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/bptree_set.h"

#include <absl/container/btree_set.h>
#include <gmock/gmock.h>
#include <mimalloc.h>

#include <random>

extern "C" {
#include "redis/zmalloc.h"
#include "redis/zset.h"
}

#include "base/gtest.h"
#include "base/init.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

using namespace std;

namespace dfly {

namespace {

template <typename Node, typename Policy>
bool ValidateNode(const Node* node, typename Node::KeyT ubound) {
  typename Policy::KeyCompareTo cmp;

  for (unsigned i = 1; i < node->NumItems(); ++i) {
    if (cmp(node->Key(i - 1), node->Key(i)) > -1)
      return false;
  }

  if (!node->IsLeaf()) {
    unsigned mask = 0;
    uint32_t subtree_cnt = node->NumItems();
    for (unsigned i = 0; i <= node->NumItems(); ++i) {
      mask |= (1 << node->Child(i)->IsLeaf());
      DCHECK_EQ(node->Child(i)->DEBUG_TreeCount(), node->Child(i)->TreeCount());
      subtree_cnt += node->Child(i)->TreeCount();
    }
    if (mask == 3)
      return false;

    if (subtree_cnt != node->TreeCount()) {
      LOG(ERROR) << "Expected " << subtree_cnt << " got " << node->TreeCount();
      return false;
    }
  }

  return cmp(node->Key(node->NumItems() - 1), ubound) == -1;
}

struct ZsetPolicy {
  struct KeyT {
    double d;
    sds s;
  };

  struct KeyCompareTo {
    int operator()(const KeyT& left, const KeyT& right) {
      if (left.d < right.d)
        return -1;
      if (left.d > right.d)
        return 1;

      // Note that sdscmp can return values outside of [-1, 1] range.
      return sdscmp(left.s, right.s);
    }
  };
};

using SDSTree = BPTree<ZsetPolicy::KeyT, ZsetPolicy>;

}  // namespace

class BPTreeSetTest : public ::testing::Test {
  using Node = detail::BPTreeNode<uint64_t>;

 protected:
  static constexpr size_t kNumElems = 7000;

  BPTreeSetTest() : mi_alloc_(mi_heap_get_backing()), bptree_(&mi_alloc_) {
  }
  static void SetUpTestSuite() {
  }

  void FillTree(unsigned factor = 1) {
    for (unsigned i = 0; i < kNumElems; ++i) {
      bptree_.Insert(i * factor);
    }
  }

  bool Validate();

  MiMemoryResource mi_alloc_;
  BPTree<uint64_t> bptree_;
  mt19937 generator_{1};
};

bool BPTreeSetTest::Validate() {
  auto* root = bptree_.DEBUG_root();
  if (!root)
    return true;

  // node, upper bound
  vector<pair<const Node*, uint64_t>> stack;

  stack.emplace_back(root, UINT64_MAX);

  while (!stack.empty()) {
    const Node* node = stack.back().first;
    uint64_t ubound = stack.back().second;
    stack.pop_back();

    if (!ValidateNode<Node, BPTreePolicy<uint64_t>>(node, ubound))
      return false;

    if (!node->IsLeaf()) {
      for (unsigned i = 0; i < node->NumItems(); ++i) {
        stack.emplace_back(node->Child(i), node->Key(i));
      }
      stack.emplace_back(node->Child(node->NumItems()), ubound);
    }
  }
  return true;
}

TEST_F(BPTreeSetTest, BPtreeInsert) {
  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(bptree_.Insert(i));
    ASSERT_EQ(i, bptree_.Size());
    ASSERT_EQ(i - 1, bptree_.GetRank(i));
    // ASSERT_TRUE(Validate()) << i;
  }
  ASSERT_TRUE(Validate());

  ASSERT_GT(mi_alloc_.used(), 56000u);
  ASSERT_LT(mi_alloc_.used(), 66000u);

  for (unsigned i = 1; i < 7000; ++i) {
    ASSERT_TRUE(bptree_.Contains(i));
  }

  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  uniform_int_distribution<uint64_t> dist(0, 100000);
  for (unsigned i = 0; i < 20000; ++i) {
    bptree_.Insert(dist(generator_));
    // ASSERT_TRUE(Validate()) << i;
  }
  ASSERT_TRUE(Validate());
  ASSERT_GT(mi_alloc_.used(), 10000u);
  LOG(INFO) << bptree_.Height() << " " << bptree_.Size();

  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);

  for (unsigned i = 20000; i > 1; --i) {
    bptree_.Insert(i);
  }
  ASSERT_TRUE(Validate());
  for (unsigned i = 2; i <= 20000; ++i) {
    ASSERT_EQ(i - 2, bptree_.GetRank(i));
  }

  LOG(INFO) << bptree_.Height() << " " << bptree_.Size();
  ASSERT_GT(mi_alloc_.used(), 20000 * 8);
  ASSERT_LT(mi_alloc_.used(), 20000 * 10);
  bptree_.Clear();
  ASSERT_EQ(mi_alloc_.used(), 0u);
}

TEST_F(BPTreeSetTest, Delete) {
  for (unsigned i = 31; i > 10; --i) {
    bptree_.Insert(i);
  }

  for (unsigned i = 1; i < 10; ++i) {
    ASSERT_FALSE(bptree_.Delete(i));
  }

  for (unsigned i = 11; i < 32; ++i) {
    ASSERT_TRUE(bptree_.Delete(i));
  }
  ASSERT_EQ(mi_alloc_.used(), 0u);
  ASSERT_EQ(bptree_.Size(), 0u);

  FillTree();

  ASSERT_GT(bptree_.NodeCount(), 2u);
  unsigned sz = bptree_.Size();
  for (unsigned i = 0; i < kNumElems; ++i) {
    --sz;
    ASSERT_EQ(bptree_.GetRank(kNumElems - 1), sz);

    ASSERT_TRUE(bptree_.Delete(i));
    ASSERT_EQ(bptree_.Size(), sz);
    // ASSERT_TRUE(Validate()) << i;
  }

  ASSERT_EQ(mi_alloc_.used(), 0u);
  ASSERT_EQ(bptree_.Size(), 0u);
  ASSERT_EQ(bptree_.Height(), 0u);
  ASSERT_EQ(bptree_.NodeCount(), 0u);

  FillTree(2);
  for (unsigned i = 0; i < 20000; ++i) {
    unsigned val = generator_() % 15000;
    bool res = bptree_.Delete(val);

    if (val % 2 == 1) {
      ASSERT_FALSE(res);
    }
    if (res) {
      ASSERT_TRUE(Validate());
    }
  }
}

TEST_F(BPTreeSetTest, Iterate) {
  FillTree(2);

  unsigned cnt = 0;
  bool res = bptree_.Iterate(31, 543, [&](uint64_t val) {
    if ((31 + cnt) * 2 != val)
      return false;
    ++cnt;
    return true;
  });
  ASSERT_EQ(543 - 31 + 1, cnt);
  ASSERT_TRUE(res);

  for (unsigned j = 0; j < 10; ++j) {
    cnt = 0;
    unsigned from = generator_() % kNumElems;
    unsigned to = from + generator_() % (kNumElems - from);
    res = bptree_.Iterate(from, to, [&](uint64_t val) {
      if ((from + cnt) * 2 != val)
        return false;
      ++cnt;
      return true;
    });

    ASSERT_EQ(to - from + 1, cnt);
    ASSERT_TRUE(res);
  }
}

TEST_F(BPTreeSetTest, Ranges) {
  FillTree(2);

  auto path = bptree_.GEQ(31);
  EXPECT_EQ(32, path.Terminal());

  path = bptree_.GEQ(32);
  EXPECT_EQ(32, path.Terminal());

  path = bptree_.GEQ(13998);
  EXPECT_EQ(13998, path.Terminal());

  path = bptree_.LEQ(14000);
  EXPECT_EQ(13998, path.Terminal());

  path = bptree_.GEQ(14000);
  EXPECT_EQ(0, path.Depth());

  ASSERT_TRUE(bptree_.Delete(0));
  path = bptree_.GEQ(0);
  EXPECT_EQ(2, path.Terminal());

  path = bptree_.LEQ(1);
  EXPECT_TRUE(path.Empty());
}

TEST_F(BPTreeSetTest, MemoryUsage) {
  zskiplist* zsl = zslCreate();
  std::vector<sds> sds_vec;

  constexpr size_t kLength = 3000;
  for (size_t i = 0; i < kLength; ++i) {
    sds_vec.push_back(sdsnew("f"));
  }
  size_t sz_before = zmalloc_used_memory_tl;
  LOG(INFO) << "zskiplist before: " << sz_before << " bytes";

  for (size_t i = 0; i < sds_vec.size(); ++i) {
    zslInsert(zsl, i, sds_vec[i]);
  }
  LOG(INFO) << "zskiplist takes: " << double(zmalloc_used_memory_tl - sz_before) / sds_vec.size()
            << " bytes per entry";
  zslFree(zsl);

  sds_vec.clear();
  for (size_t i = 0; i < kLength; ++i) {
    sds_vec.push_back(sdsnew("f"));
  }

  MiMemoryResource mi_alloc(mi_heap_get_backing());
  using AllocType = PMR_NS::polymorphic_allocator<std::pair<double, sds>>;
  AllocType alloc(&mi_alloc);
  absl::btree_set<pair<double, sds>, std::greater<pair<double, sds>>, AllocType> btree(alloc);

  ASSERT_EQ(0, mi_alloc.used());
  for (size_t i = 0; i < sds_vec.size(); ++i) {
    btree.emplace(i, sds_vec[i]);
  }
  ASSERT_GT(mi_alloc.used(), 0u);
  LOG(INFO) << "abseil btree: " << double(mi_alloc.used()) / sds_vec.size() << " bytes per entry";
  btree.clear();

  ASSERT_EQ(0, mi_alloc.used());
  SDSTree df_tree(&mi_alloc);
  for (size_t i = 0; i < sds_vec.size(); ++i) {
    btree.emplace(i, sds_vec[i]);
    VLOG(1) << "df btree: " << i << " " << double(mi_alloc.used()) / btree.size()
            << " bytes per entry";
  }
  ASSERT_GT(mi_alloc.used(), 0u);
  LOG(INFO) << "df btree: " << double(mi_alloc.used()) / sds_vec.size() << " bytes per entry";
}

TEST_F(BPTreeSetTest, InsertSDS) {
  vector<ZsetPolicy::KeyT> vals;
  for (unsigned i = 0; i < 256; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    vals.emplace_back(ZsetPolicy::KeyT{.d = 1000, .s = s});
  }

  SDSTree tree(&mi_alloc_);
  for (size_t i = 0; i < vals.size(); ++i) {
    ASSERT_TRUE(tree.Insert(vals[i]));
  }

  for (auto v : vals) {
    sdsfree(v.s);
  }
}

TEST_F(BPTreeSetTest, ReverseIterate) {
  vector<ZsetPolicy::KeyT> vals;
  for (int i = -1000; i < 1000; ++i) {
    sds s = sdsempty();

    s = sdscatfmt(s, "a%u", i);
    vals.emplace_back(ZsetPolicy::KeyT{.d = (double)i, .s = s});
  }

  SDSTree tree(&mi_alloc_);
  for (auto v : vals) {
    ASSERT_TRUE(tree.Insert(v));
    {
      double score = 0;
      tree.IterateReverse(0, 0, [&score](auto i) {
        score = i.d;
        return false;
      });
      EXPECT_EQ(score, v.d);
    }
    {
      double score = 0;
      tree.Iterate(0, 0, [&score](auto i) {
        score = i.d;
        return false;
      });
      EXPECT_EQ(score, vals[0].d);
    }
  }

  vector<int> res;
  tree.IterateReverse(0, 1, [&](auto i) {
    res.push_back(i.d);
    return true;
  });
  EXPECT_THAT(res, testing::ElementsAre(999, 998));

  for (auto v : vals) {
    sdsfree(v.s);
  }
}

static string RandomString(mt19937& rand, unsigned len) {
  const string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  string ret;
  ret.reserve(len);

  for (size_t i = 0; i < len; ++i) {
    ret += alpanum[rand() % alpanum.size()];
  }

  return ret;
}

std::vector<ZsetPolicy::KeyT> GenerateRandomPairs(unsigned len) {
  mt19937 dre(10);
  std::vector<ZsetPolicy::KeyT> vals(len, ZsetPolicy::KeyT{});
  for (unsigned i = 0; i < len; ++i) {
    vals[i].d = dre();
    vals[i].s = sdsnew(RandomString(dre, 10).c_str());
  }
  return vals;
}

static void BM_FindRandomBPTree(benchmark::State& state) {
  unsigned iters = state.range(0);
  std::vector<ZsetPolicy::KeyT> vals = GenerateRandomPairs(iters);
  SDSTree bptree;
  for (unsigned i = 0; i < iters; ++i) {
    bptree.Insert(vals[i]);
  }

  unsigned i = 0;
  while (state.KeepRunningBatch(10)) {
    for (unsigned j = 0; j < 10; ++j) {
      benchmark::DoNotOptimize(bptree.GEQ(vals[i]));
      ++i;
      if (vals.size() == i)
        i = 0;
    }
  }
  for (const auto v : vals) {
    sdsfree(v.s);
  }
}
BENCHMARK(BM_FindRandomBPTree)->Arg(1024)->Arg(1 << 16)->Arg(1 << 20);

static void BM_FindRandomZSL(benchmark::State& state) {
  zskiplist* zsl = zslCreate();
  unsigned iters = state.range(0);
  std::vector<ZsetPolicy::KeyT> vals = GenerateRandomPairs(iters);
  for (unsigned i = 0; i < iters; ++i) {
    zslInsert(zsl, vals[i].d, sdsdup(vals[i].s));
  }

  zrangespec spec;
  spec.maxex = 0;
  spec.minex = 0;

  unsigned i = 0;
  while (state.KeepRunningBatch(10)) {
    for (unsigned j = 0; j < 10; ++j) {
      spec.min = vals[i].d;
      spec.max = spec.min;
      benchmark::DoNotOptimize(zslFirstInRange(zsl, &spec));

      ++i;
      if (vals.size() == i)
        i = 0;
    }
  }

  zslFree(zsl);

  for (const auto v : vals) {
    sdsfree(v.s);
  }
}
BENCHMARK(BM_FindRandomZSL)->Arg(1024)->Arg(1 << 16)->Arg(1 << 20);

void RegisterBPTreeBench() {
  auto* tlh = mi_heap_get_backing();
  init_zmalloc_threadlocal(tlh);
};

REGISTER_MODULE_INITIALIZER(Bptree, RegisterBPTreeBench());

TEST_F(BPTreeSetTest, ForceUpdate) {
  struct Policy {
    // Similar to how it's used in SortedMap just a little simpler.
    using KeyT = int*;

    struct KeyCompareTo {
      int operator()(KeyT a, KeyT b) const {
        if (*a < *b)
          return -1;
        if (*a > *b)
          return 1;
        return 0;
      }
    };
  };

  auto gen_vector = []() {
    std::vector<std::unique_ptr<int>> tmp;
    for (size_t i = 0; i < 1000; ++i) {
      tmp.push_back(std::make_unique<int>(i));
    }
    return tmp;
  };

  std::vector<std::unique_ptr<int>> original = gen_vector();
  std::vector<std::unique_ptr<int>> modified = gen_vector();

  BPTree<int*, Policy> bptree;
  for (auto& item : original) {
    bptree.Insert(item.get());
  }

  for (auto& item : modified) {
    bptree.ForceUpdate(item.get(), item.get());
  }

  original.clear();
  size_t index = 0;
  bptree.Iterate(0, 1000, [&](int* ptr) {
    EXPECT_EQ(modified[index].get(), ptr);
    ++index;
    return true;
  });
}

}  // namespace dfly
