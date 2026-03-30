// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <gmock/gmock.h>
#include <mimalloc.h>

#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"
#include "core/page_usage/page_usage_stats.h"
#include "io/file.h"
#include "io/line_reader.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

/* quicklist compression disable */
#define QUICKLIST_NOCOMPRESS 0

namespace dfly {

using namespace std;
using namespace testing;
using absl::StrCat;

static int ql_verify_compress(const QList& ql) {
  int errors = 0;
  unsigned compress_param = ql.compress_param();
  if (compress_param > 0) {
    const auto* node = ql.Head();
    unsigned int low_raw = compress_param;
    unsigned int high_raw = ql.node_count() - compress_param;

    for (unsigned int at = 0; at < ql.node_count(); at++, node = node->next) {
      if (node && (at < low_raw || at >= high_raw)) {
        if (node->encoding != QUICKLIST_NODE_ENCODING_RAW) {
          LOG(ERROR) << "Incorrect compression: node " << at << " is compressed at depth "
                     << compress_param << " ((" << low_raw << "," << high_raw
                     << " total nodes: " << ql.node_count() << "; size: " << node->sz
                     << "; recompress: " << node->recompress;
          errors++;
        }
      } else {
        if (node->encoding != QUICKLIST_NODE_ENCODING_LZF && !node->attempted_compress) {
          LOG(ERROR) << absl::StrFormat(
              "Incorrect non-compression: node %d is NOT "
              "compressed at depth %d ((%u, %u); total "
              "nodes: %lu; size: %zu; recompress: %d; attempted: %d)",
              at, compress_param, low_raw, high_raw, ql.node_count(), node->sz, node->recompress,
              node->attempted_compress);
          errors++;
        }
      }
    }
  }
  return errors;
}

/* Verify list metadata matches physical list contents. */
static int ql_verify(const QList& ql, uint32_t nc, uint32_t count, uint32_t head_count,
                     uint32_t tail_count) {
  int errors = 0;

  if (nc != ql.node_count()) {
    LOG(ERROR) << "quicklist length wrong: expected " << nc << " got " << ql.node_count();
    errors++;
  }

  if (count != ql.Size()) {
    LOG(ERROR) << "quicklist count wrong: expected " << count << " got " << ql.Size();
    errors++;
  }

  auto* node = ql.Head();
  size_t node_size = 0;
  while (node) {
    node_size += node->count;
    node = node->next;
    CHECK(node != ql.Head());
  }

  if (node_size != ql.Size()) {
    LOG(ERROR) << "quicklist cached count not match actual count: expected " << ql.Size() << " got "
               << node_size;
    errors++;
  }

  node = ql.Tail();
  node_size = 0;
  while (node) {
    node_size += node->count;
    node = (node == ql.Head()) ? nullptr : node->prev;
  }
  if (node_size != ql.Size()) {
    LOG(ERROR) << "has different forward count than reverse count!  "
                  "Forward count is "
               << ql.Size() << ", reverse count is " << node_size;
    errors++;
  }

  if (ql.node_count() == 0 && errors == 0) {
    return 0;
  }

  if (head_count != ql.Head()->count && head_count != lpLength(ql.Head()->entry)) {
    LOG(ERROR) << absl::StrFormat("head count wrong: expected %u got cached %u vs. actual %lu",
                                  head_count, ql.Head()->count, lpLength(ql.Head()->entry));
    errors++;
  }

  if (tail_count != ql.Tail()->count && tail_count != lpLength(ql.Tail()->entry)) {
    LOG(ERROR) << "tail count wrong: expected " << tail_count << "got cached " << ql.Tail()->count
               << " vs. actual " << lpLength(ql.Tail()->entry);
    errors++;
  }

  errors += ql_verify_compress(ql);
  return errors;
}

static void SetupMalloc() {
  // configure redis lib zmalloc which requires mimalloc heap to work.
  auto* tlh = mi_heap_get_backing();
  init_zmalloc_threadlocal(tlh);
  mi_option_set(mi_option_purge_delay, -1);  // disable purging of segments (affects benchmarks)
}

class QListTest : public ::testing::Test {
 protected:
  QListTest() : mr_(mi_heap_get_backing()) {
  }

  static void SetUpTestSuite() {
    SetupMalloc();
  }

  static void TearDownTestSuite() {
    mi_heap_collect(mi_heap_get_backing(), true);

    auto cb_visit = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                       size_t block_size, void* arg) {
      LOG(ERROR) << "Unfreed allocations: block_size " << block_size
                 << ", allocated: " << area->used * block_size;
      return true;
    };

    mi_heap_visit_blocks(mi_heap_get_backing(), false /* do not visit all blocks*/, cb_visit,
                         nullptr);
  }

  vector<string> ToItems() const;

  MiMemoryResource mr_;
  QList ql_;
};

vector<string> QListTest::ToItems() const {
  vector<string> res;
  auto cb = [&](const QList::Entry& e) {
    res.push_back(e.to_string());
    return true;
  };

  ql_.Iterate(cb, 0, ql_.Size());
  return res;
}

TEST_F(QListTest, Basic) {
  EXPECT_EQ(0, ql_.Size());
  ql_.Push("abc", QList::HEAD);
  EXPECT_EQ(1, ql_.Size());
  EXPECT_TRUE(ql_.Tail() == ql_.Head());
  EXPECT_LE(ql_.MallocUsed(false), ql_.MallocUsed(true));

  auto it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Valid());  // Iterator is valid immediately.

  EXPECT_EQ("abc", it.Get().view());

  ASSERT_FALSE(it.Next());

  ql_.Push("def", QList::TAIL);
  EXPECT_EQ(2, ql_.Size());
  EXPECT_LE(ql_.MallocUsed(false), ql_.MallocUsed(true));

  it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ("def", it.Get().view());

  ASSERT_TRUE(it.Next());
  EXPECT_EQ("abc", it.Get().view());
  ASSERT_FALSE(it.Next());

  it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ("abc", it.Get().view());
  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ("def", it.Get().view());

  vector<string> items = ToItems();

  EXPECT_THAT(items, ElementsAre("abc", "def"));
  EXPECT_GT(ql_.MallocUsed(false), ql_.MallocUsed(true) * 0.8);
}

TEST_F(QListTest, ListPack) {
  string_view sv = "abcded"sv;
  uint8_t* lp1 = lpPrepend(lpNew(0), (uint8_t*)sv.data(), sv.size());
  uint8_t* lp2 = lpAppend(lpNew(0), (uint8_t*)sv.data(), sv.size());
  ASSERT_EQ(lpBytes(lp1), lpBytes(lp2));
  ASSERT_EQ(0, memcmp(lp1, lp2, lpBytes(lp1)));
  lpFree(lp1);
  lpFree(lp2);
}

TEST_F(QListTest, InsertDelete) {
  EXPECT_FALSE(ql_.Insert("abc", "def", QList::BEFORE));
  ql_.Push("abc", QList::HEAD);
  EXPECT_TRUE(ql_.Insert("abc", "def", QList::BEFORE));
  auto items = ToItems();
  EXPECT_THAT(items, ElementsAre("def", "abc"));
  EXPECT_TRUE(ql_.Insert("abc", "123456", QList::AFTER));
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("def", "abc", "123456"));

  auto it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Valid());

  // Erase the items one by one.
  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("abc", "123456"));
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("abc", it.Get().view());

  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("123456"));
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ(123456, it.Get().ival());

  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre());
  ASSERT_FALSE(it.Valid());
  EXPECT_EQ(0, ql_.Size());
}

TEST_F(QListTest, EraseLastElementInNodeAdvancesToNextNode) {
  // Regression test for iterator semantics: when erasing the last element
  // within a multi-entry node and another node follows, the iterator should
  // correctly advance to the first element of the next node.

  // Create a QList with fill=2 to ensure max 2 elements per node
  ql_ = QList(2, QUICKLIST_NOCOMPRESS);

  // Push 3 elements: this creates 2 nodes (first with 2 elements, second with 1)
  ql_.Push("first", QList::HEAD);   // Will be at index 2 after all pushes
  ql_.Push("second", QList::HEAD);  // Will be at index 1 after all pushes
  ql_.Push("third", QList::HEAD);   // Will be at index 0 after all pushes

  // Verify we have 2 nodes as expected
  ASSERT_EQ(2, ql_.node_count());
  ASSERT_EQ(3, ql_.Size());

  // Node structure should be:
  // Node 1: ["third", "second"]
  // Node 2: ["first"]

  auto items = ToItems();
  EXPECT_THAT(items, ElementsAre("third", "second", "first"));

  // Get iterator to "second" (last element in first node)
  auto it = ql_.GetIterator(1);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("second", it.Get().view());

  // Erase "second" - this is the last element in the first node
  it = ql_.Erase(it);

  // Iterator should now point to "first" (first element of the second node)
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ("first", it.Get().view());

  // Verify the list is correct
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("third", "first"));
  EXPECT_EQ(2, ql_.Size());
}

TEST_F(QListTest, PushPlain) {
  // push a value large enough to trigger plain node insertion.
  string val(9000, 'a');
  ql_.Push(val, QList::HEAD);
  auto items = ToItems();
  EXPECT_THAT(items, ElementsAre(val));
}

TEST_F(QListTest, GetNum) {
  ql_.Push("1251977", QList::HEAD);
  QList::Iterator it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(1251977, it.Get().ival());
}

TEST_F(QListTest, CompressionPlain) {
  char buf[256];
  QList::SetPackedThreshold(1);
  ql_ = QList(-2, 1);

  for (int i = 0; i < 500; i++) {
    /* Set to 256 to allow the node to be triggered to compress,
     * if it is less than 48(nocompress), the test will be successful. */
    snprintf(buf, sizeof(buf), "hello%d", i);
    ql_.Push(string_view{buf, sizeof(buf)}, QList::HEAD);
  }
  QList::SetPackedThreshold(0);

  QList::Iterator it = ql_.GetIterator(QList::TAIL);
  int i = 0;
  ASSERT_TRUE(it.Valid());
  do {
    string_view sv = it.Get().view();
    ASSERT_EQ(sizeof(buf), sv.size());
    ASSERT_TRUE(absl::StartsWith(sv, StrCat("hello", i)));
    i++;
  } while (it.Next());
  EXPECT_EQ(500, i);
}

TEST_F(QListTest, LargeValues) {
  string val(100000, 'a');
  ql_.Push(val, QList::HEAD);
  ql_.Push(val, QList::HEAD);
  ql_.Pop(QList::HEAD);
  auto items = ToItems();
  EXPECT_THAT(items, ElementsAre(val));
}

TEST_F(QListTest, RemoveListpack) {
  ql_.Push("ABC", QList::TAIL);
  ql_.Push("DEF", QList::TAIL);
  auto it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Valid());  // Iterator is valid immediately.
  ql_.Erase(it);
  it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Valid());
  it = ql_.Erase(it);
  ASSERT_FALSE(it.Valid());
}

TEST_F(QListTest, DefragListpackRaw) {
  PageUsage page_usage{CollectPageStats::YES, 100.0};
  page_usage.SetForceReallocate(true);

  ql_.Push("first", QList::TAIL);
  ql_.Push("second", QList::TAIL);

  ASSERT_EQ(ql_.DefragIfNeeded(&page_usage), 1);
  EXPECT_THAT(ToItems(), ElementsAre("first", "second"));
  ql_.Clear();
}

TEST_F(QListTest, DefragPlainTextRaw) {
  PageUsage page_usage{CollectPageStats::YES, 100.0};
  page_usage.SetForceReallocate(true);
  string big(100000, 'x');
  ql_.Push(big, QList::HEAD);
  ASSERT_EQ(ql_.DefragIfNeeded(&page_usage), 1);
  EXPECT_THAT(ToItems(), ElementsAre(big));
  ql_.Clear();
}

TEST_F(QListTest, DefragmentListpackCompressed) {
  PageUsage page_usage{CollectPageStats::YES, 100.0};
  page_usage.SetForceReallocate(true);

  // MIN_COMPRESS_BYTES = 256
  char buf[256];
  constexpr auto items_per_list = 4;
  constexpr auto total_items = 20;
  ql_ = QList{items_per_list, 1};

  for (auto i = 0; i < total_items; ++i) {
    absl::SNPrintF(buf, 256, "test__%d", i);
    ql_.Push(string_view{buf, 256}, QList::TAIL);
  }

  ASSERT_EQ(total_items / items_per_list, ql_.DefragIfNeeded(&page_usage));

  auto i = 0;
  auto it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Valid());
  do {
    auto v = it.Get().view();
    ASSERT_EQ(v.size(), 256);
    ASSERT_TRUE(absl::StartsWith(v, StrCat("test__", i)));
    ++i;
  } while (it.Next());
  ASSERT_EQ(i, total_items);
}

// MergeNodes must not follow the head_->prev circular link when looking for
// adjacent nodes to merge.  Splitting a full head node and calling MergeNodes
// on the right half used to traverse new_head->prev (= tail), merging two
// non-adjacent nodes and corrupting element order.
TEST_F(QListTest, InsertSplitHeadMergeOrder) {
  QList ql(5, 0);

  // 3 nodes: [v0..v4](head,full) -> [v5..v9] -> [v10](tail,1 elem)
  for (int i = 0; i < 11; i++) {
    ql.Push(StrCat("v", i), QList::TAIL);
  }
  ASSERT_EQ(3u, ql.node_count());

  // Insert in the middle of the full head triggers split + MergeNodes.
  ql.Insert("v2", "x", QList::BEFORE);

  vector<string> items;
  ql.Iterate(
      [&](const QList::Entry& e) {
        items.push_back(e.to_string());
        return true;
      },
      0, ql.Size());

  EXPECT_THAT(items,
              ElementsAre("v0", "v1", "x", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10"));
}

TEST_F(QListTest, InsertPivotSplitMergeMallocSize) {
  QList ql(2, 0);  // fill=2: at most 2 elements per node
  ql.Push("x", QList::TAIL);
  ql.Push("b", QList::TAIL);  // ['x','b'] — full node (count == fill)

  // Insert before 'b' on a full node triggers the split path.
  ql.Insert("b", "x", QList::BEFORE);

  // Verify malloc_size_ matches the actual sum of node sizes.
  size_t actual_sz = 0;
  for (auto* n = ql.Head(); n; n = n->next)
    actual_sz += n->sz;
  size_t node_overhead = ql.node_count() * sizeof(QList::Node) + znallocx(sizeof(QList));
  EXPECT_EQ(ql.MallocUsed(false) - node_overhead, actual_sz);

  ql.Pop(QList::TAIL);  // remove 'b' node → 1 node ['x','x']

  // Erase one element; list stays at len==1.
  // DCHECK_EQ(malloc_size_, head_->sz) at exit of Erase(Iterator) catches the drift.
  auto it = ql.GetIterator(QList::HEAD);
  ql.Erase(it);
}

using FillCompress = tuple<int, unsigned>;

class PrintToFillCompress {
 public:
  std::string operator()(const TestParamInfo<FillCompress>& info) const {
    int fill = get<0>(info.param);
    int compress = get<1>(info.param);
    string fill_str = fill >= 0 ? absl::StrCat("f", fill) : absl::StrCat("fminus", -fill);
    return absl::StrCat(fill_str, "compr", compress);
  }
};

class OptionsTest : public QListTest, public WithParamInterface<FillCompress> {};

INSTANTIATE_TEST_SUITE_P(Matrix, OptionsTest,
                         Combine(Values(-5, -4, -3, -2, -1, 0, 1, 2, 32, 66, 128, 999),
                                 Values(0, 1, 2, 3, 4, 5, 6, 10)),
                         PrintToFillCompress());

TEST_P(OptionsTest, Numbers) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  array<int64_t, 5000> nums;

  for (unsigned i = 0; i < nums.size(); i++) {
    nums[i] = -5157318210846258176 + i;
    string val = absl::StrCat(nums[i]);
    ql_.Push(val, QList::TAIL);
  }
  ql_.Push("xxxxxxxxxxxxxxxxxxxx", QList::TAIL);

  for (unsigned i = 0; i < nums.size(); i++) {
    auto it = ql_.GetIterator(i);
    ASSERT_TRUE(it.Valid());
    ASSERT_EQ(nums[i], it.Get().ival()) << i;
  }

  auto it = ql_.GetIterator(nums.size());
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ("xxxxxxxxxxxxxxxxxxxx", it.Get().view());
}

TEST_P(OptionsTest, NumbersIndex) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  long long nums[5000];
  for (int i = 0; i < 760; i++) {
    nums[i] = -5157318210846258176 + i;
    ql_.Push(absl::StrCat(nums[i]), QList::TAIL);
  }

  unsigned i = 437;
  QList::Iterator it = ql_.GetIterator(i);
  ASSERT_TRUE(it.Valid());
  do {
    ASSERT_EQ(nums[i], it.Get().ival());
    i++;
  } while (it.Next());
  ASSERT_EQ(760, i);
}

TEST_P(OptionsTest, DelRangeA) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  long long nums[5000];
  for (int i = 0; i < 33; i++) {
    nums[i] = -5157318210846258176 + i;
    ql_.Push(absl::StrCat(nums[i]), QList::TAIL);
  }

  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 2, 33, 32, 1));
  }

  /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
  ql_.Erase(0, 3);
  ql_.Erase(-29, 4000); /* make sure not loop forever */
  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 1, 1, 1, 1));
  }
  auto it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(-5157318210846258173, it.Get().ival());
}

TEST_P(OptionsTest, DelRangeB) {
  auto [fill, _] = GetParam();
  ql_ = QList(fill, QUICKLIST_NOCOMPRESS);  // ignore compress parameter

  long long nums[5000];
  for (int i = 0; i < 33; i++) {
    nums[i] = i;
    ql_.Push(absl::StrCat(nums[i]), QList::TAIL);
  }
  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 2, 33, 32, 1));
  }
  /* ltrim 5 16 (keep [5,16] inclusive = 12 remaining) */
  ql_.Erase(0, 5);
  ql_.Erase(-16, 16);
  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 1, 12, 12, 12));
  }

  auto it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(5, it.Get().ival());

  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(16, it.Get().ival());

  ql_.Push("bobobob", QList::TAIL);
  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ("bobobob", it.Get().view());

  for (int i = 0; i < 12; i++) {
    it = ql_.GetIterator(i);
    ASSERT_TRUE(it.Valid());
    EXPECT_EQ(i + 5, it.Get().ival());
  }
}

TEST_P(OptionsTest, DelRangeC) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  long long nums[5000];
  for (int i = 0; i < 33; i++) {
    nums[i] = -5157318210846258176 + i;
    ql_.Push(absl::StrCat(nums[i]), QList::TAIL);
  }
  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 2, 33, 32, 1));
  }

  /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
  ql_.Erase(0, 3);
  ql_.Erase(-29, 4000); /* make sure not loop forever */
  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 1, 1, 1, 1));
  }
  auto it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ(-5157318210846258173, it.Get().ival());
}

TEST_P(OptionsTest, DelRangeD) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  long long nums[5000];
  for (int i = 0; i < 33; i++) {
    nums[i] = -5157318210846258176 + i;
    ql_.Push(absl::StrCat(nums[i]), QList::TAIL);
  }
  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 2, 33, 32, 1));
  }
  ql_.Erase(-12, 3);

  ASSERT_EQ(30, ql_.Size());
}

TEST_P(OptionsTest, DelRangeNode) {
  auto [_, compress] = GetParam();
  ql_ = QList(-2, compress);

  for (int i = 0; i < 32; i++)
    ql_.Push(StrCat("hello", i), QList::HEAD);

  ASSERT_EQ(0, ql_verify(ql_, 1, 32, 32, 32));
  ql_.Erase(0, 32);
  ASSERT_EQ(0, ql_verify(ql_, 0, 0, 0, 0));
}

TEST_P(OptionsTest, DelRangeNodeOverflow) {
  auto [_, compress] = GetParam();
  ql_ = QList(-2, compress);

  for (int i = 0; i < 32; i++)
    ql_.Push(StrCat("hello", i), QList::HEAD);
  ASSERT_EQ(0, ql_verify(ql_, 1, 32, 32, 32));
  ql_.Erase(0, 128);
  ASSERT_EQ(0, ql_verify(ql_, 0, 0, 0, 0));
}

TEST_P(OptionsTest, DelRangeMiddle100of500) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);

  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);

  ASSERT_EQ(0, ql_verify(ql_, 16, 500, 32, 20));
  ql_.Erase(200, 100);
  ASSERT_EQ(0, ql_verify(ql_, 14, 400, 32, 20));
}

TEST_P(OptionsTest, DelLessFillAcrossNodes) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);

  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);
  ASSERT_EQ(0, ql_verify(ql_, 16, 500, 32, 20));
  ql_.Erase(60, 10);
  ASSERT_EQ(0, ql_verify(ql_, 16, 490, 32, 20));
}

TEST_P(OptionsTest, DelNegOne) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);
  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);
  ASSERT_EQ(0, ql_verify(ql_, 16, 500, 32, 20));
  ql_.Erase(-1, 1);
  ASSERT_EQ(0, ql_verify(ql_, 16, 499, 32, 19));
}

TEST_P(OptionsTest, DelNegOneOverflow) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);
  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);

  ASSERT_EQ(0, ql_verify(ql_, 16, 500, 32, 20));
  ql_.Erase(-1, 128);

  ASSERT_EQ(0, ql_verify(ql_, 16, 499, 32, 19));
}

TEST_P(OptionsTest, DelNeg100From500) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);
  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);
  ql_.Erase(-100, 100);

  QList::Iterator it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello400", it.Get());
  ASSERT_EQ(0, ql_verify(ql_, 13, 400, 32, 16));
}

TEST_P(OptionsTest, DelMin10_5_from50) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);

  for (int i = 0; i < 50; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);
  ASSERT_EQ(0, ql_verify(ql_, 2, 50, 32, 18));
  ql_.Erase(-10, 5);
  ASSERT_EQ(0, ql_verify(ql_, 2, 45, 32, 13));
}

TEST_P(OptionsTest, DelElems) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  const char* words[] = {"abc", "foo", "bar", "foobar", "foobared", "zap", "bar", "test", "foo"};
  const char* result[] = {"abc", "foo", "foobar", "foobared", "zap", "test", "foo"};
  const char* resultB[] = {"abc", "foo", "foobar", "foobared", "zap", "test"};

  for (int i = 0; i < 9; i++)
    ql_.Push(words[i], QList::TAIL);

  /* lrem 0 bar */
  auto iter = ql_.GetIterator(QList::HEAD);
  while (iter.Valid()) {
    if (iter.Get() == "bar") {
      iter = ql_.Erase(iter);
      // iter now points to next element, don't call Next()
    } else {
      if (!iter.Next())
        break;
    }
  }
  EXPECT_THAT(ToItems(), ElementsAreArray(result));

  ql_.Push("foo", QList::TAIL);

  /* lrem -2 foo */
  iter = ql_.GetIterator(QList::TAIL);
  int del = 2;
  while (iter.Valid()) {
    if (iter.Get() == "foo") {
      iter = ql_.Erase(iter);
      del--;
      if (del == 0)
        break;
      // iter now points to next element, don't call Next()
    } else {
      if (!iter.Next())
        break;
    }
  }

  /* check result of lrem -2 foo */
  /* (we're ignoring the '2' part and still deleting all foo
   * because we only have two foo) */
  EXPECT_THAT(ToItems(), ElementsAreArray(resultB));
}

TEST_P(OptionsTest, IterateReverse) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);

  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i), QList::HEAD);
  QList::Iterator it = ql_.GetIterator(QList::TAIL);
  int i = 0;
  ASSERT_TRUE(it.Valid());
  do {
    ASSERT_EQ(StrCat("hello", i), it.Get());
    i++;
  } while (it.Next());
  ASSERT_EQ(500, i);
  ASSERT_EQ(0, ql_verify(ql_, 16, 500, 20, 32));
}

TEST_P(OptionsTest, Iterate500) {
  auto [_, compress] = GetParam();
  ql_ = QList(32, compress);
  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i), QList::HEAD);

  QList::Iterator it = ql_.GetIterator(QList::HEAD);
  int i = 499, count = 0;
  ASSERT_TRUE(it.Valid());
  do {
    QList::Entry entry = it.Get();
    ASSERT_EQ(StrCat("hello", i), entry);
    i--;
    count++;
  } while (it.Next());
  EXPECT_EQ(500, count);
  ASSERT_EQ(0, ql_verify(ql_, 16, 500, 20, 32));

  it = ql_.GetIterator(QList::TAIL);
  i = 0;
  ASSERT_TRUE(it.Valid());
  do {
    ASSERT_EQ(StrCat("hello", i), it.Get());
    i++;
  } while (it.Next());
  EXPECT_EQ(500, i);
}

TEST_P(OptionsTest, IterateAfterOne) {
  auto [_, compress] = GetParam();
  ql_ = QList(-2, compress);
  ql_.Push("hello", QList::HEAD);

  QList::Iterator it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  ql_.Insert(it, "abc", QList::AFTER);

  ASSERT_EQ(0, ql_verify(ql_, 1, 2, 2, 2));

  /* verify results */
  it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello", it.Get());

  it = ql_.GetIterator(1);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("abc", it.Get());
}

TEST_P(OptionsTest, IterateDelete) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  ql_.Push("abc", QList::TAIL);
  ql_.Push("def", QList::TAIL);
  ql_.Push("hij", QList::TAIL);
  ql_.Push("jkl", QList::TAIL);
  ql_.Push("oop", QList::TAIL);

  QList::Iterator it = ql_.GetIterator(QList::HEAD);
  while (it.Valid()) {
    if (it.Get() == "hij") {
      it = ql_.Erase(it);
    } else {
      it.Next();
    }
  }

  ASSERT_THAT(ToItems(), ElementsAre("abc", "def", "jkl", "oop"));
}

TEST_P(OptionsTest, InsertBeforeOne) {
  auto [_, compress] = GetParam();
  ql_ = QList(-2, compress);

  ql_.Push("hello", QList::HEAD);
  QList::Iterator it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  ql_.Insert(it, "abc", QList::BEFORE);
  ql_verify(ql_, 1, 2, 2, 2);

  /* verify results */
  it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("abc", it.Get());

  it = ql_.GetIterator(1);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello", it.Get());
}

TEST_P(OptionsTest, InsertWithHeadFull) {
  auto [_, compress] = GetParam();
  ql_ = QList(4, compress);

  for (int i = 0; i < 10; i++)
    ql_.Push(StrCat("hello", i), QList::TAIL);

  ql_.set_fill(-1);
  QList::Iterator it = ql_.GetIterator(-10);
  ASSERT_TRUE(it.Valid());

  char buf[4096] = {0};
  ql_.Insert(it, string_view{buf, sizeof(buf)}, QList::BEFORE);
  ql_verify(ql_, 4, 11, 1, 2);
}

TEST_P(OptionsTest, InsertWithTailFull) {
  auto [_, compress] = GetParam();
  ql_ = QList(4, compress);
  for (int i = 0; i < 10; i++)
    ql_.Push(StrCat("hello", i), QList::HEAD);

  ql_.set_fill(-1);
  QList::Iterator it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Valid());

  char buf[4096] = {0};
  ql_.Insert(it, string_view{buf, sizeof(buf)}, QList::AFTER);
  ql_verify(ql_, 4, 11, 2, 1);
}

TEST_P(OptionsTest, InsertOnceWhileIterating) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);

  ql_.Push("abc", QList::TAIL);
  ql_.set_fill(1);

  ql_.Push("def", QList::TAIL);
  ql_.set_fill(fill);
  ql_.Push("bob", QList::TAIL);
  ql_.Push("foo", QList::TAIL);
  ql_.Push("zoo", QList::TAIL);

  /* insert "bar" before "bob" while iterating over list. */
  QList::Iterator it = ql_.GetIterator(QList::HEAD);
  if (it.Valid()) {
    do {
      if (it.Get() == "bob") {
        ql_.Insert(it, "bar", QList::BEFORE);
        break; /* didn't we fix insert-while-iterating? */
      }
    } while (it.Next());
  }
  EXPECT_THAT(ToItems(), ElementsAre("abc", "def", "bar", "bob", "foo", "zoo"));
}

TEST_P(OptionsTest, InsertBefore250NewInMiddleOf500Elements) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);
  for (int i = 0; i < 500; i++) {
    string val = StrCat("hello", i);
    val.resize(32);
    ql_.Push(val, QList::TAIL);
  }

  for (int i = 0; i < 250; i++) {
    QList::Iterator it = ql_.GetIterator(250);
    ASSERT_TRUE(it.Valid());
    ql_.Insert(it, StrCat("abc", i), QList::BEFORE);
  }

  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 25, 750, 32, 20));
  }
}

TEST_P(OptionsTest, InsertAfter250NewInMiddleOf500Elements) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);
  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i), QList::HEAD);

  for (int i = 0; i < 250; i++) {
    QList::Iterator it = ql_.GetIterator(250);
    ASSERT_TRUE(it.Valid());
    ql_.Insert(it, StrCat("abc", i), QList::AFTER);
  }

  ASSERT_EQ(750, ql_.Size());

  if (fill == 32) {
    ASSERT_EQ(0, ql_verify(ql_, 26, 750, 20, 32));
  }
}

TEST_P(OptionsTest, NextPlain) {
  auto [_, compress] = GetParam();
  ql_ = QList(-2, compress);

  QList::SetPackedThreshold(3);

  const char* strings[] = {"hello1", "hello2", "h3", "h4", "hello5"};

  for (int i = 0; i < 5; ++i)
    ql_.Push(strings[i], QList::HEAD);

  QList::Iterator it = ql_.GetIterator(QList::TAIL);
  int j = 0;

  ASSERT_TRUE(it.Valid());
  do {
    ASSERT_EQ(strings[j], it.Get());
    j++;
  } while (it.Next());
}

TEST_P(OptionsTest, IndexFrom500) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);
  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);

  QList::Iterator it = ql_.GetIterator(1);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello2", it.Get());
  it = ql_.GetIterator(200);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello201", it.Get());

  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello500", it.Get());

  it = ql_.GetIterator(-2);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello499", it.Get());

  it = ql_.GetIterator(-100);
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ("hello401", it.Get());

  it = ql_.GetIterator(500);
  ASSERT_FALSE(it.Valid());
}

static void BM_QListCompress(benchmark::State& state) {
  SetupMalloc();

  string path = base::ProgramRunfile("testdata/list.txt.zst");
  io::Result<io::Source*> src = io::OpenUncompressed(path);
  CHECK(src) << src.error();
  io::LineReader lr(*src, TAKE_OWNERSHIP);
  string_view line;
  vector<string> lines;
  while (lr.Next(&line)) {
    lines.push_back(string(line));
  }

  VLOG(1) << "Read " << lines.size() << " lines " << state.range(0);
  while (state.KeepRunning()) {
    QList ql(-2, state.range(0));

    for (const string& l : lines) {
      ql.Push(l, QList::TAIL);
    }
    DVLOG(1) << ql.node_count() << ", " << ql.MallocUsed(true);
  }
  CHECK_EQ(0, zmalloc_used_memory_tl);
}
BENCHMARK(BM_QListCompress)
    ->ArgsProduct({{1, 4, 0}});  // compression depth:
                                 // 0 - no compression, 1 - compress all nodes but edges,
                                 // 4 - compress all but 4 nodes from edges.

static void BM_QListUncompress(benchmark::State& state) {
  SetupMalloc();

  string path = base::ProgramRunfile("testdata/list.txt.zst");
  io::Result<io::Source*> src = io::OpenUncompressed(path);
  CHECK(src) << src.error();
  io::LineReader lr(*src, TAKE_OWNERSHIP);
  string_view line;
  QList ql(-2, state.range(0));
  QList::stats.compression_attempts = 0;

  CHECK_EQ(QList::stats.compressed_bytes, 0u);
  CHECK_EQ(QList::stats.raw_compressed_bytes, 0u);

  size_t line_len = 0;
  while (lr.Next(&line)) {
    ql.Push(line, QList::TAIL);
    line_len += line.size();
  }

  if (ql.compress_param() > 0) {
    CHECK_GT(QList::stats.compression_attempts, 0u);
    CHECK_GT(QList::stats.compressed_bytes, 0u);
    CHECK_GT(QList::stats.raw_compressed_bytes, QList::stats.compressed_bytes);
  }

  LOG(INFO) << "MallocUsed " << ql.compress_param() << ": " << ql.MallocUsed(true) << ", "
            << ql.MallocUsed(false);
  size_t exp_count = ql.Size();

  while (state.KeepRunning()) {
    unsigned actual_count = 0, actual_len = 0;
    ql.Iterate(
        [&](const QList::Entry& e) {
          actual_len += e.view().size();
          ++actual_count;
          return true;
        },
        0, -1);
    CHECK_EQ(exp_count, actual_count);
    CHECK_EQ(line_len, actual_len);
  }
}
BENCHMARK(BM_QListUncompress)->ArgsProduct({{1, 4, 0}});

class QListZstdTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    SetupMalloc();
  }

  void TearDown() override {
    // Reset thread-local ZSTD dictionary between tests so each test starts clean.
    QList::ShutdownThread();
  }

  // Generate Celery-like JSON entries.
  void PopulateWithCeleryData(QList& ql, unsigned count) {
    for (unsigned i = 0; i < count; ++i) {
      string id = to_string(100000 + i);
      string entry =
          "{\"body\": \"W10=\", \"content-encoding\": \"utf-8\", "
          "\"content-type\": \"application/json\", "
          "\"headers\": {\"lang\": \"py\", \"task\": \"process_job\", "
          "\"id\": \"b3e4b923-8a77-4053-aff0-" +
          id +
          "\", \"shadow\": null, \"eta\": null, "
          "\"expires\": null, \"group\": null, \"retries\": 0, "
          "\"timelimit\": [null, null], "
          "\"root_id\": \"b3e4b923-8a77-4053-aff0-" +
          id +
          "\", \"parent_id\": null, "
          "\"argsrepr\": \"('job" +
          to_string(i) +
          "',)\", \"kwargsrepr\": \"{}\", "
          "\"origin\": \"gen917779@hut\"}, "
          "\"properties\": {\"correlation_id\": \"b3e4b923\", "
          "\"reply_to\": \"9933040c\", \"delivery_mode\": 2, "
          "\"delivery_info\": {\"exchange\": \"\", \"routing_key\": \"my_queue\"}, "
          "\"priority\": 0}}";
      ql.Push(entry, QList::TAIL);
    }
  }
};

TEST_F(QListZstdTest, CompressAndReadAll) {
  QList ql(-1, 0);            // 4KB nodes, no depth-based compression (ZSTD dict replaces it)
  ql.set_compr_threshold(1);  // threshold 1 = trigger as soon as possible
  PopulateWithCeleryData(ql, 500);

  size_t after = ql.MallocUsed(true);
  LOG(INFO) << "Node count: " << ql.node_count() << ", total entries: " << ql.Size()
            << ", MallocUsed: " << after;

  // Verify all entries are readable.
  unsigned count = 0;
  ql.Iterate(
      [&](const QList::Entry& e) {
        EXPECT_NE(e.data(), nullptr);
        EXPECT_GT(e.view().size(), 100u);
        ++count;
        return true;
      },
      0, -1);
  EXPECT_EQ(count, 500u);
}

TEST_F(QListZstdTest, PushAfterCompress) {
  QList ql(-1, 0);
  ql.set_compr_threshold(1);
  PopulateWithCeleryData(ql, 500);

  // Push new entries after compression.
  ql.Push("new_head_entry", QList::HEAD);
  ql.Push("new_tail_entry", QList::TAIL);
  EXPECT_EQ(ql.Size(), 502u);

  // Verify head and tail are readable.
  auto it = ql.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.Get().view(), "new_head_entry");

  it = ql.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Valid());
  EXPECT_EQ(it.Get().view(), "new_tail_entry");
}

TEST_F(QListZstdTest, PopAfterCompress) {
  QList ql(-1, 0);
  ql.set_compr_threshold(1);
  PopulateWithCeleryData(ql, 500);

  EXPECT_EQ(ql.Size(), 500u);

  string head = ql.Pop(QList::HEAD);
  string tail = ql.Pop(QList::TAIL);
  EXPECT_EQ(ql.Size(), 498u);
  EXPECT_FALSE(head.empty());
  EXPECT_FALSE(tail.empty());
}

TEST_F(QListZstdTest, PopDrainsHeadNode) {
  QList ql(-1, 0);  // fill=-1 means 4KB nodes
  ql.set_compr_threshold(1);
  PopulateWithCeleryData(ql, 500);

  unsigned initial_nodes = ql.node_count();
  ASSERT_GE(initial_nodes, 3u);

  // Pop enough elements from HEAD to delete the head node entirely,
  // promoting a formerly-compressed interior node to head.
  while (ql.node_count() == initial_nodes) {
    string val = ql.Pop(QList::HEAD);
    ASSERT_FALSE(val.empty());
  }
  // Head node was deleted and a new head was promoted.
  EXPECT_EQ(ql.node_count(), initial_nodes - 1);

  // Continue popping — the new head must be decompressed and valid.
  string val = ql.Pop(QList::HEAD);
  EXPECT_FALSE(val.empty());
  EXPECT_GT(val.size(), 100u);
}

TEST_F(QListZstdTest, SmallListSkipped) {
  QList ql(-2, 0);  // compress=0 so ZSTD path is active (LZF disabled)
  PopulateWithCeleryData(ql, 5);

  size_t size = ql.MallocUsed(true);
  // Set threshold higher than the list size — dict should not be trained.
  ql.set_compr_threshold(size + 1000);

  auto initial_compressions = QList::stats.zstd_dict_compressions;
  PopulateWithCeleryData(ql, 5);
  EXPECT_EQ(initial_compressions, QList::stats.zstd_dict_compressions);
}

TEST_F(QListZstdTest, IndexAccess) {
  QList ql(-1, 0);
  ql.set_compr_threshold(1);
  PopulateWithCeleryData(ql, 500);

  // Access by positive index.
  auto it = ql.GetIterator(50);
  ASSERT_TRUE(it.Valid());
  auto entry = it.Get();
  EXPECT_NE(entry.data(), nullptr);
  EXPECT_GT(entry.view().size(), 100u);

  // Access by negative index (from tail).
  it = ql.GetIterator(-1);
  ASSERT_TRUE(it.Valid());
  entry = it.Get();
  EXPECT_NE(entry.data(), nullptr);
}

TEST_F(QListZstdTest, IncrementalCompression) {
  // Verify that a newly interior node gets compressed incrementally.
  QList ql(-1, 0);
  ql.set_compr_threshold(1);
  PopulateWithCeleryData(ql, 500);

  // Head and tail must be uncompressed.
  EXPECT_EQ(ql.Head()->encoding, QUICKLIST_NODE_ENCODING_RAW);
  EXPECT_EQ(ql.Tail()->encoding, QUICKLIST_NODE_ENCODING_RAW);

  // Remember the old head node — it will become interior after a head push
  // that creates a new node.
  const QList::Node* old_head = ql.Head();

  // Push enough data to force a new head node (fill=-1 means 4KB nodes).
  for (int i = 0; i < 20; ++i) {
    ql.Push("padding_head_" + to_string(i), QList::HEAD);
  }

  // The old head should now be an interior node and compressed.
  // (It's not head_ anymore, and it's not tail.)
  EXPECT_NE(ql.Head(), old_head);
  EXPECT_TRUE(old_head->IsCompressed());

  // New head/tail must remain uncompressed.
  EXPECT_EQ(ql.Head()->encoding, QUICKLIST_NODE_ENCODING_RAW);
  EXPECT_EQ(ql.Tail()->encoding, QUICKLIST_NODE_ENCODING_RAW);

  // Verify all entries are still readable.
  unsigned count = 0;
  ql.Iterate(
      [&](const QList::Entry& e) {
        ++count;
        return true;
      },
      0, -1);
  EXPECT_EQ(count, ql.Size());
}

TEST_F(QListZstdTest, IncompressibleDataNotCompressed) {
  // Train a dictionary with compressible Celery data.
  QList ql_train(-1, 0);
  ql_train.set_compr_threshold(1);
  PopulateWithCeleryData(ql_train, 500);

  // Dictionary is now trained in thread-local state.
  // Create a new list with random (incompressible) data.
  QList ql(-1, 0);
  ql.set_compr_threshold(1);

  auto initial_bad = QList::stats.bad_compression_attempts;
  auto initial_attempts = QList::stats.compression_attempts;

  // Push random binary data - should not compress well with the Celery-trained dict.
  std::mt19937 rng(42);
  for (unsigned i = 0; i < 200; ++i) {
    string random_blob(512, '\0');
    for (auto& c : random_blob) {
      c = static_cast<char>(rng() % 256);
    }
    ql.Push(random_blob, QList::TAIL);
  }

  // Verify that compression was attempted but mostly rejected.
  uint64_t attempts = QList::stats.compression_attempts - initial_attempts;
  uint64_t bad = QList::stats.bad_compression_attempts - initial_bad;
  EXPECT_GT(attempts, 0u);
  EXPECT_GT(bad, 0u);

  // Verify data integrity.
  unsigned count = 0;
  ql.Iterate(
      [&](const QList::Entry& e) {
        ++count;
        return true;
      },
      0, -1);
  EXPECT_EQ(count, ql.Size());
}

TEST_F(QListZstdTest, StatsTracking) {
  auto initial_attempts = QList::stats.compression_attempts;
  auto initial_successes = QList::stats.zstd_dict_compressions;
  auto initial_bad = QList::stats.bad_compression_attempts;

  QList ql(-1, 0);
  ql.set_compr_threshold(1);
  PopulateWithCeleryData(ql, 500);

  uint64_t attempts = QList::stats.compression_attempts - initial_attempts;
  uint64_t successes = QList::stats.zstd_dict_compressions - initial_successes;
  uint64_t bad = QList::stats.bad_compression_attempts - initial_bad;

  EXPECT_GT(attempts, 0u);
  EXPECT_GT(successes, 0u);
  EXPECT_EQ(attempts, successes + bad);
  // For Celery data, compression should be very effective.
  EXPECT_GT(successes, bad);
}

}  // namespace dfly
