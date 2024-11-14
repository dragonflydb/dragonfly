// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <gmock/gmock.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;
using namespace testing;

static int _ql_verify_compress(const QList& ql) {
  int errors = 0;
  unsigned compress_param = ql.compress_param();
  if (compress_param > 0) {
    const quicklistNode* node = ql.Head();
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
    node = node->prev;
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

  if (ql.Head() && head_count != ql.Head()->count && head_count != lpLength(ql.Head()->entry)) {
    LOG(ERROR) << absl::StrFormat("head count wrong: expected %u got cached %u vs. actual %lu",
                                  head_count, ql.Head()->count, lpLength(ql.Head()->entry));
    errors++;
  }

  if (ql.Tail() && tail_count != ql.Tail()->count && tail_count != lpLength(ql.Tail()->entry)) {
    LOG(ERROR) << "tail count wrong: expected " << tail_count << "got cached " << ql.Tail()->count
               << " vs. actual " << lpLength(ql.Tail()->entry);
    errors++;
  }

  errors += _ql_verify_compress(ql);
  return errors;
}

class QListTest : public ::testing::Test {
 protected:
  QListTest() : mr_(mi_heap_get_backing()) {
  }

  static void SetUpTestSuite() {
    // configure redis lib zmalloc which requires mimalloc heap to work.
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
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

  auto it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Next());  // Needed to initialize the iterator.

  EXPECT_EQ("abc", it.Get().view());

  ASSERT_FALSE(it.Next());

  ql_.Push("def", QList::TAIL);
  EXPECT_EQ(2, ql_.Size());

  it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("def", it.Get().view());

  ASSERT_TRUE(it.Next());
  EXPECT_EQ("abc", it.Get().view());
  ASSERT_FALSE(it.Next());

  it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("abc", it.Get().view());
  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("def", it.Get().view());

  vector<string> items = ToItems();

  EXPECT_THAT(items, ElementsAre("abc", "def"));
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
  ASSERT_TRUE(it.Next());

  // Erase the items one by one.
  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("abc", "123456"));
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("abc", it.Get().view());

  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre("123456"));
  ASSERT_TRUE(it.Next());
  ASSERT_EQ(123456, it.Get().ival());

  it = ql_.Erase(it);
  items = ToItems();
  EXPECT_THAT(items, ElementsAre());
  ASSERT_FALSE(it.Next());
  EXPECT_EQ(0, ql_.Size());
}

TEST_F(QListTest, PushPlain) {
  // push a value large enough to trigger plain node insertion.
  string val(9000, 'a');
  ql_.Push(val, QList::HEAD);
  auto items = ToItems();
  EXPECT_THAT(items, ElementsAre(val));
}

using FillCompress = tuple<int, unsigned>;

class PrintToFillCompress {
 public:
  std::string operator()(const TestParamInfo<FillCompress>& info) const {
    int fill = get<0>(info.param);
    int compress = get<1>(info.param);
    string fill_str = fill >= 0 ? absl::StrCat("f", fill) : absl::StrCat("fminus", -fill);
    return absl::StrCat(fill_str, "compress", compress);
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
    ASSERT_TRUE(it.Next());
    ASSERT_EQ(nums[i], it.Get().ival()) << i;
  }

  auto it = ql_.GetIterator(nums.size());
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("xxxxxxxxxxxxxxxxxxxx", it.Get().view());
}

TEST_P(OptionsTest, DelRangeA) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);
  long long nums[5000];
  for (int i = 0; i < 33; i++) {
    nums[i] = -5157318210846258176 + i;
    ql_.Push(absl::StrCat(nums[i]), QList::TAIL);
  }
  if (fill == 32)
    ql_verify(ql_, 2, 33, 32, 1);

  /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
  ql_.Erase(0, 3);
  ql_.Erase(-29, 4000); /* make sure not loop forever */
  if (fill == 32)
    ql_verify(ql_, 1, 1, 1, 1);

  auto it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
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
  if (fill == 32)
    ql_verify(ql_, 2, 33, 32, 1);

  /* ltrim 5 16 (keep [5,16] inclusive = 12 remaining) */
  ql_.Erase(0, 5);
  ql_.Erase(-16, 16);
  if (fill == 32)
    ql_verify(ql_, 1, 12, 12, 12);

  auto it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(5, it.Get().ival());

  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ(16, it.Get().ival());

  ql_.Push("bobobob", QList::TAIL);
  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Next());
  EXPECT_EQ("bobobob", it.Get().view());

  for (int i = 0; i < 12; i++) {
    it = ql_.GetIterator(i);
    ASSERT_TRUE(it.Next());
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
  if (fill == 32)
    ql_verify(ql_, 2, 33, 32, 1);

  /* ltrim 3 3 (keep [3,3] inclusive = 1 remaining) */
  ql_.Erase(0, 3);
  ql_.Erase(-29, 4000); /* make sure not loop forever */
  if (fill == 32)
    ql_verify(ql_, 1, 1, 1, 1);
  auto it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
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
  if (fill == 32)
    ql_verify(ql_, 2, 33, 32, 1);
  ql_.Erase(-12, 3);

  ASSERT_EQ(30, ql_.Size());
}

}  // namespace dfly
