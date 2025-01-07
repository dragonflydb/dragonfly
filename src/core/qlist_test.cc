// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/qlist.h"

#include <absl/strings/match.h>
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
using absl::StrCat;

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
  EXPECT_TRUE(ql_.Tail() == ql_.Head());
  EXPECT_LE(ql_.MallocUsed(false), ql_.MallocUsed(true));

  auto it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Next());  // Needed to initialize the iterator.

  EXPECT_EQ("abc", it.Get().view());

  ASSERT_FALSE(it.Next());

  ql_.Push("def", QList::TAIL);
  EXPECT_EQ(2, ql_.Size());
  EXPECT_LE(ql_.MallocUsed(false), ql_.MallocUsed(true));

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

TEST_F(QListTest, GetNum) {
  ql_.Push("1251977", QList::HEAD);
  QList::Iterator it = ql_.GetIterator(QList::HEAD);
  ASSERT_TRUE(it.Next());
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
  while (it.Next()) {
    string_view sv = it.Get().view();
    ASSERT_EQ(sizeof(buf), sv.size());
    ASSERT_TRUE(absl::StartsWith(sv, StrCat("hello", i)));
    i++;
  }
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
  ASSERT_TRUE(it.Next());  // must call Next to initialize the iterator.
  ql_.Erase(it);
  it = ql_.GetIterator(QList::TAIL);
  ASSERT_TRUE(it.Next());
  it = ql_.Erase(it);
  ASSERT_FALSE(it.Next());
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
  while (it.Next()) {
    ASSERT_EQ(nums[i], it.Get().ival());
    i++;
  }
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
  ASSERT_TRUE(it.Next());
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
  while (iter.Next()) {
    if (iter.Get() == "bar") {
      iter = ql_.Erase(iter);
    }
  }
  EXPECT_THAT(ToItems(), ElementsAreArray(result));

  ql_.Push("foo", QList::TAIL);

  /* lrem -2 foo */
  iter = ql_.GetIterator(QList::TAIL);
  int del = 2;
  while (iter.Next()) {
    if (iter.Get() == "foo") {
      iter = ql_.Erase(iter);
      del--;
    }
    if (del == 0)
      break;
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
  while (it.Next()) {
    ASSERT_EQ(StrCat("hello", i), it.Get());
    i++;
  }
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
  while (it.Next()) {
    QList::Entry entry = it.Get();
    ASSERT_EQ(StrCat("hello", i), entry);
    i--;
    count++;
  }
  EXPECT_EQ(500, count);
  ASSERT_EQ(0, ql_verify(ql_, 16, 500, 20, 32));

  it = ql_.GetIterator(QList::TAIL);
  i = 0;
  while (it.Next()) {
    ASSERT_EQ(StrCat("hello", i), it.Get());
    i++;
  }
  EXPECT_EQ(500, i);
}

TEST_P(OptionsTest, IterateAfterOne) {
  auto [_, compress] = GetParam();
  ql_ = QList(-2, compress);
  ql_.Push("hello", QList::HEAD);

  QList::Iterator it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
  ql_.Insert(it, "abc", QList::AFTER);

  ASSERT_EQ(0, ql_verify(ql_, 1, 2, 2, 2));

  /* verify results */
  it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("hello", it.Get());

  it = ql_.GetIterator(1);
  ASSERT_TRUE(it.Next());
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
  int i = 0;
  while (it.Next()) {
    if (it.Get() == "hij") {
      it = ql_.Erase(it);
    }
    i++;
  }

  ASSERT_EQ(5, i);

  ASSERT_THAT(ToItems(), ElementsAre("abc", "def", "jkl", "oop"));
}

TEST_P(OptionsTest, InsertBeforeOne) {
  auto [_, compress] = GetParam();
  ql_ = QList(-2, compress);

  ql_.Push("hello", QList::HEAD);
  QList::Iterator it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
  ql_.Insert(it, "abc", QList::BEFORE);
  ql_verify(ql_, 1, 2, 2, 2);

  /* verify results */
  it = ql_.GetIterator(0);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("abc", it.Get());

  it = ql_.GetIterator(1);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("hello", it.Get());
}

TEST_P(OptionsTest, InsertWithHeadFull) {
  auto [_, compress] = GetParam();
  ql_ = QList(4, compress);

  for (int i = 0; i < 10; i++)
    ql_.Push(StrCat("hello", i), QList::TAIL);

  ql_.set_fill(-1);
  QList::Iterator it = ql_.GetIterator(-10);
  ASSERT_TRUE(it.Next());

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
  ASSERT_TRUE(it.Next());

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
  while (it.Next()) {
    if (it.Get() == "bob") {
      ql_.Insert(it, "bar", QList::BEFORE);
      break; /* didn't we fix insert-while-iterating? */
    }
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
    ASSERT_TRUE(it.Next());
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
    ASSERT_TRUE(it.Next());
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

  while (it.Next()) {
    ASSERT_EQ(strings[j], it.Get());
    j++;
  }
}

TEST_P(OptionsTest, IndexFrom500) {
  auto [fill, compress] = GetParam();
  ql_ = QList(fill, compress);
  for (int i = 0; i < 500; i++)
    ql_.Push(StrCat("hello", i + 1), QList::TAIL);

  QList::Iterator it = ql_.GetIterator(1);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("hello2", it.Get());
  it = ql_.GetIterator(200);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("hello201", it.Get());

  it = ql_.GetIterator(-1);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("hello500", it.Get());

  it = ql_.GetIterator(-2);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("hello499", it.Get());

  it = ql_.GetIterator(-100);
  ASSERT_TRUE(it.Next());
  ASSERT_EQ("hello401", it.Get());

  it = ql_.GetIterator(500);
  ASSERT_FALSE(it.Next());
}

}  // namespace dfly
