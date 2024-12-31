// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/compact_object.h"

#include <absl/strings/str_cat.h>
#include <mimalloc.h>
#include <xxhash.h>

#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/detail/bitpacking.h"
#include "core/flat_set.h"
#include "core/mi_memory_resource.h"
#include "core/string_set.h"

extern "C" {
#include "redis/intset.h"
#include "redis/redis_aux.h"
#include "redis/stream.h"
#include "redis/zmalloc.h"
}

namespace dfly {

XXH64_hash_t kSeed = 24061983;
constexpr size_t kRandomStartIndex = 24;
constexpr size_t kRandomStep = 26;
constexpr float kUnderUtilizedRatio = 1.0f;  // ensure that we would detect
using namespace std;
using namespace jsoncons;
using namespace jsoncons::jsonpath;

void PrintTo(const CompactObj& cobj, std::ostream* os) {
  if (cobj.ObjType() == OBJ_STRING) {
    *os << "'" << cobj.ToString() << "' ";
    return;
  }
  *os << "cobj: [" << cobj.ObjType() << "]";
}

// This is for the mimalloc test - being able to find an address in memory
// where we have memory underutilzation
// see issue number 448 (https://github.com/dragonflydb/dragonfly/issues/448)
std::vector<void*> AllocateForTest(int size, std::size_t allocate_size, int factor1 = 1,
                                   int factor2 = 1) {
  const int kAllocRandomChangeSize = 13;  // just some random value
  std::vector<void*> ptrs;
  for (int index = 0; index < size; index++) {
    auto alloc_size =
        index % kAllocRandomChangeSize == 0 ? allocate_size * factor1 : allocate_size * factor2;
    auto heap_alloc = mi_heap_get_backing();
    void* ptr = mi_heap_malloc(heap_alloc, alloc_size);
    ptrs.push_back(ptr);
  }
  return ptrs;
}

bool HasUnderutilizedMemory(const std::vector<void*>& ptrs, float ratio) {
  auto it = std::find_if(ptrs.begin(), ptrs.end(), [&](auto p) {
    int r = p && zmalloc_page_is_underutilized(p, ratio);
    return r > 0;
  });
  return it != ptrs.end();
}

// Go over ptrs vector and free memory at locations every "steps".
// This is so that we will trigger the under utilization - some
// pages will have "holes" in them and we are expecting to find these pages.
void DeallocateAtRandom(size_t steps, std::vector<void*>* ptrs) {
  for (size_t i = kRandomStartIndex; i < ptrs->size(); i += steps) {
    mi_free(ptrs->at(i));
    ptrs->at(i) = nullptr;
  }
}

static void InitThreadStructs() {
  auto* tlh = mi_heap_get_backing();
  init_zmalloc_threadlocal(tlh);
  SmallString::InitThreadLocal(tlh);
  thread_local MiMemoryResource mi_resource(tlh);
  CompactObj::InitThreadLocal(&mi_resource);
};

static void CheckEverythingDeallocated() {
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

class CompactObjectTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    InitRedisTables();  // to initialize server struct.

    InitThreadStructs();
  }

  static void TearDownTestSuite() {
    CheckEverythingDeallocated();
  }

  CompactObj cobj_;
  string tmp_;
};

TEST_F(CompactObjectTest, WastedMemoryDetection) {
  size_t allocated = 0, commited = 0, wasted = 0;
  // By setting the threshold to high value we are expecting
  // To find locations where we have wasted memory
  float ratio = 0.8;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);
  EXPECT_EQ(allocated, 0);
  EXPECT_EQ(commited, 0);
  EXPECT_EQ(wasted, (commited - allocated));

  std::size_t allocated_mem = 64;
  auto* myheap = mi_heap_get_backing();

  void* p1 = mi_heap_malloc(myheap, 64);

  void* ptrs_end[50];
  for (size_t i = 0; i < 50; ++i) {
    ptrs_end[i] = mi_heap_malloc(myheap, 128);
    allocated_mem += 128;
  }

  allocated = commited = wasted = 0;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);
  EXPECT_EQ(allocated, allocated_mem);
  EXPECT_GT(commited, allocated_mem);
  EXPECT_EQ(wasted, (commited - allocated));
  void* ptr[50];
  // allocate 50
  for (size_t i = 0; i < 50; ++i) {
    ptr[i] = mi_heap_malloc(myheap, 256);
    allocated_mem += 256;
  }

  // At this point all the blocks has committed > 0 and used > 0
  // and since we expecting to find these locations, the size of
  // wasted == commited memory - allocated memory.
  allocated = commited = wasted = 0;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);
  EXPECT_EQ(allocated, allocated_mem);
  EXPECT_GT(commited, allocated_mem);
  EXPECT_EQ(wasted, (commited - allocated));

  // free 50/50 -
  for (size_t i = 0; i < 50; ++i) {
    mi_free(ptr[i]);
    allocated_mem -= 256;
  }

  // After all the memory at block size 256 is free, we would have commited there
  // but the used is expected to be 0, so the number now is different from the
  // case above
  allocated = commited = wasted = 0;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);
  EXPECT_EQ(allocated, allocated_mem);
  EXPECT_GT(commited, allocated_mem);
  // since we release all 256 memory block, it should not be counted
  EXPECT_EQ(wasted, (commited - allocated));
  for (size_t i = 0; i < 50; ++i) {
    mi_free(ptrs_end[i]);
  }
  mi_free(p1);

  // Now that its all freed, we are not expecting to have any wasted memory any more
  allocated = commited = wasted = 0;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);
  EXPECT_EQ(allocated, 0);
  EXPECT_GT(commited, allocated);
  EXPECT_EQ(wasted, (commited - allocated));

  mi_collect(false);
}

TEST_F(CompactObjectTest, WastedMemoryDontCount) {
  // The commited memory per blocks are:
  // 64bit => 4K
  // 128bit => 8k
  // 256 => 16k
  // and so on, which mean every n * sizeof(ptr) ^ 2 == 2^11*2*(n-1) (where n starts with 1)
  constexpr std::size_t kExpectedFor256MemWasted = 0x4000;  // memory block 256
  auto* myheap = mi_heap_get_backing();

  size_t allocated = 0, commited = 0, wasted = 0;
  // By setting the threshold to a very low number
  // we don't expect to find and locations where memory is wasted
  float ratio = 0.01;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);
  EXPECT_EQ(allocated, 0);
  EXPECT_EQ(commited, 0);
  EXPECT_EQ(wasted, (commited - allocated));

  std::size_t allocated_mem = 64;

  void* p1 = mi_heap_malloc(myheap, 64);

  void* ptrs_end[50];
  for (size_t i = 0; i < 50; ++i) {
    ptrs_end[i] = mi_heap_malloc(myheap, 128);
    (void)p1;
    allocated_mem += 128;
  }

  void* ptr[50];

  // allocate 50
  for (size_t i = 0; i < 50; ++i) {
    ptr[i] = mi_heap_malloc(myheap, 256);
    allocated_mem += 256;
  }
  allocated = commited = wasted = 0;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);
  // Threshold is low so we are not expecting any wasted memory to be found.
  EXPECT_EQ(allocated, allocated_mem);
  EXPECT_GT(commited, allocated_mem);
  EXPECT_EQ(wasted, 0);

  // free 50/50 -
  for (size_t i = 0; i < 50; ++i) {
    mi_free(ptr[i]);
    allocated_mem -= 256;
  }
  allocated = commited = wasted = 0;
  zmalloc_get_allocator_wasted_blocks(ratio, &allocated, &commited, &wasted);

  EXPECT_EQ(allocated, allocated_mem);
  EXPECT_GT(commited, allocated_mem);
  // We will detect only wasted memory for block size of
  // 256 - and all of it is wasted.
  EXPECT_EQ(wasted, kExpectedFor256MemWasted);
  // Threshold is low so we are not expecting any wasted memory to be found.
  for (size_t i = 0; i < 50; ++i) {
    mi_free(ptrs_end[i]);
  }
  mi_free(p1);

  mi_collect(false);
}

TEST_F(CompactObjectTest, NonInline) {
  string s(22, 'a');
  CompactObj obj{s};

  uint64_t expected_val = XXH3_64bits_withSeed(s.data(), s.size(), kSeed);
  EXPECT_EQ(18261733907982517826UL, expected_val);
  EXPECT_EQ(expected_val, obj.HashCode());
  EXPECT_EQ(s, obj);

  s.assign(25, 'b');
  obj.SetString(s);
  EXPECT_EQ(s, obj);
  EXPECT_EQ(s.size(), obj.Size());
}

TEST_F(CompactObjectTest, InlineAsciiEncoded) {
  string s = "key:0000000000000";
  uint64_t expected_val = XXH3_64bits_withSeed(s.data(), s.size(), kSeed);
  CompactObj obj{s};
  EXPECT_EQ(expected_val, obj.HashCode());
  EXPECT_EQ(s.size(), obj.Size());
}

TEST_F(CompactObjectTest, Int) {
  cobj_.SetString("0");
  EXPECT_EQ(0, cobj_.TryGetInt());
  EXPECT_EQ(1, cobj_.Size());
  EXPECT_EQ(cobj_, "0");
  EXPECT_EQ("0", cobj_.GetSlice(&tmp_));
  EXPECT_EQ(OBJ_STRING, cobj_.ObjType());

  cobj_.SetExpire(true);
  cobj_.SetString("42");
  EXPECT_EQ(8181779779123079347, cobj_.HashCode());
  EXPECT_EQ(OBJ_ENCODING_INT, cobj_.Encoding());
  EXPECT_EQ(2, cobj_.Size());
  EXPECT_TRUE(cobj_.HasExpire());
}

TEST_F(CompactObjectTest, MediumString) {
  string tmp(511, 'b');

  cobj_.SetString(tmp);
  EXPECT_EQ(tmp.size(), cobj_.Size());

  cobj_.SetString(tmp);
  EXPECT_EQ(tmp.size(), cobj_.Size());
  cobj_.Reset();

  tmp.assign(27463, 'c');
  cobj_.SetString(tmp);
  EXPECT_EQ(27463, cobj_.Size());
}

TEST_F(CompactObjectTest, AsciiUtil) {
  std::string_view data{"aaaaaabb"};
  uint8_t buf[32];

  char outbuf[32] = "xxxxxxxxxxxxxx";
  detail::ascii_pack_simd(data.data(), 7, buf);
  detail::ascii_unpack_simd(buf, 7, outbuf);

  ASSERT_EQ('x', outbuf[7]) << outbuf;
  std::string_view actual{outbuf, 7};
  ASSERT_EQ(data.substr(0, 7), actual);

  string data3;
  for (unsigned i = 0; i < 13; ++i) {
    data3.append("12345678910");
  }
  string act_str(data3.size(), 'y');
  std::vector<uint8_t> binvec(detail::binpacked_len(data3.size()));
  detail::ascii_pack_simd2(data3.data(), data3.size(), binvec.data());
  detail::ascii_unpack_simd(binvec.data(), data3.size(), act_str.data());

  ASSERT_EQ(data3, act_str);
}

TEST_F(CompactObjectTest, IntSet) {
  intset* is = intsetNew();
  cobj_.InitRobj(OBJ_SET, kEncodingIntSet, is);

  EXPECT_EQ(0, cobj_.Size());
  is = (intset*)cobj_.RObjPtr();
  uint8_t success = 0;

  is = intsetAdd(is, 10, &success);
  EXPECT_EQ(1, success);
  is = intsetAdd(is, 10, &success);
  EXPECT_EQ(0, success);
  cobj_.SetRObjPtr(is);

  EXPECT_GT(cobj_.MallocUsed(), 0);
}

TEST_F(CompactObjectTest, ZSet) {
  // unrelated, checking that sds static encoding works.
  // it is used in zset special strings.
  char kMinStrData[] =
      "\110"
      "minstring";
  EXPECT_EQ(9, sdslen(kMinStrData + 1));

  cobj_.InitRobj(OBJ_ZSET, OBJ_ENCODING_LISTPACK, lpNew(0));

  EXPECT_EQ(OBJ_ZSET, cobj_.ObjType());
  EXPECT_EQ(OBJ_ENCODING_LISTPACK, cobj_.Encoding());
}

TEST_F(CompactObjectTest, Hash) {
  uint8_t* lp = lpNew(0);
  lp = lpAppend(lp, reinterpret_cast<const uint8_t*>("foo"), 3);
  lp = lpAppend(lp, reinterpret_cast<const uint8_t*>("barrr"), 5);
  cobj_.InitRobj(OBJ_HASH, kEncodingListPack, lp);
  EXPECT_EQ(OBJ_HASH, cobj_.ObjType());
  EXPECT_EQ(1, cobj_.Size());
}

TEST_F(CompactObjectTest, SBF) {
  cobj_.SetSBF(1000, 0.001, 2);
  EXPECT_EQ(cobj_.ObjType(), OBJ_SBF);
  EXPECT_GT(cobj_.MallocUsed(), 0);
}

TEST_F(CompactObjectTest, MimallocUnderutilzation) {
  // We are testing with the same object size allocation here
  // This test is for https://github.com/dragonflydb/dragonfly/issues/448
  size_t allocation_size = 94;
  int count = 2000;
  std::vector<void*> ptrs = AllocateForTest(count, allocation_size);
  bool found = HasUnderutilizedMemory(ptrs, kUnderUtilizedRatio);
  ASSERT_FALSE(found);
  DeallocateAtRandom(kRandomStep, &ptrs);
  found = HasUnderutilizedMemory(ptrs, kUnderUtilizedRatio);
  ASSERT_TRUE(found);
  for (auto* ptr : ptrs) {
    mi_free(ptr);
  }
}

TEST_F(CompactObjectTest, MimallocUnderutilzationDifferentSizes) {
  // This test uses different objects sizes to cover more use cases
  // related to issue https://github.com/dragonflydb/dragonfly/issues/448
  size_t allocation_size = 97;
  int count = 2000;
  int mem_factor_1 = 3;
  int mem_factor_2 = 2;
  std::vector<void*> ptrs = AllocateForTest(count, allocation_size, mem_factor_1, mem_factor_2);
  bool found = HasUnderutilizedMemory(ptrs, kUnderUtilizedRatio);
  ASSERT_FALSE(found);
  DeallocateAtRandom(kRandomStep, &ptrs);
  found = HasUnderutilizedMemory(ptrs, kUnderUtilizedRatio);
  ASSERT_TRUE(found);
  for (auto* ptr : ptrs) {
    mi_free(ptr);
  }
}

TEST_F(CompactObjectTest, MimallocUnderutilzationWithRealloc) {
  // This test is checking underutilzation with reallocation as well as deallocation
  // of the memory - see issue https://github.com/dragonflydb/dragonfly/issues/448
  size_t allocation_size = 102;
  int count = 2000;
  int mem_factor_1 = 4;
  int mem_factor_2 = 1;

  std::vector<void*> ptrs = AllocateForTest(count, allocation_size, mem_factor_1, mem_factor_2);
  bool found = HasUnderutilizedMemory(ptrs, kUnderUtilizedRatio);
  ASSERT_FALSE(found);
  DeallocateAtRandom(kRandomStep, &ptrs);

  //  This is another case, where we are filling the "gaps" by doing re-allocations
  //  in this case, since we are not setting all the values back it should still have
  //  places that are not used. Plus since we are not looking at the first page
  //  other pages should be underutilized.
  for (size_t i = kRandomStartIndex; i < ptrs.size(); i += kRandomStep) {
    if (!ptrs[i]) {
      ptrs[i] = mi_heap_malloc(mi_heap_get_backing(), allocation_size);
    }
  }
  found = HasUnderutilizedMemory(ptrs, kUnderUtilizedRatio);
  ASSERT_TRUE(found);
  for (auto* ptr : ptrs) {
    mi_free(ptr);
  }
}

TEST_F(CompactObjectTest, JsonTypeTest) {
  using namespace jsoncons;
  // This test verify that we can set a json type
  // and that we "know", it JSON and not a string
  std::string_view json_str = R"(
    {"firstName":"John","lastName":"Smith","age":27,"weight":135.25,"isAlive":true,
    "address":{"street":"21 2nd Street","city":"New York","state":"NY","zipcode":"10021-3100"},
    "phoneNumbers":[{"type":"home","number":"212 555-1234"},{"type":"office","number":"646 555-4567"}],
    "children":[],"spouse":null}
  )";
  std::optional<JsonType> json_option2 =
      JsonFromString(R"({"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}})", CompactObj::memory_resource());

  cobj_.SetString(json_str);
  ASSERT_TRUE(cobj_.ObjType() == OBJ_STRING);  // we set this as a string
  JsonType* failed_json = cobj_.GetJson();
  ASSERT_TRUE(failed_json == nullptr);
  ASSERT_TRUE(cobj_.ObjType() == OBJ_STRING);
  std::optional<JsonType> json_option = JsonFromString(json_str, CompactObj::memory_resource());
  ASSERT_TRUE(json_option.has_value());
  cobj_.SetJson(std::move(json_option.value()));
  ASSERT_TRUE(cobj_.ObjType() == OBJ_JSON);  // and now this is a JSON type
  JsonType* json = cobj_.GetJson();
  ASSERT_TRUE(json != nullptr);
  ASSERT_TRUE(json->contains("firstName"));
  // set second object make sure that we don't have any memory issue
  ASSERT_TRUE(json_option2.has_value());
  cobj_.SetJson(std::move(json_option2.value()));
  ASSERT_TRUE(cobj_.ObjType() == OBJ_JSON);  // still is a JSON type
  json = cobj_.GetJson();
  ASSERT_TRUE(json != nullptr);
  ASSERT_TRUE(json->contains("b"));
  ASSERT_FALSE(json->contains("firstName"));
  std::optional<JsonType> set_array = JsonFromString("", CompactObj::memory_resource());
  // now set it to string again
  cobj_.SetString(R"({"a":{}, "b":{"a":1}, "c":{"a":1, "b":2}})");
  ASSERT_TRUE(cobj_.ObjType() == OBJ_STRING);  // we set this as a string
  failed_json = cobj_.GetJson();
  ASSERT_TRUE(failed_json == nullptr);
}

TEST_F(CompactObjectTest, JsonTypeWithPathTest) {
  std::string_view books_json =
      R"({"books":[{
            "category": "fiction",
            "title" : "A Wild Sheep Chase",
            "author" : "Haruki Murakami"
        },{
            "category": "fiction",
            "title" : "The Night Watch",
            "author" : "Sergei Lukyanenko"
        },{
            "category": "fiction",
            "title" : "The Comedians",
            "author" : "Graham Greene"
        },{
            "category": "memoir",
            "title" : "The Night Watch",
            "author" : "Phillips, David Atlee"
        }]})";
  std::optional<JsonType> json_array = JsonFromString(books_json, CompactObj::memory_resource());
  ASSERT_TRUE(json_array.has_value());
  cobj_.SetJson(std::move(json_array.value()));
  ASSERT_TRUE(cobj_.ObjType() == OBJ_JSON);  // and now this is a JSON type
  auto f = [](const auto& /*path*/, JsonType& book) {
    if (book.at("category") == "memoir" && !book.contains("price")) {
      book.try_emplace("price", 140.0);
    }
  };
  JsonType* json = cobj_.GetJson();
  ASSERT_TRUE(json != nullptr);
  auto allocator_set = jsoncons::combine_allocators(json->get_allocator());
  jsonpath::json_replace(allocator_set, *json, "$.books[*]"sv, f);

  // Check whether we've changed the entry for json in place
  // we should have prices only for memoir books
  JsonType* json2 = cobj_.GetJson();
  ASSERT_TRUE(json != nullptr);
  ASSERT_TRUE(json->contains("books"));
  for (auto&& book : (*json2)["books"].array_range()) {
    // make sure that we add prices only to "memoir"
    if (book.at("category") == "memoir") {
      ASSERT_TRUE(book.contains("price"));
    } else {
      ASSERT_FALSE(book.contains("price"));
    }
  }
}

// Test listpack defragmentation.
// StringMap has built-in defragmantation that is tested in its own test suite.
TEST_F(CompactObjectTest, DefragHash) {
  auto build_str = [](size_t i) { return string(111, 'v') + to_string(i); };

  vector<uint8_t*> lps(10'00);

  for (size_t i = 0; i < lps.size(); i++) {
    uint8_t* lp = lpNew(100);
    for (size_t j = 0; j < 100; j++) {
      auto s = build_str(j);
      lp = lpAppend(lp, reinterpret_cast<const unsigned char*>(s.data()), s.length());
    }
    DCHECK_EQ(lpLength(lp), 100u);
    lps[i] = lp;
  }

  for (size_t i = 0; i < lps.size(); i++) {
    if (i % 10 == 0)
      continue;
    lpFree(lps[i]);
  }

  // Find a listpack that is located on a underutilized page
  uint8_t* target_lp = nullptr;
  for (size_t i = 0; i < lps.size(); i += 10) {
    if (zmalloc_page_is_underutilized(lps[i], 0.8))
      target_lp = lps[i];
  }
  CHECK_NE(target_lp, nullptr);

  // Trigger re-allocation
  cobj_.InitRobj(OBJ_HASH, kEncodingListPack, target_lp);
  ASSERT_TRUE(cobj_.DefragIfNeeded(0.8));

  // Check the pointer changes as the listpack needed defragmentation
  auto lp = (uint8_t*)cobj_.RObjPtr();
  EXPECT_NE(lp, target_lp) << "must have changed due to realloc";

  uint8_t* fptr = lpFirst(lp);
  for (size_t i = 0; i < 100; i++) {
    int64_t len;
    auto* s = lpGet(fptr, &len, nullptr);

    string_view sv{reinterpret_cast<const char*>(s), static_cast<uint64_t>(len)};
    EXPECT_EQ(sv, build_str(i));

    fptr = lpNext(lp, fptr);
  }

  for (size_t i = 0; i < lps.size(); i += 10) {
    if (lps[i] != target_lp)
      lpFree(lps[i]);
  }
}

TEST_F(CompactObjectTest, DefragSet) {
  // This is still not implemented
  StringSet* s = CompactObj::AllocateMR<StringSet>();
  s->Add("str");
  cobj_.InitRobj(OBJ_SET, kEncodingStrMap2, s);
  ASSERT_FALSE(cobj_.DefragIfNeeded(0.8));
}

TEST_F(CompactObjectTest, RawInterface) {
  string str(50, 'a'), tmp, owned;
  cobj_.SetString(str);
  {
    auto raw_blob = cobj_.GetRawString();
    EXPECT_LT(raw_blob.view().size(), str.size());

    raw_blob.MakeOwned();
    cobj_.SetExternal(0, 10);  // dummy external pointer
    cobj_.Materialize(raw_blob.view(), true);

    EXPECT_EQ(str, cobj_.GetSlice(&tmp));
  }

  str.assign(50, char(200));  // non ascii
  cobj_.SetString(str);

  {
    auto raw_blob = cobj_.GetRawString();

    EXPECT_EQ(raw_blob.view(), str);

    raw_blob.MakeOwned();
    cobj_.SetExternal(0, 10);  // dummy external pointer
    cobj_.Materialize(raw_blob.view(), true);

    EXPECT_EQ(str, cobj_.GetSlice(&tmp));
  }
}

TEST_F(CompactObjectTest, lpGetInteger) {
  int64_t val = -1;
  uint8_t* lp = lpNew(0);
  for (int j = 0; j < 60; ++j) {
    lp = lpAppendInteger(lp, val);
    val *= 2;
  }
  val = 1;
  for (int j = 0; j < 600; ++j) {
    string str(j * 500, 'a');
    lp = lpAppend(lp, reinterpret_cast<const uint8_t*>(str.data()), str.size());
  }
  uint8_t* ptr = lpFirst(lp);
  while (ptr) {
    int64_t len1, len2;
    uint8_t* val1 = lpGet(ptr, &len1, nullptr);
    int res = lpGetInteger(ptr, &len2);
    if (res) {
      ASSERT_EQ(len1, len2);
      ASSERT_TRUE(val1 == NULL);
    } else {
      ASSERT_TRUE(val1 != NULL);
    }
    ptr = lpNext(lp, ptr);
  }
  lpFree(lp);
}

static void ascii_pack_naive(const char* ascii, size_t len, uint8_t* bin) {
  const char* end = ascii + len;

  unsigned i = 0;
  while (ascii + 8 <= end) {
    for (i = 0; i < 7; ++i) {
      *bin++ = (ascii[0] >> i) | (ascii[1] << (7 - i));
      ++ascii;
    }
    ++ascii;
  }

  // epilog - we do not pack since we have less than 8 bytes.
  while (ascii < end) {
    *bin++ = *ascii++;
  }
}

static void ascii_unpack_naive(const uint8_t* bin, size_t ascii_len, char* ascii) {
  constexpr uint8_t kM = 0x7F;
  uint8_t p = 0;
  unsigned i = 0;

  while (ascii_len >= 8) {
    for (i = 0; i < 7; ++i) {
      uint8_t src = *bin;  // keep on stack in case we unpack inplace.
      *ascii++ = (p >> (8 - i)) | ((src << i) & kM);
      p = src;
      ++bin;
    }

    ascii_len -= 8;
    *ascii++ = p >> 1;
  }

  DCHECK_LT(ascii_len, 8u);
  for (i = 0; i < ascii_len; ++i) {
    *ascii++ = *bin++;
  }
}

static void BM_PackNaive(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  while (state.KeepRunning()) {
    ascii_pack_naive(val.data(), val.size(), buf);
  }
}
BENCHMARK(BM_PackNaive);

static void BM_Pack(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  while (state.KeepRunning()) {
    detail::ascii_pack(val.data(), val.size(), buf);
  }
}
BENCHMARK(BM_Pack);

static void BM_Pack2(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  while (state.KeepRunning()) {
    detail::ascii_pack(val.data(), val.size(), buf);
  }
}
BENCHMARK(BM_Pack2);

static void BM_PackSimd(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  while (state.KeepRunning()) {
    detail::ascii_pack_simd(val.data(), val.size(), buf);
  }
}
BENCHMARK(BM_PackSimd);

static void BM_PackSimd2(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  while (state.KeepRunning()) {
    detail::ascii_pack_simd2(val.data(), val.size(), buf);
  }
}
BENCHMARK(BM_PackSimd2);

static void BM_UnpackNaive(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  detail::ascii_pack(val.data(), val.size(), buf);

  while (state.KeepRunning()) {
    ascii_unpack_naive(buf, val.size(), val.data());
  }
}
BENCHMARK(BM_UnpackNaive);

static void BM_Unpack(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  detail::ascii_pack(val.data(), val.size(), buf);

  while (state.KeepRunning()) {
    detail::ascii_unpack(buf, val.size(), val.data());
  }
}
BENCHMARK(BM_Unpack);

static void BM_UnpackSimd(benchmark::State& state) {
  string val(1024, 'a');
  uint8_t buf[1024];

  detail::ascii_pack(val.data(), val.size(), buf);

  while (state.KeepRunning()) {
    detail::ascii_unpack_simd(buf, val.size(), val.data());
  }
}
BENCHMARK(BM_UnpackSimd);

static void BM_LpCompare(benchmark::State& state) {
  std::mt19937_64 rd;
  uint8_t* lp = lpNew(0);
  for (unsigned i = 0; i < 100; ++i) {
    lp = lpAppendInteger(lp, rd() % (1ULL << 48));
  }

  string val = absl::StrCat(1ULL << 49);
  while (state.KeepRunning()) {
    uint8_t* elem = lpLast(lp);
    while (elem) {
      lpCompare(elem, reinterpret_cast<const uint8_t*>(val.data()), val.size());
      elem = lpPrev(lp, elem);
    }
  }
  lpFree(lp);
}
BENCHMARK(BM_LpCompare);

static void BM_LpCompareInt(benchmark::State& state) {
  std::mt19937_64 rd;
  uint8_t* lp = lpNew(0);
  for (unsigned i = 0; i < 100; ++i) {
    lp = lpAppendInteger(lp, rd() % (1ULL << 48));
  }

  int64_t val = 1ULL << 49;
  while (state.KeepRunning()) {
    uint8_t* elem = lpLast(lp);
    int64_t sz;
    while (elem) {
      DCHECK_NE(0xFF, *elem);
      lpGetInteger(elem, &sz);
      int res = sz == val;
      benchmark::DoNotOptimize(res);
      elem = lpPrev(lp, elem);
    }
  }
  lpFree(lp);
}
BENCHMARK(BM_LpCompareInt);

static void BM_LpGet(benchmark::State& state) {
  unsigned version = state.range(0);
  uint8_t* lp = lpNew(0);
  int64_t val = -1;
  for (unsigned i = 0; i < 60; ++i) {
    lp = lpAppendInteger(lp, val);
    val *= 2;
  }

  while (state.KeepRunning()) {
    uint8_t* elem = lpLast(lp);
    int64_t ival;
    if (version == 1) {
      while (elem) {
        unsigned char* value = lpGet(elem, &ival, NULL);
        benchmark::DoNotOptimize(value);
        elem = lpPrev(lp, elem);
      }
    } else {
      while (elem) {
        int res = lpGetInteger(elem, &ival);
        benchmark::DoNotOptimize(res);
        elem = lpPrev(lp, elem);
      }
    }
  }
  lpFree(lp);
}
BENCHMARK(BM_LpGet)->Arg(1)->Arg(2);

extern "C" int lpStringToInt64(const char* s, unsigned long slen, int64_t* value);

static void BM_LpString2Int(benchmark::State& state) {
  int version = state.range(0);
  std::mt19937_64 rd;
  vector<string> values;
  for (unsigned i = 0; i < 1000; ++i) {
    int64_t val = rd();
    values.push_back(absl::StrCat(val));
  }

  int64_t ival = 0;
  while (state.KeepRunning()) {
    for (const auto& val : values) {
      int res = version == 1 ? lpStringToInt64(val.data(), val.size(), &ival)
                             : absl::SimpleAtoi(val, &ival);
      benchmark::DoNotOptimize(res);
    }
  }
}
BENCHMARK(BM_LpString2Int)->Arg(1)->Arg(2);

}  // namespace dfly
