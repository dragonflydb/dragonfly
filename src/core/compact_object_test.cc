// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/compact_object.h"

#include <mimalloc.h>
#include <xxhash.h>
#include <absl/strings/str_cat.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/flat_set.h"
#include "core/mi_memory_resource.h"

extern "C" {
#include "redis/dict.h"
#include "redis/intset.h"
#include "redis/object.h"
#include "redis/redis_aux.h"
#include "redis/zmalloc.h"
}

namespace dfly {

XXH64_hash_t kSeed = 24061983;
using namespace std;

void PrintTo(const CompactObj& cobj, std::ostream* os) {
  if (cobj.ObjType() == OBJ_STRING) {
    *os << "'" << cobj.ToString() << "' ";
    return;
  }
  *os << "cobj: [" << cobj.ObjType() << "]";
}

class CompactObjectTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    InitRedisTables();  // to initialize server struct.

    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
    SmallString::InitThreadLocal(tlh);
    CompactObj::InitThreadLocal(pmr::get_default_resource());
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

  CompactObj cobj_;
  string tmp_;
};

TEST_F(CompactObjectTest, Basic) {
  robj* rv = createRawStringObject("foo", 3);
  cobj_.ImportRObj(rv);
  return;

  CompactObj a;
  a.SetExpire(true);
  a.SetFlag(true);
  a.SetString("val");
  string res;
  a.GetString(&res);
  EXPECT_EQ("val", res);
  EXPECT_TRUE(a.HasExpire());
  EXPECT_TRUE(a.HasFlag());

  CompactObj b("vala");
  EXPECT_NE(a, b);

  CompactObj c = a.AsRef();
  EXPECT_EQ(a, c);
  EXPECT_TRUE(c.HasExpire());
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

  char ascii2[] = "xxxxxxxxxxxxxx";
  detail::ascii_pack(data.data(), 7, buf);
  detail::ascii_unpack(buf, 7, ascii2);

  ASSERT_EQ('x', ascii2[7]) << ascii2;
  std::string_view actual{ascii2, 7};
  ASSERT_EQ(data.substr(0, 7), actual);
}

TEST_F(CompactObjectTest, IntSet) {
  robj* src = createIntsetObject();
  cobj_.ImportRObj(src);
  EXPECT_EQ(OBJ_SET, cobj_.ObjType());
  EXPECT_EQ(kEncodingIntSet, cobj_.Encoding());

  EXPECT_EQ(0, cobj_.Size());
  intset* is = (intset*)cobj_.RObjPtr();
  uint8_t success = 0;

  is = intsetAdd(is, 10, &success);
  EXPECT_EQ(1, success);
  is = intsetAdd(is, 10, &success);
  EXPECT_EQ(0, success);
  cobj_.SetRObjPtr(is);

  EXPECT_GT(cobj_.MallocUsed(), 0);
}

TEST_F(CompactObjectTest, HSet) {
  robj* src = createHashObject();
  cobj_.ImportRObj(src);

  EXPECT_EQ(OBJ_HASH, cobj_.ObjType());
  EXPECT_EQ(OBJ_ENCODING_LISTPACK, cobj_.Encoding());

  robj* os = cobj_.AsRObj();

  sds key1 = sdsnew("key1");
  sds val1 = sdsnew("val1");

  // returns 0 on insert.
  EXPECT_EQ(0, hashTypeSet(os, key1, val1, HASH_SET_TAKE_FIELD | HASH_SET_TAKE_VALUE));
  cobj_.SyncRObj();
}

TEST_F(CompactObjectTest, ZSet) {
  // unrelated, checking that sds static encoding works.
  // it is used in zset special strings.
  char kMinStrData[] =
      "\110"
      "minstring";
  EXPECT_EQ(9, sdslen(kMinStrData + 1));

  robj* src = createZsetListpackObject();
  cobj_.ImportRObj(src);

  EXPECT_EQ(OBJ_ZSET, cobj_.ObjType());
  EXPECT_EQ(OBJ_ENCODING_LISTPACK, cobj_.Encoding());
}

TEST_F(CompactObjectTest, FlatSet) {
  size_t allocated1, resident1, active1;
  size_t allocated2, resident2, active2;

  zmalloc_get_allocator_info(&allocated1, &active1, &resident1);
  dict *d = dictCreate(&setDictType);
  constexpr size_t kTestSize = 2000;

  for (size_t i = 0; i < kTestSize; ++i) {
    sds key = sdsnew("key:000000000000");
    key = sdscatfmt(key, "%U", i);
    dictEntry *de = dictAddRaw(d, key,NULL);
    de->v.val = NULL;
  }

  zmalloc_get_allocator_info(&allocated2, &active2, &resident2);
  size_t dict_used = allocated2 - allocated1;
  dictRelease(d);

  zmalloc_get_allocator_info(&allocated2, &active2, &resident2);
  EXPECT_EQ(allocated2, allocated1);

  MiMemoryResource mr(mi_heap_get_backing());

  FlatSet fs(&mr);
  for (size_t i = 0; i < kTestSize; ++i) {
    string s = absl::StrCat("key:000000000000", i);
    fs.Add(s);
  }
  zmalloc_get_allocator_info(&allocated2, &active2, &resident2);
  size_t fs_used = allocated2 - allocated1;
  LOG(INFO) << "dict used: " << dict_used << " fs used: " << fs_used;
  EXPECT_LT(fs_used + 8 * kTestSize, dict_used);
}

}  // namespace dfly
