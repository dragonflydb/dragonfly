// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/compact_object.h"

#include <mimalloc.h>
#include <xxhash.h>

#include "base/gtest.h"
#include "base/logging.h"

extern "C" {
#include "redis/object.h"
#include "redis/redis_aux.h"
#include "redis/zmalloc.h"
}

namespace dfly {
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

    init_zmalloc_threadlocal();
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

  a.SetString("val");
  string res;
  a.GetString(&res);
  EXPECT_EQ("val", res);

  CompactObj b("vala");
  EXPECT_NE(a, b);

  CompactObj c = a.AsRef();
  EXPECT_EQ(a, c);
}

TEST_F(CompactObjectTest, NonInline) {
  string s(22, 'a');
  CompactObj obj{s};
  XXH64_hash_t seed = 24061983;
  uint64_t expected_val = XXH3_64bits_withSeed(s.data(), s.size(), seed);
  EXPECT_EQ(18261733907982517826UL, expected_val);
  EXPECT_EQ(expected_val, obj.HashCode());
  EXPECT_EQ(s, obj);

  s.assign(25, 'b');
  obj.SetString(s);
  EXPECT_EQ(s, obj);
}

TEST_F(CompactObjectTest, Int) {
  cobj_.SetString("0");
  EXPECT_EQ(0, cobj_.TryGetInt());
  EXPECT_EQ(cobj_, "0");
  EXPECT_EQ("0", cobj_.GetSlice(&tmp_));
  EXPECT_EQ(OBJ_STRING, cobj_.ObjType());
  cobj_.SetString("42");
  EXPECT_EQ(8181779779123079347, cobj_.HashCode());
  EXPECT_EQ(OBJ_ENCODING_INT, cobj_.Encoding());
}

TEST_F(CompactObjectTest, MediumString) {
  string tmp(512, 'b');
  cobj_.SetString(tmp);
  cobj_.SetString(tmp);
  cobj_.Reset();
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
  EXPECT_EQ(OBJ_ENCODING_INTSET, cobj_.Encoding());

  robj* os = cobj_.AsRObj();
  EXPECT_EQ(0, setTypeSize(os));
  sds val1 = sdsnew("10");
  sds val2 = sdsdup(val1);

  EXPECT_EQ(1, setTypeAdd(os, val1));
  EXPECT_EQ(0, setTypeAdd(os, val2));
  EXPECT_EQ(OBJ_ENCODING_INTSET, os->encoding);
  sdsfree(val1);
  sdsfree(val2);
  cobj_.SyncRObj();
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
  // unrelated, checking sds static encoding used in zset special strings.
  char kMinStrData[] = "\110" "minstring";
  EXPECT_EQ(9, sdslen(kMinStrData + 1));

}

}  // namespace dfly
