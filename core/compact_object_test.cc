// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/compact_object.h"

#include <xxhash.h>

#include "base/gtest.h"

extern "C" {
#include "redis/object.h"
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
  static void SetUpTestCase() {
    init_zmalloc_threadlocal();
    CompactObj::InitThreadLocal(pmr::get_default_resource());
  }

  CompactObj cs_;
  string tmp_;
};

TEST_F(CompactObjectTest, Basic) {
  robj* rv = createRawStringObject("foo", 3);
  cs_.ImportRObj(rv);

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
  cs_.SetString("0");
  EXPECT_EQ(0, cs_.TryGetInt());
  EXPECT_EQ(cs_, "0");
  EXPECT_EQ("0", cs_.GetSlice(&tmp_));
  EXPECT_EQ(OBJ_STRING, cs_.ObjType());
  cs_.SetString("42");
  EXPECT_EQ(8181779779123079347, cs_.HashCode());
  EXPECT_EQ(OBJ_ENCODING_INT, cs_.Encoding());
}

TEST_F(CompactObjectTest, MediumString) {
  CompactObj obj;
  string tmp(512, 'b');
  obj.SetString(tmp);
  obj.SetString(tmp);
  obj.Reset();
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

}  // namespace dfly
