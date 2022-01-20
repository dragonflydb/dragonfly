// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "core/compact_object.h"

#include <xxhash.h>

#include "base/gtest.h"

extern "C" {
#include "redis/object.h"
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
  CompactObj a{s};
  XXH64_hash_t seed = 24061983;
  uint64_t expected_val = XXH3_64bits_withSeed(s.data(), s.size(), seed);
  EXPECT_EQ(18261733907982517826UL, expected_val);
  EXPECT_EQ(expected_val, a.HashCode());
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

}  // namespace dfly
