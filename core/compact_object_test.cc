// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/compact_object.h"
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

TEST_F(CompactObjectTest, Int) {
  cs_.SetString("0");
  EXPECT_EQ(0, cs_.TryGetInt());
  EXPECT_EQ(cs_, "0");
  EXPECT_EQ("0", cs_.GetSlice(&tmp_));
  EXPECT_EQ(OBJ_STRING, cs_.ObjType());
  cs_.SetString("42");
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
