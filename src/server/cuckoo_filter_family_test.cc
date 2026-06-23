// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {

class CuckooFilterFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(CuckooFilterFamilyTest, Reserve) {
  auto resp = Run("cf.reserve cf1 1000");
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(Run("type cf1"), "MBbloomCF");

  resp = Run("cf.reserve cf1 1000");
  EXPECT_THAT(resp, ErrArg("item exists"));

  resp = Run("cf.reserve cf2 0");
  EXPECT_THAT(resp, ErrArg("capacity must be greater than 0"));
}

TEST_F(CuckooFilterFamilyTest, ReserveWithOptions) {
  auto resp = Run("cf.reserve cf1 1000 bucketsize 4 maxiterations 10 expansion 2");
  EXPECT_EQ(resp, "OK");

  resp = Run("cf.reserve cf2 1000 bucketsize 0");
  EXPECT_THAT(resp, ErrArg("bucket size must be between 1 and 255"));

  resp = Run("cf.reserve cf3 1000 bucketsize 256");
  EXPECT_THAT(resp, ErrArg("bucket size must be between 1 and 255"));

  resp = Run("cf.reserve cf4 1000 maxiterations 0");
  EXPECT_THAT(resp, ErrArg("max iterations must be between 1 and 65535"));

  resp = Run("cf.reserve cf5 1000 expansion 32768");
  EXPECT_THAT(resp, ErrArg("expansion must be between 0 and 32767"));
}

TEST_F(CuckooFilterFamilyTest, WrongType) {
  Run("set str1 foo");
  auto resp = Run("cf.reserve str1 1000");
  EXPECT_THAT(resp, ErrArg("WRONGTYPE"));
}

// RDB serialization isn't implemented yet for cuckoo filters (see rdb_save.cc). DUMP must fail
// cleanly instead of crashing the server until that lands.
TEST_F(CuckooFilterFamilyTest, DumpFailsCleanly) {
  ASSERT_EQ(Run("cf.reserve cf1 1000"), "OK");
  auto resp = Run({"dump", "cf1"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

}  // namespace dfly
