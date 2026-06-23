// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <absl/strings/str_cat.h>

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
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

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

TEST_F(CuckooFilterFamilyTest, AddAutoCreatesAndAllowsDuplicates) {
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_EQ(Run("type f1"), "MBbloomCF");

  // CF.ADD allows duplicate insertions.
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
}

TEST_F(CuckooFilterFamilyTest, AddNxPreventsDuplicates) {
  EXPECT_THAT(Run({"cf.addnx", "cf", "k1"}), IntArg(1));
  EXPECT_THAT(Run({"cf.addnx", "cf", "k1"}), IntArg(0));

  // CF.ADD still allows the duplicate that CF.ADDNX rejected.
  EXPECT_THAT(Run({"cf.add", "cf", "k1"}), IntArg(1));
}

TEST_F(CuckooFilterFamilyTest, AddWrongArity) {
  EXPECT_THAT(Run({"cf.add"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"cf.add", "f1"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"cf.addnx"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"cf.addnx", "f1"}), ErrArg("wrong number of arguments"));
}

TEST_F(CuckooFilterFamilyTest, AddWrongType) {
  Run("set str1 foo");
  EXPECT_THAT(Run({"cf.add", "str1", "foo"}), ErrArg("WRONGTYPE"));
  EXPECT_THAT(Run({"cf.addnx", "str1", "foo"}), ErrArg("WRONGTYPE"));
}

TEST_F(CuckooFilterFamilyTest, AddFilterFull) {
  ASSERT_EQ(Run("cf.reserve cf 4 expansion 0"), "OK");
  for (int i = 0; i < 4; ++i) {
    EXPECT_THAT(Run({"cf.add", "cf", absl::StrCat(i)}), IntArg(1));
  }
  EXPECT_THAT(Run({"cf.add", "cf", "overflow"}), ErrArg("Filter is full"));
}

TEST_F(CuckooFilterFamilyTest, Exists) {
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.exists", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.exists", "f1", "bar"}), IntArg(0));

  // Missing key returns 0, not an error.
  EXPECT_THAT(Run({"cf.exists", "nonexist-key", "blah"}), IntArg(0));
}

TEST_F(CuckooFilterFamilyTest, ExistsWrongArity) {
  EXPECT_THAT(Run({"cf.exists"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"cf.exists", "key"}), ErrArg("wrong number of arguments"));
}

TEST_F(CuckooFilterFamilyTest, ExistsWrongType) {
  Run("set str1 foo");
  EXPECT_THAT(Run({"cf.exists", "str1", "foo"}), IntArg(0));
}

TEST_F(CuckooFilterFamilyTest, MExists) {
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.add", "f1", "bar"}), IntArg(1));
  EXPECT_THAT(Run({"cf.add", "f1", "baz"}), IntArg(1));

  EXPECT_THAT(Run({"cf.mexists", "f1", "foo", "bar", "baz"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
  EXPECT_THAT(Run({"cf.mexists", "f1", "foo", "nope"}),
              RespArray(ElementsAre(IntArg(1), IntArg(0))));

  // Missing key returns an all-zero array, not an error.
  EXPECT_THAT(Run({"cf.mexists", "nonexist-key", "blah"}), RespArray(ElementsAre(IntArg(0))));
}

TEST_F(CuckooFilterFamilyTest, MExistsWrongArity) {
  EXPECT_THAT(Run({"cf.mexists"}), ErrArg("wrong number of arguments"));
  EXPECT_THAT(Run({"cf.mexists", "key"}), ErrArg("wrong number of arguments"));
}

TEST_F(CuckooFilterFamilyTest, MExistsWrongType) {
  Run("set str1 foo");
  EXPECT_THAT(Run({"cf.mexists", "str1", "foo"}), RespArray(ElementsAre(IntArg(0))));
}

}  // namespace dfly
