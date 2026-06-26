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

TEST_F(CuckooFilterFamilyTest, DumpAndRestore) {
  ASSERT_EQ(Run("cf.reserve cf1 1000 bucketsize 4 maxiterations 10 expansion 2"), "OK");
  EXPECT_THAT(Run({"cf.add", "cf1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.add", "cf1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.add", "cf1", "bar"}), IntArg(1));

  auto dump = Run({"dump", "cf1"}).GetBuf();
  EXPECT_EQ(Run({"restore", "cf2", "0", ToSV(dump)}), "OK");

  EXPECT_EQ(Run("type cf2"), "MBbloomCF");
  EXPECT_THAT(Run({"cf.count", "cf2", "foo"}), IntArg(2));
  EXPECT_THAT(Run({"cf.exists", "cf2", "bar"}), IntArg(1));
  EXPECT_THAT(Run({"cf.exists", "cf2", "nope"}), IntArg(0));

  auto resp = Run({"cf.info", "cf2"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        "Size", testing::_, "Number of buckets", testing::_, "Number of filters",
                        IntArg(1), "Number of items inserted", IntArg(3), "Number of items deleted",
                        IntArg(0), "Bucket size", IntArg(4), "Expansion rate", IntArg(2),
                        "Max iterations", IntArg(10))));
}

TEST_F(CuckooFilterFamilyTest, DumpAndRestoreAfterExpansion) {
  // Force growth past the first sub-filter so the dump covers num_filters > 1.
  ASSERT_EQ(Run("cf.reserve cf1 4 expansion 2"), "OK");
  for (int i = 0; i < 100; ++i) {
    EXPECT_THAT(Run({"cf.add", "cf1", absl::StrCat(i)}), IntArg(1));
  }

  auto dump = Run({"dump", "cf1"}).GetBuf();
  EXPECT_EQ(Run({"restore", "cf2", "0", ToSV(dump)}), "OK");

  for (int i = 0; i < 100; ++i) {
    EXPECT_THAT(Run({"cf.exists", "cf2", absl::StrCat(i)}), IntArg(1)) << i;
  }
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

TEST_F(CuckooFilterFamilyTest, Info) {
  ASSERT_EQ(Run("cf.reserve cf1 1000 bucketsize 4 maxiterations 10 expansion 2"), "OK");
  EXPECT_THAT(Run({"cf.add", "cf1", "foo"}), IntArg(1));

  auto resp = Run({"cf.info", "cf1"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        "Size", testing::_, "Number of buckets", testing::_, "Number of filters",
                        IntArg(1), "Number of items inserted", IntArg(1), "Number of items deleted",
                        IntArg(0), "Bucket size", IntArg(4), "Expansion rate", IntArg(2),
                        "Max iterations", IntArg(10))));
}

TEST_F(CuckooFilterFamilyTest, InfoMissingKey) {
  EXPECT_THAT(Run({"cf.info", "nonexist-key"}), ErrArg("no such key"));
}

TEST_F(CuckooFilterFamilyTest, Count) {
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.count", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.count", "f1", "bar"}), IntArg(0));

  // Missing key returns 0, not an error.
  EXPECT_THAT(Run({"cf.count", "nonexist-key", "blah"}), IntArg(0));
}

TEST_F(CuckooFilterFamilyTest, CountAfterDuplicateAdds) {
  // CF.ADD never dedups, so repeated adds of the same item should each bump the count.
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.count", "f1", "foo"}), IntArg(3));

  EXPECT_THAT(Run({"cf.del", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.count", "f1", "foo"}), IntArg(2));
}

TEST_F(CuckooFilterFamilyTest, Del) {
  EXPECT_THAT(Run({"cf.add", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.del", "f1", "foo"}), IntArg(1));
  EXPECT_THAT(Run({"cf.exists", "f1", "foo"}), IntArg(0));
}

TEST_F(CuckooFilterFamilyTest, DelNonExistentItem) {
  ASSERT_EQ(Run("cf.reserve cf1 1000"), "OK");
  EXPECT_THAT(Run({"cf.del", "cf1", "nope"}), IntArg(0));
}

TEST_F(CuckooFilterFamilyTest, DelMissingKey) {
  EXPECT_THAT(Run({"cf.del", "nonexist-key", "foo"}), ErrArg("no such key"));
}

TEST_F(CuckooFilterFamilyTest, Compact) {
  ASSERT_EQ(Run("cf.reserve cf1 4"), "OK");
  for (int i = 0; i < 30; ++i) {
    EXPECT_THAT(Run({"cf.add", "cf1", absl::StrCat(i)}), IntArg(1));
  }
  for (int i = 0; i < 29; ++i) {
    EXPECT_THAT(Run({"cf.del", "cf1", absl::StrCat(i)}), IntArg(1));
  }

  // Explicit CF.COMPACT should succeed even though CF.DEL's automatic compaction has
  // likely already run by this point — it's just a no-op/cheap pass in that case.
  EXPECT_EQ(Run({"cf.compact", "cf1"}), "OK");
  EXPECT_THAT(Run({"cf.exists", "cf1", "29"}), IntArg(1));
}

TEST_F(CuckooFilterFamilyTest, CompactMissingKey) {
  EXPECT_THAT(Run({"cf.compact", "nonexist-key"}), ErrArg("no such key"));
}

TEST_F(CuckooFilterFamilyTest, Insert) {
  auto resp = Run({"cf.insert", "cf", "items", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
  EXPECT_EQ(Run("type cf"), "MBbloomCF");

  // Duplicates are allowed (like CF.ADD).
  resp = Run({"cf.insert", "cf", "items", "a", "a"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1))));
}

TEST_F(CuckooFilterFamilyTest, InsertWithCapacity) {
  auto resp = Run({"cf.insert", "cf", "capacity", "500", "items", "x"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1))));
}

TEST_F(CuckooFilterFamilyTest, InsertNocreate) {
  // NOCREATE on missing key returns an error.
  EXPECT_THAT(Run({"cf.insert", "cf", "nocreate", "items", "a"}), ErrArg("no such key"));

  // NOCREATE on existing key works fine.
  ASSERT_EQ(Run("cf.reserve cf 1000"), "OK");
  auto resp = Run({"cf.insert", "cf", "nocreate", "items", "a"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1))));
}

TEST_F(CuckooFilterFamilyTest, InsertMissingItemsKeyword) {
  EXPECT_THAT(Run({"cf.insert", "cf", "a", "b"}), ErrArg("ITEMS"));
}

TEST_F(CuckooFilterFamilyTest, InsertWrongType) {
  Run("set str1 foo");
  EXPECT_THAT(Run({"cf.insert", "str1", "items", "a"}), ErrArg("WRONGTYPE"));
}

TEST_F(CuckooFilterFamilyTest, InsertNx) {
  auto resp = Run({"cf.insertnx", "cf", "items", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));

  // Existing items return 0 (like CF.ADDNX).
  resp = Run({"cf.insertnx", "cf", "items", "a", "d"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(0), IntArg(1))));
}

TEST_F(CuckooFilterFamilyTest, InsertNxNocreate) {
  EXPECT_THAT(Run({"cf.insertnx", "cf", "nocreate", "items", "a"}), ErrArg("no such key"));

  ASSERT_EQ(Run("cf.reserve cf 1000"), "OK");
  auto resp = Run({"cf.insertnx", "cf", "nocreate", "items", "a"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1))));
}

TEST_F(CuckooFilterFamilyTest, InsertNxWrongType) {
  Run("set str1 foo");
  EXPECT_THAT(Run({"cf.insertnx", "str1", "items", "a"}), ErrArg("WRONGTYPE"));
}

}  // namespace dfly
