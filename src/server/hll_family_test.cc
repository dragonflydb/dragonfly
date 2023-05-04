// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hll_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class HllFamilyTest : public BaseFamilyTest {
 public:
 protected:
  static unsigned NumLocked() {
    atomic_uint count = 0;
    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      for (const auto& db : shard->db_slice().databases()) {
        for (const auto& [key, lock] : db->trans_locks) {
          if (!lock.IsFree()) {
            ++count;
          }
        }
      }
    });
    return count;
  }
};

TEST_F(HllFamilyTest, Simple) {
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 1);
}

TEST_F(HllFamilyTest, MultipleValues) {
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "2"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "3"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "3", "4"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 4);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "5"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 5);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3", "4", "5"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 5);
}

TEST_F(HllFamilyTest, AddInvalid) {
  EXPECT_EQ(Run({"set", "key", "..."}), "OK");
  EXPECT_THAT(Run({"pfadd", "key", "1"}), ErrArg(HllFamily::kInvalidHllErr));
  EXPECT_THAT(Run({"pfcount", "key"}), ErrArg(HllFamily::kInvalidHllErr));
}

TEST_F(HllFamilyTest, OtherType) {
  Run({"zadd", "key", "1", "a"});
  EXPECT_THAT(Run({"pfadd", "key", "1"}),
              ErrArg("Operation against a key holding the wrong kind of value"));
  EXPECT_THAT(Run({"pfcount", "key"}),
              ErrArg("Operation against a key holding the wrong kind of value"));
}

TEST_F(HllFamilyTest, CountEmpty) {
  EXPECT_EQ(CheckedInt({"pfcount", "nonexisting"}), 0);
}

TEST_F(HllFamilyTest, CountInvalid) {
  EXPECT_EQ(Run({"set", "key", "..."}), "OK");
  EXPECT_THAT(Run({"pfcount", "key"}), ErrArg(HllFamily::kInvalidHllErr));
}

TEST_F(HllFamilyTest, CountMultiple) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key1"}), 3);

  EXPECT_EQ(CheckedInt({"pfadd", "key2", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key2"}), 3);

  EXPECT_EQ(CheckedInt({"pfadd", "key3", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 2);

  EXPECT_EQ(CheckedInt({"pfadd", "key4", "4", "5"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key4"}), 2);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key4"}), 5);

  EXPECT_EQ(CheckedInt({"pfcount", "non-existing-key1", "non-existing-key2"}), 0);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "non-existing-key"}), 3);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key3"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3", "key4"}), 5);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3", "key4", "non-existing"}), 5);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key4"}), 5);
}

TEST_F(HllFamilyTest, MergeToNew) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "4", "5"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key1", "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 5);
}

TEST_F(HllFamilyTest, MergeToExisting) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "4", "5"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key2", "key1"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key2"}), 5);
}

TEST_F(HllFamilyTest, MergeNonExisting) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key1", "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 3);
}

TEST_F(HllFamilyTest, MergeOverlapping) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key3", "1", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key4", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key5", "3"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key6", "key1", "key2", "key3", "key4", "key5"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key6"}), 3);
}

TEST_F(HllFamilyTest, MergeInvalid) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(Run({"set", "key2", "..."}), "OK");
  EXPECT_THAT(Run({"pfmerge", "key1", "key2"}), ErrArg(HllFamily::kInvalidHllErr));
  EXPECT_EQ(CheckedInt({"pfcount", "key1"}), 3);
  EXPECT_EQ(NumLocked(), 0);
}

}  // namespace dfly
