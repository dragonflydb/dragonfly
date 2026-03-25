// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/serializer_base.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "server/test_utils.h"

namespace dfly {

class SerializerBaseTest : public BaseFamilyTest, public SerializerBase {
 public:
  SerializerBaseTest() {
  }

 protected:
  using SerializerBase::BucketPhase;
  using SerializerBase::FinishBucketIteration;
  using SerializerBase::MarkBucketSerializing;

  size_t BucketCount() const {
    return bucket_states_.size();
  }

  unsigned SerializeBucket(DbIndex /*db_index*/, PrimeTable::bucket_iterator /*it*/,
                           bool /* on_update */) override {
    return 0;
  }

  void SerializeFetchedEntry(const TieredDelayedEntry& tde, const PrimeValue& pv) override {
  }
};

// --- State-machine tests ---

TEST_F(SerializerBaseTest, MarkThenFinishNoneDelayed) {
  constexpr BucketIdentity bid = 0x1000;

  EXPECT_EQ(0u, BucketCount());
  MarkBucketSerializing(bid);
  EXPECT_EQ(1u, BucketCount());

  FinishBucketIteration(bid);
  EXPECT_EQ(0u, BucketCount());
}

TEST_F(SerializerBaseTest, MultipleBucketsIndependent) {
  constexpr BucketIdentity bid1 = 0x1000;
  constexpr BucketIdentity bid2 = 0x2000;
  constexpr BucketIdentity bid3 = 0x3000;

  MarkBucketSerializing(bid1);
  MarkBucketSerializing(bid2);
  MarkBucketSerializing(bid3);
  EXPECT_EQ(3u, BucketCount());

  FinishBucketIteration(bid2);
  EXPECT_EQ(2u, BucketCount());
}

}  // namespace dfly
