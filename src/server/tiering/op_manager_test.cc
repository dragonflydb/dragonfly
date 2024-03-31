// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/op_manager.h"

#include <gtest/gtest.h>

#include <memory>

#include "absl/container/flat_hash_map.h"
#include "server/tiering/common.h"
#include "server/tiering/test_common.h"
#include "util/fibers/fibers.h"

namespace dfly::tiering {

using namespace std;
using namespace std::string_literals;

struct OpManagerTest : PoolTestBase, OpManager {
  void Open() {
    EXPECT_FALSE(OpManager::Open("op_manager_test_backing"));
  }

  void Close() {
    OpManager::Close();
    EXPECT_EQ(unlink("op_manager_test_backing"), 0);
  }

  void ReportStashed(EntryId id, DiskSegment segment) {
    stashed_[id] = segment;
  }

  void ReportFetched(EntryId id, std::string_view value, DiskSegment segment) {
  }

  absl::flat_hash_map<EntryId, DiskSegment> stashed_;
};

TEST_F(OpManagerTest, SimpleStashes) {
  pp_->at(0)->Await([this] {
    Open();

    for (unsigned i = 0; i < 100; i++) {
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "cancelled")));
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "cancelled")));
      EXPECT_FALSE(Stash(i, absl::StrCat("VALUE", i, "real")));
    }

    while (stashed_.size() < 100)
      util::ThisFiber::SleepFor(1ms);

    for (unsigned i = 0; i < 100; i++) {
      EXPECT_GE(stashed_[i].offset, i > 0);
      EXPECT_EQ(stashed_[i].length, 10 + (i > 9));
      EXPECT_EQ(Read(i, stashed_[i]).get(), absl::StrCat("VALUE", i, "real"));
    }

    Close();
  });
}

}  // namespace dfly::tiering
