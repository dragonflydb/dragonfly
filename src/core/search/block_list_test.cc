// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/block_list.h"

#include <algorithm>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly::search {

using namespace std;

class BlockListTest : public ::testing::Test {
 protected:
};

TEST_F(BlockListTest, Basic) {
  BlockList blst;
  blst.Insert(1);
}

}  // namespace dfly::search
