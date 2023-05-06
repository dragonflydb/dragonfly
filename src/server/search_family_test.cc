// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class SearchFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(SearchFamilyTest, CreateIndex) {
  EXPECT_EQ(Run({"ft.create", "idx", "ON", "HASH", "PREFIX", "1", "prefix"}), "OK");
}

TEST_F(SearchFamilyTest, Simple) {
  EXPECT_EQ(Run({"ft.create", "i1", "ON", "HASH", "PREFIX", "1", "d:"}), "OK");
  Run({"hset", "d:1", "foo", "baz", "k", "v"});
  Run({"hset", "d:2", "foo", "bar", "k", "v"});
  Run({"hset", "d:3", "foo", "bad", "k", "v"});

  {
    auto resp = Run({"ft.search", "i1", "@foo:bar"});
    EXPECT_THAT(resp, ArrLen(2));  // single key-data pair of d:2

    auto doc = resp.GetVec();
    EXPECT_EQ(doc[0], "d:2");
    EXPECT_THAT(doc[1], ArrLen(4));  // foo and k pairs
  }
  {
    auto resp = Run({"ft.search", "i1", "@foo:bar | @foo:baz"});
    EXPECT_THAT(resp, ArrLen(2 * 2));
  }
  {
    auto resp = Run({"ft.search", "i1", "@foo:(bar|baz|bad)"});
    EXPECT_THAT(resp, ArrLen(3 * 2));
  }
  {
    auto resp = Run({"ft.search", "i1", "@foo:none"});
    EXPECT_THAT(resp, ArrLen(0));
  }
}

}  // namespace dfly
