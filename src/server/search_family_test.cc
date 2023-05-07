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

const auto kNoResults = IntArg(0);  // tests auto destruct single element arrays

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
    EXPECT_THAT(resp, ArrLen(1 + 2));  // single key-data pair of d:2

    auto doc = resp.GetVec();
    EXPECT_THAT(doc[0], IntArg(1));
    EXPECT_EQ(doc[1], "d:2");
    EXPECT_THAT(doc[2], ArrLen(4));  // foo and k pairs
  }

  EXPECT_THAT(Run({"ft.search", "i1", "@foo:bar | @foo:baz"}), ArrLen(1 + 2 * 2));
  EXPECT_THAT(Run({"ft.search", "i1", "@foo:(bar|baz|bad)"}), ArrLen(1 + 3 * 2));

  EXPECT_THAT(Run({"ft.search", "i1", "@foo:none"}), kNoResults);

  EXPECT_THAT(Run({"ft.search", "iNone", "@foo:bar"}), ErrArg("iNone: no such index"));
  EXPECT_THAT(Run({"ft.search", "i1", "@@NOTAQUERY@@"}), ErrArg("Syntax error"));

  // w: prefix is not part of index
  Run({"hset", "w:2", "foo", "this", "k", "v"});
  EXPECT_THAT(Run({"ft.search", "i1", "@foo:this"}), kNoResults);
}

TEST_F(SearchFamilyTest, NoPrefix) {
  EXPECT_EQ(Run({"ft.create", "i1"}), "OK");
  Run({"hset", "d:1", "a", "one", "k", "v"});
  Run({"hset", "d:2", "b", "two", "k", "v"});
  Run({"hset", "d:3", "c", "three", "k", "v"});

  EXPECT_THAT(Run({"ft.search", "i1", "one | three"}), ArrLen(1 + 2 * 2));
}

}  // namespace dfly
