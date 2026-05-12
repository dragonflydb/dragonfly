// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/stemmer.h"

#include "base/gtest.h"

namespace dfly::search {

TEST(StemmerTest, EnglishMorphology) {
  Stemmer s{"english"};

  EXPECT_EQ(s.Stem("learning"), "learn");
  EXPECT_EQ(s.Stem("learn"), "learn");
  EXPECT_EQ(s.Stem("learns"), "learn");
  EXPECT_EQ(s.Stem("learned"), "learn");

  EXPECT_EQ(s.Stem("running"), "run");
  EXPECT_EQ(s.Stem("ran"), "ran");  // irregular: Porter does not unify
  EXPECT_EQ(s.Stem("runs"), "run");
}

TEST(StemmerTest, EmptyInput) {
  Stemmer s{"english"};
  EXPECT_EQ(s.Stem(""), "");
}

TEST(StemmerTest, MoveSemantics) {
  Stemmer a{"english"};
  Stemmer b{std::move(a)};
  EXPECT_EQ(b.Stem("running"), "run");

  Stemmer c{"english"};
  c = std::move(b);
  EXPECT_EQ(c.Stem("learning"), "learn");
}

}  // namespace dfly::search
