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

TEST(StemmerTest, TryCreate) {
  EXPECT_TRUE(Stemmer::TryCreate("english").has_value());
  EXPECT_TRUE(Stemmer::TryCreate("french").has_value());
  EXPECT_TRUE(Stemmer::TryCreate("german").has_value());
  EXPECT_TRUE(Stemmer::TryCreate("en").has_value());  // ISO 639 alias
  EXPECT_FALSE(Stemmer::TryCreate("klingon").has_value());
  EXPECT_FALSE(Stemmer::TryCreate("").has_value());
}

TEST(StemmerTest, GermanMorphology) {
  Stemmer s{"german"};
  EXPECT_EQ(s.Stem("Haus"), "Haus");
  EXPECT_EQ(s.Stem("Häuser"), "Haus");
  EXPECT_EQ(s.Stem("Hauses"), "Haus");
}

TEST(StemmerPoolTest, LazyInstantiation) {
  StemmerPool pool;
  Stemmer* en = pool.Get("english");
  ASSERT_NE(en, nullptr);
  EXPECT_EQ(en->Stem("learning"), "learn");

  // Same language hands back the same instance (proves caching).
  EXPECT_EQ(pool.Get("english"), en);

  Stemmer* de = pool.Get("german");
  ASSERT_NE(de, nullptr);
  EXPECT_NE(de, en);
  EXPECT_EQ(de->Stem("Häuser"), "Haus");

  EXPECT_EQ(pool.Get("klingon"), nullptr);
}

}  // namespace dfly::search
