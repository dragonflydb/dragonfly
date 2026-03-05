// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_stats.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/test_utils.h"

using namespace testing;

namespace {

auto get_value(std::string_view row) -> std::string {
  static constexpr std::string_view bytes = " bytes";
  auto value = absl::StripAsciiWhitespace(row.substr(row.find(':') + 1));
  if (value.ends_with(bytes))
    value.remove_suffix(bytes.length());
  return {value.begin(), value.end()};
}

}  // namespace

namespace dfly {

class StringStatsTest : public BaseFamilyTest {
 protected:
  struct ParsedBucket {
    uint64_t total_strings = 0;
    uint64_t unique_strings = 0;
    uint64_t total_bytes = 0;
    double average_length = 0;
    uint64_t estimated_savings = 0;
  };

  static std::optional<ParsedBucket> ParseStats(std::string_view output) {
    std::vector<std::string_view> rows = absl::StrSplit(output, "\n", absl::SkipWhitespace());
    for (auto& row : rows)
      row = absl::StripAsciiWhitespace(row);

    auto it = rows.begin();
    while (it != rows.end() && !it->starts_with("Strings"))
      ++it;

    if (it == rows.end())
      return std::nullopt;

    ParsedBucket bucket;
    EXPECT_NE(it, rows.end());
    EXPECT_TRUE(absl::SimpleAtoi(get_value(*++it), &bucket.total_strings));
    EXPECT_NE(it, rows.end());
    EXPECT_TRUE(absl::SimpleAtoi(get_value(*++it), &bucket.unique_strings));
    EXPECT_NE(it, rows.end());
    EXPECT_TRUE(absl::SimpleAtoi(get_value(*++it), &bucket.total_bytes));
    EXPECT_NE(it, rows.end());
    EXPECT_TRUE(absl::SimpleAtod(get_value(*++it), &bucket.average_length));
    EXPECT_NE(it, rows.end());
    EXPECT_TRUE(absl::SimpleAtoi(get_value(*++it), &bucket.estimated_savings));
    return bucket;
  }
};

TEST_F(StringStatsTest, HashWithDuplicateFields) {
  for (int i = 0; i < 100; ++i) {
    Run({"HSET", absl::StrCat("user:", i), "name", absl::StrCat("name_", i), "email",
         absl::StrCat("email_", i), "age", absl::StrCat(20 + i)});
  }

  const auto resp = Run({"DEBUG", "UNIQ-STRS"});

  EXPECT_THAT(resp.GetString(), HasSubstr("hash"));

  const auto bucket = ParseStats(resp.GetString());
  EXPECT_TRUE(bucket.has_value());

  EXPECT_EQ(bucket->total_strings, 300);
  EXPECT_LE(bucket->unique_strings, 5);
  EXPECT_GE(bucket->unique_strings, 2);
  EXPECT_GT(bucket->estimated_savings, 0);
}

TEST_F(StringStatsTest, SetWithUniqueMembers) {
  for (int i = 0; i < 10; ++i) {
    Run({"SADD", absl::StrCat("set:", i), absl::StrCat("unique_member_", i, "_a"),
         absl::StrCat("unique_member_", i, "_b"), absl::StrCat("unique_member_", i, "_c")});
  }

  const auto resp = Run({"DEBUG", "UNIQ-STRS"});

  const auto bucket = ParseStats(resp.GetString());
  EXPECT_TRUE(bucket.has_value());

  EXPECT_EQ(bucket->total_strings, 30);
  EXPECT_NEAR(bucket->unique_strings, 30, 3);
  EXPECT_LE(bucket->estimated_savings, bucket->total_bytes * 0.15);
}

TEST_F(StringStatsTest, SetWithDuplicateMembers) {
  for (int i = 0; i < 50; ++i) {
    Run({"SADD", absl::StrCat("set:", i), "alpha", "beta", "gamma"});
  }

  const auto resp = Run({"DEBUG", "UNIQ-STRS"});

  const auto bucket = ParseStats(resp.GetString());
  EXPECT_TRUE(bucket.has_value());

  EXPECT_EQ(bucket->total_strings, 150);
  EXPECT_LE(bucket->unique_strings, 5);
  EXPECT_GE(bucket->unique_strings, 2);
  EXPECT_GT(bucket->estimated_savings, 0);
}

TEST_F(StringStatsTest, MultipleTypes) {
  for (int i = 0; i < 10; ++i) {
    Run({"HSET", absl::StrCat("h:", i), "field", "value"});
    Run({"SADD", absl::StrCat("s:", i), "member"});
  }

  const auto resp = Run({"DEBUG", "UNIQ-STRS"});
  const std::string output = resp.GetString();

  EXPECT_THAT(output, HasSubstr("hash"));
  EXPECT_THAT(output, HasSubstr("set"));
}

TEST_F(StringStatsTest, EmptyDatabase) {
  const auto resp = Run({"DEBUG", "UNIQ-STRS"});
  const std::string output = resp.GetString();

  EXPECT_THAT(output, HasSubstr("___begin unique string stats___"));
  EXPECT_THAT(output, HasSubstr("___end unique string stats___"));

  auto bucket = ParseStats(output);
  EXPECT_FALSE(bucket.has_value());
}

}  // namespace dfly
