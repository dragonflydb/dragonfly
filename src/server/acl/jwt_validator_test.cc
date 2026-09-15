// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/acl/jwt_validator.h"

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>
#include <absl/flags/reflection.h>

#include "base/gtest.h"

ABSL_DECLARE_FLAG(bool, jwt_validate);
ABSL_DECLARE_FLAG(std::string, jwt_validate_url);

namespace dfly::acl {

using namespace std;

class JwtValidatorTest : public testing::Test {
 protected:
  void SetUp() override {
    // Flags are process-global; snapshot and restore so tests can freely flip them
    // without affecting others (gtest doesn't run these in a fresh process each time).
    saved_validate_ = absl::GetFlag(FLAGS_jwt_validate);
    saved_validate_url_ = absl::GetFlag(FLAGS_jwt_validate_url);
  }

  void TearDown() override {
    absl::SetFlag(&FLAGS_jwt_validate, saved_validate_);
    absl::SetFlag(&FLAGS_jwt_validate_url, saved_validate_url_);
  }

 private:
  bool saved_validate_;
  string saved_validate_url_;
};

TEST_F(JwtValidatorTest, IsEnabledFalseWhenFlagOff) {
  absl::SetFlag(&FLAGS_jwt_validate, false);
  absl::SetFlag(&FLAGS_jwt_validate_url, "http://localhost:8080/validate");
  EXPECT_FALSE(JwtValidator::IsEnabled());
}

TEST_F(JwtValidatorTest, IsEnabledFalseWhenUrlEmpty) {
  absl::SetFlag(&FLAGS_jwt_validate, true);
  absl::SetFlag(&FLAGS_jwt_validate_url, "");
  // Misconfiguration -- must fail closed to "disabled" (falls back to password auth)
  // rather than crash or treat every AUTH as a JWT with nowhere to validate it.
  EXPECT_FALSE(JwtValidator::IsEnabled());
}

TEST_F(JwtValidatorTest, IsEnabledTrueWhenBothSet) {
  absl::SetFlag(&FLAGS_jwt_validate, true);
  absl::SetFlag(&FLAGS_jwt_validate_url, "http://localhost:8080/validate");
  EXPECT_TRUE(JwtValidator::IsEnabled());
}

TEST_F(JwtValidatorTest, ValidateOverrideBypassesNetwork) {
  JwtValidator jwt;
  jwt.SetValidateFuncForTest([](string_view token) -> optional<JwtValidator::ValidationResult> {
    if (token == "good-token")
      return JwtValidator::ValidationResult{"alice"};
    return nullopt;
  });

  auto ok = jwt.Validate("good-token");
  ASSERT_TRUE(ok.has_value());
  EXPECT_EQ(ok->username, "alice");
  // Default-constructed ValidationResult never expires.
  EXPECT_EQ(ok->expires_at, chrono::steady_clock::time_point::max());

  auto rejected = jwt.Validate("bad-token");
  EXPECT_FALSE(rejected.has_value());
}

TEST_F(JwtValidatorTest, ValidateOverridePropagatesExpiry) {
  const auto expiry = chrono::steady_clock::now() + chrono::seconds(60);
  JwtValidator jwt;
  jwt.SetValidateFuncForTest([&](string_view) -> optional<JwtValidator::ValidationResult> {
    JwtValidator::ValidationResult result{"bob"};
    result.expires_at = expiry;
    return result;
  });

  auto result = jwt.Validate("whatever");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->username, "bob");
  EXPECT_EQ(result->expires_at, expiry);
}

TEST_F(JwtValidatorTest, EachInstanceHasIndependentOverride) {
  // JwtValidator is intentionally not a singleton (see review discussion) -- confirm two
  // instances don't share state, since DoAuth constructs a fresh one per AUTH call.
  JwtValidator jwt_a;
  jwt_a.SetValidateFuncForTest([](string_view) -> optional<JwtValidator::ValidationResult> {
    return JwtValidator::ValidationResult{"from_a"};
  });

  JwtValidator jwt_b;
  jwt_b.SetValidateFuncForTest([](string_view) -> optional<JwtValidator::ValidationResult> {
    return JwtValidator::ValidationResult{"from_b"};
  });

  EXPECT_EQ(jwt_a.Validate("token")->username, "from_a");
  EXPECT_EQ(jwt_b.Validate("token")->username, "from_b");
}

}  // namespace dfly::acl
