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
ABSL_DECLARE_FLAG(bool, auth_validate);
ABSL_DECLARE_FLAG(std::string, auth_validate_url);

namespace dfly::acl {

using namespace std;

class AuthValidatorTest : public testing::Test {
 protected:
  void SetUp() override {
    absl::SetFlag(&FLAGS_auth_validate, false);
    absl::SetFlag(&FLAGS_jwt_validate, false);
  }

 private:
  absl::FlagSaver flag_saver_;
};

TEST_F(AuthValidatorTest, IsEnabledFalseWhenFlagOff) {
  absl::SetFlag(&FLAGS_auth_validate_url, "http://localhost:8080/validate");
  EXPECT_FALSE(AuthValidator::IsEnabled());
}

TEST_F(AuthValidatorTest, MissingUrlFailsClosed) {
  absl::SetFlag(&FLAGS_auth_validate, true);
  absl::SetFlag(&FLAGS_auth_validate_url, "");
  EXPECT_TRUE(AuthValidator::IsEnabled());
  EXPECT_FALSE(AuthValidator{}.Validate("alice", "password"));
}

TEST_F(AuthValidatorTest, IsEnabledTrueWhenBothSet) {
  absl::SetFlag(&FLAGS_auth_validate, true);
  absl::SetFlag(&FLAGS_auth_validate_url, "http://localhost:8080/validate");
  EXPECT_TRUE(AuthValidator::IsEnabled());
}

TEST_F(AuthValidatorTest, LegacyJwtRequiresUrl) {
  absl::SetFlag(&FLAGS_jwt_validate, true);
  absl::SetFlag(&FLAGS_jwt_validate_url, "");
  EXPECT_FALSE(AuthValidator::IsEnabled());
  absl::SetFlag(&FLAGS_jwt_validate_url, "http://localhost:8080/validate");
  EXPECT_TRUE(AuthValidator::IsEnabled());
  absl::SetFlag(&FLAGS_jwt_validate, false);
  EXPECT_FALSE(AuthValidator::IsEnabled());
}

TEST_F(AuthValidatorTest, NewModeOverridesLegacyJwt) {
  absl::SetFlag(&FLAGS_jwt_validate, true);
  absl::SetFlag(&FLAGS_jwt_validate_url, "http://localhost:8080/validate");
  absl::SetFlag(&FLAGS_auth_validate, true);
  absl::SetFlag(&FLAGS_auth_validate_url, "ldap://localhost");
  EXPECT_TRUE(AuthValidator::IsEnabled());
  EXPECT_FALSE(AuthValidator{}.Validate("alice", "password"));
}

TEST_F(AuthValidatorTest, ValidateOverrideBypassesNetwork) {
  AuthValidator validator;
  validator.SetValidateFuncForTest(
      [](string_view username, string_view password) -> optional<AuthValidator::ValidationResult> {
        if (username == "alice" && password == "good-token")
          return AuthValidator::ValidationResult{"alice"};
        return nullopt;
      });

  auto ok = validator.Validate("alice", "good-token");
  ASSERT_TRUE(ok.has_value());
  EXPECT_EQ(ok->username, "alice");
  EXPECT_EQ(ok->expires_at, chrono::steady_clock::time_point::max());

  auto rejected = validator.Validate("alice", "bad-token");
  EXPECT_FALSE(rejected.has_value());
}

TEST_F(AuthValidatorTest, ValidateOverridePropagatesExpiry) {
  const auto expiry = chrono::steady_clock::now() + chrono::seconds(60);
  AuthValidator validator;
  validator.SetValidateFuncForTest(
      [&](string_view, string_view) -> optional<AuthValidator::ValidationResult> {
        AuthValidator::ValidationResult result{"bob"};
        result.expires_at = expiry;
        return result;
      });

  auto result = validator.Validate("bob", "whatever");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->username, "bob");
  EXPECT_EQ(result->expires_at, expiry);
}

TEST_F(AuthValidatorTest, EachInstanceHasIndependentOverride) {
  AuthValidator validator_a;
  validator_a.SetValidateFuncForTest(
      [](string_view, string_view) -> optional<AuthValidator::ValidationResult> {
        return AuthValidator::ValidationResult{"from_a"};
      });

  AuthValidator validator_b;
  validator_b.SetValidateFuncForTest(
      [](string_view, string_view) -> optional<AuthValidator::ValidationResult> {
        return AuthValidator::ValidationResult{"from_b"};
      });

  EXPECT_EQ(validator_a.Validate("alice", "token")->username, "from_a");
  EXPECT_EQ(validator_b.Validate("alice", "token")->username, "from_b");
}

}  // namespace dfly::acl
