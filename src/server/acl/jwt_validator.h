// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <chrono>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

namespace dfly::acl {

// Delegates JWT validation to an external HTTP API instead of checking a password
// against the local ACL user registry. Configured via --jwt_validate_url (endpoint)
// and --jwt_validate (on/off, runtime-mutable). When enabled, every AUTH credential is
// treated as a JWT and sent to the endpoint; the returned username must map to a
// pre-provisioned ACL user. No local caching -- every AUTH hits the endpoint directly.
class JwtValidator {
 public:
  struct ValidationResult {
    std::string username;
    // Defaults to "never expires"; set only when the API attaches an "exp" to the token.
    std::chrono::steady_clock::time_point expires_at = std::chrono::steady_clock::time_point::max();
  };

  // True if --jwt_validate is set AND --jwt_validate_url is non-empty.
  static bool IsEnabled();

  // Validates `token` against the external API. Returns nullopt on any failure
  // (invalid/rejected token, timeout, connection error, malformed response).
  std::optional<ValidationResult> Validate(std::string_view token);

  // Test-only: allows unit tests to bypass the real network call.
  void SetValidateFuncForTest(std::function<std::optional<ValidationResult>(std::string_view)> f);

 private:
  std::function<std::optional<ValidationResult>(std::string_view)> validate_override_;
};

}  // namespace dfly::acl
