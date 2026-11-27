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

// Delegates AUTH credentials to a trusted HTTP agent configured by --auth_validate_url.
class AuthValidator {
 public:
  struct ValidationResult {
    std::string username;
    std::chrono::steady_clock::time_point expires_at = std::chrono::steady_clock::time_point::max();
  };

  static bool IsEnabled();

  // Tokens are supplied in the password field; nullopt rejects authentication.
  std::optional<ValidationResult> Validate(std::string_view username, std::string_view password);

  void SetValidateFuncForTest(
      std::function<std::optional<ValidationResult>(std::string_view, std::string_view)> f);

 private:
  std::function<std::optional<ValidationResult>(std::string_view, std::string_view)>
      validate_override_;
};

}  // namespace dfly::acl
