// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/acl/jwt_validator.h"

#include <absl/flags/flag.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include <boost/beast/http/string_body.hpp>
#include <ctime>

#include "base/logging.h"
#include "core/json/json_object.h"
#include "util/http/http_client.h"

ABSL_FLAG(bool, auth_validate, false,
          "Delegate AUTH to --auth_validate_url (startup-only). Overrides legacy JWT flags. "
          "A missing or invalid URL rejects AUTH.");
ABSL_FLAG(std::string, auth_validate_url, "",
          "Trusted HTTP auth agent URL. POST JSON {username, password}; tokens use the password "
          "field. Reply with {valid: bool, username: string, exp: unix seconds (optional)}. "
          "The returned username must name an active ACL user. No TLS or local caching.");
ABSL_FLAG(uint32_t, auth_validate_timeout_ms, 300,
          "External auth HTTP timeout (ms); timeout rejects AUTH.");

ABSL_FLAG(bool, jwt_validate, false,
          "Deprecated: use --auth_validate. Sends the legacy token body.");
ABSL_FLAG(std::string, jwt_validate_url, "", "Deprecated: use --auth_validate_url.");
ABSL_FLAG(uint32_t, jwt_validate_timeout_ms, 300, "Deprecated: use --auth_validate_timeout_ms.");

namespace dfly::acl {

using namespace std;
using namespace util;
using http::Client;

namespace {

struct ValidationConfig {
  string url;
  uint32_t timeout_ms;
  bool token_only = false;
};

optional<ValidationConfig> GetValidationConfig() {
  if (absl::GetFlag(FLAGS_auth_validate)) {
    return ValidationConfig{absl::GetFlag(FLAGS_auth_validate_url),
                            absl::GetFlag(FLAGS_auth_validate_timeout_ms)};
  }
  if (absl::GetFlag(FLAGS_jwt_validate)) {
    LOG_FIRST_N(WARNING, 1) << "jwt_validate is deprecated; migrate to auth_validate";
    const string url = absl::GetFlag(FLAGS_jwt_validate_url);
    if (!url.empty())
      return ValidationConfig{url, absl::GetFlag(FLAGS_jwt_validate_timeout_ms), true};
  }
  return nullopt;
}

struct ParsedUrl {
  string host;
  string port{"80"};
  string path{"/"};
};

optional<ParsedUrl> ParseHttpUrl(string_view url) {
  constexpr string_view kScheme = "http://";
  if (!absl::StartsWith(url, kScheme))
    return nullopt;
  url.remove_prefix(kScheme.size());

  ParsedUrl out;
  size_t slash = url.find('/');
  string_view authority = slash == string_view::npos ? url : url.substr(0, slash);
  out.path = slash == string_view::npos ? "/" : string(url.substr(slash));

  size_t colon = authority.find(':');
  if (colon == string_view::npos) {
    out.host = string(authority);
  } else {
    out.host = string(authority.substr(0, colon));
    out.port = string(authority.substr(colon + 1));
  }
  if (out.host.empty())
    return nullopt;

  uint32_t port_num = 0;
  if (!absl::SimpleAtoi(out.port, &port_num) || port_num == 0 || port_num >= (1u << 16))
    return nullopt;

  return out;
}

optional<AuthValidator::ValidationResult> ValidateRequest(const string& url, uint32_t timeout_ms,
                                                          const TmpJson& body) {
  optional<ParsedUrl> parsed = ParseHttpUrl(url);
  if (!parsed) {
    LOG_FIRST_N(ERROR, 1) << "External auth validation URL is not a valid http:// URL";
    return nullopt;
  }

  namespace bh = boost::beast::http;
  using ResponseType = bh::response<bh::string_body>;

  bh::request<bh::string_body> req{bh::verb::post, parsed->path, 11 /*http 1.1*/};
  req.set(bh::field::host, parsed->host);
  req.set(bh::field::content_type, "application/json");
  req.body() = body.to_string();
  req.prepare_payload();

  ProactorBase* proactor = ProactorBase::me();
  DCHECK(proactor) << "External auth validation must run on a proactor fiber";

  Client client{proactor};
  // Bounds connect+send+recv (see romange/helio#645); only blocks the calling fiber.
  client.set_timeout_ms(timeout_ms);

  std::error_code connect_ec = client.Connect(parsed->host, parsed->port);
  if (connect_ec) {
    LOG_EVERY_N(WARNING, 100) << "External auth validation - connection error to " << parsed->host
                              << ":" << parsed->port << ": " << connect_ec.message();
    return nullopt;
  }

  ResponseType res;
  Client::BoostError send_ec = client.Send(req, &res);
  if (send_ec) {
    LOG_EVERY_N(WARNING, 100) << "External auth validation - HTTP request error: "
                              << send_ec.message();
    return nullopt;
  }

  if (res.result() != bh::status::ok) {
    VLOG(1) << "External auth validation - API rejected credentials, status " << res.result();
    return nullopt;
  }

  std::optional<TmpJson> parsed_body = JsonFromString(res.body());
  if (!parsed_body) {
    LOG_EVERY_N(WARNING, 100) << "External auth validation - malformed (non-JSON) API response";
    return nullopt;
  }
  if (!parsed_body->contains("valid") || !parsed_body->at("valid").is_bool() ||
      !parsed_body->at("valid").as_bool()) {
    return nullopt;
  }
  if (!parsed_body->contains("username") || !parsed_body->at("username").is_string()) {
    LOG_EVERY_N(WARNING, 100) << "External auth validation - API response missing 'username' field";
    return nullopt;
  }
  AuthValidator::ValidationResult result{parsed_body->at("username").as_string()};

  // "exp" (optional) determines how long the connection stays authenticated before a
  // reauth is required (see ConnectionContext::auth_expires_at).
  if (!parsed_body->contains("exp") || !parsed_body->at("exp").is_int64()) {
    return result;
  }

  const int64_t exp_epoch_sec = parsed_body->at("exp").as<int64_t>();
  const auto exp_wall = chrono::system_clock::from_time_t(static_cast<time_t>(exp_epoch_sec));
  const auto now_wall = chrono::system_clock::now();
  if (exp_wall <= now_wall) {
    // The API says this token is already expired -- treat as an outright rejection.
    return nullopt;
  }

  // steady_clock drives the actual expiry check; only the duration is derived from
  // the API's wall-clock "exp".
  result.expires_at = chrono::steady_clock::now() + (exp_wall - now_wall);
  return result;
}

}  // namespace

bool AuthValidator::IsEnabled() {
  return GetValidationConfig().has_value();
}

void AuthValidator::SetValidateFuncForTest(
    std::function<std::optional<ValidationResult>(std::string_view, std::string_view)> f) {
  validate_override_ = std::move(f);
}

optional<AuthValidator::ValidationResult> AuthValidator::Validate(string_view username,
                                                                  string_view password) {
  if (validate_override_)
    return validate_override_(username, password);
  auto config = GetValidationConfig();
  if (!config)
    return nullopt;

  TmpJson body{jsoncons::json_object_arg};
  if (config->token_only) {
    body["token"] = string(password);
  } else {
    body["username"] = string(username);
    body["password"] = string(password);
  }
  return ValidateRequest(config->url, config->timeout_ms, body);
}

}  // namespace dfly::acl
