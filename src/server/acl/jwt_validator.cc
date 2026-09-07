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

ABSL_FLAG(std::string, jwt_validate_url, "",
          "Endpoint used to validate AUTH credentials in JWT mode (see --jwt_validate). "
          "Format: http://host[:port]/path (plain HTTP, no TLS). The endpoint must reply "
          "with JSON {\"valid\": bool, \"username\": string, \"exp\": <unix seconds>} "
          "where `username` names a pre-provisioned ACL user and `exp` (optional) sets "
          "how long the connection stays authenticated before a reauth is required. "
          "Expected to be a local/trusted agent (e.g. localhost) reachable without TLS -- "
          "Dragonfly does not cache validation results itself, so every AUTH is a round "
          "trip to this endpoint.");

ABSL_FLAG(bool, jwt_validate, false,
          "Master on/off switch for JWT-mode auth. When true, every AUTH credential is "
          "sent to --jwt_validate_url for validation instead of being checked against "
          "the local ACL password store; --jwt_validate_url must already be set (at "
          "startup) for this to take effect. Runtime-mutable via CONFIG SET, so JWT auth "
          "can be toggled on/off without a restart once the endpoint is configured.");

ABSL_FLAG(uint32_t, jwt_validate_timeout_ms, 300,
          "Deadline (ms) for the whole JWT validation HTTP call (connect + request), "
          "enforced by JwtValidator itself since the underlying HTTP client has no "
          "built-in timeout. A validator that doesn't respond in time causes AUTH to "
          "be rejected rather than left waiting indefinitely.");

namespace dfly::acl {

using namespace std;
using namespace util;
using http::Client;

namespace {

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

}  // namespace

bool JwtValidator::IsEnabled() {
  if (!absl::GetFlag(FLAGS_jwt_validate))
    return false;
  if (absl::GetFlag(FLAGS_jwt_validate_url).empty()) {
    // Misconfigured: fall back to password auth rather than reject every AUTH.
    LOG_FIRST_N(ERROR, 1)
        << "jwt_validate is enabled but jwt_validate_url is empty; falling back to "
           "password auth until jwt_validate_url is configured (requires a restart)";
    return false;
  }
  return true;
}

void JwtValidator::SetValidateFuncForTest(
    std::function<std::optional<ValidationResult>(std::string_view)> f) {
  validate_override_ = std::move(f);
}

optional<JwtValidator::ValidationResult> JwtValidator::Validate(string_view token) {
  if (validate_override_) {
    return validate_override_(token);
  }

  const string url = absl::GetFlag(FLAGS_jwt_validate_url);
  optional<ParsedUrl> parsed = ParseHttpUrl(url);
  if (!parsed) {
    LOG_FIRST_N(ERROR, 1) << "jwt_validate_url is set but not a valid http:// URL: " << url;
    return nullopt;
  }

  namespace bh = boost::beast::http;
  using ResponseType = bh::response<bh::string_body>;

  bh::request<bh::string_body> req{bh::verb::post, parsed->path, 11 /*http 1.1*/};
  req.set(bh::field::host, parsed->host);
  req.set(bh::field::content_type, "application/json");
  TmpJson body{jsoncons::json_object_arg};
  body["token"] = string(token);
  req.body() = body.to_string();
  req.prepare_payload();

  ProactorBase* proactor = ProactorBase::me();
  DCHECK(proactor) << "JwtValidator::Validate must run on a proactor fiber";

  Client client{proactor};
  // Bounds connect+send+recv (see romange/helio#645); only blocks the calling fiber.
  client.set_timeout_ms(absl::GetFlag(FLAGS_jwt_validate_timeout_ms));

  std::error_code connect_ec = client.Connect(parsed->host, parsed->port);
  if (connect_ec) {
    LOG_EVERY_N(WARNING, 100) << "JWT validation - connection error to " << parsed->host << ":"
                              << parsed->port << ": " << connect_ec.message();
    return nullopt;
  }

  ResponseType res;
  Client::BoostError send_ec = client.Send(req, &res);
  if (send_ec) {
    LOG_EVERY_N(WARNING, 100) << "JWT validation - HTTP request error: " << send_ec.message();
    return nullopt;
  }

  if (res.result() != bh::status::ok) {
    VLOG(1) << "JWT validation - API rejected token, status " << res.result();
    return nullopt;
  }

  std::optional<TmpJson> parsed_body = JsonFromString(res.body());
  if (!parsed_body) {
    LOG_EVERY_N(WARNING, 100) << "JWT validation - malformed (non-JSON) API response";
    return nullopt;
  }
  if (!parsed_body->contains("valid") || !parsed_body->at("valid").is_bool() ||
      !parsed_body->at("valid").as_bool()) {
    return nullopt;
  }
  if (!parsed_body->contains("username") || !parsed_body->at("username").is_string()) {
    LOG_EVERY_N(WARNING, 100) << "JWT validation - API response missing 'username' field";
    return nullopt;
  }
  ValidationResult result{parsed_body->at("username").as_string()};

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

}  // namespace dfly::acl
