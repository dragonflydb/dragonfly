// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "wrapped_json_path.h"

#include <absl/strings/str_cat.h>
#include <glog/logging.h>

#include <memory>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>

#include "base/flags.h"
#include "server/error.h"

// ABSL_FLAG(bool, jsonpathv2, true,
//           "If true uses Dragonfly jsonpath implementation, "
//           "otherwise uses legacy jsoncons implementation.");

namespace dfly {

template <typename T> void JsonV1CallbackResult<T>::AddValue(T value) {
  if (!result) {
    result.emplace(std::move(value));
  }
}

template <typename T> bool JsonV1CallbackResult<T>::IsV1() const {
  return true;
}

template <typename T> void JsonV2CallbackResult<T>::AddValue(T value) {
  result.emplace_back(std::move(value));
}

template <typename T>
JsonCallbackResultPtr<T> CreateJsonCallbackResult(bool legacy_mode_is_enabled) {
  if (legacy_mode_is_enabled) {
    return std::make_unique<JsonV1CallbackResult<T>>();
  }
  return std::make_unique<JsonV2CallbackResult<T>>();
}

template <typename T> JsonV1CallbackResult<T>& AsV1(JsonCallbackResult<T>& eval_result) {
  return dynamic_cast<JsonV1CallbackResult<T>&>(eval_result);
}

template <typename T> JsonV2CallbackResult<T>& AsV2(JsonCallbackResult<T>& eval_result) {
  return dynamic_cast<JsonV2CallbackResult<T>&>(eval_result);
}

WrappedJsonPath::WrappedJsonPath(json::Path json_path, StringOrView path, bool is_legacy_mode_path)
    : parsed_path_(std::move(json_path)),
      path_(std::move(path)),
      is_legacy_mode_path_(is_legacy_mode_path) {
}

WrappedJsonPath::WrappedJsonPath(JsonExpression expression, StringOrView path,
                                 bool is_legacy_mode_path)
    : parsed_path_(std::move(expression)),
      path_(std::move(path)),
      is_legacy_mode_path_(is_legacy_mode_path) {
}

template <typename T>
JsonCallbackResultPtr<T> WrappedJsonPath::Evaluate(const JsonType* json_entry,
                                                   JsonPathCallback<T> cb) const {
  return Evaluate(json_entry, cb, IsLegacyModePath());
}

template <typename T>
JsonCallbackResultPtr<T> WrappedJsonPath::Evaluate(const JsonType* json_entry,
                                                   JsonPathCallback<T> cb,
                                                   bool legacy_mode_is_enabled) const {
  auto eval_result = CreateJsonCallbackResult<T>(legacy_mode_is_enabled);

  auto eval_callback = [&cb, &eval_result](std::string_view path,
                                           const JsonType& val) {  // TODO(path or key)
    eval_result->AddValue(cb(path, val));
  };

  if (HoldsJsonPath()) {
    const auto& json_path = AsJsonPath();
    json::EvaluatePath(json_path, *json_entry,
                       [&eval_callback](std::optional<std::string_view> key, const JsonType& val) {
                         eval_callback(key ? *key : std::string_view{}, val);
                       });
  } else {
    const auto& json_expression = AsJsonExpression();
    json_expression.evaluate(*json_entry, eval_callback);
  }

  return std::move(eval_result);  // TODO(Remove move)
}

std::error_code WrappedJsonPath::Replace(JsonType* json_entry, json::MutateCallback cb) const {
  if (HoldsJsonPath()) {
    const auto& json_path = AsJsonPath();
    json::MutatePath(json_path, cb, json_entry);  // TODO(in callback key not path)
    return {};
  }

  using evaluator_t = jsoncons::jsonpath::detail::jsonpath_evaluator<JsonType, JsonType&>;
  using value_type = evaluator_t::value_type;
  using reference = evaluator_t::reference;
  using json_selector_t = evaluator_t::path_expression_type;

  jsoncons::jsonpath::custom_functions<JsonType> funcs =
      jsoncons::jsonpath::custom_functions<JsonType>();

  std::error_code ec;
  jsoncons::jsonpath::detail::static_resources<value_type, reference> static_resources(funcs);
  evaluator_t e;

  json_selector_t expr = e.compile(static_resources, path_.view(), ec);
  RETURN_ON_ERR(ec);

  jsoncons::jsonpath::detail::dynamic_resources<value_type, reference> resources;

  auto f = [&cb](const jsoncons::jsonpath::basic_path_node<char>& path, JsonType& val) {
    cb(jsoncons::jsonpath::to_string(path), &val);
  };

  expr.evaluate(
      resources, *json_entry, json_selector_t::path_node_type{}, *json_entry, std::move(f),
      jsoncons::jsonpath::result_options::nodups | jsoncons::jsonpath::result_options::path);
  return ec;
}

bool WrappedJsonPath::RefersToRootElement() const {
  auto path = path_.view();
  return path.empty() || path == kV1PathRootElement || path == kV2PathRootElement;
}

bool WrappedJsonPath::IsLegacyModePath() const {
  return is_legacy_mode_path_;
}

bool WrappedJsonPath::HoldsJsonPath() const {
  return std::holds_alternative<json::Path>(parsed_path_);
}

const json::Path& WrappedJsonPath::AsJsonPath() const {
  return std::get<json::Path>(parsed_path_);
}

const JsonExpression& WrappedJsonPath::AsJsonExpression() const {
  return std::get<JsonExpression>(parsed_path_);
}

ParseResult<JsonExpression> ParseJsonPathAsExpression(std::string_view path) {
  std::error_code ec;
  JsonExpression res = MakeJsonPathExpr(path, ec);
  if (ec)
    return nonstd::make_unexpected(kSyntaxErr);
  return res;
}

ParseResult<WrappedJsonPath> ParseJsonPath(StringOrView path, bool is_legacy_mode_path) {
  if (true) {
    auto path_result = json::ParsePath(path.view());
    RETURN_UNEXPECTED(path_result);
    return WrappedJsonPath{std::move(path_result).value(), std::move(path), is_legacy_mode_path};
  }

  auto expr_result = ParseJsonPathAsExpression(path.view());
  RETURN_UNEXPECTED(expr_result);
  return WrappedJsonPath{std::move(expr_result).value(), std::move(path), is_legacy_mode_path};
}

ParseResult<WrappedJsonPath> ParseJsonPathV1(std::string_view path) {
  if (path == WrappedJsonPath::kV1PathRootElement) {
    return ParseJsonPath(StringOrView::FromView(WrappedJsonPath::kV2PathRootElement), true);
  }

  std::string v2_path =
      absl::StrCat(WrappedJsonPath::kV2PathRootElement, path.front() != '.' ? "." : "",
                   path);  // Convert to V2 path
  return ParseJsonPath(StringOrView::FromString(std::move(v2_path)), true);
}

ParseResult<WrappedJsonPath> ParseJsonPathV2(std::string_view path) {
  return ParseJsonPath(StringOrView::FromView(path), false);
}

bool IsJsonPathV2(std::string_view path) {
  return path.front() == '$';
}

ParseResult<WrappedJsonPath> ParseJsonPath(std::string_view path) {
  DCHECK(!path.empty());
  return IsJsonPathV2(path) ? ParseJsonPathV2(path) : ParseJsonPathV1(path);
}

}  // namespace dfly
