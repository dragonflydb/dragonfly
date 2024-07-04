// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>
#include <utility>
#include <variant>

#include "core/json/json_object.h"
#include "core/json/path.h"
#include "core/string_or_view.h"

namespace dfly {

using JsonExpression = jsoncons::jsonpath::jsonpath_expression<JsonType>;

template <typename T>
using JsonPathEvaluateCallback = absl::FunctionRef<T(std::string_view, const JsonType&)>;

template <typename T> class JsonCallbackResult {
 public:
  using JsonV1Result = std::optional<T>;
  using JsonV2Result = std::vector<T>;

  explicit JsonCallbackResult(bool legacy_mode_is_enabled)
      : legacy_mode_is_enabled_(legacy_mode_is_enabled) {
    if (!legacy_mode_is_enabled_) {
      result_ = JsonV2Result{};
    }
  }

  void AddValue(T&& value) {
    if (IsV1()) {
      AsV1().emplace(std::forward<T>(value));
    } else {
      AsV2().emplace_back(std::forward<T>(value));
    }
  }

  bool IsV1() const {
    return legacy_mode_is_enabled_;
  }

  JsonV1Result& AsV1() {
    return std::get<JsonV1Result>(result_);
  }

  JsonV2Result& AsV2() {
    return std::get<JsonV2Result>(result_);
  }

 private:
  std::variant<JsonV1Result, JsonV2Result> result_;
  bool legacy_mode_is_enabled_;
};

class WrappedJsonPath {
 public:
  static constexpr std::string_view kV1PathRootElement = ".";
  static constexpr std::string_view kV2PathRootElement = "$";

  WrappedJsonPath(json::Path json_path, StringOrView path, bool is_legacy_mode_path)
      : parsed_path_(std::move(json_path)),
        path_(std::move(path)),
        is_legacy_mode_path_(is_legacy_mode_path) {
  }

  WrappedJsonPath(JsonExpression expression, StringOrView path, bool is_legacy_mode_path)
      : parsed_path_(std::move(expression)),
        path_(std::move(path)),
        is_legacy_mode_path_(is_legacy_mode_path) {
  }

  template <typename T>
  JsonCallbackResult<T> Evaluate(const JsonType* json_entry, JsonPathEvaluateCallback<T> cb) const {
    return Evaluate(json_entry, cb, IsLegacyModePath());
  }

  template <typename T>
  JsonCallbackResult<T> Evaluate(const JsonType* json_entry, JsonPathEvaluateCallback<T> cb,
                                 bool legacy_mode_is_enabled) const {
    JsonCallbackResult<T> eval_result{legacy_mode_is_enabled};

    auto eval_callback = [&cb, &eval_result](std::string_view path, const JsonType& val) {
      eval_result.AddValue(cb(path, val));
    };

    if (HoldsJsonPath()) {
      const auto& json_path = AsJsonPath();
      json::EvaluatePath(
          json_path, *json_entry,
          [&eval_callback](std::optional<std::string_view> key, const JsonType& val) {
            eval_callback(key ? *key : std::string_view{}, val);
          });
    } else {
      const auto& json_expression = AsJsonExpression();
      json_expression.evaluate(*json_entry, eval_callback);
    }

    return eval_result;
  }

  bool IsLegacyModePath() const {
    return is_legacy_mode_path_;
  }

 private:
  bool HoldsJsonPath() const {
    return std::holds_alternative<json::Path>(parsed_path_);
  }

  const json::Path& AsJsonPath() const {
    return std::get<json::Path>(parsed_path_);
  }

  const JsonExpression& AsJsonExpression() const {
    return std::get<JsonExpression>(parsed_path_);
  }

 private:
  std::variant<json::Path, JsonExpression> parsed_path_;
  StringOrView path_;
  bool is_legacy_mode_path_;
};

}  // namespace dfly
