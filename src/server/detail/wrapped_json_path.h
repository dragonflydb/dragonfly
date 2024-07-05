// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <string_view>
#include <utility>
#include <variant>

#include "core/json/json_object.h"
#include "core/json/path.h"
#include "core/string_or_view.h"

namespace dfly {

using Nothing = boost::blank;
using JsonExpression = jsoncons::jsonpath::jsonpath_expression<JsonType>;

template <typename T>
using JsonPathEvaluateCallback = absl::FunctionRef<T(std::string_view, const JsonType&)>;

template <typename T = Nothing> class MutateCallbackResult {
 public:
  MutateCallbackResult() = default;

  explicit MutateCallbackResult(bool should_be_deleted_) : should_be_deleted(should_be_deleted_) {
  }

  MutateCallbackResult(bool should_be_deleted_, T&& value)
      : should_be_deleted(should_be_deleted_), value_(std::forward<T>(value)) {
  }

  bool HasValue() const {
    return value_.has_value();
  }

  T&& GetValue() && {
    return std::move(value_).value();
  }

  bool should_be_deleted;

 private:
  std::optional<T> value_;
};

template <typename T>
using JsonPathMutateCallback =
    absl::FunctionRef<MutateCallbackResult<T>(std::optional<std::string_view>, JsonType*)>;

namespace details {

template <typename T> void OptionalEmplace(std::optional<T>& optional, T&& value) {
  optional.emplace(std::forward<T>(value));
}

template <typename T>
void OptionalEmplace(std::optional<std::optional<T>>& optional, std::optional<T>&& value) {
  if (value.has_value()) {
    optional.emplace(std::forward<std::optional<T>>(value));
  }
}

}  // namespace details

template <typename T> class JsonCallbackResult {
 public:
  using JsonV1Result = std::optional<T>;
  using JsonV2Result = std::vector<T>;

  JsonCallbackResult() = default;

  explicit JsonCallbackResult(bool legacy_mode_is_enabled)
      : legacy_mode_is_enabled_(legacy_mode_is_enabled) {
    if (!legacy_mode_is_enabled_) {
      result_ = JsonV2Result{};
    }
  }

  void AddValue(T&& value) {
    if (IsV1()) {
      details::OptionalEmplace(AsV1(), std::forward<T>(value));
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

 public:
  std::error_code error_code;

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

  template <typename T>
  JsonCallbackResult<T> Mutate(JsonType* json_entry, JsonPathMutateCallback<T> cb) const {
    JsonCallbackResult<T> mutate_result{IsLegacyModePath()};

    auto mutate_callback = [&cb, &mutate_result](std::optional<std::string_view> path,
                                                 JsonType* val) -> bool {
      auto res = cb(path, val);
      if (res.HasValue()) {
        mutate_result.AddValue(std::move(res).GetValue());
      }
      return res.should_be_deleted;
    };

    if (HoldsJsonPath()) {
      const auto& json_path = AsJsonPath();
      json::MutatePath(json_path, mutate_callback, json_entry);
    } else {
      using evaluator_t = jsoncons::jsonpath::detail::jsonpath_evaluator<JsonType, JsonType&>;
      using value_type = evaluator_t::value_type;
      using reference = evaluator_t::reference;
      using json_selector_t = evaluator_t::path_expression_type;

      jsoncons::jsonpath::custom_functions<JsonType> funcs =
          jsoncons::jsonpath::custom_functions<JsonType>();

      auto& ec = mutate_result.error_code;
      jsoncons::jsonpath::detail::static_resources<value_type, reference> static_resources(funcs);
      evaluator_t e;

      json_selector_t expr = e.compile(static_resources, path_.view(), ec);
      if (ec) {
        return mutate_result;
      }

      jsoncons::jsonpath::detail::dynamic_resources<value_type, reference> resources;

      auto f = [&mutate_callback](const jsoncons::jsonpath::basic_path_node<char>& path,
                                  JsonType& val) {
        mutate_callback(jsoncons::jsonpath::to_string(path), &val);
      };

      expr.evaluate(
          resources, *json_entry, json_selector_t::path_node_type{}, *json_entry, std::move(f),
          jsoncons::jsonpath::result_options::nodups | jsoncons::jsonpath::result_options::path);
    }
    return mutate_result;
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
