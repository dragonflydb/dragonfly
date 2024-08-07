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
#include "facade/op_status.h"
#include "glog/logging.h"

namespace dfly {

using facade::OpResult;
using facade::OpStatus;
using Nothing = std::monostate;
using JsonExpression = jsoncons::jsonpath::jsonpath_expression<JsonType>;

template <typename T>
using JsonPathEvaluateCallback = absl::FunctionRef<T(std::string_view, const JsonType&)>;

template <typename T = Nothing> struct MutateCallbackResult {
  MutateCallbackResult() = default;

  MutateCallbackResult(bool should_be_deleted_, T value_)
      : should_be_deleted(should_be_deleted_), value(std::move(value_)) {
  }

  bool should_be_deleted = false;
  std::optional<T> value;
};

template <typename T>
using JsonPathMutateCallback =
    absl::FunctionRef<MutateCallbackResult<T>(std::optional<std::string_view>, JsonType*)>;

namespace details {

template <typename T> void OptionalEmplace(T value, std::optional<T>* optional) {
  optional->emplace(std::move(value));
}

template <typename T>
void OptionalEmplace(std::optional<T> value, std::optional<std::optional<T>>* optional) {
  if (value.has_value()) {
    optional->emplace(std::move(value));
  }
}

}  // namespace details

template <typename T> class JsonCallbackResult {
 public:
  /* In the case of a restricted path (legacy mode), the result consists of a single value */
  using JsonV1Result = std::optional<T>;

  /* In the case of an enhanced path (starts with $), the result is an array of multiple values */
  using JsonV2Result = std::vector<T>;

  JsonCallbackResult() = default;

  explicit JsonCallbackResult(bool legacy_mode_is_enabled, bool save_first_result = false)
      : save_first_result_(save_first_result) {
    if (!legacy_mode_is_enabled) {
      result_ = JsonV2Result{};
    }
  }

  void AddValue(T value) {
    if (IsV1()) {
      if (!save_first_result_) {
        details::OptionalEmplace(std::move(value), &AsV1());
      } else {
        auto& as_v1 = AsV1();
        if (!as_v1.has_value()) {
          details::OptionalEmplace(std::move(value), &as_v1);
        }
      }
    } else {
      AsV2().emplace_back(std::move(value));
    }
  }

  bool IsV1() const {
    return std::holds_alternative<JsonV1Result>(result_);
  }

  JsonV1Result& AsV1() {
    return std::get<JsonV1Result>(result_);
  }

  JsonV2Result& AsV2() {
    return std::get<JsonV2Result>(result_);
  }

  const JsonV1Result& AsV1() const {
    return std::get<JsonV1Result>(result_);
  }

  const JsonV2Result& AsV2() const {
    return std::get<JsonV2Result>(result_);
  }

 private:
  std::variant<JsonV1Result, JsonV2Result> result_;
  bool save_first_result_ = false;
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
  JsonCallbackResult<T> Evaluate(const JsonType* json_entry, JsonPathEvaluateCallback<T> cb,
                                 bool save_first_result) const {
    return Evaluate(json_entry, cb, save_first_result, IsLegacyModePath());
  }

  template <typename T>
  JsonCallbackResult<T> Evaluate(const JsonType* json_entry, JsonPathEvaluateCallback<T> cb,
                                 bool save_first_result, bool legacy_mode_is_enabled) const {
    JsonCallbackResult<T> eval_result{legacy_mode_is_enabled, save_first_result};

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
  OpResult<JsonCallbackResult<T>> Mutate(JsonType* json_entry, JsonPathMutateCallback<T> cb) const {
    JsonCallbackResult<T> mutate_result{IsLegacyModePath()};

    auto mutate_callback = [&cb, &mutate_result](std::optional<std::string_view> path,
                                                 JsonType* val) -> bool {
      auto res = cb(path, val);
      if (res.value.has_value()) {
        mutate_result.AddValue(std::move(res.value).value());
      }
      return res.should_be_deleted;
    };

    if (HoldsJsonPath()) {
      const auto& json_path = AsJsonPath();
      json::MutatePath(json_path, mutate_callback, json_entry);
    } else {
      using namespace jsoncons::jsonpath;
      using namespace jsoncons::jsonpath::detail;
      using Evaluator = jsonpath_evaluator<JsonType, JsonType&>;
      using ValueType = Evaluator::value_type;
      using Reference = Evaluator::reference;
      using JsonSelector = Evaluator::path_expression_type;

      custom_functions<JsonType> funcs = custom_functions<JsonType>();

      std::error_code ec;
      static_resources<ValueType, Reference> static_resources(funcs);
      Evaluator e;

      JsonSelector expr = e.compile(static_resources, path_.view(), ec);
      if (ec) {
        VLOG(1) << "Failed to mutate json with error: " << ec.message();
        return OpStatus::SYNTAX_ERR;
      }

      dynamic_resources<ValueType, Reference> resources;

      auto f = [&mutate_callback](const basic_path_node<char>& path, JsonType& val) {
        mutate_callback(to_string(path), &val);
      };

      expr.evaluate(resources, *json_entry, JsonSelector::path_node_type{}, *json_entry,
                    std::move(f), result_options::nodups | result_options::path);
    }
    return mutate_result;
  }

  bool IsLegacyModePath() const {
    return is_legacy_mode_path_;
  }

  bool RefersToRootElement() const {
    auto path = path_.view();
    return path.empty() || path == kV1PathRootElement || path == kV2PathRootElement;
  }

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
