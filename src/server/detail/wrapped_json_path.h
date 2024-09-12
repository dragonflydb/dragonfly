// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <string_view>
#include <utility>
#include <variant>

#include "base/logging.h"
#include "core/json/json_object.h"
#include "core/json/path.h"
#include "core/string_or_view.h"
#include "facade/op_status.h"

namespace dfly {

using facade::OpResult;
using facade::OpStatus;
using Nothing = std::monostate;
using JsonExpression = jsoncons::jsonpath::jsonpath_expression<JsonType>;

template <typename T>
using JsonPathEvaluateCallback = absl::FunctionRef<T(std::string_view, const JsonType&)>;

template <typename T = Nothing> struct MutateCallbackResult {
  MutateCallbackResult() {
  }

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

template <typename T>
void OptionalEmplace(bool keep_defined, std::optional<T> src, std::optional<T>* dest) {
  if (!keep_defined || !dest->has_value()) {
    dest->swap(src);
  }
}

template <typename T> void OptionalEmplace(bool keep_defined, T src, T* dest) {
  if (!keep_defined) {
    *dest = std::move(src);
  }
}

}  // namespace details

template <typename T> class JsonCallbackResult {
  template <typename V> struct is_optional : std::false_type {};

  template <typename V> struct is_optional<std::optional<V>> : std::true_type {};

 public:
  JsonCallbackResult() {
  }

  JsonCallbackResult(bool legacy_mode_is_enabled, bool save_first_result, bool empty_is_nil)
      : only_save_first_(save_first_result),
        is_legacy_(legacy_mode_is_enabled),
        empty_is_nil_(empty_is_nil) {
  }

  void AddValue(T value) {
    if (result_.empty() || !IsV1()) {
      result_.push_back(std::move(value));
      return;
    }

    details::OptionalEmplace(only_save_first_, std::move(value), &result_.front());
  }

  bool IsV1() const {
    return is_legacy_;
  }

  const T& AsV1() const {
    return result_.front();
  }

  const absl::InlinedVector<T, 2>& AsV2() const {
    return std::move(result_);
  }

  bool Empty() const {
    return result_.empty();
  }

  bool ShouldSendNil() const {
    return is_legacy_ && empty_is_nil_ && result_.empty();
  }

  bool ShouldSendWrongType() const {
    if (is_legacy_) {
      if (result_.empty() && !empty_is_nil_)
        return true;

      if constexpr (is_optional<T>::value) {
        return !result_.front().has_value();
      }
    }
    return false;
  }

 private:
  absl::InlinedVector<T, 2> result_;

  bool only_save_first_ = false;
  bool is_legacy_ = false;
  bool empty_is_nil_ = false;
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
    JsonCallbackResult<T> eval_result{legacy_mode_is_enabled, save_first_result, true};

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
  OpResult<JsonCallbackResult<std::optional<T>>> Mutate(JsonType* json_entry,
                                                        JsonPathMutateCallback<T> cb) const {
    JsonCallbackResult<std::optional<T>> mutate_result{IsLegacyModePath(), false, false};

    auto mutate_callback = [&cb, &mutate_result](std::optional<std::string_view> path,
                                                 JsonType* val) -> bool {
      auto res = cb(path, val);
      if (res.value.has_value()) {
        mutate_result.AddValue(std::move(res.value).value());
      } else if (!mutate_result.IsV1()) {
        mutate_result.AddValue(std::nullopt);
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
