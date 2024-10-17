// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

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

enum class JsonPathType { kV2, kLegacy /*Or V1*/ };
constexpr JsonPathType kDefaultJsonPathType = JsonPathType::kV2;

struct CallbackResultOptions {
 public:
  enum class SavingOrder { kSaveFirst, kSaveLast };
  enum class OnEmpty { kSendNil, kSendWrongType };

  // Default options for WrappedJsonPath::Evaluate
  static CallbackResultOptions DefaultEvaluateOptions() {
    return CallbackResultOptions{OnEmpty::kSendNil};
  }

  static CallbackResultOptions DefaultEvaluateOptions(SavingOrder saving_order) {
    return {saving_order, OnEmpty::kSendNil};
  }

  // Default options for WrappedJsonPath::Mutate
  static CallbackResultOptions DefaultMutateOptions() {
    return CallbackResultOptions{OnEmpty::kSendWrongType};
  }

  explicit CallbackResultOptions(OnEmpty on_empty_) : on_empty(on_empty_) {
  }

  CallbackResultOptions(JsonPathType path_type_, SavingOrder saving_order_, OnEmpty on_empty_)
      : path_type(path_type_), saving_order(saving_order_), on_empty(on_empty_) {
  }

  std::optional<JsonPathType> path_type;
  SavingOrder saving_order{SavingOrder::kSaveLast};
  OnEmpty on_empty;

 private:
  CallbackResultOptions(SavingOrder saving_order_, OnEmpty on_empty_)
      : saving_order(saving_order_), on_empty(on_empty_) {
  }
};

template <typename T> class JsonCallbackResult {
  template <typename V> struct is_optional : std::false_type {};

  template <typename V> struct is_optional<std::optional<V>> : std::true_type {};

 public:
  using SavingOrder = CallbackResultOptions::SavingOrder;
  using OnEmpty = CallbackResultOptions::OnEmpty;

  JsonCallbackResult()
      : options_(kDefaultJsonPathType, SavingOrder::kSaveLast, OnEmpty::kSendWrongType) {
  }

  explicit JsonCallbackResult(CallbackResultOptions options) : options_(options) {
  }

  void AddValue(T value) {
    if (result_.empty() || !IsV1()) {
      result_.push_back(std::move(value));
      return;
    }

    details::OptionalEmplace(options_.saving_order == SavingOrder::kSaveFirst, std::move(value),
                             &result_.front());
  }

  bool IsV1() const {
    return options_.path_type == JsonPathType::kLegacy;
  }

  const T& AsV1() const {
    return result_.front();
  }

  const auto& AsV2() const {
    return result_;
  }

  bool Empty() const {
    return result_.empty();
  }

  bool ShouldSendNil() const {
    return IsV1() && options_.on_empty == OnEmpty::kSendNil && result_.empty();
  }

  bool ShouldSendWrongType() const {
    if (IsV1()) {
      if (result_.empty() && options_.on_empty == OnEmpty::kSendWrongType)
        return true;

      if constexpr (is_optional<T>::value) {
        return !result_.front().has_value();
      }
    }
    return false;
  }

 private:
  std::vector<T> result_;
  CallbackResultOptions options_;
};

class WrappedJsonPath {
 public:
  static constexpr std::string_view kV1PathRootElement = ".";
  static constexpr std::string_view kV2PathRootElement = "$";

  WrappedJsonPath(json::Path json_path, StringOrView path, JsonPathType path_type)
      : parsed_path_(std::move(json_path)), path_(std::move(path)), path_type_(path_type) {
  }

  WrappedJsonPath(JsonExpression expression, StringOrView path, JsonPathType path_type)
      : parsed_path_(std::move(expression)), path_(std::move(path)), path_type_(path_type) {
  }

  template <typename T>
  JsonCallbackResult<T> Evaluate(const JsonType* json_entry, JsonPathEvaluateCallback<T> cb,
                                 CallbackResultOptions options) const {
    JsonCallbackResult<T> eval_result{InitializePathType(options)};

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
                                                        JsonPathMutateCallback<T> cb,
                                                        CallbackResultOptions options) const {
    JsonCallbackResult<std::optional<T>> mutate_result{InitializePathType(options)};

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
    return path_type_ == JsonPathType::kLegacy;
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
  CallbackResultOptions InitializePathType(CallbackResultOptions options) const {
    if (!options.path_type) {
      options.path_type = path_type_;
    }
    return options;
  }

 private:
  std::variant<json::Path, JsonExpression> parsed_path_;
  StringOrView path_;
  JsonPathType path_type_ = kDefaultJsonPathType;
};

}  // namespace dfly
