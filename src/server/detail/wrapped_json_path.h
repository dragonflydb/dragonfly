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

namespace details {
template <typename T>
void OptionalEmplace(bool keep_defined, std::optional<T> src, std::optional<T>* dest);

template <typename T> void OptionalEmplace(bool keep_defined, T src, T* dest);
}  // namespace details

template <typename T>
using JsonPathReadOnlyCallback = absl::FunctionRef<T(std::string_view, const JsonType&)>;

template <typename T = Nothing> struct MutateCallbackResult {
  bool should_be_deleted = false;
  std::optional<T> value;
};

template <typename T>
using JsonPathMutateCallback =
    absl::FunctionRef<MutateCallbackResult<T>(std::optional<std::string_view>, JsonType*)>;

enum class JsonPathType { kV2, kLegacy /*Or V1*/ };
constexpr JsonPathType kDefaultJsonPathType = JsonPathType::kV2;

struct CallbackResultOptions {
 public:
  enum class SavingOrder { kSaveFirst, kSaveLast };
  enum class OnEmpty { kSendNil, kSendWrongType };

  // Default options for WrappedJsonPath::ExecuteReadOnlyCallback
  static CallbackResultOptions DefaultReadOnlyOptions(
      SavingOrder saving_order = SavingOrder::kSaveLast);
  // Default options for WrappedJsonPath::ExecuteMutateCallback
  static CallbackResultOptions DefaultMutateOptions();

  OnEmpty on_empty;
  SavingOrder saving_order{SavingOrder::kSaveLast};
  std::optional<JsonPathType> path_type{std::nullopt};
};

template <typename T> class JsonCallbackResult {
 private:
  template <typename V> struct is_optional : std::false_type {};

  template <typename V> struct is_optional<std::optional<V>> : std::true_type {};

 public:
  using SavingOrder = CallbackResultOptions::SavingOrder;
  using OnEmpty = CallbackResultOptions::OnEmpty;

  JsonCallbackResult() = default;

  explicit JsonCallbackResult(CallbackResultOptions options);

  void AddValue(T value);

  bool Empty() const;

  bool IsV1() const;
  const T& AsV1() const;
  const auto& AsV2() const;

  bool ShouldSendNil() const;
  bool ShouldSendWrongType() const;

 private:
  std::vector<T> result_;
  CallbackResultOptions options_{OnEmpty::kSendWrongType, SavingOrder::kSaveLast,
                                 kDefaultJsonPathType};
};

class WrappedJsonPath {
 public:
  static constexpr std::string_view kV1PathRootElement = ".";
  static constexpr std::string_view kV2PathRootElement = "$";

  WrappedJsonPath(json::Path json_path, StringOrView path, JsonPathType path_type);

  WrappedJsonPath(JsonExpression expression, StringOrView path, JsonPathType path_type);

  template <typename T>
  JsonCallbackResult<T> ExecuteReadOnlyCallback(const JsonType* json_entry,
                                                JsonPathReadOnlyCallback<T> cb,
                                                CallbackResultOptions options) const;

  template <typename T>
  OpResult<JsonCallbackResult<std::optional<T>>> ExecuteMutateCallback(
      JsonType* json_entry, JsonPathMutateCallback<T> cb, CallbackResultOptions options) const;

  bool IsLegacyModePath() const;

  bool RefersToRootElement() const;

  // Returns true if this is internal implementation of json path
  // Check AsJsonPath
  bool HoldsJsonPath() const;

  // Internal implementation of json path
  const json::Path& AsJsonPath() const;
  // Jsoncons implementation of json path
  const JsonExpression& AsJsonExpression() const;

  // Returns the path as a string_view.
  std::string_view Path() const;

 private:
  CallbackResultOptions InitializePathType(CallbackResultOptions options) const;

  std::variant<json::Path, JsonExpression> parsed_path_;
  StringOrView path_;
  JsonPathType path_type_ = kDefaultJsonPathType;
};

// Implementation
/******************************************************************/
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

inline CallbackResultOptions CallbackResultOptions::DefaultReadOnlyOptions(
    SavingOrder saving_order) {
  return CallbackResultOptions{OnEmpty::kSendNil, saving_order};
}

inline CallbackResultOptions CallbackResultOptions::DefaultMutateOptions() {
  return CallbackResultOptions{OnEmpty::kSendWrongType};
}

template <typename T>
JsonCallbackResult<T>::JsonCallbackResult(CallbackResultOptions options) : options_(options) {
}

template <typename T> void JsonCallbackResult<T>::AddValue(T value) {
  if (result_.empty() || !IsV1()) {
    result_.push_back(std::move(value));
    return;
  }

  details::OptionalEmplace(options_.saving_order == SavingOrder::kSaveFirst, std::move(value),
                           &result_.front());
}

template <typename T> bool JsonCallbackResult<T>::Empty() const {
  return result_.empty();
}

template <typename T> bool JsonCallbackResult<T>::IsV1() const {
  return options_.path_type == JsonPathType::kLegacy;
}

template <typename T> const T& JsonCallbackResult<T>::AsV1() const {
  return result_.front();
}

template <typename T> const auto& JsonCallbackResult<T>::AsV2() const {
  return result_;
}

template <typename T> bool JsonCallbackResult<T>::ShouldSendNil() const {
  return IsV1() && options_.on_empty == OnEmpty::kSendNil && result_.empty();
}

template <typename T> bool JsonCallbackResult<T>::ShouldSendWrongType() const {
  if (IsV1()) {
    if (result_.empty() && options_.on_empty == OnEmpty::kSendWrongType)
      return true;

    if constexpr (is_optional<T>::value) {
      return !result_.front().has_value();
    }
  }
  return false;
}

inline WrappedJsonPath::WrappedJsonPath(json::Path json_path, StringOrView path,
                                        JsonPathType path_type)
    : parsed_path_(std::move(json_path)), path_(std::move(path)), path_type_(path_type) {
}

inline WrappedJsonPath::WrappedJsonPath(JsonExpression expression, StringOrView path,
                                        JsonPathType path_type)
    : parsed_path_(std::move(expression)), path_(std::move(path)), path_type_(path_type) {
}

template <typename T>
JsonCallbackResult<T> WrappedJsonPath::ExecuteReadOnlyCallback(
    const JsonType* json_entry, JsonPathReadOnlyCallback<T> cb,
    CallbackResultOptions options) const {
  JsonCallbackResult<T> read_result{InitializePathType(options)};

  auto eval_callback = [&cb, &read_result](std::string_view path, const JsonType& val) {
    read_result.AddValue(cb(path, val));
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

  return read_result;
}

template <typename T>
OpResult<JsonCallbackResult<std::optional<T>>> WrappedJsonPath::ExecuteMutateCallback(
    JsonType* json_entry, JsonPathMutateCallback<T> cb, CallbackResultOptions options) const {
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

    expr.evaluate(resources, *json_entry, JsonSelector::path_node_type{}, *json_entry, std::move(f),
                  result_options::nodups | result_options::path);
  }
  return mutate_result;
}

inline bool WrappedJsonPath::IsLegacyModePath() const {
  return path_type_ == JsonPathType::kLegacy;
}

inline bool WrappedJsonPath::RefersToRootElement() const {
  auto path = path_.view();
  return path.empty() || path == kV1PathRootElement || path == kV2PathRootElement;
}

inline bool WrappedJsonPath::HoldsJsonPath() const {
  return std::holds_alternative<json::Path>(parsed_path_);
}

inline const json::Path& WrappedJsonPath::AsJsonPath() const {
  return std::get<json::Path>(parsed_path_);
}

inline const JsonExpression& WrappedJsonPath::AsJsonExpression() const {
  return std::get<JsonExpression>(parsed_path_);
}

inline std::string_view WrappedJsonPath::Path() const {
  return path_.view();
}

inline CallbackResultOptions WrappedJsonPath::InitializePathType(
    CallbackResultOptions options) const {
  if (!options.path_type) {
    options.path_type = path_type_;
  }
  return options;
}

}  // namespace dfly
