// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/str_cat.h>
#include <glog/logging.h>

#include <boost/none.hpp>
#include <boost/none_t.hpp>
#include <memory>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>

#include "base/flags.h"
#include "core/json/json_object.h"
#include "core/json/path.h"
#include "core/string_or_view.h"
#include "helio/io/io.h"
#include "server/error.h"

ABSL_DECLARE_FLAG(bool, jsonpathv2);

namespace dfly {

using JsonExpression = jsoncons::jsonpath::jsonpath_expression<JsonType>;

template <typename T>
using JsonPathEvaluateCallback = absl::FunctionRef<T(std::string_view, const JsonType&)>;

class Nothing {};

template <typename T = Nothing> struct MutateCallbackResult {
  bool should_be_deleted;
  std::optional<T> value{};
};

template <typename T = Nothing>
using JsonPathMutateCallback =
    absl::FunctionRef<MutateCallbackResult<T>(std::optional<std::string_view>, JsonType*)>;

template <typename T> class JsonCallbackResult {
 public:
  virtual void AddValue(T value) = 0;

  virtual bool IsV1() const {
    return false;
  }

  virtual ~JsonCallbackResult() = default;
};

template <typename T> class JsonV1CallbackResult final : public JsonCallbackResult<T> {
 public:
  void AddValue(T value) override {
    if (!result) {
      result.emplace(std::move(value));
    }
  }

  bool IsV1() const override {
    return true;
  }

  std::optional<T> result;
};

template <typename T> class JsonV2CallbackResult final : public JsonCallbackResult<T> {
 public:
  void AddValue(T value) override {
    result.emplace_back(std::move(value));
  }

  std::vector<T> result;
};

template <typename T> using JsonCallbackResultPtr = std::unique_ptr<JsonCallbackResult<T>>;

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

  WrappedJsonPath(const WrappedJsonPath&) = delete;
  WrappedJsonPath(WrappedJsonPath&&) = default;

  WrappedJsonPath& operator=(const WrappedJsonPath&) = delete;
  WrappedJsonPath& operator=(WrappedJsonPath&&) = delete;

  template <typename T>
  JsonCallbackResultPtr<T> Evaluate(const JsonType* json_entry,
                                    JsonPathEvaluateCallback<T> cb) const {
    return Evaluate(json_entry, cb, IsLegacyModePath());
  }

  template <typename T>
  JsonCallbackResultPtr<T> Evaluate(const JsonType* json_entry, JsonPathEvaluateCallback<T> cb,
                                    bool legacy_mode_is_enabled) const {
    auto eval_result = CreateJsonCallbackResult<T>(legacy_mode_is_enabled);

    auto eval_callback = [&cb, &eval_result](std::string_view path,
                                             const JsonType& val) {  // TODO(path or key)
      eval_result->AddValue(cb(path, val));
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

    return std::move(eval_result);  // TODO(Remove move)
  }

  template <typename T>
  JsonCallbackResultPtr<T> Mutate(JsonType* json_entry, JsonPathMutateCallback<T> cb) const {
    auto mutate_result = CreateJsonCallbackResult<T>(IsLegacyModePath());

    auto mutate_callback = [&cb, &mutate_result](std::optional<std::string_view> path,
                                                 JsonType* val) -> bool {
      auto res = cb(path, val);
      if (res.value) {
        mutate_result->AddValue(std::move(res.value.value()));
      }
      return res.should_be_deleted;
    };

    if (HoldsJsonPath()) {
      const auto& json_path = AsJsonPath();
      json::MutatePath(json_path, mutate_callback, json_entry);  // TODO(in callback key not path)
      return mutate_result;
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
    // RETURN_ON_ERR(ec); TODO(process error)

    jsoncons::jsonpath::detail::dynamic_resources<value_type, reference> resources;

    auto f = [&mutate_callback](const jsoncons::jsonpath::basic_path_node<char>& path,
                                JsonType& val) {
      mutate_callback(jsoncons::jsonpath::to_string(path), &val);
    };

    expr.evaluate(
        resources, *json_entry, json_selector_t::path_node_type{}, *json_entry, std::move(f),
        jsoncons::jsonpath::result_options::nodups | jsoncons::jsonpath::result_options::path);
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
