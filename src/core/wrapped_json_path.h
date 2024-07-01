// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string_view>
#include <system_error>
#include <variant>

#include "core/json/json_object.h"
#include "core/json/path.h"
#include "core/string_or_view.h"
#include "helio/io/io.h"

namespace dfly {

using JsonExpression = jsoncons::jsonpath::jsonpath_expression<JsonType>;

template <typename T>
using JsonPathCallback = absl::FunctionRef<T(std::string_view, const JsonType&)>;

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
  void AddValue(T value) override;
  bool IsV1() const override;

  std::optional<T> result;
};

template <typename T> class JsonV2CallbackResult final : public JsonCallbackResult<T> {
 public:
  void AddValue(T value) override;

  std::vector<T> result;
};

template <typename T> using JsonCallbackResultPtr = std::unique_ptr<JsonCallbackResult<T>>;

template <typename T>
JsonCallbackResultPtr<T> CreateJsonCallbackResult(bool legacy_mode_is_enabled);

template <typename T> JsonV1CallbackResult<T>& AsV1(JsonCallbackResult<T>& eval_result);

template <typename T> JsonV2CallbackResult<T>& AsV2(JsonCallbackResult<T>& eval_result);

class WrappedJsonPath {
 public:
  static constexpr std::string_view kV1PathRootElement = ".";
  static constexpr std::string_view kV2PathRootElement = "$";

  WrappedJsonPath(json::Path json_path, StringOrView path, bool is_legacy_mode_path);

  WrappedJsonPath(JsonExpression expression, StringOrView path, bool is_legacy_mode_path);

  WrappedJsonPath(const WrappedJsonPath&) = delete;
  WrappedJsonPath(WrappedJsonPath&&) = default;

  WrappedJsonPath& operator=(const WrappedJsonPath&) = delete;
  WrappedJsonPath& operator=(WrappedJsonPath&&) = delete;

  template <typename T>
  JsonCallbackResultPtr<T> Evaluate(const JsonType* json_entry, JsonPathCallback<T> cb) const;

  template <typename T>
  JsonCallbackResultPtr<T> Evaluate(const JsonType* json_entry, JsonPathCallback<T> cb,
                                    bool legacy_mode_is_enabled) const;

  std::error_code Replace(JsonType* json_entry, json::MutateCallback cb) const;

  bool IsLegacyModePath() const;

  bool RefersToRootElement() const;

  bool HoldsJsonPath() const;
  const json::Path& AsJsonPath() const;
  const JsonExpression& AsJsonExpression() const;

 private:
  std::variant<json::Path, JsonExpression> parsed_path_;
  StringOrView path_;
  bool is_legacy_mode_path_;
};

template <typename T> using ParseResult = io::Result<T, std::string>;

ParseResult<WrappedJsonPath> ParseJsonPath(std::string_view path);

}  // namespace dfly
