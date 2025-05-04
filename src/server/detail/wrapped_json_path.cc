// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/detail/wrapped_json_path.h"

namespace dfly {

CallbackResultOptions CallbackResultOptions::DefaultReadOnlyOptions() {
  return CallbackResultOptions{OnEmpty::kSendNil};
}

CallbackResultOptions CallbackResultOptions::DefaultReadOnlyOptions(SavingOrder saving_order) {
  return CallbackResultOptions{saving_order, OnEmpty::kSendNil};
}

CallbackResultOptions CallbackResultOptions::DefaultMutateOptions() {
  return CallbackResultOptions{OnEmpty::kSendWrongType};
}

CallbackResultOptions::CallbackResultOptions(OnEmpty on_empty_) : on_empty(on_empty_) {
}

CallbackResultOptions::CallbackResultOptions(JsonPathType path_type_, SavingOrder saving_order_,
                                             OnEmpty on_empty_)
    : path_type(path_type_), saving_order(saving_order_), on_empty(on_empty_) {
}

CallbackResultOptions::CallbackResultOptions(SavingOrder saving_order_, OnEmpty on_empty_)
    : saving_order(saving_order_), on_empty(on_empty_) {
}

WrappedJsonPath::WrappedJsonPath(json::Path json_path, StringOrView path, JsonPathType path_type)
    : parsed_path_(std::move(json_path)), path_(std::move(path)), path_type_(path_type) {
}

WrappedJsonPath::WrappedJsonPath(JsonExpression expression, StringOrView path,
                                 JsonPathType path_type)
    : parsed_path_(std::move(expression)), path_(std::move(path)), path_type_(path_type) {
}

bool WrappedJsonPath::IsLegacyModePath() const {
  return path_type_ == JsonPathType::kLegacy;
}

bool WrappedJsonPath::RefersToRootElement() const {
  auto path = path_.view();
  return path.empty() || path == kV1PathRootElement || path == kV2PathRootElement;
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

std::string_view WrappedJsonPath::Path() const {
  return path_.view();
}

CallbackResultOptions WrappedJsonPath::InitializePathType(CallbackResultOptions options) const {
  if (!options.path_type) {
    options.path_type = path_type_;
  }
  return options;
}

}  // namespace dfly
