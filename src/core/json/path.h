// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <string>
#include <variant>
#include <vector>

#include "src/core/json_object.h"

namespace dfly::json {

enum class SegmentType {
  IDENTIFIER = 1,  // $.identifier
  INDEX = 2,       // $.array[0]
  WILDCARD = 3,    // $.array[*] or $.*
  DESCENT = 4,     // $..identifier
};

class PathSegment {
 public:
  PathSegment() : PathSegment(SegmentType::IDENTIFIER) {
  }

  PathSegment(SegmentType type, std::string identifier = std::string())
      : type_(type), value_(std::move(identifier)) {
  }

  PathSegment(SegmentType type, unsigned index) : type_(type), value_(index) {
  }

  SegmentType type() const {
    return type_;
  }

  const std::string& identifier() const {
    return std::get<std::string>(value_);
  }

  unsigned index() const {
    return std::get<unsigned>(value_);
  }

 private:
  SegmentType type_;
  std::variant<std::string, unsigned> value_;
};

using Path = std::vector<PathSegment>;

// Passes the key name for object fields or nullopt for array elements.
// The second argument is a json value of either object fields or array elements.
using PathCallback = absl::FunctionRef<void(std::optional<std::string_view>, const JsonType&)>;

void EvaluatePath(const Path& path, const JsonType& json, PathCallback callback);

}  // namespace dfly::json
