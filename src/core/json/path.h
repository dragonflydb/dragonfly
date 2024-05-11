// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <string>
#include <variant>
#include <vector>

#include "base/expected.hpp"
#include "core/flatbuffers.h"
#include "core/json/json_object.h"

namespace dfly::json {

enum class SegmentType {
  IDENTIFIER = 1,  // $.identifier
  INDEX = 2,       // $.array[index_expr]
  WILDCARD = 3,    // $.*
  DESCENT = 4,     // $..identifier
  FUNCTION = 5,    // max($.prices[*])
};

const char* SegmentName(SegmentType type);

class AggFunction {
 public:
  using Result = std::variant<std::monostate, double, int64_t>;
  virtual ~AggFunction() {
  }

  void Apply(const JsonType& src) {
    if (valid_ != 0)
      valid_ = ApplyImpl(src);
  }

  void Apply(FlatJson src) {
    if (valid_ != 0)
      valid_ = ApplyImpl(src);
  }

  // returns null if Apply was not called or ApplyImpl failed.
  Result GetResult() const {
    return valid_ == 1 ? GetResultImpl() : Result{};
  }

 protected:
  virtual bool ApplyImpl(const JsonType& src) = 0;
  virtual bool ApplyImpl(FlatJson src) = 0;
  virtual Result GetResultImpl() const = 0;

  int valid_ = -1;
};

// Bracket index representation, IndexExpr is a closed range, i.e. both ends are inclusive.
// Single index is: <I, I>, wildcard: <0, INT_MAX>,
// [begin:end): <begin, end - 1>
// IndexExpr is 0-based, with negative indices referring to the array size of the applied object.
struct IndexExpr : public std::pair<int, int> {
  bool Empty() const {
    return first > second;
  }

  static IndexExpr All() {
    return IndexExpr{0, INT_MAX};
  }

  using pair::pair;

  // Returns subrange with length `array_len`.
  IndexExpr Normalize(size_t array_len) const;

  // Returns IndexExpr representing [left_closed, right_open) range.
  static IndexExpr HalfOpen(int left_closed, int right_open) {
    return IndexExpr(left_closed, right_open - 1);
  }
};

class PathSegment {
 public:
  PathSegment() : PathSegment(SegmentType::IDENTIFIER) {
  }

  PathSegment(SegmentType type, std::string identifier = std::string())
      : type_(type), value_(std::move(identifier)) {
  }

  PathSegment(SegmentType type, IndexExpr index) : type_(type), value_(index) {
  }

  explicit PathSegment(std::shared_ptr<AggFunction> func)
      : type_(SegmentType::FUNCTION), value_(std::move(func)) {
  }

  SegmentType type() const {
    return type_;
  }

  const std::string& identifier() const {
    return std::get<std::string>(value_);
  }

  IndexExpr index() const {
    return std::get<IndexExpr>(value_);
  }

  void Evaluate(const JsonType& json) const;
  void Evaluate(FlatJson json) const;
  AggFunction::Result GetResult() const;

 private:
  SegmentType type_;

  // shared_ptr to preserve copy semantics.
  std::variant<std::string, IndexExpr, std::shared_ptr<AggFunction>> value_;
};

using Path = std::vector<PathSegment>;

// Passes the key name for object fields or nullopt for array elements.
// The second argument is a json value of either object fields or array elements.
using PathCallback = absl::FunctionRef<void(std::optional<std::string_view>, const JsonType&)>;
using PathFlatCallback = absl::FunctionRef<void(std::optional<std::string_view>, FlatJson)>;

// Returns true if the entry should be deleted, false otherwise.
using MutateCallback = absl::FunctionRef<bool(std::optional<std::string_view>, JsonType*)>;

void EvaluatePath(const Path& path, const JsonType& json, PathCallback callback);

// Same as above but for flatbuffers.
void EvaluatePath(const Path& path, FlatJson json, PathFlatCallback callback);

// returns number of matches found with the given path.
unsigned MutatePath(const Path& path, MutateCallback callback, JsonType* json);
unsigned MutatePath(const Path& path, MutateCallback callback, FlatJson json,
                    flexbuffers::Builder* fbb);

// utility function to parse a jsonpath. Returns an error message if a parse error was
// encountered.
nonstd::expected<Path, std::string> ParsePath(std::string_view path);

// Transforms FlatJson to JsonType.
JsonType FromFlat(FlatJson src);

// Transforms JsonType to a buffer using flexbuffers::Builder.
// Does not call flexbuffers::Builder::Finish.
void FromJsonType(const JsonType& src, flexbuffers::Builder* fbb);

}  // namespace dfly::json
