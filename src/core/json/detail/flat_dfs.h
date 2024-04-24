// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <variant>

#include "base/expected.hpp"
#include "core/flatbuffers.h"
#include "core/json/detail/common.h"
#include "core/json/path.h"

namespace dfly::json::detail {

class FlatDfsItem {
 public:
  using ValueType = flexbuffers::Reference;
  using DepthState = std::pair<ValueType, unsigned>;  // object, segment_idx pair
  using AdvanceResult = nonstd::expected<DepthState, MatchStatus>;

  FlatDfsItem(ValueType val, unsigned idx = 0) : depth_state_(val, idx) {
  }

  // Returns the next object to traverse
  // or null if traverse was exhausted or the segment does not match.
  AdvanceResult Advance(const PathSegment& segment);

  unsigned segment_idx() const {
    return depth_state_.second;
  }

 private:
  ValueType obj() const {
    return depth_state_.first;
  }

  DepthState Next(ValueType obj) const {
    return {obj, depth_state_.second + segment_step_};
  }

  DepthState Exhausted() const {
    return {ValueType(), 0};
  }

  AdvanceResult Init(const PathSegment& segment);

  // For most operations we advance the path segment by 1 when we descent into the children.
  unsigned segment_step_ = 1;

  DepthState depth_state_;
  std::optional<IndexExpr> state_;
};

// Traverses a json object according to the given path and calls the callback for each matching
// field. With DESCENT segments it will match 0 or more fields in depth.
// MATCH(node, DESCENT|SUFFIX) = MATCH(node, SUFFIX) ||
// { MATCH(node->child, DESCENT/SUFFIX) for each child of node }

class FlatDfs {
 public:
  // TODO: for some operations we need to know the type of mismatches.
  static FlatDfs Traverse(absl::Span<const PathSegment> path, const flexbuffers::Reference root,
                          const PathFlatCallback& callback);
  unsigned matches() const {
    return matches_;
  }

 private:
  bool TraverseImpl(absl::Span<const PathSegment> path, const PathFlatCallback& callback);

  nonstd::expected<void, MatchStatus> PerformStep(const PathSegment& segment,
                                                  const flexbuffers::Reference node,
                                                  const PathFlatCallback& callback);

  void DoCall(const PathFlatCallback& callback, std::optional<std::string_view> key,
              const flexbuffers::Reference node) {
    ++matches_;
    callback(key, node);
  }

  unsigned matches_ = 0;
};

}  // namespace dfly::json::detail
