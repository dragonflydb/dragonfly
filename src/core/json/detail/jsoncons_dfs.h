// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <variant>

#include "base/expected.hpp"
#include "core/json/detail/common.h"
#include "core/json/json_object.h"
#include "core/json/path.h"
#include "core/overloaded.h"

namespace dfly::json::detail {

// Describes the current state of the DFS traversal for a single node inside json hierarchy.
// Specifically it holds the parent object (can be a either a real object or an array),
// and the iterator to one of its children that is currently being traversed.
template <bool IsConst> class JsonconsDfsItem {
 public:
  using ValueType = std::conditional_t<IsConst, const JsonType, JsonType>;
  using Ptr = ValueType*;
  using Ref = ValueType&;
  using ObjIterator =
      std::conditional_t<IsConst, JsonType::const_object_iterator, JsonType::object_iterator>;
  using ArrayIterator =
      std::conditional_t<IsConst, JsonType::const_array_iterator, JsonType::array_iterator>;

  using DepthState = std::pair<Ptr, unsigned>;  // object, segment_idx pair
  using AdvanceResult = nonstd::expected<DepthState, MatchStatus>;

  JsonconsDfsItem(Ptr o, unsigned idx = 0) : depth_state_(o, idx) {
  }

  // Returns the next object to traverse
  // or null if traverse was exhausted or the segment does not match.
  AdvanceResult Advance(const PathSegment& segment);

  unsigned segment_idx() const {
    return depth_state_.second;
  }

 private:
  static bool ShouldIterateAll(SegmentType type) {
    return type == SegmentType::WILDCARD || type == SegmentType::DESCENT;
  }

  ObjIterator Begin() const {
    if constexpr (IsConst) {
      return obj().object_range().cbegin();
    } else {
      return obj().object_range().begin();
    }
  }

  ArrayIterator ArrBegin() const {
    if constexpr (IsConst) {
      return obj().array_range().cbegin();
    } else {
      return obj().array_range().begin();
    }
  }

  ArrayIterator ArrEnd() const {
    if constexpr (IsConst) {
      return obj().array_range().cend();
    } else {
      return obj().array_range().end();
    }
  }

  Ref obj() const {
    return *depth_state_.first;
  }

  DepthState Next(Ref obj) const {
    return {&obj, depth_state_.second + segment_step_};
  }

  DepthState Exhausted() const {
    return {nullptr, 0};
  }

  AdvanceResult Init(const PathSegment& segment);

  // For most operations we advance the path segment by 1 when we descent into the children.
  unsigned segment_step_ = 1;

  DepthState depth_state_;
  std::variant<std::monostate, ObjIterator, std::pair<ArrayIterator, ArrayIterator>> state_;
};

// Traverses a json object according to the given path and calls the callback for each matching
// field. With DESCENT segments it will match 0 or more fields in depth.
// MATCH(node, DESCENT|SUFFIX) = MATCH(node, SUFFIX) ||
// { MATCH(node->child, DESCENT/SUFFIX) for each child of node }

class Dfs {
 public:
  using Cb = PathCallback;

  // TODO: for some operations we need to know the type of mismatches.
  static Dfs Traverse(absl::Span<const PathSegment> path, const JsonType& json, const Cb& callback);
  static Dfs Mutate(absl::Span<const PathSegment> path, const MutateCallback& callback,
                    JsonType* json);

  unsigned matches() const {
    return matches_;
  }

 private:
  bool TraverseImpl(absl::Span<const PathSegment> path, const Cb& callback);

  nonstd::expected<void, MatchStatus> PerformStep(const PathSegment& segment, const JsonType& node,
                                                  const Cb& callback);

  nonstd::expected<void, MatchStatus> MutateStep(const PathSegment& segment,
                                                 const MutateCallback& cb, JsonType* node);

  void Mutate(const PathSegment& segment, const MutateCallback& callback, JsonType* node);

  void DoCall(const Cb& callback, std::optional<std::string_view> key, const JsonType& node) {
    ++matches_;
    callback(key, node);
  }

  bool Mutate(const MutateCallback& callback, std::optional<std::string_view> key, JsonType* node) {
    ++matches_;
    return callback(key, node);
  }

  unsigned matches_ = 0;
};

template <bool IsConst>
auto JsonconsDfsItem<IsConst>::Advance(const PathSegment& segment) -> AdvanceResult {
  AdvanceResult result = std::visit(  // line break
      Overloaded{
          [&](std::monostate) { return Init(segment); },  // Init state
          [&](ObjIterator& it) -> AdvanceResult {
            if (!ShouldIterateAll(segment.type()))
              return Exhausted();

            ++it;
            return it == obj().object_range().end() ? Exhausted() : Next(it->value());
          },
          [&](std::pair<ArrayIterator, ArrayIterator>& pair) -> AdvanceResult {
            if (pair.first == pair.second)
              return Exhausted();
            ++pair.first;
            return Next(*pair.first);
          },
      },
      state_);
  return result;
}

template <bool IsConst>
auto JsonconsDfsItem<IsConst>::Init(const PathSegment& segment) -> AdvanceResult {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (obj().is_object()) {
        auto it = obj().find(segment.identifier());
        if (it != obj().object_range().end()) {
          state_ = it;
          return DepthState{&it->value(), depth_state_.second + 1};
        } else {
          return Exhausted();
        }
      }
      break;
    }
    case SegmentType::INDEX:
      if (obj().is_array()) {
        IndexExpr index = segment.index().Normalize(obj().size());
        if (index.Empty()) {
          return nonstd::make_unexpected(OUT_OF_BOUNDS);
        }

        auto start = ArrBegin() + index.first, end = ArrBegin() + index.second;
        state_ = std::make_pair(start, end);
        return Next(*start);
      }
      break;
    case SegmentType::DESCENT:
      if (segment_step_ == 1) {
        // first time, branching to return the same object but with the next segment,
        // exploring the path of ignoring the DESCENT operator.
        // Also, shift the state (segment_step) to bypass this branch next time.
        segment_step_ = 0;
        return DepthState{depth_state_.first, depth_state_.second + 1};
      }

      // Now traverse all the children but do not progress with segment path.
      // This is why segment_step_ is set to 0.
      [[fallthrough]];
    case SegmentType::WILDCARD: {
      if (obj().is_object()) {
        jsoncons::range rng = obj().object_range();
        if (rng.cbegin() == rng.cend()) {
          return Exhausted();
        }
        state_ = Begin();
        return Next(Begin()->value());
      }

      if (obj().is_array()) {
        auto start = ArrBegin(), end = ArrEnd();
        if (start == end) {
          return Exhausted();
        }
        state_ = std::make_pair(start, end - 1);  // end is inclusive
        return Next(*start);
      }
      break;
    }
    default:
      LOG(DFATAL) << "Unknown segment " << SegmentName(segment.type());
  }  // end switch

  return nonstd::make_unexpected(MISMATCH);
}

}  // namespace dfly::json::detail
