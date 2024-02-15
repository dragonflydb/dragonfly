// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/path.h"

#include <absl/types/span.h>

#include "base/expected.hpp"
#include "base/logging.h"
#include "src/core/json_object.h"
#include "src/core/overloaded.h"

using namespace std;
using nonstd::make_unexpected;

namespace dfly::json {

namespace {

bool ShouldIterateAll(SegmentType type) {
  return type == SegmentType::WILDCARD || type == SegmentType::DESCENT;
}

// Traverses a json object according to the given path and calls the callback for each matching
// field. With DESCENT segments it will match 0 or more fields in depth.
// MATCH(node, DESCENT|SUFFIX) = MATCH(node, SUFFIX) ||
// { MATCH(node->child, DESCENT/SUFFIX) for each child of node }

class Dfs {
 public:
  using Cb = PathCallback;

  // TODO: for some operations we need to know the type of mismatches.
  void Traverse(absl::Span<const PathSegment> path, const JsonType& json, const Cb& callback);

  unsigned matches() const {
    return matches_;
  }

 private:
  enum MatchStatus {
    OUT_OF_BOUNDS,
    MISMATCH,
  };

  bool TraverseImpl(absl::Span<const PathSegment> path, const Cb& callback);

  nonstd::expected<void, MatchStatus> PerformStep(const PathSegment& segment, const JsonType& node,
                                                  const Cb& callback);

  void DoCall(const Cb& callback, optional<string_view> key, const JsonType& node) {
    ++matches_;
    callback(key, node);
  }

  using DepthState = pair<const JsonType*, unsigned>;  // object, segment_idx pair
  using AdvanceResult = nonstd::expected<DepthState, MatchStatus>;

  // Describes the current state of the DFS traversal for a single node inside json hierarchy.
  // Specifically it holds the parent object (can be a either a real object or an array),
  // and the iterator to one of its children that is currently being traversed.
  class Item {
   public:
    Item(const JsonType* o, unsigned idx = 0) : depth_state_(o, idx) {
    }

    // Returns the next object to traverse
    // or null if traverse was exhausted or the segment does not match.
    AdvanceResult Advance(const PathSegment& segment);

    unsigned segment_idx() const {
      return depth_state_.second;
    }

   private:
    const JsonType& obj() const {
      return *depth_state_.first;
    }

    DepthState Next(const JsonType& obj) const {
      return {&obj, depth_state_.second + segment_step_};
    }

    DepthState Exhausted() const {
      return {nullptr, 0};
    }

    AdvanceResult Init(const PathSegment& segment);

    // For most operations we advance the path segment by 1 when we descent into the children.
    unsigned segment_step_ = 1;

    DepthState depth_state_;
    variant<monostate, JsonType::const_object_iterator, JsonType::const_array_iterator> state_;
  };

  unsigned matches_ = 0;
};

auto Dfs::Item::Advance(const PathSegment& segment) -> AdvanceResult {
  AdvanceResult result = std::visit(  // line break
      Overloaded{
          [&](monostate) { return Init(segment); },  // Init state
          [&](JsonType::const_object_iterator& it) -> AdvanceResult {
            if (!ShouldIterateAll(segment.type()))
              return Exhausted();

            ++it;
            return it == obj().object_range().end() ? Exhausted() : Next(it->value());
          },
          [&](JsonType::const_array_iterator& it) -> AdvanceResult {
            if (!ShouldIterateAll(segment.type()))
              return Exhausted();

            ++it;
            return it == obj().array_range().end() ? Exhausted() : Next(*it);
          },
      },
      state_);
  return result;
}

auto Dfs::Item::Init(const PathSegment& segment) -> AdvanceResult {
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
    case SegmentType::INDEX: {
      unsigned index = segment.index();
      if (obj().is_array()) {
        if (index >= obj().size()) {
          return make_unexpected(OUT_OF_BOUNDS);
        }
        auto it = obj().array_range().cbegin() + index;
        state_ = it;
        return Next(*it);
      }
      break;
    }

    case SegmentType::DESCENT:
      if (segment_step_ == 1) {
        // first time, branching to return the same object but with the next segment,
        // exploring the path of ignoring the DESCENT operator.
        // Alsom, shift the state (segment_step) to bypass this branch next time.
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
        state_ = rng.begin();
        return Next(rng.begin()->value());
      }

      if (obj().is_array()) {
        jsoncons::range rng = obj().array_range();
        if (rng.cbegin() == rng.cend()) {
          return Exhausted();
        }
        state_ = rng.cbegin();
        return Next(*rng.cbegin());
      }
      break;
    }
  }  // end switch

  return make_unexpected(MISMATCH);
}

inline bool IsRecursive(jsoncons::json_type type) {
  return type == jsoncons::json_type::object_value || type == jsoncons::json_type::array_value;
}

void Dfs::Traverse(absl::Span<const PathSegment> path, const JsonType& root, const Cb& callback) {
  DCHECK(!path.empty());
  if (path.size() == 1) {
    PerformStep(path[0], root, callback);
    return;
  }

  vector<Item> stack;
  stack.emplace_back(&root);

  do {
    unsigned segment_index = stack.back().segment_idx();
    const auto& path_segment = path[segment_index];

    // init or advance the current object
    AdvanceResult res = stack.back().Advance(path_segment);
    if (res && res->first != nullptr) {
      const JsonType* next = res->first;
      DVLOG(2) << "Handling now " << next->type() << " " << next->to_string();

      // We descent only if next is object or an array.
      if (IsRecursive(next->type())) {
        unsigned next_seg_id = res->second;

        if (next_seg_id + 1 < path.size()) {
          stack.emplace_back(next, next_seg_id);
        } else {
          // terminal step
          // TODO: to take into account MatchStatus
          // for `json.set foo $.a[10]` or for `json.set foo $.*.b`
          PerformStep(path[next_seg_id], *next, callback);
        }
      }
    } else {
      stack.pop_back();
    }
  } while (!stack.empty());
}

auto Dfs::PerformStep(const PathSegment& segment, const JsonType& node, const Cb& callback)
    -> nonstd::expected<void, MatchStatus> {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (!node.is_object())
        return make_unexpected(MISMATCH);

      auto it = node.find(segment.identifier());
      if (it != node.object_range().end()) {
        DoCall(callback, it->key(), it->value());
      }
    } break;
    case SegmentType::INDEX: {
      if (!node.is_array())
        return make_unexpected(MISMATCH);
      if (segment.index() >= node.size()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }
      DoCall(callback, nullopt, node[segment.index()]);
    } break;

    case SegmentType::DESCENT:
    case SegmentType::WILDCARD: {
      if (node.is_object()) {
        for (const auto& k_v : node.object_range()) {
          DoCall(callback, k_v.key(), k_v.value());
        }
      } else if (node.is_array()) {
        for (const auto& val : node.array_range()) {
          DoCall(callback, nullopt, val);
        }
      }
    } break;
  }
  return {};
}

}  // namespace

void EvaluatePath(const Path& path, const JsonType& json, PathCallback callback) {
  if (path.empty())
    return;
  Dfs().Traverse(path, json, std::move(callback));
}

}  // namespace dfly::json
