// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/path.h"

#include <absl/types/span.h>

#include "base/expected.hpp"
#include "base/logging.h"
#include "src/core/json_object.h"

using namespace std;
using nonstd::make_unexpected;

namespace dfly::json {

namespace {

template <typename... Ts> struct Overload : Ts... { using Ts::operator()...; };

template <typename... Ts> Overload(Ts...) -> Overload<Ts...>;

bool ShouldIterateAll(SegmentType type) {
  return type == SegmentType::WILDCARD;
}

class Dfs {
 public:
  using Cb = std::function<void(const JsonType&)>;

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

  void DoCall(const Cb& callback, const JsonType& node) {
    ++matches_;
    callback(node);
  }

  using AdvanceResult = nonstd::expected<const JsonType*, MatchStatus>;

  class Item {
   public:
    Item(const JsonType* o) : obj_(o) {
    }

    // Returns the next object to traverse
    // or null if traverse was exhausted or the segment does not match.
    AdvanceResult Advance(const PathSegment& segment);

   private:
    AdvanceResult Init(const PathSegment& segment);

    const JsonType* obj_;
    variant<monostate, JsonType::const_object_iterator, JsonType::const_array_iterator> state_;
  };

  unsigned matches_ = 0;
};

auto Dfs::Item::Advance(const PathSegment& segment) -> AdvanceResult {
  AdvanceResult result = std::visit(  // line break
      Overload{
          [&](monostate) { return Init(segment); },  // Init state
          [&](JsonType::const_object_iterator& it) -> AdvanceResult {
            if (!ShouldIterateAll(segment.type()))
              return nullptr;  // exhausted

            ++it;
            if (it == obj_->object_range().end()) {
              return nullptr;  // exhausted
            }
            return &it->value();
          },
          [&](JsonType::const_array_iterator& it) -> AdvanceResult {
            if (!ShouldIterateAll(segment.type()))
              return nullptr;

            ++it;
            if (it == obj_->array_range().end()) {
              return nullptr;
            }
            return &*it;
          },
      },
      state_);
  return result;
}

auto Dfs::Item::Init(const PathSegment& segment) -> AdvanceResult {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (obj_->is_object()) {
        auto it = obj_->find(segment.identifier());
        if (it != obj_->object_range().end()) {
          state_ = it;
          return &it->value();
        } else {
          return nullptr;  // exhausted
        }
      }
      break;
    }
    case SegmentType::INDEX: {
      unsigned index = segment.index();
      if (obj_->is_array()) {
        if (index >= obj_->size()) {
          return make_unexpected(OUT_OF_BOUNDS);
        }
        auto it = obj_->array_range().cbegin() + index;
        state_ = it;
        return &*it;
      }
      break;
    }

    case SegmentType::WILDCARD: {
      if (obj_->is_object()) {
        jsoncons::range rng = obj_->object_range();
        if (rng.cbegin() == rng.cend()) {
          return nullptr;
        }
        state_ = rng.begin();
        return &rng.begin()->value();
      }

      if (obj_->is_array()) {
        jsoncons::range rng = obj_->array_range();
        if (rng.cbegin() == rng.cend()) {
          return nullptr;
        }
        state_ = rng.cbegin();
        return &*rng.cbegin();
      }
      break;
    }
  }  // end switch

  return make_unexpected(MISMATCH);
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
    unsigned depth = stack.size();
    // init or advance the current object
    AdvanceResult res = stack.back().Advance(path[depth - 1]);
    if (res && *res != nullptr) {
      const JsonType* next = *res;
      DVLOG(2) << "Handling now " << next->to_string();

      if (depth + 1 < path.size()) {
        stack.emplace_back(next);
      } else {
        // terminal step
        // TODO: to take into account MatchStatus
        // for `json.set foo $.a[10]` or for `json.set foo $.*.b`
        PerformStep(path[depth], *next, callback);
      }
    } else {
      stack.pop_back();
    }
  } while (!stack.empty());
}

auto Dfs::PerformStep(const PathSegment& segment, const JsonType& node,
                      const function<void(const JsonType&)>& callback)
    -> nonstd::expected<void, MatchStatus> {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (!node.is_object())
        return make_unexpected(MISMATCH);

      auto it = node.find(segment.identifier());
      if (it != node.object_range().end()) {
        DoCall(callback, it->value());
      }
    } break;
    case SegmentType::INDEX: {
      if (!node.is_array())
        return make_unexpected(MISMATCH);
      if (segment.index() >= node.size()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }
      DoCall(callback, node[segment.index()]);
    } break;
    case SegmentType::WILDCARD: {
      if (node.is_object()) {
        for (const auto& k_v : node.object_range()) {
          DoCall(callback, k_v.value());
        }
      } else if (node.is_array()) {
        for (const auto& val : node.array_range()) {
          DoCall(callback, val);
        }
      }
    } break;
  }
  return {};
}

}  // namespace

void EvaluatePath(const Path& path, const JsonType& json,
                  std::function<void(const JsonType&)> callback) {
  if (path.empty())
    return;
  Dfs().Traverse(path, json, std::move(callback));
}

}  // namespace dfly::json
