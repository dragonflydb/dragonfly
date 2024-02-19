// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/json/path.h"

#include <absl/strings/str_cat.h>
#include <absl/types/span.h>

#include "base/logging.h"
#include "core/json/jsonpath_grammar.hh"
#include "src/core/json/driver.h"
#include "src/core/overloaded.h"

using namespace std;
using nonstd::make_unexpected;

namespace dfly::json {

namespace {

bool ShouldIterateAll(SegmentType type) {
  return type == SegmentType::WILDCARD || type == SegmentType::DESCENT;
}

enum MatchStatus {
  OUT_OF_BOUNDS,
  MISMATCH,
};

// Describes the current state of the DFS traversal for a single node inside json hierarchy.
// Specifically it holds the parent object (can be a either a real object or an array),
// and the iterator to one of its children that is currently being traversed.
template <bool IsConst> class DfsItem {
 public:
  using ValueType = std::conditional_t<IsConst, const JsonType, JsonType>;
  using Ptr = ValueType*;
  using Ref = ValueType&;
  using ObjIterator =
      std::conditional_t<IsConst, JsonType::const_object_iterator, JsonType::object_iterator>;
  using ArrayIterator =
      std::conditional_t<IsConst, JsonType::const_array_iterator, JsonType::array_iterator>;

  using DepthState = pair<Ptr, unsigned>;  // object, segment_idx pair
  using AdvanceResult = nonstd::expected<DepthState, MatchStatus>;

  DfsItem(Ptr o, unsigned idx = 0) : depth_state_(o, idx) {
  }

  // Returns the next object to traverse
  // or null if traverse was exhausted or the segment does not match.
  AdvanceResult Advance(const PathSegment& segment);

  unsigned segment_idx() const {
    return depth_state_.second;
  }

 private:
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
  variant<monostate, ObjIterator, ArrayIterator> state_;
};

// Traverses a json object according to the given path and calls the callback for each matching
// field. With DESCENT segments it will match 0 or more fields in depth.
// MATCH(node, DESCENT|SUFFIX) = MATCH(node, SUFFIX) ||
// { MATCH(node->child, DESCENT/SUFFIX) for each child of node }

class Dfs {
 public:
  using Cb = PathCallback;

  // TODO: for some operations we need to know the type of mismatches.
  void Traverse(absl::Span<const PathSegment> path, const JsonType& json, const Cb& callback);
  void Mutate(absl::Span<const PathSegment> path, const MutateCallback& callback, JsonType* json);

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

  void DoCall(const Cb& callback, optional<string_view> key, const JsonType& node) {
    ++matches_;
    callback(key, node);
  }

  bool Mutate(const MutateCallback& callback, optional<string_view> key, JsonType* node) {
    ++matches_;
    return callback(key, node);
  }

  unsigned matches_ = 0;
};

template <bool IsConst>
auto DfsItem<IsConst>::Advance(const PathSegment& segment) -> AdvanceResult {
  AdvanceResult result = std::visit(  // line break
      Overloaded{
          [&](monostate) { return Init(segment); },  // Init state
          [&](ObjIterator& it) -> AdvanceResult {
            if (!ShouldIterateAll(segment.type()))
              return Exhausted();

            ++it;
            return it == obj().object_range().end() ? Exhausted() : Next(it->value());
          },
          [&](ArrayIterator& it) -> AdvanceResult {
            if (!ShouldIterateAll(segment.type()))
              return Exhausted();

            ++it;
            return it == obj().array_range().end() ? Exhausted() : Next(*it);
          },
      },
      state_);
  return result;
}

template <bool IsConst> auto DfsItem<IsConst>::Init(const PathSegment& segment) -> AdvanceResult {
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
        auto it = ArrBegin() + index;
        state_ = it;
        return Next(*it);
      }
      break;
    }

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
        jsoncons::range rng = obj().array_range();
        if (rng.cbegin() == rng.cend()) {
          return Exhausted();
        }
        state_ = ArrBegin();
        return Next(*ArrBegin());
      }
      break;
    }
    default:
      LOG(DFATAL) << "Unknown segment " << SegmentName(segment.type());
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

  using ConstItem = DfsItem<true>;
  vector<ConstItem> stack;
  stack.emplace_back(&root);

  do {
    unsigned segment_index = stack.back().segment_idx();
    const auto& path_segment = path[segment_index];

    // init or advance the current object
    ConstItem::AdvanceResult res = stack.back().Advance(path_segment);
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

void Dfs::Mutate(absl::Span<const PathSegment> path, const MutateCallback& callback,
                 JsonType* json) {
  DCHECK(!path.empty());
  if (path.size() == 1) {
    MutateStep(path[0], callback, json);
    return;
  }

  using Item = DfsItem<false>;
  vector<Item> stack;
  stack.emplace_back(json);

  do {
    unsigned segment_index = stack.back().segment_idx();
    const auto& path_segment = path[segment_index];

    // init or advance the current object
    Item::AdvanceResult res = stack.back().Advance(path_segment);
    if (res && res->first != nullptr) {
      JsonType* next = res->first;
      DVLOG(2) << "Handling now " << next->type() << " " << next->to_string();

      // We descent only if next is object or an array.
      if (IsRecursive(next->type())) {
        unsigned next_seg_id = res->second;

        if (next_seg_id + 1 < path.size()) {
          stack.emplace_back(next, next_seg_id);
        } else {
          MutateStep(path[next_seg_id], callback, next);
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
    default:
      LOG(DFATAL) << "Unknown segment " << SegmentName(segment.type());
  }
  return {};
}

auto Dfs::MutateStep(const PathSegment& segment, const MutateCallback& cb, JsonType* node)
    -> nonstd::expected<void, MatchStatus> {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (!node->is_object())
        return make_unexpected(MISMATCH);

      auto it = node->find(segment.identifier());
      if (it != node->object_range().end()) {
        if (Mutate(cb, it->key(), &it->value())) {
          node->erase(it);
        }
      }
    } break;
    case SegmentType::INDEX: {
      if (!node->is_array())
        return make_unexpected(MISMATCH);
      if (segment.index() >= node->size()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }
      if (Mutate(cb, nullopt, &node[segment.index()])) {
        node->erase(node->array_range().begin() + segment.index());
      }
    } break;

    case SegmentType::DESCENT:
    case SegmentType::WILDCARD: {
      if (node->is_object()) {
        auto it = node->object_range().begin();
        while (it != node->object_range().end()) {
          it = Mutate(cb, it->key(), &it->value()) ? node->erase(it) : it + 1;
        }
      } else if (node->is_array()) {
        auto it = node->array_range().begin();
        while (it != node->array_range().end()) {
          it = Mutate(cb, nullopt, &*it) ? node->erase(it) : it + 1;
        }
      }
    } break;
    case SegmentType::FUNCTION:
      LOG(DFATAL) << "Function segment is not supported for mutation";
      break;
  }
  return {};
}

class JsonPathDriver : public json::Driver {
 public:
  string msg;
  void Error(const json::location& l, const std::string& msg) final {
    this->msg = absl::StrCat("Error: ", msg);
  }
};

}  // namespace

const char* SegmentName(SegmentType type) {
  switch (type) {
    case SegmentType::IDENTIFIER:
      return "IDENTIFIER";
    case SegmentType::INDEX:
      return "INDEX";
    case SegmentType::WILDCARD:
      return "WILDCARD";
    case SegmentType::DESCENT:
      return "DESCENT";
    case SegmentType::FUNCTION:
      return "FUNCTION";
  }
  return nullptr;
}

void PathSegment::Evaluate(const JsonType& json) const {
  CHECK(type() == SegmentType::FUNCTION);
  AggFunction* func = std::get<shared_ptr<AggFunction>>(value_).get();
  CHECK(func);
  func->Apply(json);
}

JsonType PathSegment::GetResult() const {
  CHECK(type() == SegmentType::FUNCTION);
  const auto& func = std::get<shared_ptr<AggFunction>>(value_).get();
  CHECK(func);
  return func->GetResult();
}

void EvaluatePath(const Path& path, const JsonType& json, PathCallback callback) {
  if (path.empty())
    return;

  if (path.front().type() != SegmentType::FUNCTION) {
    Dfs().Traverse(path, json, std::move(callback));
    return;
  }

  // Handling the case of `func($.somepath)`
  // We pass our own callback to gather all the results and then call the function.
  JsonType result(JsonType::null());
  absl::Span<const PathSegment> path_tail(path.data() + 1, path.size() - 1);

  const PathSegment& func_segment = path.front();

  if (path_tail.empty()) {
    LOG(DFATAL) << "Invalid path";  // parser should not allow this.
  } else {
    Dfs().Traverse(path_tail, json, [&](auto, const JsonType& val) { func_segment.Evaluate(val); });
  }
  callback(nullopt, func_segment.GetResult());
}

nonstd::expected<json::Path, string> ParsePath(string_view path) {
  if (path.size() > 8192)
    return nonstd::make_unexpected("Path too long");

  VLOG(2) << "Parsing path: " << path;

  JsonPathDriver driver;
  Parser parser(&driver);

  driver.SetInput(string(path));
  int res = parser();
  if (res != 0) {
    return nonstd::make_unexpected(driver.msg);
  }

  return driver.TakePath();
}

void MutatePath(const Path& path, MutateCallback callback, JsonType* json) {
  if (path.empty())
    return;
  Dfs().Mutate(path, callback, json);
}

}  // namespace dfly::json
