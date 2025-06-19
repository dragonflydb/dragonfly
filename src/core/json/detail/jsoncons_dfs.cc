// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// clang-format off
#include <glog/logging.h>
// clang-format on

#include "core/json/detail/jsoncons_dfs.h"

namespace dfly::json::detail {

using namespace std;
using nonstd::make_unexpected;

ostream& operator<<(ostream& os, const PathSegment& ps) {
  os << SegmentName(ps.type());
  return os;
}

inline bool IsRecursive(jsoncons::json_type type) {
  return type == jsoncons::json_type::object_value || type == jsoncons::json_type::array_value;
}

Dfs Dfs::Traverse(absl::Span<const PathSegment> path, const JsonType& root, const Cb& callback) {
  DCHECK(!path.empty());

  Dfs dfs;

  if (path.size() == 1) {
    dfs.PerformStep(path[0], root, callback);
    return dfs;
  }

  using ConstItem = JsonconsDfsItem<true>;
  vector<ConstItem> stack;
  stack.emplace_back(&root);

  do {
    unsigned segment_index = stack.back().segment_idx();
    const auto& path_segment = path[segment_index];

    // init or advance the current object
    DVLOG(2) << "Advance segment [" << segment_index << "] " << path_segment;
    ConstItem::AdvanceResult res = stack.back().Advance(path_segment);
    if (res && res->first != nullptr) {
      const JsonType* next = res->first;

      // We descent only if next is object or an array.
      if (IsRecursive(next->type())) {
        unsigned next_seg_id = res->second;

        if (next_seg_id + 1 < path.size()) {
          DVLOG(2) << "Exploring node[" << stack.size() << "] " << next->type() << " "
                   << next->to_string();
          stack.emplace_back(next, next_seg_id);
        } else {
          DVLOG(2) << "Terminal node[" << stack.size() << "] " << next->type() << " "
                   << next->to_string() << ", segment:" << path[next_seg_id];
          // terminal step
          // TODO: to take into account MatchStatus
          // for `json.set foo $.a[10]` or for `json.set foo $.*.b`
          dfs.PerformStep(path[next_seg_id], *next, callback);
        }
      }
    } else {
      stack.pop_back();
    }
  } while (!stack.empty());

  return dfs;
}

Dfs Dfs::Mutate(absl::Span<const PathSegment> path, const MutateCallback& callback,
                JsonType* json) {
  DCHECK(!path.empty());

  Dfs dfs;

  if (path.size() == 1) {
    dfs.MutateStep(path[0], callback, json);
    return dfs;
  }

  // Use vector to maintain order
  std::vector<JsonType*> nodes_to_mutate;

  using Item = detail::JsonconsDfsItem<false>;
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
          // Terminal step: collect node for mutation
          nodes_to_mutate.push_back(next);
        }
      }
    } else {
      // If Advance failed (e.g., MISMATCH or OUT_OF_BOUNDS), the current node itself
      // might still be a terminal match because of the previous DESCENT segment.
      // Instead of mutating immediately (which could break ordering guarantees),
      // collect the node and defer mutation until after traversal.
      if (!res && segment_index > 0 && path[segment_index - 1].type() == SegmentType::DESCENT &&
          stack.back().get_segment_step() == 0) {
        if (segment_index + 1 == path.size()) {
          // Terminal node discovered via DESCENT – store for later processing.
          nodes_to_mutate.push_back(stack.back().obj_ptr());
        }
      }
      stack.pop_back();
    }
  } while (!stack.empty());

  // Apply mutations after DFS traversal is complete
  const PathSegment& terminal_segment = path.back();

  for (auto it = nodes_to_mutate.begin(); it != nodes_to_mutate.end(); ++it) {
    dfs.MutateStep(terminal_segment, callback, *it);
  }

  return dfs;
}

Dfs Dfs::Delete(absl::Span<const PathSegment> path, JsonType* json) {
  DCHECK(!path.empty());

  Dfs dfs;

  if (path.size() == 1) {
    dfs.DeleteStep(path[0], json);
    return dfs;
  }

  using Item = detail::JsonconsDfsItem<false>;
  vector<Item> stack;
  stack.emplace_back(json);

  do {
    unsigned segment_index = stack.back().segment_idx();
    const auto& path_segment = path[segment_index];

    Item::AdvanceResult res = stack.back().Advance(path_segment);
    if (res && res->first != nullptr) {
      JsonType* next = res->first;

      if (IsRecursive(next->type())) {
        unsigned next_seg_id = res->second;

        if (next_seg_id + 1 < path.size()) {
          stack.emplace_back(next, next_seg_id);
        } else {
          // Terminal step: perform deletion immediately
          // At this point we're in the deepest level, so safe to delete
          dfs.DeleteStep(path[next_seg_id], next);
        }
      }
    } else {
      if (!res && segment_index > 0 && path[segment_index - 1].type() == SegmentType::DESCENT &&
          stack.back().get_segment_step() == 0) {
        if (segment_index + 1 == path.size()) {
          // Terminal node discovered via DESCENT - safe to delete immediately
          // as we're backtracking
          dfs.DeleteStep(path[segment_index], stack.back().obj_ptr());
        }
      }
      stack.pop_back();
    }
  } while (!stack.empty());

  return dfs;
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
      IndexExpr index = segment.index().Normalize(node.size());
      if (index.Empty()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }
      for (; index.first <= index.second; ++index.first) {
        DoCall(callback, nullopt, node[index.first]);
      }
    } break;

    case SegmentType::DESCENT:
    case SegmentType::WILDCARD: {
      if (node.is_object()) {
        for (const auto& k_v : node.object_range()) {
          DoCall(callback, k_v.key(), k_v.value());
        }
      } else if (node.is_array()) {
        for (const auto& item : node.array_range()) {
          DoCall(callback, nullopt, item);
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
        cb(it->key(), &it->value());
      }
    } break;
    case SegmentType::INDEX: {
      if (!node->is_array())
        return make_unexpected(MISMATCH);
      IndexExpr index = segment.index().Normalize(node->size());
      if (index.Empty()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }

      while (index.first <= index.second) {
        auto it = node->array_range().begin() + index.first;
        cb(nullopt, &*it);
        ++index.first;
      }
    } break;

    case SegmentType::DESCENT:
    case SegmentType::WILDCARD: {
      if (node->is_object()) {
        auto it = node->object_range().begin();
        while (it != node->object_range().end()) {
          cb(it->key(), &it->value());
          ++it;
        }
      } else if (node->is_array()) {
        auto it = node->array_range().begin();
        while (it != node->array_range().end()) {
          cb(nullopt, &*it);
          ++it;
        }
      }
    } break;
    case SegmentType::FUNCTION:
      LOG(DFATAL) << "Function segment is not supported for mutation";
      break;
  }
  return {};
}

auto Dfs::DeleteStep(const PathSegment& segment, JsonType* node)
    -> nonstd::expected<void, MatchStatus> {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (!node->is_object())
        return make_unexpected(MISMATCH);

      auto it = node->find(segment.identifier());
      if (it != node->object_range().end()) {
        node->erase(it);
        ++matches_;
      }
    } break;
    case SegmentType::INDEX: {
      if (!node->is_array())
        return make_unexpected(MISMATCH);
      IndexExpr index = segment.index().Normalize(node->size());
      if (index.Empty()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }

      // Delete from end to beginning to maintain indices
      for (int i = index.second; i >= index.first; --i) {
        auto it = node->array_range().begin() + i;
        node->erase(it);
        ++matches_;
      }
    } break;

    case SegmentType::DESCENT:
    case SegmentType::WILDCARD: {
      size_t initial_size = node->size();
      node->clear();
      matches_ += initial_size;
    } break;
    case SegmentType::FUNCTION:
      LOG(DFATAL) << "Function segment is not supported for deletion";
      break;
  }
  return {};
}

}  // namespace dfly::json::detail
