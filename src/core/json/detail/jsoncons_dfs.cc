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
          dfs.MutateStep(path[next_seg_id], callback, next);
        }
      }
    } else {
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
        if (Mutate(cb, it->key(), &it->value())) {
          node->erase(it);
        }
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
        if (Mutate(cb, nullopt, &*it)) {
          node->erase(it);
          --index.second;
        } else {
          ++index.first;
        }
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

}  // namespace dfly::json::detail
