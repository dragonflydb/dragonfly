// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/json/detail/flat_dfs.h"

#include "base/logging.h"

namespace dfly::json::detail {

using namespace std;
using nonstd::make_unexpected;

inline bool IsRecursive(flexbuffers::Type type) {
  return type == flexbuffers::FBT_MAP || type == flexbuffers::FBT_VECTOR;
}

// Binary search of a key, returns UINT_MAX if not found.
unsigned FindByKey(const flexbuffers::TypedVector& keys, const char* elem) {
  unsigned s = 0, end = keys.size();
  while (s < end) {
    unsigned mid = (s + end) / 2;
    flexbuffers::String mid_elem = keys[mid].AsString();
    int res = strcmp(elem, mid_elem.c_str());
    if (res < 0) {
      end = mid;
    } else if (res > 0) {
      s = mid + 1;
    } else {
      return mid;
    }
  }
  return UINT_MAX;
}

auto FlatDfsItem::Init(const PathSegment& segment) -> AdvanceResult {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (obj().IsMap()) {
        auto map = obj().AsMap();
        flexbuffers::TypedVector keys = map.Keys();
        unsigned index = FindByKey(keys, segment.identifier().c_str());
        if (index == UINT_MAX) {
          return Exhausted();
        }
        state_.emplace(index, index);
        return DepthState{obj().AsVector()[index], depth_state_.second + 1};
      }
      break;
    }
    case SegmentType::INDEX: {
      auto vec = obj().AsVector();
      IndexExpr index = segment.index().Normalize(vec.size());
      if (index.Empty()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }

      state_ = index;
      return Next(vec[index.first]);
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
      auto vec = obj().AsVector();
      if (vec.size() == 0) {
        return Exhausted();
      }
      state_ = IndexExpr::All();
      return Next(vec[0]);
    } break;

    default:
      LOG(DFATAL) << "Unknown segment " << SegmentName(segment.type());
  }  // end switch

  return nonstd::make_unexpected(MISMATCH);
}

auto FlatDfsItem::Advance(const PathSegment& segment) -> AdvanceResult {
  if (!state_) {
    return Init(segment);
  }

  ++state_->first;
  if (state_->Empty())
    return Exhausted();
  auto vec = obj().AsVector();

  return Next(vec[state_->first]);
}

FlatDfs FlatDfs::Traverse(absl::Span<const PathSegment> path, const flexbuffers::Reference root,
                          const PathFlatCallback& callback) {
  DCHECK(!path.empty());
  FlatDfs dfs;

  if (path.size() == 1) {
    dfs.PerformStep(path[0], root, callback);
    return dfs;
  }

  using ConstItem = FlatDfsItem;
  vector<ConstItem> stack;
  stack.emplace_back(root);

  do {
    unsigned segment_index = stack.back().segment_idx();
    const auto& path_segment = path[segment_index];

    // init or advance the current object
    ConstItem::AdvanceResult res = stack.back().Advance(path_segment);
    if (res && !res->first.IsNull()) {
      const flexbuffers::Reference next = res->first;
      DVLOG(2) << "Handling now " << next.GetType() << " " << next.ToString();

      // We descent only if next is object or an array.
      if (IsRecursive(next.GetType())) {
        unsigned next_seg_id = res->second;

        if (next_seg_id + 1 < path.size()) {
          stack.emplace_back(next, next_seg_id);
        } else {
          // terminal step
          // TODO: to take into account MatchStatus
          // for `json.set foo $.a[10]` or for `json.set foo $.*.b`
          dfs.PerformStep(path[next_seg_id], next, callback);
        }
      }
    } else {
      stack.pop_back();
    }
  } while (!stack.empty());

  return dfs;
}

auto FlatDfs::PerformStep(const PathSegment& segment, const flexbuffers::Reference node,
                          const PathFlatCallback& callback) -> nonstd::expected<void, MatchStatus> {
  switch (segment.type()) {
    case SegmentType::IDENTIFIER: {
      if (!node.IsMap())
        return make_unexpected(MISMATCH);
      auto map = node.AsMap();
      flexbuffers::Reference value = map[segment.identifier().c_str()];
      if (!value.IsNull()) {
        DoCall(callback, string_view{segment.identifier()}, value);
      }
    } break;
    case SegmentType::INDEX: {
      if (!node.IsUntypedVector())
        return make_unexpected(MISMATCH);
      auto vec = node.AsVector();
      IndexExpr index = segment.index().Normalize(vec.size());
      if (index.Empty()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }
      for (; index.first <= index.second; ++index.first)
        DoCall(callback, nullopt, vec[index.first]);
    } break;

    case SegmentType::DESCENT:
    case SegmentType::WILDCARD: {
      auto vec = node.AsVector();       // always succeeds
      auto keys = node.AsMap().Keys();  // always succeeds
      string str;
      for (size_t i = 0; i < vec.size(); ++i) {
        flexbuffers::Reference key = keys[i];
        optional<string_view> opt_key;
        if (key.IsString()) {
          str = key.ToString();
          opt_key = str;
        }
        DoCall(callback, opt_key, vec[i]);
      }
    } break;
    default:
      LOG(DFATAL) << "Unknown segment " << SegmentName(segment.type());
  }
  return {};
}

}  // namespace dfly::json::detail
