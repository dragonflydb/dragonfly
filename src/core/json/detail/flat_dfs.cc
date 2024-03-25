// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/json/detail/flat_dfs.h"

#include "base/logging.h"

namespace dfly::json::detail {

using namespace std;
using nonstd::make_unexpected;

inline bool IsRecursive(flexbuffers::Type type) {
  return false;
}

// TODO: to finish everything
auto FlatDfsItem::Init(const PathSegment& segment) -> AdvanceResult {
  return AdvanceResult{};
}

auto FlatDfsItem::Advance(const PathSegment& segment) -> AdvanceResult {
  if (state_ == kInit) {
    return Init(segment);
  }

  if (!ShouldIterateAll(segment.type()))
    return Exhausted();

  ++state_;
  auto vec = obj().AsVector();
  if (state_ >= vec.size())
    return Exhausted();
  return Next(vec[state_]);
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
      if (!node.IsVector())
        return make_unexpected(MISMATCH);
      auto vec = node.AsVector();
      if (segment.index() >= vec.size()) {
        return make_unexpected(OUT_OF_BOUNDS);
      }
      DoCall(callback, nullopt, vec[segment.index()]);
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
