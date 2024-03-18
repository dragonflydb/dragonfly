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
  return AdvanceResult{};
}

FlatDfs FlatDfs::Traverse(absl::Span<const PathSegment> path, const flexbuffers::Reference root,
                          const PathFlatCallback& callback) {
  DCHECK(!path.empty());
  FlatDfs dfs;

  return dfs;
}

auto FlatDfs::PerformStep(const PathSegment& segment, const flexbuffers::Reference node,
                          const PathFlatCallback& callback) -> nonstd::expected<void, MatchStatus> {
  return {};
}

}  // namespace dfly::json::detail
