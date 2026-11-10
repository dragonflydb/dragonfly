// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace dfly {
namespace search {

enum class TagType { PREFIX, SUFFIX, INFIX, REGULAR, WILDCARD };

// Comparison of a tag range clause such as `@field:>{value}`.
enum class TagRangeOp { LESS, LESS_EQ, GREATER, GREATER_EQ };

}  // namespace search
}  // namespace dfly
