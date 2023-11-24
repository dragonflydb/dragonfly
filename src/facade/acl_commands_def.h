// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace dfly::acl {
// Special flag/mask for all
constexpr uint32_t NONE = 0;
constexpr uint32_t ALL = std::numeric_limits<uint32_t>::max();
}  // namespace dfly::acl
