// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly {

// Allocates an sds string that has an additional space at the end that
// sds does is not aware of. Useful when you need to allocate immutable
// sds string (keys) with metadata attached to them.
char* AllocSdsWithSpace(uint32_t strlen, uint32_t space);

}  // namespace dfly
