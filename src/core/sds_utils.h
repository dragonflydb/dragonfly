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

// Updates the expire time of the sds object. The offset is the number of bytes
void SdsUpdateExpireTime(const void* obj, uint32_t time_at, uint32_t offset);

}  // namespace dfly
