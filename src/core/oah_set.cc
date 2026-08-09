// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_set.h"

namespace dfly {

void OAHSet::Fill(OAHSet* other) {
  assert(other->entries_.empty());
  other->Reserve(UpperBoundSize());
  other->set_time(time_now());
  for (auto it = begin(), it_end = end(); it != it_end; ++it) {
    // Copy the stored (already-encoded) content verbatim -- no decode/re-encode round-trip.
    const oah::key::Stored s = (*it).StoredKey();
    const uint32_t ttl = it.HasExpiry() ? it.ExpiryTime() - time_now() : UINT32_MAX;
    other->AddImpl({s.content, s.header.content_size}, s.header.len, ttl);
  }
}

}  // namespace dfly
