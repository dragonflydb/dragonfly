// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#ifndef WITH_COLLECTION_CMDS
#include "base/logging.h"
#include "server/hset_family.h"
#include "server/set_family.h"
#include "server/stream_family.h"

namespace dfly {

using namespace std;

namespace {
void Fail() {
  CHECK(false) << "Compiled without command support";
}
}  // namespace

StreamMemTracker::StreamMemTracker() {
}

void StreamMemTracker::UpdateStreamSize(PrimeValue& pv) const {
}

StringMap* HSetFamily::ConvertToStrMap(uint8_t* lp) {
  Fail();
  return nullptr;
}

StringSet* SetFamily::ConvertToStrSet(const intset* is, size_t expected_len) {
  Fail();
  return nullptr;
}

uint32_t SetFamily::MaxIntsetEntries() {
  Fail();
  return 0;
}

}  // namespace dfly

#endif
