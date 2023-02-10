// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/base/macros.h>

#include "base/logging.h"
#include "core/intent_lock.h"

namespace dfly {

const char* IntentLock::ModeName(Mode m) {
  switch (m) {
    case IntentLock::SHARED:
      return "SHARED";
    case IntentLock::EXCLUSIVE:
      return "EXCLUSIVE";
  }

  ABSL_UNREACHABLE();
}

void IntentLock::VerifyDebug() {
  constexpr uint32_t kMsb = 1ULL << (sizeof(cnt_[0]) * 8 - 1);
  DCHECK_EQ(0u, cnt_[0] & kMsb);
  DCHECK_EQ(0u, cnt_[1] & kMsb);
}

}  // namespace dfly
