// Copyright 205, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/flat_hash_set.h>

#include "util/fibers/synchronization.h"

namespace dfly {

#ifdef NDEBUG
#define DEBUG_SYNC(n, f)
#else
#define DEBUG_SYNC(n, f)          \
  {                               \
    if (debug_sync_point.Find(n)) \
      f();                        \
  }
#endif

class DebugSyncPoint {
 public:
  void Add(std::string_view name) {
    util::fb2::LockGuard lk(mu_);
    sync_points_.emplace(name);
  }

  void Del(std::string_view name) {
    util::fb2::LockGuard lk(mu_);
    sync_points_.erase(name);
  }

  bool Find(std::string_view name) {
    return sync_points_.find(name) != sync_points_.end();
  }

 private:
  absl::flat_hash_set<std::string> sync_points_;
  util::fb2::Mutex mu_;
};

inline DebugSyncPoint debug_sync_point;

}  // namespace dfly
