// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include "util/fibers/synchronization.h"

namespace dfly {

struct ConditionFlag {
  util::fb2::CondVarAny cond_var;
  bool flag = false;
};

// Helper class used to guarantee atomicity between serialization of buckets
class BucketSerializationGuard {
 public:
  explicit BucketSerializationGuard(ConditionFlag* enclosing) : enclosing_(enclosing) {
    util::fb2::NoOpLock noop_lk_;
    enclosing_->cond_var.wait(noop_lk_, [this]() { return !enclosing_->flag; });
    enclosing_->flag = true;
  }

  ~BucketSerializationGuard() {
    enclosing_->flag = false;
    enclosing_->cond_var.notify_one();
  }

 private:
  ConditionFlag* enclosing_;
};

}  // namespace dfly
