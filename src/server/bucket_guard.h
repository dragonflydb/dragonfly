// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#include "util/fibers/synchronization.h"

namespace dfly {
// Helper class used to guarantee atomicity between serialization of buckets
template <typename T> class BucketSerializationGuard {
 public:
  explicit BucketSerializationGuard(T* enclosing) : enclosing_(enclosing) {
    util::fb2::NoOpLock noop_lk_;
    enclosing_->bucket_ser_cond_.wait(noop_lk_,
                                      [this]() { return !enclosing_->bucket_ser_in_progress_; });
    enclosing_->bucket_ser_in_progress_ = true;
  }

  ~BucketSerializationGuard() {
    enclosing_->bucket_ser_in_progress_ = false;
    enclosing_->bucket_ser_cond_.notify_one();
  }

 private:
  T* enclosing_;
};

}  // namespace dfly
