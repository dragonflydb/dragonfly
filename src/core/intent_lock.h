// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <assert.h>

#include <ostream>

#pragma once

namespace dfly {

// SHARED - can be acquired multiple times as long as other intents are absent.
// EXCLUSIVE - is acquired only if it's the only lock recorded.
// Transactions at the head of tx-queue are considered to be the ones that acquired the lock
class IntentLock {
 public:
  enum Mode { SHARED = 0, EXCLUSIVE = 1 };

  // Returns true if lock was acquired. In any case, the intent is recorded.
  bool Acquire(Mode m) {
    ++cnt_[m];

    if (cnt_[1 ^ int(m)])
      return false;
    return m == SHARED || cnt_[EXCLUSIVE] == 1;
  }

  // Returns true if lock can be acquired using `m` mode.
  bool Check(Mode m) const {
    unsigned s = cnt_[EXCLUSIVE];
    if (s)
      return false;

    return (m == SHARED) ? true : cnt_[SHARED] == 0;
  }

  // Returns true if this lock would block transactions from running unless they are at the head
  // of the transaction queue (first ones)
  bool IsContended() const {
    return (cnt_[EXCLUSIVE] > 1) || (cnt_[EXCLUSIVE] == 1 && cnt_[SHARED] > 0);
  }

  // A heuristic function to estimate the contention amount with a single score.
  unsigned ContentionScore() const {
    return cnt_[EXCLUSIVE] * 256 + cnt_[SHARED];
  }

  void Release(Mode m, unsigned val = 1) {
    assert(cnt_[m] >= val);

    cnt_[m] -= val;
    // return cnt_[m] == 0 ? cnt_[1 ^ int(m)] : 0;
  }

  bool IsFree() const {
    return (cnt_[0] | cnt_[1]) == 0;
  }

  static const char* ModeName(Mode m);

  void VerifyDebug();

  friend std::ostream& operator<<(std::ostream& o, const IntentLock& lock) {
    return o << "{SHARED: " << lock.cnt_[0] << ", EXCLUSIVE: " << lock.cnt_[1] << "}";
  }

 private:
  unsigned cnt_[2] = {0, 0};
};

}  // namespace dfly
