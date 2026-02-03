// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly {

class ExpirePeriod {
 public:
  static constexpr size_t kMaxGenId = 15;

  ExpirePeriod() : val_(0), gen_(0), precision_(0) {
    static_assert(sizeof(ExpirePeriod) == 8);  // TODO
  }

  explicit ExpirePeriod(uint64_t ms, unsigned gen = 0) : ExpirePeriod() {
    Set(ms);
  }

  // always returns milliseconds value.
  uint64_t duration_ms() const {
    return precision_ ? uint64_t(val_) * 1000 : val_;
  }

  // generation id for the base of this duration.
  // when we update the generation, we need to update the value as well according to this
  // logic:
  // new_val = (old_val + old_base) - new_base.
  unsigned generation_id() const {
    return gen_;
  }

  void Set(uint64_t ms);

  bool is_second_precision() { return precision_ == 1;}

 private:
  uint64_t val_ : 59;
  uint64_t gen_ : 4;
  uint64_t precision_ : 1;  // 0 - ms, 1 - sec.
};

inline void ExpirePeriod::Set(uint64_t ms) {
  constexpr uint64_t kBarrier = (1ULL << 48);

  if (ms < kBarrier) {
    val_ = ms;
    precision_ = 0;   // ms
    return;
  }

  precision_ = 1;
  if (ms < kBarrier << 10) {
    ms = (ms + 500) / 1000;   // seconds
  }
  val_ = ms >= kBarrier ? kBarrier - 1 : ms;
}

}  // namespace dfly
