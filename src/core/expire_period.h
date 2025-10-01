// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace dfly {

// ExpirePeriod encapsulates the expiration period of a key.
// It can represent periods in milliseconds up to ~49 days and in seconds up to ~136 years.
// If value in milliseconds is too high, it switches to seconds precision.
// And if it's still too high, it silently clamps to the max value.
class ExpirePeriod {
 public:
  static constexpr uint32_t kMaxGenId = 15;

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

  bool is_second_precision() const {
    return precision_ == 1;
  }

 private:
  uint32_t val_;
  uint8_t gen_ : 2;        // generation id.
  uint8_t precision_ : 1;  // 0 - ms, 1 - sec.
};

inline void ExpirePeriod::Set(uint64_t ms) {
  if (ms < UINT32_MAX) {
    val_ = ms;       // about 49 days in ms.
    precision_ = 0;  // ms
    return;
  }

  precision_ = 1;  // seconds
  if (ms < UINT64_MAX / 2) {
    ms = (ms + 500) / 1000;
    val_ = ms >= UINT32_MAX ? UINT32_MAX - 1 : ms;
  } else {
    val_ = UINT32_MAX - 1;
  }
}

}  // namespace dfly
