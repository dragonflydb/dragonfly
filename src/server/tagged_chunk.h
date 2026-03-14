// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <array>
#include <cstdint>

#include "io/io.h"

namespace dfly {

enum class ChunkHeaderTag {
  Baseline = 0,
};

class TagStrippingSource : public io::Source {
 public:
  explicit TagStrippingSource(Source* upstream) : upstream_{upstream} {
  }

  io::Result<unsigned long> ReadSome(const iovec* v, uint32_t len) override;

 private:
  io::Result<unsigned long> ReadHeader();
  absl::InlinedVector<iovec, 4> GetCappedVec(const iovec* v, uint32_t len) const;

  Source* upstream_;
  std::array<char, 8> header_{};

  uint32_t remaining_payload_bytes_ = 0;
  bool eof_{false};
};

}  // namespace dfly
