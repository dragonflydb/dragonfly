// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <system_error>

#include "base/expected.hpp"
#include "io/io.h"
#include "server/error.h"

namespace dfly {

using nonstd::make_unexpected;

#define SET_OR_RETURN(expr, dest)              \
  do {                                         \
    auto exp_val = (expr);                     \
    if (!exp_val) {                            \
      VLOG(1) << "Error while calling " #expr; \
      return exp_val.error();                  \
    }                                          \
    dest = std::move(exp_val.value());         \
  } while (0)

#define SET_OR_UNEXPECT(expr, dest)            \
  {                                            \
    auto exp_res = (expr);                     \
    if (!exp_res)                              \
      return make_unexpected(exp_res.error()); \
    dest = std::move(exp_res.value());         \
  }

// Represents meta information for an encoded packed unsigned integer.
struct PackedUIntMeta {
  // Initialize by first byte in sequence.
  PackedUIntMeta(uint8_t first_byte) : first_byte{first_byte} {
  }

  // Get underlying RDB type.
  int Type() const;

  // Get additional size in bytes (excluding first one).
  unsigned ByteSize() const;

  uint8_t first_byte;
};

// Saves an packed unsigned integer. The first two bits in the first byte are used to
// hold the encoding type. See the RDB_* definitions for more information
// on the types of encoding. buf must be at least 9 bytes.
unsigned WritePackedUInt(uint64_t value, io::MutableBytes dest);

// Deserialize packed unsigned integer.
io::Result<uint64_t> ReadPackedUInt(PackedUIntMeta meta, io::Bytes source);

}  // namespace dfly
