#include "server/serializer_commons.h"

extern "C" {
#include "redis/rdb.h"
}

#include <absl/base/internal/endian.h>

#include <system_error>

#include "base/logging.h"

using namespace std;

namespace dfly {

int PackedUIntMeta::Type() const {
  return (first_byte & 0xC0) >> 6;
}

unsigned PackedUIntMeta::ByteSize() const {
  switch (Type()) {
    case RDB_ENCVAL:
    case RDB_6BITLEN:
      return 0;
    case RDB_14BITLEN:
      return 1;
  };
  switch (first_byte) {
    case RDB_32BITLEN:
      return 4;
    case RDB_64BITLEN:
      return 8;
  };
  return 0;
}

/* Saves an encoded unsigned integer. The first two bits in the first byte are used to
 * hold the encoding type. See the RDB_* definitions for more information
 * on the types of encoding. buf must be at least 9 bytes.
 * */
unsigned WritePackedUInt(uint64_t value, io::MutableBytes buf) {
  if (value < (1 << 6)) {
    /* Save a 6 bit value */
    buf[0] = (value & 0xFF) | (RDB_6BITLEN << 6);
    return 1;
  }

  if (value < (1 << 14)) {
    /* Save a 14 bit value */
    buf[0] = ((value >> 8) & 0xFF) | (RDB_14BITLEN << 6);
    buf[1] = value & 0xFF;
    return 2;
  }

  if (value <= UINT32_MAX) {
    /* Save a 32 bit value */
    buf[0] = RDB_32BITLEN;
    absl::big_endian::Store32(buf.data() + 1, value);
    return 1 + 4;
  }

  /* Save a 64 bit value */
  buf[0] = RDB_64BITLEN;
  absl::big_endian::Store64(buf.data() + 1, value);
  return 1 + 8;
}

io::Result<uint64_t> ReadPackedUInt(PackedUIntMeta meta, io::Bytes bytes) {
  DCHECK(meta.ByteSize() <= bytes.size());
  switch (meta.Type()) {
    case RDB_ENCVAL:
    case RDB_6BITLEN:
      return meta.first_byte & 0x3F;
    case RDB_14BITLEN:
      return ((meta.first_byte & 0x3F) << 8) | bytes[0];
  };
  switch (meta.first_byte) {
    case RDB_32BITLEN:
      return absl::big_endian::Load32(bytes.data());
    case RDB_64BITLEN:
      return absl::big_endian::Load64(bytes.data());
  };
  return make_unexpected(make_error_code(errc::illegal_byte_sequence));
}

}  // namespace dfly
