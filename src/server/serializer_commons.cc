#include "server/serializer_commons.h"

extern "C" {
#include "redis/rdb.h"
}

#include <absl/base/internal/endian.h>

#include <system_error>

#include "base/logging.h"

using namespace std;

namespace dfly {

/* Saves an encoded unsigned integer. The first two bits in the first byte are used to
 * hold the encoding type. See the RDB_* definitions for more information
 * on the types of encoding. buf must be at least 9 bytes.
 * */
unsigned WritePackedUInt(uint64_t value, uint8_t* buf) {
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
    absl::big_endian::Store32(buf + 1, value);
    return 1 + 4;
  }

  /* Save a 64 bit value */
  buf[0] = RDB_64BITLEN;
  absl::big_endian::Store64(buf + 1, value);
  return 1 + 8;
}

io::Result<uint64_t> ReadPackedUInt(io::Source* source) {
  uint8_t buf[10];
  size_t read = 0;

  uint8_t first = 0;
  SET_OR_UNEXPECT(source->Read(io::MutableBytes{&first, 1}), read);
  if (read != 1)
    return make_unexpected(make_error_code(errc::bad_message));

  int type = (first & 0xC0) >> 6;
  switch (type) {
    case RDB_6BITLEN:
      return first & 0x3F;
    case RDB_14BITLEN:
      SET_OR_UNEXPECT(source->Read(io::MutableBytes{buf, 1}), read);
      if (read != 1)
        return make_unexpected(make_error_code(errc::bad_message));
      return ((first & 0x3F) << 8) | buf[0];
    case RDB_32BITLEN:
      SET_OR_UNEXPECT(source->Read(io::MutableBytes{buf, 4}), read);
      if (read != 4)
        return make_unexpected(make_error_code(errc::bad_message));
      return absl::big_endian::Load32(buf);
    case RDB_64BITLEN:
      SET_OR_UNEXPECT(source->Read(io::MutableBytes{buf, 8}), read);
      if (read != 8)
        return make_unexpected(make_error_code(errc::bad_message));
      return absl::big_endian::Load64(buf);
    default:
      return make_unexpected(make_error_code(errc::illegal_byte_sequence));
  }
}

}  // namespace dfly
