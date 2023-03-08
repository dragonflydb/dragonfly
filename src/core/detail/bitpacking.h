// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>

namespace dfly {

namespace detail {

bool validate_ascii_fast(const char* src, size_t len);

// unpacks 8->7 encoded blob back to ascii.
// generally, we can not unpack inplace because ascii (dest) buffer is 8/7 bigger than
// the source buffer.
// however, if binary data is positioned on the right of the ascii buffer with empty space on the
// left than we can unpack inplace.
void ascii_unpack(const uint8_t* bin, size_t ascii_len, char* ascii);
void ascii_unpack_simd(const uint8_t* bin, size_t ascii_len, char* ascii);

// packs ascii string (does not verify) into binary form saving 1 bit per byte on average (12.5%).
void ascii_pack(const char* ascii, size_t len, uint8_t* bin);
void ascii_pack2(const char* ascii, size_t len, uint8_t* bin);

// SIMD implementation 1 of ascii_pack.
void ascii_pack_simd(const char* ascii, size_t len, uint8_t* bin);

// SIMD implementation 2 of ascii_pack.
void ascii_pack_simd2(const char* ascii, size_t len, uint8_t* bin);

bool compare_packed(const uint8_t* packed, const char* ascii, size_t ascii_len);

// maps ascii len to 7-bit packed length. Each 8 bytes are converted to 7 bytes.
inline constexpr size_t binpacked_len(size_t ascii_len) {
  return (ascii_len * 7 + 7) / 8; /* rounded up */
}

}  // namespace detail
}  // namespace dfly
