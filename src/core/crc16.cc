// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/crc16.h"

#include <array>

#include "base/endian.h"

namespace dfly {

namespace {

constexpr unsigned kSlices = 16;
using Crc16Tables = std::array<std::array<uint16_t, 256>, kSlices>;

// kTables[0] is the classic byte-at-a-time table. kTables[k][b] is the CRC of byte b followed by
// k zero bytes, so XOR-ing kTables[n - 1 - i][block[i]] over an n-byte block advances the CRC
// over the whole block at once ("slicing-by-n").
constexpr Crc16Tables MakeTables() {
  Crc16Tables tables{};
  for (unsigned b = 0; b < 256; ++b) {
    uint16_t crc = b << 8;
    for (unsigned bit = 0; bit < 8; ++bit)
      crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : crc << 1;
    tables[0][b] = crc;
  }
  for (unsigned k = 1; k < kSlices; ++k) {
    for (unsigned b = 0; b < 256; ++b) {
      uint16_t prev = tables[k - 1][b];
      tables[k][b] = (prev << 8) ^ tables[0][prev >> 8];
    }
  }
  return tables;
}

alignas(64) constexpr Crc16Tables kTables = MakeTables();

// Folds 8 block bytes, packed into `v` with the first byte in the lowest bits, that are followed
// by `trailing` more bytes in the block.
inline uint16_t Fold8(uint64_t v, unsigned trailing) {
  uint16_t res = 0;
  for (unsigned i = 0; i < 8; ++i)
    res ^= kTables[trailing + 7 - i][(v >> (8 * i)) & 0xff];
  return res;
}

}  // namespace

uint16_t Crc16(std::string_view data) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(data.data());
  size_t len = data.size();
  uint16_t crc = 0;

  // The CRC register is XOR-ed into the first two bytes of a block, most significant byte first.
  auto load_with_crc = [&crc](const uint8_t* block) {
    return absl::little_endian::Load64(block) ^ ((crc >> 8) | ((crc & 0xff) << 8));
  };

  for (; len >= 16; len -= 16, p += 16)
    crc = Fold8(load_with_crc(p), 8) ^ Fold8(absl::little_endian::Load64(p + 8), 0);

  if (len >= 8) {
    crc = Fold8(load_with_crc(p), 0);
    len -= 8;
    p += 8;
  }

  for (; len > 0; --len, ++p)
    crc = (crc << 8) ^ kTables[0][(crc >> 8) ^ *p];

  return crc;
}

}  // namespace dfly
