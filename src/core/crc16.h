// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>

namespace dfly {

// CRC16-XMODEM (polynomial 0x1021, zero init, no reflection) - the checksum that Redis Cluster
// uses to map keys to hash slots. Returns the same value as redis crc16(), but consumes 16 bytes
// per iteration instead of one, which makes it several times faster for typical keys.
uint16_t Crc16(std::string_view data);

}  // namespace dfly
