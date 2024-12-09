// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

extern "C" {
#include "redis/rdb.h"
}

//  Custom types: Range 30-35 is used by DF RDB types.
constexpr uint8_t RDB_TYPE_JSON = 30;
constexpr uint8_t RDB_TYPE_HASH_WITH_EXPIRY = 31;
constexpr uint8_t RDB_TYPE_SET_WITH_EXPIRY = 32;
constexpr uint8_t RDB_TYPE_SBF = 33;

constexpr bool rdbIsObjectTypeDF(uint8_t type) {
  return __rdbIsObjectType(type) || (type == RDB_TYPE_JSON) ||
         (type == RDB_TYPE_HASH_WITH_EXPIRY) || (type == RDB_TYPE_SET_WITH_EXPIRY) ||
         (type == RDB_TYPE_SBF);
}

//  Opcodes: Range 200-240 is used by DF extensions.

// This opcode is sent by the master Dragonfly instance to a replica
// to notify that it finished streaming static data and is ready
// to switch to the stable state replication phase.
constexpr uint8_t RDB_OPCODE_FULLSYNC_END = 200;

constexpr uint8_t RDB_OPCODE_COMPRESSED_ZSTD_BLOB_START = 201;
constexpr uint8_t RDB_OPCODE_COMPRESSED_LZ4_BLOB_START = 202;
constexpr uint8_t RDB_OPCODE_COMPRESSED_BLOB_END = 203;

constexpr uint8_t RDB_OPCODE_JOURNAL_BLOB = 210;

// A full sync will continue to send information in journal blobs until the replica
// sends a `DFLY STARTSTABLE` to the master.
// We use this opcode to synchronize the journal offsets at the end of the full sync,
// so it is always sent at the end of the RDB stream.
constexpr uint8_t RDB_OPCODE_JOURNAL_OFFSET = 211;

constexpr uint8_t RDB_OPCODE_DF_MASK = 220; /* Mask for key properties */

// RDB_OPCODE_DF_MASK define 4byte field with next flags
constexpr uint32_t DF_MASK_FLAG_STICKY = (1 << 0);
constexpr uint32_t DF_MASK_FLAG_MC_FLAGS = (1 << 1);
