// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

// Range 200-240 is used by DF extensions.

// This opcode is sent by the master Dragonfly instance to a replica
// to notify that it finished streaming static data and is ready
// to switch to the stable state replication phase.
const uint8_t RDB_OPCODE_FULLSYNC_END = 200;
const uint8_t RDB_OPCODE_COMPRESSED_BLOB_START = 201;
const uint8_t RDB_OPCODE_COMPRESSED_BLOB_END = 202;
const uint8_t RDB_OPCODE_JOURNAL_BLOB = 203;
