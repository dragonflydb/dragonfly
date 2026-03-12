// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

namespace dfly {

extern const char kGitTag[];
extern const char kGitSha[];
extern const char kGitClean[];
extern const char kBuildTime[];

const char* GetVersion();

// An enum for internal versioning of dragonfly specific behavior.
// Please document for each new entry what the behavior changes are
// and to which released versions this corresponds.
enum class DflyVersion {
  // 1.4  <= ver <= 1.10
  // - Supports receiving ACKs from replicas
  // - Sends version back on REPLCONF capa dragonfly
  VER1,

  // 1.11 <= ver
  // Supports limited partial sync
  VER2,

  // 1.15 < ver
  // ACL with user replication
  VER3,

  // - Periodic lag checks from master to replica
  VER4,

  // - Support partial sync from different master
  VER5,

  // 1.37 <= ver
  // - Per-shard search index definitions (search-index AUX on every flow)
  // - HNSW index serialization opcodes (RDB_OPCODE_VECTOR_INDEX, RDB_OPCODE_SHARD_DOC_INDEX)
  // - hnsw-index-metadata AUX field
  VER6,

  // Always points to the latest version
  CURRENT_VER = VER6,
};

}  // namespace dfly
