// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string>
#include <vector>

#include "core/search/base.h"
#include "core/search/hnsw_index.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class Service;

// Dispatches a search command (FT.CREATE / FT.SYNUPDATE) from a serialized AUX string.
void LoadSearchCommandFromAux(Service* service, std::string&& def, std::string_view command_name,
                              std::string_view error_context, bool add_NX = false);

// Pending index key-to-DocId mappings to apply after indices are created.
struct PendingIndexMapping {
  std::string index_name;
  std::vector<std::pair<std::string, search::DocId>> mappings;
};

// Deferred HNSW graph nodes for restoration when shard counts differ.
// The entry-point travels with the nodes inside RDB_OPCODE_VECTOR_INDEX.
struct PendingHnswNodes {
  std::string index_name;
  std::string field_name;
  search::HnswIndexMetadata metadata;
  std::vector<search::HnswNodeData> nodes;
};

// Shared context for collecting search-related state across multiple RdbLoader instances
// during a single load session. Consumed by PerformPostLoad after all loaders finish.
//
// Thread-safe: all mutating methods lock internally.
class RdbLoadContext {
 public:
  RdbLoadContext() = default;

  RdbLoadContext(const RdbLoadContext&) = delete;
  RdbLoadContext& operator=(const RdbLoadContext&) = delete;

  void AddPendingSynonymCommand(std::string cmd);
  void AddPendingIndexMapping(uint32_t shard_id, PendingIndexMapping mapping);
  void AddPendingHnswNodes(PendingHnswNodes nodes);
  void SetMasterShardCount(uint32_t count);

  // Performs post load procedures while still remaining in global LOADING state.
  // Called once immediately after loading the snapshot / full sync succeeded from the coordinator.
  void PerformPostLoad(Service* service, bool is_error = false);

 private:
  std::vector<std::string> TakePendingSynonymCommands();
  absl::flat_hash_map<uint32_t, std::vector<PendingIndexMapping>> TakePendingIndexMappings();
  std::vector<PendingHnswNodes> TakePendingHnswNodes();

  // Pre-distributed key mappings indexed by target shard_id.
  // Per-shard: index_name -> keys in doc_id order (vector index = doc_id).
  using PerShardMappings = std::vector<absl::flat_hash_map<std::string, std::vector<std::string>>>;

  // Remaps HNSW node global_ids, restores HNSW graphs, and pre-distributes key mappings by
  // target shard. The internal remap table is local and freed when this function returns.
  // Failed indices are excluded from the returned mappings so they fall back to a full rebuild.
  PerShardMappings RemapHnswForDifferentShardCount(
      const absl::flat_hash_map<uint32_t, std::vector<PendingIndexMapping>>& index_mappings,
      std::vector<PendingHnswNodes>& pending_nodes);

  mutable util::fb2::Mutex mu_;
  std::vector<std::string> pending_synonym_cmds_ ABSL_GUARDED_BY(mu_);
  absl::flat_hash_map<uint32_t, std::vector<PendingIndexMapping>> pending_index_mappings_
      ABSL_GUARDED_BY(mu_);
  std::vector<PendingHnswNodes> pending_hnsw_nodes_ ABSL_GUARDED_BY(mu_);
  uint32_t master_shard_count_ = 0;  // Set identically by all loaders from AUX field.
};

}  // namespace dfly
