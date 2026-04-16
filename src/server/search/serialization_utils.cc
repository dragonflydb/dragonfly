// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/serialization_utils.h"

#include "base/logging.h"
#include "server/common.h"
#include "server/engine_shard.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/namespaces.h"
#include "server/rdb_extensions.h"
#include "server/rdb_save.h"
#include "server/search/doc_index.h"
#include "server/search/global_hnsw_index.h"

namespace dfly {

#ifdef WITH_SEARCH

std::error_code SearchSerializer::SerializeIndexMapping(
    uint32_t shard_id, std::string_view index_name,
    std::span<const std::pair<std::string, search::DocId>> mappings) const {
  // Format: [RDB_OPCODE_SHARD_DOC_INDEX, shard_id, index_name, mapping_count,
  //          then for each mapping: key_string, doc_id]
  RETURN_ON_ERR(serializer_->WriteOpcode(RDB_OPCODE_SHARD_DOC_INDEX));
  RETURN_ON_ERR(serializer_->SaveLen(shard_id));
  RETURN_ON_ERR(serializer_->SaveString(index_name));
  RETURN_ON_ERR(serializer_->SaveLen(mappings.size()));

  for (const auto& [key, doc_id] : mappings) {
    RETURN_ON_ERR(serializer_->SaveString(key));
    RETURN_ON_ERR(serializer_->SaveLen(doc_id));
  }

  push_fun_();
  return {};
}

void SearchSerializer::SerializeIndexMappings() const {
  // Get all HNSW index names from the global registry
  absl::flat_hash_set<std::string> hnsw_index_names =
      GlobalHnswIndexRegistry::Instance().GetIndexNames();

  auto* indices = db_slice_->shard_owner()->search_indices();
  uint32_t shard_id = db_slice_->shard_owner()->shard_id();

  for (const auto& index_name : hnsw_index_names) {
    auto* index = indices->GetIndex(index_name);
    if (!index) {
      continue;
    }

    auto mappings = index->SerializeKeyIndex();
    if (mappings.empty()) {
      continue;
    }

    auto ec = SerializeIndexMapping(shard_id, index_name, mappings);
    static_cast<void>(ec);
  }
}

void SearchSerializer::SerializeGlobalHnswIndices() const {
  // Serialize HNSW global indices for shard 0 only
  if (db_slice_->shard_owner()->shard_id() != 0)
    return;

  auto all_indices = GlobalHnswIndexRegistry::Instance().GetAll();
  if (all_indices.empty())
    return;

  // Buffer all HNSW mutations at the doc_index level so they don't reach
  // the HNSW graph while we hold the read lock for serialization.
  shard_set->RunBriefInParallel([](EngineShard* es) {
    for (const auto& index_name : es->search_indices()->GetIndexNames()) {
      if (auto* shard_index = es->search_indices()->GetIndex(index_name)) {
        shard_index->SetHnswSerializing();
      }
    }
  });

  // Preallocate buffer for HNSW entry serialization.
  std::vector<uint8_t> tmp_buf;

  for (const auto& [index_key, index] : all_indices) {
    {
      // Acquire a read lock to ensure a consistent snapshot of the graph.
      auto read_lock = index->GetReadLock();

      // Format: [RDB_OPCODE_VECTOR_INDEX, index_name, elements_number,
      //          then for each node: binary encoded entry via SaveHNSWEntry]
      if (auto ec = serializer_->WriteOpcode(RDB_OPCODE_VECTOR_INDEX); ec) {
        continue;
      }
      if (auto ec = serializer_->SaveString(index_key); ec) {
        continue;
      }

      size_t node_count = index->GetNodeCount();
      if (auto ec = serializer_->SaveLen(node_count); ec) {
        continue;
      }

      constexpr size_t kBatchSize = 1000;
      for (size_t i = 0; i < node_count; i += kBatchSize) {
        size_t batch_end = std::min(i + kBatchSize, node_count);
        auto nodes = index->GetNodesRange(i, batch_end);
        for (const auto& node : nodes) {
          tmp_buf.resize(node.TotalSize());
          if (auto ec = serializer_->SaveHNSWEntry(node, absl::MakeSpan(tmp_buf)); ec)
            break;
        }
      }
    }  // read_lock released here

    // Flush after completing entire index to avoid splitting HNSW data across compressed blobs.
    // The HNSW loader expects all nodes for an index to be readable in one pass.
    push_fun_();
  }

  // Drain buffered HNSW updates on all shards and transition back to kBuilding.
  // Uses DrainSerializationUpdates which only acts on indices in kSerializing
  // state, so it cannot interfere with concurrent restoration.
  shard_set->AwaitRunningOnShardQueue([](EngineShard* es) {
    OpArgs op_args{es, nullptr,
                   DbContext{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()}};
    for (const auto& index_name : es->search_indices()->GetIndexNames()) {
      if (auto* shard_index = es->search_indices()->GetIndex(index_name)) {
        shard_index->DrainSerializationUpdates(op_args);
      }
    }
  });
}
#endif

}  // namespace dfly
