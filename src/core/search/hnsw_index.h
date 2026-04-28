// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

#include "core/search/mrmw_mutex.h"
#include "core/search/search.h"

namespace dfly::search {

// Wire format for HNSW index AUX. Only the entry point is persisted: capacity is
// derived from max(internal_id)+1 in the node set and maxlevel from the entry-point
// node's level (hnswlib pairs enterpoint_node_ with maxlevel_, and node levels are
// immutable after creation).
struct HnswIndexMetadata {
  size_t enterpoint_node = 0;
};

// Node data structure for HNSW serialization
struct HnswNodeData {
  uint32_t internal_id;
  GlobalDocId global_id;
  int level;
  std::vector<std::vector<uint32_t>> levels_links;  // Links for each level (0 to level)

  // Returns the total serialized size in bytes.
  // Format: internal_id(4) + global_id(8) + level(4)
  //         + for each level: links_num(4) + links(4 each)
  size_t TotalSize() const {
    size_t size = 4 + 8 + 4;  // internal_id + global_id + level
    for (const auto& links : levels_links) {
      size += 4 + links.size() * 4;  // links_num + links
    }
    return size;
  }
};

struct HnswlibAdapter;
class HnswVectorIndex {
 public:
  explicit HnswVectorIndex(const search::SchemaField::VectorParams& params, bool copy_vector,
                           PMR_NS::memory_resource* mr = PMR_NS::get_default_resource());

  ~HnswVectorIndex();

  bool Add(search::GlobalDocId id, const search::DocumentAccessor& doc, std::string_view field);

  void Remove(search::GlobalDocId id);

  bool IsVectorCopied() const {
    return copy_vector_;
  }

  std::vector<std::pair<float, GlobalDocId>> Knn(float* target, size_t k,
                                                 std::optional<size_t> ef) const;
  std::vector<std::pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                                 const std::vector<GlobalDocId>& allowed) const;
  std::vector<std::pair<float, GlobalDocId>> SubsetKnn(float* target, size_t k,
                                                       const std::vector<GlobalDocId>& docs) const;

  // Returns all documents within radius, with their distances.
  std::vector<std::pair<float, GlobalDocId>> RangeQuery(float* target, float radius) const;

  size_t GetDim() const {
    return dim_;
  }

  // Get metadata for serialization
  HnswIndexMetadata GetMetadata() const;

  // Current graph maxlevel_. Exposed for introspection and tests that need to
  // verify invariants preserved by RestoreFromNodes (entry point must sit at maxlevel).
  int GetMaxLevel() const;

  // Get total number of nodes in the index
  size_t GetNodeCount() const;

  // Get nodes in the specified range [start, end)
  // Returns vector of node data for serialization
  std::vector<HnswNodeData> GetNodesRange(size_t start, size_t end) const;

  // Restore graph structure from serialized nodes with metadata.
  // Restores links only; vector data must be populated separately via UpdateVectorData.
  // Returns false if the metadata is inconsistent with the node set (e.g. the entry
  // point is missing from the serialized nodes) — caller should then leave the index
  // empty and let the higher-level rebuild path repopulate it from the keyspace.
  bool RestoreFromNodes(const std::vector<HnswNodeData>& nodes, const HnswIndexMetadata& metadata);

  // Update vector data for an existing node (used after RestoreFromNodes)
  // This populates the vector data for a node that already has graph links
  bool UpdateVectorData(GlobalDocId id, const DocumentAccessor& doc, std::string_view field);

  // Acquire a read lock on the internal MRMW mutex.
  // Use this during serialization to block concurrent Add/Remove (write) operations.
  MRMWMutexLock GetReadLock() const;

  // Approximate in-memory footprint of this HNSW graph, in bytes.
  size_t GetMemoryUsage() const;

 private:
  bool copy_vector_;
  size_t dim_;
  std::unique_ptr<HnswlibAdapter> adapter_;
};

}  // namespace dfly::search
