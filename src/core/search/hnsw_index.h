// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

#include "core/search/mrmw_mutex.h"
#include "core/search/search.h"

namespace dfly::search {

// Metadata structure for HNSW index serialization
// Contains the key parameters needed to restore the index state
struct HnswIndexMetadata {
  size_t max_elements = 0;       // Maximum number of elements the index can hold
  size_t cur_element_count = 0;  // Current number of elements in the index
  int maxlevel = -1;             // Maximum level of the graph
  size_t enterpoint_node = 0;    // Entry point node for the graph
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
  void Remove(search::GlobalDocId id, const search::DocumentAccessor& doc, std::string_view field);

  bool IsVectorCopied() const {
    return copy_vector_;
  }

  std::vector<std::pair<float, GlobalDocId>> Knn(float* target, size_t k,
                                                 std::optional<size_t> ef) const;
  std::vector<std::pair<float, GlobalDocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                                 const std::vector<GlobalDocId>& allowed) const;
  std::vector<std::pair<float, GlobalDocId>> SubsetKnn(float* target, size_t k,
                                                       const std::vector<GlobalDocId>& docs) const;

  // Get metadata for serialization
  HnswIndexMetadata GetMetadata() const;

  // Set metadata (used during restoration)
  void SetMetadata(const HnswIndexMetadata& metadata);

  // Get total number of nodes in the index
  size_t GetNodeCount() const;

  // Get nodes in the specified range [start, end)
  // Returns vector of node data for serialization
  std::vector<HnswNodeData> GetNodesRange(size_t start, size_t end) const;

  // Restore graph structure from serialized nodes with metadata
  // This restores the HNSW graph links but NOT the vector data
  // Vector data must be populated separately via UpdateVectorData
  void RestoreFromNodes(const std::vector<HnswNodeData>& nodes, const HnswIndexMetadata& metadata);

  // Update vector data for an existing node (used after RestoreFromNodes)
  // This populates the vector data for a node that already has graph links
  bool UpdateVectorData(GlobalDocId id, const DocumentAccessor& doc, std::string_view field);

  // Acquire a read lock on the internal MRMW mutex.
  // Use this during serialization to block concurrent Add/Remove (write) operations.
  std::unique_ptr<MRMWMutexLock> GetReadLock() const;

 private:
  bool copy_vector_;
  size_t dim_;
  std::unique_ptr<HnswlibAdapter> adapter_;
};

}  // namespace dfly::search
