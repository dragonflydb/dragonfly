// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/search.h"

namespace dfly::search {

// Metadata structure for HNSW index serialization
// Contains the key parameters needed to restore the index state
struct HnswIndexMetadata {
  size_t max_elements = 0;       // Maximum number of elements the index can hold
  size_t cur_element_count = 0;  // Current number of elements in the index
  int maxlevel = -1;             // Maximum level of the graph
  size_t enterpoint_node = 0;    // Entry point node for the graph
  size_t M = 0;                  // Number of established connections per element
  size_t maxM = 0;               // Maximum connections for upper layers
  size_t maxM0 = 0;              // Maximum connections for layer 0
  size_t ef_construction = 0;    // Size of dynamic candidate list for construction
  double mult = 0.0;             // Multiplier for random level generation
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

  // Get metadata for serialization
  HnswIndexMetadata GetMetadata() const;

 private:
  bool copy_vector_;
  size_t dim_;
  VectorSimilarity sim_;
  std::unique_ptr<HnswlibAdapter> adapter_;
};

}  // namespace dfly::search
