// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/search.h"

namespace dfly::search {

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

 private:
  bool copy_vector_;
  size_t dim_;
  VectorSimilarity sim_;
  std::unique_ptr<HnswlibAdapter> adapter_;
};

}  // namespace dfly::search
