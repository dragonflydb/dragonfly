// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <map>
#include <optional>
#include <vector>

#include "core/search/base.h"
#include "core/search/compressed_sorted_set.h"

namespace dfly::search {

// Index for integer fields.
// Range bounds are queried in logarithmic time, iteration is constant.
struct NumericIndex : public BaseIndex {
  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  std::vector<DocId> Range(int64_t l, int64_t r) const;

 private:
  absl::btree_set<std::pair<int64_t, DocId>> entries_;
};

// Base index for string based indices.
struct BaseStringIndex : public BaseIndex {
  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  // Used by Add & Remove to tokenize text value
  virtual absl::flat_hash_set<std::string> Tokenize(std::string_view value) const = 0;

  // Pointer is valid as long as index is not mutated. Nullptr if not found
  const CompressedSortedSet* Matching(std::string_view str) const;

 protected:
  absl::flat_hash_map<std::string, CompressedSortedSet> entries_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TextIndex : public BaseStringIndex {
  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TagIndex : public BaseStringIndex {
  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;
};

// Index for vector fields.
// Only supports lookup by id.
struct VectorIndex : public BaseIndex {
  VectorIndex(size_t dim, VectorSimilarity sim);

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  const float* Get(DocId doc) const;
  std::pair<size_t /*dim*/, VectorSimilarity> Info() const;

 private:
  size_t dim_;
  VectorSimilarity sim_;
  std::vector<float> entries_;
};

}  // namespace dfly::search
