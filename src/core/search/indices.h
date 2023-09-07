// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <map>
#include <memory>
#include <memory_resource>
#include <optional>
#include <vector>

#include "core/search/base.h"
#include "core/search/compressed_sorted_set.h"

namespace dfly::search {

// Index for integer fields.
// Range bounds are queried in logarithmic time, iteration is constant.
struct NumericIndex : public BaseIndex {
  NumericIndex(std::pmr::memory_resource* mr);

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  std::vector<DocId> Range(int64_t l, int64_t r) const;

 private:
  using Entry = std::pair<int64_t, DocId>;
  absl::btree_set<Entry, std::less<Entry>, std::pmr::polymorphic_allocator<Entry>> entries_;
};

// Base index for string based indices.
struct BaseStringIndex : public BaseIndex {
  BaseStringIndex(std::pmr::memory_resource* mr);

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  // Used by Add & Remove to tokenize text value
  virtual absl::flat_hash_set<std::string> Tokenize(std::string_view value) const = 0;

  // Pointer is valid as long as index is not mutated. Nullptr if not found
  const CompressedSortedSet* Matching(std::string_view str) const;

 protected:
  CompressedSortedSet* GetOrCreate(std::string_view word);

  template <typename K, typename V, typename H, typename E, typename A>
  static auto WithPmr(absl::flat_hash_map<K, V, H, E, A> m)
      -> absl::flat_hash_map<K, V, H, E, std::pmr::polymorphic_allocator<std::pair<const K, V>>>;

  decltype(WithPmr(absl::flat_hash_map<std::string, CompressedSortedSet>{})) entries_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TextIndex : public BaseStringIndex {
  TextIndex(std::pmr::memory_resource* mr) : BaseStringIndex(mr) {
  }

  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TagIndex : public BaseStringIndex {
  TagIndex(std::pmr::memory_resource* mr) : BaseStringIndex(mr) {
  }

  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;
};

struct BaseVectorIndex : public BaseIndex {
  std::pair<size_t /*dim*/, VectorSimilarity> Info() const;

 protected:
  BaseVectorIndex(size_t dim, VectorSimilarity sim);

  size_t dim_;
  VectorSimilarity sim_;
};

// Index for vector fields.
// Only supports lookup by id.
struct FlatVectorIndex : public BaseVectorIndex {
  FlatVectorIndex(size_t dim, VectorSimilarity sim, std::pmr::memory_resource* mr);

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  const float* Get(DocId doc) const;

 private:
  std::pmr::vector<float> entries_;
};

struct HnswlibAdapter;

struct HnswVectorIndex : public BaseVectorIndex {
  HnswVectorIndex(size_t dim, VectorSimilarity sim, size_t capacity, std::pmr::memory_resource* mr);
  ~HnswVectorIndex();

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  std::vector<std::pair<float, DocId>> Knn(float* target, size_t k) const;
  std::vector<std::pair<float, DocId>> Knn(float* target, size_t k,
                                           const std::vector<DocId>& allowed) const;

 private:
  std::unique_ptr<HnswlibAdapter> adapter_;
};

}  // namespace dfly::search
