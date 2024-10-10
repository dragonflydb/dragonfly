// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/search/base.h"
#include "core/search/block_list.h"
#include "core/search/compressed_sorted_set.h"

// TODO: move core field definitions out of big header
#include "core/search/search.h"

namespace dfly::search {

// Index for integer fields.
// Range bounds are queried in logarithmic time, iteration is constant.
struct NumericIndex : public BaseIndex {
  explicit NumericIndex(PMR_NS::memory_resource* mr);

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  std::vector<DocId> Range(double l, double r) const;

 private:
  using Entry = std::pair<double, DocId>;
  absl::btree_set<Entry, std::less<Entry>, PMR_NS::polymorphic_allocator<Entry>> entries_;
};

// Base index for string based indices.
template <typename C> struct BaseStringIndex : public BaseIndex {
  using Container = BlockList<C>;

  BaseStringIndex(PMR_NS::memory_resource* mr, bool case_sensitive);

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  // Used by Add & Remove to tokenize text value
  virtual absl::flat_hash_set<std::string> Tokenize(std::string_view value) const = 0;

  // Pointer is valid as long as index is not mutated. Nullptr if not found
  const Container* Matching(std::string_view str) const;

  // Returns all the terms that appear as keys in the reverse index.
  std::vector<std::string> GetTerms() const;

 protected:
  Container* GetOrCreate(std::string_view word);

  struct PmrEqual {
    using is_transparent = void;
    bool operator()(const PMR_NS::string& lhs, const PMR_NS::string& rhs) const {
      return lhs == rhs;
    }
    bool operator()(const PMR_NS::string& lhs, const std::string_view& rhs) const {
      return lhs == rhs;
    }
  };

  struct PmrHash {
    using is_transparent = void;
    size_t operator()(const std::string_view& sv) const {
      return absl::Hash<std::string_view>()(sv);
    }
    size_t operator()(const PMR_NS::string& pmrs) const {
      return operator()(std::string_view{pmrs.data(), pmrs.size()});
    }
  };

  bool case_sensitive_ = false;

  absl::flat_hash_map<PMR_NS::string, Container, PmrHash, PmrEqual,
                      PMR_NS::polymorphic_allocator<std::pair<PMR_NS::string, Container>>>
      entries_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TextIndex : public BaseStringIndex<CompressedSortedSet> {
  using StopWords = absl::flat_hash_set<std::string>;

  TextIndex(PMR_NS::memory_resource* mr, const StopWords* stopwords)
      : BaseStringIndex(mr, false), stopwords_{stopwords} {
  }

  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;

 private:
  const StopWords* stopwords_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TagIndex : public BaseStringIndex<SortedVector> {
  TagIndex(PMR_NS::memory_resource* mr, SchemaField::TagParams params)
      : BaseStringIndex(mr, params.case_sensitive), separator_{params.separator} {
  }

  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;

 private:
  char separator_;
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
  FlatVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource* mr);

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  const float* Get(DocId doc) const;

 private:
  PMR_NS::vector<float> entries_;
};

struct HnswlibAdapter;

struct HnswVectorIndex : public BaseVectorIndex {
  HnswVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource* mr);
  ~HnswVectorIndex();

  void Add(DocId id, DocumentAccessor* doc, std::string_view field) override;
  void Remove(DocId id, DocumentAccessor* doc, std::string_view field) override;

  std::vector<std::pair<float, DocId>> Knn(float* target, size_t k, std::optional<size_t> ef) const;
  std::vector<std::pair<float, DocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                           const std::vector<DocId>& allowed) const;

 private:
  std::unique_ptr<HnswlibAdapter> adapter_;
};

}  // namespace dfly::search
