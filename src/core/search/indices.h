// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "absl/functional/function_ref.h"
#include "base/pmr/memory_resource.h"
#include "core/search/base.h"
#include "core/search/block_list.h"
#include "core/search/compressed_sorted_set.h"
#include "core/search/rax_tree.h"

// TODO: move core field definitions out of big header
#include "core/search/search.h"

namespace dfly::search {

// Index for integer fields.
// Range bounds are queried in logarithmic time, iteration is constant.
struct NumericIndex : public BaseIndex {
  explicit NumericIndex(PMR_NS::memory_resource* mr);

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  std::vector<DocId> Range(double l, double r) const;

  std::optional<std::vector<DocId>> GetAllResults() const override {
    return Range(-std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity());
  }

 private:
  using Entry = std::pair<double, DocId>;
  absl::btree_set<Entry, std::less<Entry>, PMR_NS::polymorphic_allocator<Entry>> entries_;
};

// Base index for string based indices.
template <typename C> struct BaseStringIndex : public BaseIndex {
  using Container = BlockList<C>;
  using VecOrPtr = std::variant<std::vector<DocId>, const Container*>;

  BaseStringIndex(PMR_NS::memory_resource* mr, bool case_sensitive, bool with_suffixtrie);

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  // Pointer is valid as long as index is not mutated. Nullptr if not found
  const Container* Matching(std::string_view str, bool strip_whitespace = true) const;

  // Iterate over all Matching on prefix.
  void MatchingPrefix(std::string_view prefix, absl::FunctionRef<void(const Container*)> cb) const;

  // Get all entries matching suffix query. Faster if suffix trie is built.
  void MatchingSuffix(std::string_view suffix, absl::FunctionRef<void(const Container*)> cb) const;

  void MatchingInfix(std::string_view prefix, absl::FunctionRef<void(const Container*)> cb) const;

  // Returns all the terms that appear as keys in the reverse index.
  std::vector<std::string> GetTerms() const;

  std::optional<std::vector<DocId>> GetAllResults() const override;

 protected:
  using StringList = DocumentAccessor::StringList;

  // Used by Add & Remove to get strings from document
  virtual std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                               std::string_view field) const = 0;

  // Used by Add & Remove to tokenize text value
  virtual absl::flat_hash_set<std::string> Tokenize(std::string_view value) const = 0;

  static Container* GetOrCreate(search::RaxTreeMap<Container>* map, std::string_view word);
  static void Remove(search::RaxTreeMap<Container>* map, DocId id, std::string_view word);

  bool case_sensitive_ = false;
  search::RaxTreeMap<Container> entries_;
  std::optional<search::RaxTreeMap<Container>> suffix_trie_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TextIndex : public BaseStringIndex<CompressedSortedSet> {
  using StopWords = absl::flat_hash_set<std::string>;

  TextIndex(PMR_NS::memory_resource* mr, const StopWords* stopwords, const Synonyms* synonyms,
            bool with_suffixtrie);

 protected:
  std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                       std::string_view field) const override;
  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;

 private:
  const StopWords* stopwords_;
  const Synonyms* synonyms_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TagIndex : public BaseStringIndex<SortedVector> {
  TagIndex(PMR_NS::memory_resource* mr, SchemaField::TagParams params)
      : BaseStringIndex(mr, params.case_sensitive, false), separator_{params.separator} {
  }

 protected:
  std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                       std::string_view field) const override;
  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;

 private:
  char separator_;
};

struct BaseVectorIndex : public BaseIndex {
  std::pair<size_t /*dim*/, VectorSimilarity> Info() const;

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override final;

 protected:
  BaseVectorIndex(size_t dim, VectorSimilarity sim);

  using VectorPtr = decltype(std::declval<OwnedFtVector>().first);
  virtual void AddVector(DocId id, const VectorPtr& vector) = 0;

  size_t dim_;
  VectorSimilarity sim_;
};

// Index for vector fields.
// Only supports lookup by id.
struct FlatVectorIndex : public BaseVectorIndex {
  FlatVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource* mr);

  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  const float* Get(DocId doc) const;

  // Return all documents that have vectors in this index
  std::optional<std::vector<DocId>> GetAllResults() const override;

 protected:
  void AddVector(DocId id, const VectorPtr& vector) override;

 private:
  PMR_NS::vector<float> entries_;
};

struct HnswlibAdapter;

// This index does't have GetAllResults method
// because it's not possible to get all vectors from the index
// It depends on the Hnswlib implementation
// TODO: Consider adding GetAllResults method in the future
struct HnswVectorIndex : public BaseVectorIndex {
  HnswVectorIndex(const SchemaField::VectorParams& params, PMR_NS::memory_resource* mr);
  ~HnswVectorIndex();

  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  std::vector<std::pair<float, DocId>> Knn(float* target, size_t k, std::optional<size_t> ef) const;
  std::vector<std::pair<float, DocId>> Knn(float* target, size_t k, std::optional<size_t> ef,
                                           const std::vector<DocId>& allowed) const;

 protected:
  void AddVector(DocId id, const VectorPtr& vector) override;

 private:
  std::unique_ptr<HnswlibAdapter> adapter_;
};

}  // namespace dfly::search
