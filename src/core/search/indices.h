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

  BaseStringIndex(PMR_NS::memory_resource* mr, bool case_sensitive);

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  // Pointer is valid as long as index is not mutated. Nullptr if not found
  const Container* Matching(std::string_view str, bool strip_whitespace = true) const;

  // Iterate over all Matching on prefix.
  void MatchingPrefix(std::string_view prefix, absl::FunctionRef<void(const Container*)> cb) const;

  // Returns all the terms that appear as keys in the reverse index.
  std::vector<std::string> GetTerms() const;

 protected:
  using StringList = DocumentAccessor::StringList;

  // Used by Add & Remove to get strings from document
  virtual std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                               std::string_view field) const = 0;

  // Used by Add & Remove to tokenize text value
  virtual absl::flat_hash_set<std::string> Tokenize(std::string_view value) const = 0;

  Container* GetOrCreate(std::string_view word);

  bool case_sensitive_ = false;
  search::RaxTreeMap<Container> entries_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TextIndex : public BaseStringIndex<CompressedSortedSet> {
  using StopWords = absl::flat_hash_set<std::string>;

  TextIndex(PMR_NS::memory_resource* mr, const StopWords* stopwords, const Synonyms* synonyms)
      : BaseStringIndex(mr, false), stopwords_{stopwords}, synonyms_{synonyms} {
  }

  std::optional<std::vector<DocId>> GetAllResults() const override {
    std::vector<DocId> result;
    std::vector<DocId> temp;
    for (const auto& term : GetTerms()) {
      if (auto* docs = Matching(term)) {
        temp.assign(docs->begin(), docs->end());
        result.insert(result.end(), temp.begin(), temp.end());
      }
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
  }

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
      : BaseStringIndex(mr, params.case_sensitive), separator_{params.separator} {
  }

  std::optional<std::vector<DocId>> GetAllResults() const override {
    std::vector<DocId> result;
    std::vector<DocId> temp;
    for (const auto& tag : GetTerms()) {
      if (auto* docs = Matching(tag)) {
        temp.assign(docs->begin(), docs->end());
        result.insert(result.end(), temp.begin(), temp.end());
      }
    }
    std::sort(result.begin(), result.end());
    result.erase(std::unique(result.begin(), result.end()), result.end());
    return result;
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
  std::optional<std::vector<DocId>> GetAllResults() const override {
    std::vector<DocId> result;
    size_t num_vectors = entries_.size() / dim_;
    result.reserve(num_vectors);

    for (DocId id = 0; id < num_vectors; ++id) {
      // Check if the vector is not zero (all elements are 0)
      const float* vec = Get(id);
      bool is_zero_vector = true;

      for (size_t i = 0; i < dim_; ++i) {
        if (vec[i] != 0.0f) {
          is_zero_vector = false;
          break;
        }
      }

      if (!is_zero_vector) {
        result.push_back(id);
      }
    }

    return result;
  }

 protected:
  void AddVector(DocId id, const VectorPtr& vector) override;

 private:
  PMR_NS::vector<float> entries_;
};

struct HnswlibAdapter;

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
