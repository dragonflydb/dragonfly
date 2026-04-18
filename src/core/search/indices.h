// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

// Wrong warning reported when geometry.hpp is loaded
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
#include <boost/geometry.hpp>
#ifndef __clang__
#pragma GCC diagnostic pop
#endif

#include <absl/functional/function_ref.h>

#include <memory>
#include <optional>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/page_usage/page_usage_stats.h"
#include "core/search/base.h"
#include "core/search/block_list.h"
#include "core/search/compressed_sorted_set.h"
#include "core/search/range_tree.h"
#include "core/search/rax_tree.h"

// TODO: move core field definitions out of big header
#include "common/string_or_view.h"
#include "core/search/search.h"

namespace dfly::search {

// Index for integer fields.
// Range bounds are queried in logarithmic time, iteration is constant.
struct NumericIndex : public BaseIndex {
  // Temporary base class for range tree.
  // It is used to use two different range trees depending on the flag use_range_tree.
  // If the flag is true, RangeTree is used, otherwise a simple implementation with btree_set.
  struct RangeTreeBase {
    virtual void Add(DocId id, absl::Span<double> values) = 0;
    virtual void Remove(DocId id, absl::Span<double> values) = 0;

    // Returns all DocIds that match the range [l, r].
    virtual RangeResult Range(double l, double r) const = 0;

    // Returns all DocIds that have non-null values in the index.
    virtual std::vector<DocId> GetAllDocIds() const = 0;

    virtual void FinalizeInitialization(){};

    virtual ~RangeTreeBase() = default;
  };

  // max_range_block_size is the maximum number of entries in a single range block.
  // It is used in RangeTree. Check RangeTree for details.
  explicit NumericIndex(size_t max_range_block_size, PMR_NS::memory_resource* mr);

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  void FinalizeInitialization() override;

  RangeResult Range(double l, double r) const;

  std::vector<DocId> GetAllDocsWithNonNullValues() const override;

 private:
  std::unique_ptr<RangeTreeBase> range_tree_;
};

// Base index for string based indices.
template <typename C> struct BaseStringIndex : public BaseIndex {
  using Container = BlockList<C>;
  using VecOrPtr = std::variant<std::vector<DocId>, const Container*>;

  // TextIndex (CompressedSortedSet) supports TF storage and BM25 scoring; TagIndex does not.
  static constexpr bool kIsScored = std::is_same_v<C, CompressedSortedSet>;

  BaseStringIndex(PMR_NS::memory_resource* mr, bool case_sensitive, bool with_suffixtrie);

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  // Pointer is valid as long as index is not mutated. Nullptr if not found
  const Container* Matching(std::string_view str, bool strip_whitespace = true) const;

  // Iterate over all nodes matching on prefix.
  void MatchPrefix(std::string_view prefix, absl::FunctionRef<void(const Container*)> cb) const;

  // Iterate over all nodes matching suffix query. Faster if suffix trie is built.
  void MatchSuffix(std::string_view suffix, absl::FunctionRef<void(const Container*)> cb) const;

  // Iterate over all nodes matching infix query. Faster if suffix trie is built.
  void MatchInfix(std::string_view prefix, absl::FunctionRef<void(const Container*)> cb) const;

  // Same as above but also pass the matched term string to the callback (for scoring).
  void MatchPrefixWithTerm(
      std::string_view prefix,
      absl::FunctionRef<void(std::string_view term, const Container*)> cb) const;
  void MatchSuffixWithTerm(
      std::string_view suffix,
      absl::FunctionRef<void(std::string_view term, const Container*)> cb) const;
  void MatchInfixWithTerm(
      std::string_view infix,
      absl::FunctionRef<void(std::string_view term, const Container*)> cb) const;

  // Returns all the terms that appear as keys in the reverse index.
  std::vector<std::string> GetTerms() const;

  std::vector<DocId> GetAllDocsWithNonNullValues() const override;

  // Per-field BM25 scoring support: document length in this specific field.
  uint32_t GetFieldDocLength(DocId doc) const {
    return doc < field_doc_lengths_.size() ? field_doc_lengths_[doc] : 0;
  }

  // Average document length for this field.
  // Denominator is field_num_docs_ (docs with non-empty content in this field),
  // not the total index doc count, so sparse fields get correct BM25 normalization.
  double GetFieldAvgDocLen() const {
    return field_num_docs_ > 0 ? static_cast<double>(field_total_docs_len_) / field_num_docs_ : 0.0;
  }

  // Number of documents that have content in this field.
  size_t GetFieldNumDocs() const {
    return field_num_docs_;
  }

 protected:
  using StringList = DocumentAccessor::StringList;

  // Used by Add & Remove to get strings from document
  virtual std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                               std::string_view field) const = 0;

  // Used by Add & Remove to tokenize text value. Returns token -> frequency map.
  virtual absl::flat_hash_map<std::string, uint32_t> Tokenize(std::string_view value) const = 0;

  cmn::StringOrView NormalizeQueryWord(std::string_view word) const;
  static Container* GetOrCreate(search::RaxTreeMap<Container>* map, std::string_view word,
                                bool store_freq = false);
  static void Remove(search::RaxTreeMap<Container>* map, DocId id, std::string_view word);

  bool case_sensitive_ = false;
  bool unique_ids_ = true;  // If true, docs ids are unique in the index, otherwise they can repeat.
  search::RaxTreeMap<Container> entries_;
  std::optional<search::RaxTreeMap<Container>> suffix_trie_;

  // Per-field BM25 scoring data (only meaningful for TextIndex / CompressedSortedSet).
  // Note: field_doc_lengths_ only grows (like FlatVectorIndex::entries_). Slots are zeroed
  // on Remove but the vector is not shrunk. DocIds are recycled via free_ids_, so slots
  // get reused over time.
  std::vector<uint32_t> field_doc_lengths_;  // DocId -> sum of TF in this field
  size_t field_total_docs_len_ = 0;
  size_t field_num_docs_ = 0;  // Number of docs with non-empty content in this field
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
  absl::flat_hash_map<std::string, uint32_t> Tokenize(std::string_view value) const override;

 private:
  const StopWords* stopwords_;
  const Synonyms* synonyms_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TagIndex : public BaseStringIndex<SortedVector<DocId>> {
  TagIndex(PMR_NS::memory_resource* mr, SchemaField::TagParams params)
      : BaseStringIndex(mr, params.case_sensitive, params.with_suffixtrie),
        separator_{params.separator} {
  }

  DefragmentResult Defragment(PageUsage* page_usage) override;

 protected:
  std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                       std::string_view field) const override;
  absl::flat_hash_map<std::string, uint32_t> Tokenize(std::string_view value) const override;

 private:
  char separator_;
  std::string next_defrag_entry_;
  std::string next_defrag_suffix_entry_;
};

struct BaseVectorIndex : public BaseIndex {
  std::pair<size_t /*dim*/, VectorSimilarity> Info() const;

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override final;

 protected:
  BaseVectorIndex(size_t dim, VectorSimilarity sim);

  virtual void AddVector(DocId id, const void* vector) = 0;

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
  std::vector<DocId> GetAllDocsWithNonNullValues() const override;

 protected:
  void AddVector(DocId id, const void* vector) override;

 private:
  PMR_NS::vector<float> entries_;
};

struct GeoIndex : public BaseIndex {
  using point =
      boost::geometry::model::point<double, 2,
                                    boost::geometry::cs::geographic<boost::geometry::degree>>;
  using index_entry = std::pair<point, DocId>;

  explicit GeoIndex(PMR_NS::memory_resource* mr);
  ~GeoIndex();

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  std::vector<DocId> RadiusSearch(double lon, double lat, double radius, std::string_view arg);
  std::vector<DocId> GetAllDocsWithNonNullValues() const override;

 private:
  using rtree = boost::geometry::index::rtree<index_entry, boost::geometry::index::linear<16>>;
  std::unique_ptr<rtree> rtree_;
};

// Defragments a map like data structure. The values in the map must have a `Defragment` method.
// Works with rax tree map and hash based maps
template <typename Container> struct DefragmentMap {
  using ValueType = Container::value_type;
  using Iterator = Container::iterator;

  DefragmentMap(Container& container, std::string* key) : key{key} {
    if (key->empty()) {
      it = container.end();
    } else if constexpr (requires { container.lower_bound(*key); }) {
      it = container.lower_bound(*key);
    } else {
      it = container.find(*key);
    }

    if (it == container.end()) {
      it = container.begin();
    }

    end = container.end();
  }

  // The key is set if the defragmentation has to stop mid way due to depleted quota
  DefragmentResult Defragment(PageUsage* page_usage) {
    if (page_usage->QuotaDepleted()) {
      return DefragmentResult{.quota_depleted = true, .objects_moved = 0};
    }

    DefragmentResult result;
    for (; it != end; ++it) {
      const auto& [k, map] = *it;
      if (result.Merge(DefragmentIndex(map, page_usage)).quota_depleted) {
        *key = k;
        break;
      }
    }

    if (it == end) {
      key->clear();
    }

    return result;
  }

 private:
  template <typename T> static auto DefragmentIndex(T& t, PageUsage* page_usage) {
    if constexpr (requires { t->Defragment(page_usage); }) {
      return t->Defragment(page_usage);
    } else {
      return t.Defragment(page_usage);
    }
  }

  std::string* key;
  Iterator it;
  Iterator end;
};

}  // namespace dfly::search
