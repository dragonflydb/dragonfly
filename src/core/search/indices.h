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
#include "core/search/search.h"
#include "core/string_or_view.h"

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

  // Returns all the terms that appear as keys in the reverse index.
  std::vector<std::string> GetTerms() const;

  std::vector<DocId> GetAllDocsWithNonNullValues() const override;

 protected:
  using StringList = DocumentAccessor::StringList;

  // Used by Add & Remove to get strings from document
  virtual std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                               std::string_view field) const = 0;

  // Used by Add & Remove to tokenize text value
  virtual absl::flat_hash_set<std::string> Tokenize(std::string_view value) const = 0;

  StringOrView NormalizeQueryWord(std::string_view word) const;
  static Container* GetOrCreate(search::RaxTreeMap<Container>* map, std::string_view word);
  static void Remove(search::RaxTreeMap<Container>* map, DocId id, std::string_view word);

  bool case_sensitive_ = false;
  bool unique_ids_ = true;  // If true, docs ids are unique in the index, otherwise they can repeat.
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
struct TagIndex : public BaseStringIndex<SortedVector<DocId>> {
  TagIndex(PMR_NS::memory_resource* mr, SchemaField::TagParams params)
      : BaseStringIndex(mr, params.case_sensitive, params.with_suffixtrie),
        separator_{params.separator} {
  }

  DefragmentResult Defragment(PageUsage* page_usage) override;

 protected:
  std::optional<StringList> GetStrings(const DocumentAccessor& doc,
                                       std::string_view field) const override;
  absl::flat_hash_set<std::string> Tokenize(std::string_view value) const override;

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
  std::vector<DocId> GetAllDocsWithNonNullValues() const override;

 protected:
  void AddVector(DocId id, const VectorPtr& vector) override;

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
template <typename Container, typename ItFunc> struct DefragmentMap {
  // ItFunc is necessary because RaxTreeMap does not allow copying or moving iterators, so this
  // class cannot accept begin,end iterators from caller. It must construct the begin iterator
  using Iterator = std::invoke_result_t<ItFunc>;
  DefragmentMap(Container& container, ItFunc&& f)
      : container(container), it(f()), end(container.end()) {
  }

  // Deref should be true if the value is wrapped in a pointer. Set here instead of at class level
  // to allow overriding without specifying other parameters.
  template <bool Deref = false>
  // The key is set if the defragmentation has to stop mid way due to depleted quota
  DefragmentResult Defragment(PageUsage* page_usage, std::string* key) {
    if (page_usage->QuotaDepleted()) {
      return DefragmentResult{.quota_depleted = true, .objects_moved = 0};
    }

    DefragmentResult result;
    for (; it != end; ++it) {
      const auto& [k, map] = *it;
      DefragmentResult r;
      if constexpr (Deref) {
        r = map->Defragment(quota_usec, page_usage);
      } else {
        r = map.Defragment(quota_usec, page_usage);
      }
      if (result.Merge(std::move(r)).quota_depleted) {
        *key = k;
        break;
      }
    }

    if (it == end) {
      key->clear();
    }

    return result;
  }

  Container& container;
  Iterator it;
  Iterator end;
};

}  // namespace dfly::search
