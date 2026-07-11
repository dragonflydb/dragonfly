// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <cmath>
#include <memory>
#include <optional>
#include <string>
#include <variant>

#include "base/pmr/memory_resource.h"
#include "core/search/base.h"
#include "core/search/range_tree.h"
#include "core/search/scoring.h"
#include "core/search/synonyms.h"

namespace dfly::search {

struct AstNode;
struct TextIndex;
struct AstKnnNode;
struct AstVectorRangeNode;

// Optional FILTER
struct OptionalNumericFilter : public OptionalFilterBase {
  OptionalNumericFilter(size_t lo, size_t hi) : empty_(false), lo_(lo), hi_(hi) {
  }

  bool IsEmpty() const override {
    return empty_;
  }

  AstNode Node(std::string field) override;

  void AddRange(size_t lo, size_t hi) {
    if (empty_) {
      return;
    }
    if ((hi_ < lo) || (hi < lo_)) {
      empty_ = true;
    } else {
      lo_ = std::max(lo_, lo);
      hi_ = std::min(hi_, hi);
    }
  }

 private:
  bool empty_;
  size_t lo_;
  size_t hi_;
};

// Describes a specific index field
struct SchemaField {
  enum FieldType { TAG, TEXT, NUMERIC, VECTOR, GEO };
  enum FieldFlags : uint8_t { NOINDEX = 1 << 0, SORTABLE = 1 << 1 };

  struct VectorParams {
    static constexpr double kDefaultHnswEpsilon = 0.01;
    // Deliberate sanity cap: rejects non-finite and absurd values. Set far above any practical
    // overscan factor (epsilon of a few already explores almost the whole graph), so it never
    // rejects a realistic value.
    static constexpr double kMaxHnswEpsilon = 1e6;

    // Schema epsilon (FT.CREATE) allows 0, which maps to the default via
    // NormalizeSchemaHnswEpsilon.
    static bool IsValidSchemaHnswEpsilon(double epsilon) {
      return epsilon >= 0 && epsilon <= kMaxHnswEpsilon && std::isfinite(epsilon);
    }

    // Runtime override ($EPSILON / RANGE EPSILON) must be strictly positive.
    static bool IsValidRuntimeHnswEpsilon(double epsilon) {
      return epsilon > 0 && epsilon <= kMaxHnswEpsilon && std::isfinite(epsilon);
    }

    static double NormalizeSchemaHnswEpsilon(double epsilon) {
      return epsilon == 0 ? kDefaultHnswEpsilon : epsilon;
    }

    bool use_hnsw = false;

    size_t dim = 0u;                              // dimension of knn vectors
    VectorSimilarity sim = VectorSimilarity::L2;  // similarity type
    size_t capacity = 1000;                       // initial capacity
    size_t hnsw_ef_construction = 200;
    size_t hnsw_m = 16;
    VectorDataType data_type = VectorDataType::FLOAT32;
    uint32_t hnsw_ef_runtime = 10;
    double hnsw_epsilon = kDefaultHnswEpsilon;
  };

  struct TagParams {
    char separator = ',';
    bool case_sensitive = false;
    bool with_suffixtrie = false;  // see TextParams
  };

  struct TextParams {
    // Sanity cap mirroring VectorParams::kMaxHnswEpsilon: rejects absurd weights so the schema
    // weight folded into the effective term frequency cannot overflow BM25 to inf/NaN.
    static constexpr double kMaxWeight = 1e6;

    static bool IsValidWeight(double weight) {
      return weight >= 0 && weight <= kMaxWeight && std::isfinite(weight);
    }

    // if enabled, suffix trie is build for efficient suffix and infix queries
    bool with_suffixtrie = false;
    bool no_stem = false;
    double weight = 1.0;
  };

  struct NumericParams {
    // Block size of the range tree
    // Check RangeTree for details.
    size_t block_size = RangeTree::kDefaultMaxRangeBlockSize;
  };

  bool IsIndexableHnswField() const {
    return type == VECTOR && !(flags & NOINDEX) && std::get<VectorParams>(special_params).use_hnsw;
  }

  using ParamsVariant =
      std::variant<std::monostate, VectorParams, TagParams, TextParams, NumericParams>;

  FieldType type;
  uint8_t flags;
  std::string short_name;  // equal to ident if none provided
  ParamsVariant special_params{std::monostate{}};
};

// Describes the fields of an index
struct Schema {
  // List of fields by identifier.
  absl::flat_hash_map<std::string /*identifier*/, SchemaField> fields;

  // Mapping for short field names (aliases).
  absl::flat_hash_map<std::string /* short name*/, std::string /*identifier*/> field_names;

  std::string default_language = "english";
  std::string language_field;  // doc field providing per-doc language; empty = none

  // Return identifier for alias if found, otherwise return passed value
  std::string_view LookupAlias(std::string_view alias) const;

  // Return alias for identifier if found, otherwise return passed value
  std::string_view LookupIdentifier(std::string_view identifier) const;
};

struct IndicesOptions {
  IndicesOptions();
  explicit IndicesOptions(absl::flat_hash_set<std::string> stopwords)
      : stopwords{std::move(stopwords)} {
  }

  absl::flat_hash_set<std::string> stopwords;
  bool custom_stopwords = false;  // true when STOPWORDS was explicitly set in FT.CREATE
  // When true, TEXT posting lists do not store token positions. Saves memory but
  // disables phrase queries. Set via NOOFFSETS in FT.CREATE.
  bool no_offsets = false;
};

// BM25 scoring statistics are now tracked per-field inside each TextIndex.
// See BaseStringIndex::GetFieldDocLength() and GetFieldAvgDocLen().

// Collection of indices for all fields in schema
class FieldIndices {
 public:
  // Create indices based on schema and options. Both must outlive the indices
  FieldIndices(const Schema& schema, const IndicesOptions& options, PMR_NS::memory_resource* mr,
               const Synonyms* synonyms);

  // Returns true if document was added
  bool Add(DocId doc, const DocumentAccessor& access);
  void Remove(DocId doc, const DocumentAccessor& access);

  BaseIndex* GetIndex(std::string_view field) const;
  BaseSortIndex* GetSortIndex(std::string_view field) const;
  std::vector<TextIndex*> GetAllTextIndices() const;

  const std::vector<DocId>& GetAllDocs() const;
  const Schema& GetSchema() const;

  const Synonyms* GetSynonyms() const;

  // True if `term` (case-folded) is a stopword for this index. Mirrors the index-time stopword
  // check so query terms that are stopwords can be dropped instead of matching nothing.
  bool IsStopWord(std::string_view term) const;

  SortableValue GetSortIndexValue(DocId doc, std::string_view field_identifier) const;

  void FinalizeInitialization();

  DefragmentResult Defragment(PageUsage* page_usage);

  // Returns memory used by containers with default allocator, not tracked through search local_mr_
  size_t GetNonPmrMemoryUsage() const;

 private:
  void CreateIndices(PMR_NS::memory_resource* mr);
  void CreateSortIndices();

  const Schema& schema_;
  const IndicesOptions& options_;
  // These containers use default allocators — tracked manually via GetNonPmrMemoryUsage().
  // If adding new default-allocator containers, update GetNonPmrMemoryUsage() accordingly.
  std::vector<DocId> all_ids_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<BaseIndex>> indices_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<BaseSortIndex>> sort_indices_;
  const Synonyms* synonyms_;

  std::string next_defrag_field_;
  std::string next_defrag_sort_field_;
};

struct AlgorithmProfile {
  struct ProfileEvent {
    std::string descr;
    size_t micros;         // time event took in microseconds
    size_t depth;          // tree depth of event
    size_t num_processed;  // number of results processed by the event
  };

  std::vector<ProfileEvent> events;
};

// Represents a search result returned from the search algorithm.
struct SearchResult {
  size_t total;  // how many documents were matched in total

  // The ids of the matched documents, in result order
  std::vector<DocId> ids;

  // Vector distances keyed by DocId (order is carried by `ids`). Populated for KNN/VECTOR_RANGE.
  absl::flat_hash_map<DocId, float> knn_scores;

  // Text relevance scores keyed by DocId. Populated when a scorer is active.
  absl::flat_hash_map<DocId, float> text_scores;
  float max_text_score = 0;

  // If profiling was enabled
  std::optional<AlgorithmProfile> profile;

  // If an error occurred, last recent one
  std::string error;
};

struct KnnScoreSortOption {
  std::string_view score_field_alias;
  size_t limit = std::numeric_limits<size_t>::max();
};

// SearchAlgorithm allows searching field indices with a query
class SearchAlgorithm {
 public:
  SearchAlgorithm();
  ~SearchAlgorithm();

  // Init with query and optional filters and return true if successful.
  bool Init(std::string_view query, const QueryParams* params,
            const OptionalFilters* filters = nullptr);

  // Search on given index with predefined limit for cutting off result ids.
  // When global_stats is non-null, scorers see cluster-wide counts instead of
  // values local to `index`.
  SearchResult Search(const FieldIndices* index,
                      size_t cuttoff_limit = std::numeric_limits<size_t>::max(),
                      const GlobalScoringStats* global_stats = nullptr) const;

  // This shard's contribution to GlobalScoringStats. Requires Init().
  ShardScoringStats CollectScoringStats(const FieldIndices* index) const;

  std::optional<KnnScoreSortOption> GetKnnScoreSortOption() const;

  bool IsKnnQuery() const;

  AstKnnNode* GetKnnNode() const;

  std::unique_ptr<AstNode> PopKnnNode();

  const AstVectorRangeNode* GetVectorRangeNode() const;

  // All VECTOR_RANGE leaves in the query tree, in DFS order.
  std::vector<const AstVectorRangeNode*> CollectVectorRangeNodes() const;

  bool IsBareVectorRange() const;

  // True when the query is `AND[VECTOR_RANGE, filters...]`.
  bool IsAndedVectorRange() const;

  // Detaches the range from the top-level AND (replacing it with match-all) and returns it,
  // leaving query_ as the runnable pre-filter. Requires IsAndedVectorRange().
  std::unique_ptr<AstNode> ExtractVectorRangeAsPrefilter();

  void EnableProfiling();

  void SetScorer(ScorerSpec scorer);

 private:
  bool profiling_enabled_ = false;
  std::optional<ScorerSpec> scorer_;
  std::unique_ptr<AstNode> query_;
  std::optional<KnnScoreSortOption> knn_hnsw_score_sort_option_;
};

}  // namespace dfly::search
