// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>

#include "base/pmr/memory_resource.h"
#include "core/search/base.h"

namespace dfly::search {

struct AstNode;
struct TextIndex;

// Describes a specific index field
struct SchemaField {
  enum FieldType { TAG, TEXT, NUMERIC, VECTOR };
  enum FieldFlags : uint8_t { NOINDEX = 1 << 0, SORTABLE = 1 << 1 };

  struct VectorParams {
    bool use_hnsw = false;

    size_t dim = 0u;                              // dimension of knn vectors
    VectorSimilarity sim = VectorSimilarity::L2;  // similarity type
    size_t capacity = 1000;                       // initial capacity
    size_t hnsw_ef_construction = 200;
    size_t hnsw_m = 16;
  };

  struct TagParams {
    char separator = ',';
    bool case_sensitive = false;
  };

  using ParamsVariant = std::variant<std::monostate, VectorParams, TagParams>;

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
};

// Collection of indices for all fields in schema
class FieldIndices {
 public:
  // Create indices based on schema and options. Both must outlive the indices
  FieldIndices(const Schema& schema, const IndicesOptions& options, PMR_NS::memory_resource* mr);

  // Returns true if document was added
  bool Add(DocId doc, const DocumentAccessor& access);
  void Remove(DocId doc, const DocumentAccessor& access);

  BaseIndex* GetIndex(std::string_view field) const;
  BaseSortIndex* GetSortIndex(std::string_view field) const;
  std::vector<TextIndex*> GetAllTextIndices() const;

  const std::vector<DocId>& GetAllDocs() const;
  const Schema& GetSchema() const;

  SortableValue GetSortIndexValue(DocId doc, std::string_view field_identifier) const;

 private:
  void CreateIndices(PMR_NS::memory_resource* mr);
  void CreateSortIndices(PMR_NS::memory_resource* mr);

  const Schema& schema_;
  const IndicesOptions& options_;
  std::vector<DocId> all_ids_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<BaseIndex>> indices_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<BaseSortIndex>> sort_indices_;
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

  // number of matches before any aggregation, used by multi-shard optimizations
  size_t pre_aggregation_total;

  // The ids of the matched documents
  std::vector<DocId> ids;

  // Contains final scores if an aggregation was present
  std::vector<ResultScore> scores;

  // If profiling was enabled
  std::optional<AlgorithmProfile> profile;

  // If an error occurred, last recent one
  std::string error;
};

struct AggregationInfo {
  std::optional<size_t> limit;
  std::string_view alias;
  bool descending;
};

// SearchAlgorithm allows searching field indices with a query
class SearchAlgorithm {
 public:
  SearchAlgorithm();
  ~SearchAlgorithm();

  // Init with query and return true if successful.
  bool Init(std::string_view query, const QueryParams* params, const SortOption* sort = nullptr);

  SearchResult Search(const FieldIndices* index,
                      size_t limit = std::numeric_limits<size_t>::max()) const;

  // if enabled, return limit & alias for knn query
  std::optional<AggregationInfo> HasAggregation() const;

  void EnableProfiling();

 private:
  bool profiling_enabled_ = false;
  std::unique_ptr<AstNode> query_;
};

}  // namespace dfly::search
