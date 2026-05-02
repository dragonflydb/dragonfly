// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/mi_memory_resource.h"
#include "core/search/base.h"
#include "core/search/hnsw_index.h"
#include "core/search/search.h"
#include "core/search/sort_indices.h"
#include "core/search/synonyms.h"
#include "server/search/aggregator.h"
#include "server/search/index_join.h"
#include "server/stats.h"
#include "server/table.h"

namespace dfly {

using StringVec = std::vector<std::string>;

namespace search {
struct IndexBuilder;
}  // namespace search

struct BaseAccessor;

using SearchDocData = absl::flat_hash_map<std::string /*field*/, search::SortableValue /*value*/>;
using Synonyms = search::Synonyms;

std::string_view SearchFieldTypeToString(search::SchemaField::FieldType);

struct SerializedSearchDoc {
  search::DocId id;
  std::string key;
  SearchDocData values;
  float knn_score = 0;
  float text_score = 0;
  search::SortableValue sort_score;
};

struct SearchResult {
  SearchResult() = default;

  SearchResult(size_t total_hits, std::vector<SerializedSearchDoc> docs,
               std::optional<search::AlgorithmProfile> profile)
      : total_hits{total_hits}, docs{std::move(docs)}, profile{std::move(profile)} {
  }

  SearchResult(facade::ErrorReply error) : error{std::move(error)} {
  }

  size_t total_hits;
  std::vector<SerializedSearchDoc> docs;
  std::optional<search::AlgorithmProfile> profile;

  std::optional<facade::ErrorReply> error;
};

// Field reference with optional alias as parsed from RETURN [field AS alias], LOAD, etc...
struct FieldReference {
  explicit FieldReference(std::string_view name, std::string_view alias = "")
      : name_{name}, alias_{alias} {
  }

  std::string_view Identifier(const search::Schema& schema, bool is_json) const {
    return (is_json && IsJsonPath(name_)) ? name_ : schema.LookupAlias(name_);
  }

  std::string_view Name() const {
    return name_;
  }

  std::string_view OutputName() const {
    return alias_.empty() ? name_ : alias_;
  }

 private:
  static bool IsJsonPath(std::string_view name);

  std::string_view name_, alias_;
};

enum class SortOrder { ASC, DESC };

struct SearchParams {
  struct SortOption {
    FieldReference field;
    SortOrder order = SortOrder::ASC;

    bool IsSame(const search::KnnScoreSortOption& knn_sort) const {
      return knn_sort.score_field_alias == field.OutputName();
    }
  };

  // Parameters for "LIMIT offset total": select total amount documents with a specific offset from
  // the whole result set
  size_t limit_offset = 0;
  size_t limit_total = 10;

  bool with_sortkeys = false;

  /*
  1. If not set -> return all fields
  2. If set but empty -> no fields should be returned
  3. If set and not empty -> return only these fields
  */
  std::optional<std::vector<FieldReference>> return_fields;

  /*
    Fields that should be also loaded from the document.

    Only one of load_fields and return_fields should be set.
  */
  std::optional<std::vector<FieldReference>> load_fields;

  std::optional<SortOption> sort_option;

  search::OptionalFilters optional_filters;

  search::QueryParams query_params;

  bool with_scores = false;           // WITHSCORES flag
  search::ScorerFn scorer = nullptr;  // SCORER parameter (null = not set)

  bool ShouldReturnAllFields() const {
    return !return_fields.has_value();
  }

  bool IdsOnly() const {
    return return_fields && return_fields->empty();
  }

  bool ShouldReturnField(std::string_view alias) const;
};

struct AggregateParams {
  struct JoinParams {
    // Fist field is the index name, second is the field name.
    using Field = std::pair<std::string, std::string>;

    struct Condition {
      Condition(std::string_view field_, std::string_view foreign_index_,
                std::string_view foreign_field_)
          : field{field_}, foreign_field{Field{foreign_index_, foreign_field_}} {
      }

      std::string field;
      Field foreign_field;
    };

    std::string index;
    std::string index_alias;
    std::vector<Condition> conditions;
    std::string query = "*";
  };

  /* Can have 2 scenarios:
      1. No joins - then this is ignored
      2. Has joins and SORTBY ... LIMIT option - then this is used to sort/limit right after join
      3. Has joins and LIMIT option - then this is used to limit right after join.
     Next aggregation steps after first LIMIT or first SORTBY will be applied on the final result,
     after loading the data for all joined documents. */
  struct JoinAggregateParams {
    static constexpr size_t kDefaultLimit = std::numeric_limits<size_t>::max();

    bool HasLimit() const {
      return limit_total != kDefaultLimit;
    }

    bool HasValue() const {
      return HasLimit() || sort.has_value();
    }

    size_t limit_offset = 0;
    size_t limit_total = kDefaultLimit;
    std::optional<aggregate::SortParams> sort;
  };

  std::string_view index, query;
  search::QueryParams params;

  std::vector<JoinParams> joins;
  JoinAggregateParams join_agg_params;

  std::optional<std::vector<FieldReference>> load_fields;
  std::vector<aggregate::AggregationStep> steps;

  bool add_scores = false;            // ADDSCORES flag
  search::ScorerFn scorer = nullptr;  // SCORER parameter (null = not set)

  // Set only for multi-shard scoring queries; not owned.
  const search::GlobalScoringStats* global_scoring_stats = nullptr;
};

// Stores basic info about a document index.
struct DocIndex {
  enum DataType : uint8_t { HASH, JSON };

  // Get numeric OBJ_ code
  uint8_t GetObjCode() const;

  // Return true if the following document (key, obj_code) is tracked by this index.
  bool Matches(std::string_view key, unsigned obj_code) const;

  std::string name;
  search::Schema schema;
  search::IndicesOptions options;
  std::vector<std::string> prefixes;
  DataType type{HASH};
};

struct DocIndexInfo {
  DocIndex base_index;
  size_t num_docs = 0;

  bool indexing = false;
  float percent_indexed = 1;

  // HNSW metadata for vector index (if present)
  // TODO: move to schema
  std::optional<search::HnswIndexMetadata> hnsw_metadata = std::nullopt;

  // Build original ft.create command that can be used to re-create this index
  std::string BuildRestoreCommand() const;
};

class ShardDocIndices;

// Extraction cache: ensures each sds field is swapped at most once per document removal,
// even when multiple search indices reference the same hash field.
using FieldExtractionCache = absl::flat_hash_map<std::string_view, std::shared_ptr<void>>;

// Per-shard wrapper around a global HnswVectorIndex. Encapsulates shard-local state
// (preserved field data while HNSW ops are buffered) and delegates to the global index.
// One instance per HNSW field per shard.
class HnswShardIndex {
 public:
  HnswShardIndex(std::shared_ptr<search::HnswVectorIndex> global_index, std::string field_ident);

  bool Add(search::GlobalDocId id, const BaseAccessor& doc);

  void Remove(search::GlobalDocId id);

  // Update vector data for an existing HNSW node (used during restoration).
  bool UpdateVectorData(search::GlobalDocId id, const BaseAccessor& doc);

  bool IsVectorCopied() const;

  void ClearPreservedData();

  // Preserve old sds entry for borrowed-vector mode so HNSW pointers stay valid.
  void MaybePreserveField(PrimeValue& pv, absl::Span<const std::string_view> modified_fields,
                          FieldExtractionCache* cache);

  const std::string& field_ident() const {
    return field_ident_;
  }

  search::HnswVectorIndex* global_index() {
    return global_index_.get();
  }

  const search::HnswVectorIndex* global_index() const {
    return global_index_.get();
  }

 private:
  std::shared_ptr<search::HnswVectorIndex> global_index_;
  std::string field_ident_;  // actual hash key, not AS alias

  // Old sds entries kept alive while HNSW ops are buffered (serializing/restoring).
  // shared_ptr because multiple search indices may reference the same sds.
  std::vector<std::shared_ptr<void>> preserved_field_data_;
};

// Stores internal search indices for documents of a document index on a specific shard.
class ShardDocIndex {
  friend class ShardDocIndices;
  friend struct search::IndexBuilder;

  using DocId = search::DocId;
  using GlobalDocId = search::GlobalDocId;

  // Used in FieldsValuesPerDocId to store values for each field per document
  using FieldsValues = absl::InlinedVector<search::SortableValue, 4>;

 public:
  // DocKeyIndex manages mapping document keys to ids and vice versa through a simple interface.
  struct DocKeyIndex {
    // Hash/Eq for heterogeneous lookup on TrackedIdsMap with string_view keys.
    struct StringViewHash {
      using is_transparent = void;
      size_t operator()(std::string_view sv) const {
        return absl::HashOf(sv);
      }
    };
    struct StringViewEq {
      using is_transparent = void;
      bool operator()(std::string_view a, std::string_view b) const {
        return a == b;
      }
    };

    using TrackedIdsMap = absl::flat_hash_map<
        search::StatelessString, DocId, StringViewHash, StringViewEq,
        StatelessSearchAllocator<std::pair<const search::StatelessString, DocId>>>;

    DocId Add(std::string_view key);

    // Like Add but always allocates a fresh DocId, never reusing free_ids_.
    // Used during restored CursorLoop to avoid colliding with HNSW node ids.
    DocId AddNew(std::string_view key);

    void Remove(DocId id);

    std::string_view Get(DocId id) const;
    bool IsValid(DocId id) const;
    std::optional<DocId> Find(std::string_view key) const;
    size_t Size() const;

    const TrackedIdsMap& GetDocKeysMap() const {
      return ids_;
    }

    // Serialization: returns pairs of (key, doc_id) for all active mappings
    std::vector<std::pair<std::string, DocId>> Serialize() const;

    // Restore key-to-docId mappings from serialized data (RDB load)
    void Restore(const std::vector<std::pair<std::string, search::DocId>>& mappings);

    // Restore from remapped keys in doc_id order (vector index = doc_id).
    void Restore(const std::vector<std::string>& keys);

   private:
    TrackedIdsMap ids_;
    search::StatelessVector<search::StatelessString> keys_;
    search::StatelessVector<DocId> free_ids_;
    DocId last_id_ = 0;
  };
  // Index must be rebuilt at least once after intialization
  explicit ShardDocIndex(std::shared_ptr<const DocIndex> index);

  // Possibly blocking to stop indexing job
  ~ShardDocIndex();

  // Perform search on all indexed documents and return results.
  // When global_stats is non-null, scorers see cluster-wide counts.
  SearchResult Search(const OpArgs& op_args, const SearchParams& params,
                      search::SearchAlgorithm* search_algo, bool is_knn_prefilter,
                      const search::GlobalScoringStats* global_stats) const;

  // This shard's contribution to a GlobalScoringStats. search_algo must be
  // Init()-ed.
  search::ShardScoringStats CollectScoringStats(search::SearchAlgorithm* search_algo) const;

  // Perform search and load requested values - note params might be interpreted differently.
  std::vector<SearchDocData> SearchForAggregator(const OpArgs& op_args,
                                                 const AggregateParams& params,
                                                 search::SearchAlgorithm* search_algo) const;

  // Load and serialize docs for aggregation from pre-computed (DocId, distance) pairs.
  // Used by the HNSW VECTOR_RANGE path in FT.AGGREGATE, where doc ids and distances come
  // from the global HNSW index rather than a per-shard search.
  std::vector<SearchDocData> LoadHnswRangeDocsForAggregator(
      const OpArgs& op_args, const AggregateParams& params,
      absl::Span<const std::pair<search::DocId, float>> doc_distances, std::string_view score_alias,
      const absl::flat_hash_map<search::DocId, float>& text_score_map) const;

  // Methods needed for join operation
  join::Vector<join::OwnedEntry> PreagregateDataForJoin(
      const OpArgs& op_args, absl::Span<const std::string_view> join_fields,
      search::SearchAlgorithm* search_algo) const;

  using FieldsValuesPerDocId = absl::flat_hash_map<DocId, FieldsValues>;
  FieldsValuesPerDocId LoadKeysData(const OpArgs& op_args,
                                    const absl::flat_hash_set<search::DocId>& doc_ids,
                                    absl::Span<const std::string_view> fields_to_load) const;

  // Return whether base index matches
  bool Matches(std::string_view key, unsigned obj_code) const;

  std::optional<ShardDocIndex::DocId> GetDocId(std::string_view key, const DbContext& db_cntx);

  std::optional<ShardDocIndex::DocId> AddDoc(std::string_view key, const DbContext& db_cntx,
                                             const PrimeValue& pv);

  void RemoveDoc(DocId id, const DbContext& db_cntx, const PrimeValue& pv);

  DocIndexInfo GetInfo() const;

  io::Result<StringVec, facade::ErrorReply> GetTagVals(std::string_view field) const;

  // Get synonym manager for this shard
  const Synonyms& GetSynonyms() const {
    return synonyms_;
  }

  Synonyms& GetSynonyms() {
    return synonyms_;
  }

  // Rebuild indices only for documents containing terms from the updated synonym group
  void RebuildForGroup(const OpArgs& op_args, const std::string_view& group_id,
                       const std::vector<std::string_view>& terms);

  // Public access to key index for direct operations (e.g., when dropping index with DD)
  // TODO: replace with keys() view
  const DocKeyIndex& key_index() const {
    return key_index_;
  }

  void AddDocToGlobalVectorIndex(ShardDocIndex::DocId doc_id, const DbContext& db_cntx,
                                 PrimeValue* pv);

  // Remove doc from all HNSW indices. When hnsw_state_ != kBuilding, the remove
  // is buffered in pending_vector_updates_ and old sds entries are preserved so
  // HNSW pointers remain valid until the buffer is drained.
  // modified_fields: when non-empty, only preserve fields being mutated.
  // cache: shared across search indices so each sds field is extracted at most once.
  void RemoveDocFromGlobalVectorIndex(ShardDocIndex::DocId doc_id, const DbContext& db_cntx,
                                      PrimeValue& pv,
                                      absl::Span<const std::string_view> modified_fields,
                                      FieldExtractionCache* cache);

  // Clear preserved field data on all per-shard HNSW indices.
  // Called after DrainPendingVectorUpdates completes buffered operations.
  void ClearAllHnswPreservedData();

  // Rebuild global vector indices from restored key index, updating vector data
  // for nodes whose graph structure was already restored from RDB.
  void RestoreGlobalVectorIndices(std::string_view index_name, const OpArgs& op_args);

  // Transition to kSerializing — buffer all HNSW ops until drain.
  // Called before HNSW graph serialization starts.
  void SetHnswSerializing();

  // Drain buffered HNSW updates and set kBuilding. Called after restoration
  // completes (kRestoring -> kBuilding) or after serialization finishes
  // (kSerializing -> kBuilding).
  void DrainPendingVectorUpdates(const OpArgs& op_args);

  // Drain only if in kSerializing state. No-op for kRestoring/kProhibit,
  // so serialization cannot interfere with a concurrent restoration.
  void DrainSerializationUpdates(const OpArgs& op_args);

  // Serialize doc and return with key name
  using SerializedEntryWithKey = std::optional<std::pair<std::string_view, SearchDocData>>;
  SerializedEntryWithKey SerializeDocWithKey(
      search::DocId id, const OpArgs& op_args, const search::Schema& schema,
      const std::optional<std::vector<FieldReference>>& return_fields);

  search::DefragmentResult Defragment(PageUsage* page_usage) {
    if (indices_) {
      return indices_->Defragment(page_usage);
    }
    return search::DefragmentResult{false, 0};
  }

  std::vector<std::pair<std::string, DocId>> SerializeKeyIndex() const {
    return key_index_.Serialize();
  }

  // Restore key-to-docId mappings from serialized data (RDB load)
  void RestoreKeyIndex(const std::vector<std::pair<std::string, search::DocId>>& mappings) {
    key_index_.Restore(mappings);
  }

  // Restore from remapped keys in doc_id order (vector index = doc_id).
  void RestoreKeyIndex(const std::vector<std::string>& keys) {
    key_index_.Restore(keys);
  }

  // Returns memory used by containers with default allocator, not tracked through search local_mr_
  size_t GetNonPmrMemoryUsage() const;

 private:
  // Common doc-loading loop used by SearchForAggregator and LoadHnswRangeDocsForAggregator.
  // Loads, serializes, and (optionally) injects the YIELD_DISTANCE_AS alias for each doc.
  std::vector<SearchDocData> LoadDocEntriesWithScores(
      const OpArgs& op_args, const AggregateParams& params, absl::Span<const search::DocId> ids,
      std::string_view score_alias, const absl::flat_hash_map<search::DocId, float>& score_map,
      const absl::flat_hash_map<search::DocId, float>& text_score_map) const;

  // Clears internal data. Traverses all matching documents and assigns ids.
  void Rebuild(const OpArgs& op_args, PMR_NS::memory_resource* mr, bool is_restored = false);

  // Cancel builder if in progress
  void CancelBuilder();

  using LoadedEntry = std::pair<std::string_view, std::unique_ptr<BaseAccessor>>;
  std::optional<LoadedEntry> LoadEntry(search::DocId id, const OpArgs& op_args) const;

  // Behaviour identical to SortIndex::Sort for non-sortable fields that need to be fetched first
  std::vector<search::SortableValue> KeepTopKSorted(std::vector<DocId>* ids, size_t limit,
                                                    const SearchParams::SortOption& sort,
                                                    const OpArgs& op_args) const;

  // Remove a DocId from all HNSW indices for this index.
  void RemoveFromAllHnswIndices(search::DocId doc_id);

  // Initialize per-shard HNSW wrappers from schema + global registry.
  void InitHnswShardIndices();

 private:
  std::shared_ptr<const DocIndex> base_;
  std::optional<search::FieldIndices> indices_;
  DocKeyIndex key_index_;
  Synonyms synonyms_;

  std::unique_ptr<search::IndexBuilder> builder_;

  // Per-shard HNSW wrappers, one per indexed vector field.
  std::vector<HnswShardIndex> hnsw_shard_indices_;

  // HNSW vector index lifecycle state.
  // kProhibit: default after InitIndex during LOADING. All HNSW ops buffered.
  // kRestoring: set by Rebuild(is_restored=true). Ops buffered until drain.
  // kSerializing: set before HNSW serialization. Ops buffered until drain.
  // kBuilding: normal operation. HNSW adds/removes execute immediately.
  enum class HnswState : uint8_t { kProhibit, kRestoring, kSerializing, kBuilding };

  absl::flat_hash_set<std::string> pending_vector_updates_;
  HnswState hnsw_state_ = HnswState::kProhibit;
};

// Stores shard doc indices by name on a specific shard.
class ShardDocIndices {
 public:
  ShardDocIndices();

  // Get sharded document index by its name or nullptr if not found
  ShardDocIndex* GetIndex(std::string_view name);

  // Init index: create shard local state for given index with given name.
  // Build if instance is in active state.
  void InitIndex(const OpArgs& op_args, std::string_view name,
                 std::shared_ptr<const DocIndex> index);

  // Drop index, return the dropped index if it existed or nullptr otherwise
  std::unique_ptr<ShardDocIndex> DropIndex(std::string_view name);

  // Drop all indices
  void DropAllIndices();

  // Rebuild all indices
  void RebuildAllIndices(const OpArgs& op_args, bool is_restored);

  // Block until construction of all indices finishes
  void BlockUntilConstructionEnd();

  std::vector<std::string> GetIndexNames() const;

  /* Use AddDoc and RemoveDoc only if pv object type is json or hset */
  void AddDoc(std::string_view key, const DbContext& db_cnt, PrimeValue* pv);

  // Remove doc from all matching indices. When HNSW ops are buffered
  // (serializing/restoring), external vector data is preserved so HNSW
  // pointers remain valid until the buffer is drained.
  // pv is non-const because preservation swaps sds entries in StringMap.
  void RemoveDoc(std::string_view key, const DbContext& db_cnt, PrimeValue& pv,
                 absl::Span<const std::string_view> modified_fields = {});

  size_t GetUsedMemory() const;
  SearchStats GetStats() const;  // combines stats for all indices

  search::DefragmentResult Defragment(PageUsage* page_usage);

 private:
  // Clean caches that might have data from this index
  void DropIndexCache(const dfly::ShardDocIndex& shard_doc_index);

 private:
  MiMemoryResource local_mr_;
  absl::flat_hash_map<std::string, std::unique_ptr<ShardDocIndex>> indices_;

  std::string next_defrag_index_;
};

}  // namespace dfly
