// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <chrono>
#include <type_traits>
#include <variant>

#include "absl/container/flat_hash_set.h"
#include "base/logging.h"
#include "core/overloaded.h"
#include "core/search/ast_expr.h"
#include "core/search/compressed_sorted_set.h"
#include "core/search/index_result.h"
#include "core/search/indices.h"
#include "core/search/query_driver.h"
#include "core/search/sort_indices.h"
#include "core/search/tag_types.h"
#include "core/search/vector_utils.h"

using namespace std;

namespace dfly::search {

namespace {

AstExpr ParseQuery(std::string_view query, const QueryParams* params,
                   const OptionalFilters* filters) {
  QueryDriver driver{};
  driver.ResetScanner();
  driver.SetParams(params);
  driver.SetInput(std::string{query});
  (void)Parser (&driver)();  // can throw
  driver.SetOptionalFilters(filters);
  return driver.Take();
}

// GCC 12 yields a wrong warning in a deeply inlined call in UnifyResults, only ignoring the whole
// scope solves it
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

struct ProfileBuilder {
  struct NodeFormatter {
    template <TagType T> void operator()(std::string* out, const AstAffixNode<T>& node) const {
      out->append(node.affix);
    }
    void operator()(std::string* out, const AstTagsNode::TagValue& value) const {
      visit([this, out](const auto& n) { this->operator()(out, n); }, value);
    }
  };

  string GetNodeInfo(const AstNode& node) {
    Overloaded node_info{
        [](monostate) -> string { return ""s; },
        [](const AstTermNode& n) { return absl::StrCat("Term{", n.affix, "}"); },
        [](const AstPrefixNode& n) { return absl::StrCat("Prefix{", n.affix, "}"); },
        [](const AstSuffixNode& n) { return absl::StrCat("Suffix{", n.affix, "}"); },
        [](const AstInfixNode& n) { return absl::StrCat("Infix{", n.affix, "}"); },
        [](const AstRangeNode& n) { return absl::StrCat("Range{", n.lo, "<>", n.hi, "}"); },
        [](const AstLogicalNode& n) {
          auto op = n.op == AstLogicalNode::AND ? "and" : "or";
          return absl::StrCat("Logical{n=", n.nodes.size(), ",o=", op, "}");
        },
        [](const AstTagsNode& n) {
          return absl::StrCat("Tags{", absl::StrJoin(n.tags, ",", NodeFormatter()), "}");
        },
        [](const AstFieldNode& n) { return absl::StrCat("Field{", n.field, "}"); },
        [](const AstKnnNode& n) { return absl::StrCat("KNN{l=", n.limit, "}"); },
        [](const AstNegateNode& n) { return absl::StrCat("Negate{}"); },
        [](const AstStarNode& n) { return absl::StrCat("Star{}"); },
        [](const AstStarFieldNode& n) { return absl::StrCat("StarField{}"); },
        [](const AstGeoNode& n) {
          return absl::StrCat("Geo{", n.lat, " ", n.lon, " ", n.radius, " ", n.unit, "}");
        },
    };
    return visit(node_info, node.Variant());
  }

  using Tp = std::chrono::steady_clock::time_point;

  Tp Start() {
    depth_++;
    return chrono::steady_clock::now();
  }

  void Finish(Tp start, const AstNode& node, const IndexResult& result) {
    DCHECK_GE(depth_, 1u);
    auto took = chrono::steady_clock::now() - start;
    size_t micros = chrono::duration_cast<chrono::microseconds>(took).count();
    auto descr = GetNodeInfo(node);
    profile_.events.push_back({std::move(descr), micros, depth_ - 1, result.ApproximateSize()});
    depth_--;
  }

  AlgorithmProfile Take() {
    reverse(profile_.events.begin(), profile_.events.end());
    return std::move(profile_);
  }

 private:
  size_t depth_;
  AlgorithmProfile profile_;
};

struct BasicSearch {
  using LogicOp = AstLogicalNode::LogicOp;

  BasicSearch(const FieldIndices* indices) : indices_{indices} {
  }

  void EnableProfiling() {
    profile_builder_ = ProfileBuilder{};
  }

  BaseIndex* GetBaseIndex(string_view field) {
    auto index = indices_->GetIndex(field);
    if (!index) {
      error_ = absl::StrCat("Invalid field: ", field);
      return nullptr;
    }
    return index;
  }

  // Get casted sub index by field
  template <typename T> T* GetIndex(string_view field) {
    static_assert(is_base_of_v<BaseIndex, T>);

    auto base_index = GetBaseIndex(field);
    if (!base_index) {
      return nullptr;
    }

    auto* casted_ptr = dynamic_cast<T*>(base_index);
    if (!casted_ptr) {
      error_ = absl::StrCat("Wrong access type for field: ", field);
      return nullptr;
    }

    return casted_ptr;
  }

  BaseSortIndex* GetSortIndex(string_view field) {
    auto index = indices_->GetSortIndex(field);
    if (!index) {
      error_ = absl::StrCat("Invalid sort field: ", field);
      return nullptr;
    }

    return index;
  }

  // Collect all index results from F(C[i])
  template <typename C, typename F>
  vector<IndexResult> GetSubResults(const C& container, const F& f) {
    vector<IndexResult> sub_results(container.size());
    for (size_t i = 0; i < container.size(); i++)
      sub_results[i] = IndexResult{f(container[i])};
    return sub_results;
  }

  void Merge(IndexResult matched, IndexResult* current_ptr, LogicOp op) {
    IndexResult& current = *current_ptr;
    auto vec = MergeIndexResults(matched, current, op);
    current = IndexResult{std::move(vec)};
  }

  // Efficiently unify multiple sub results with specified logical op
  IndexResult UnifyResults(vector<IndexResult>&& sub_results, LogicOp op) {
    if (sub_results.empty())
      return IndexResult{};

    // Unifying from smallest to largest is more efficient.
    // AND: the result only shrinks, so starting with the smallest is most optimal.
    // OR: unifying smaller sets first reduces the number of element traversals on average.
    sort(sub_results.begin(), sub_results.end(),
         [](const auto& l, const auto& r) { return l.ApproximateSize() < r.ApproximateSize(); });

    IndexResult out{std::move(sub_results[0])};
    for (auto& matched : absl::MakeSpan(sub_results).subspan(1))
      Merge(std::move(matched), &out, op);
    return out;
  }

  template <typename C, typename F>
  IndexResult CollectMatches(BaseStringIndex<C>* index, std::string_view word, F&& f) {
    IndexResult result{};
    invoke(f, *index, word,
           [&result, this](const auto* c) { Merge(IndexResult{c}, &result, LogicOp::OR); });
    return result;
  }

  IndexResult Search(monostate, string_view) {
    return IndexResult{};
  }

  IndexResult Search(const AstStarNode& node, string_view active_field) {
    DCHECK(active_field.empty());
    return IndexResult{&indices_->GetAllDocs()};
  }

  IndexResult Search(const AstStarFieldNode& node, string_view active_field) {
    // Try to get a sort index first, as `@field:*` might imply wanting sortable behavior
    BaseSortIndex* sort_index = indices_->GetSortIndex(active_field);
    if (sort_index) {
      return IndexResult{sort_index->GetAllDocsWithNonNullValues()};
    }

    // If sort index doesn't exist try regular index
    BaseIndex* base_index = GetBaseIndex(active_field);
    return base_index ? IndexResult{base_index->GetAllDocsWithNonNullValues()} : IndexResult{};
  }

  template <TagType T> IndexResult Search(const AstAffixNode<T>& node, string_view active_field) {
    vector<TextIndex*> indices;
    if (!active_field.empty()) {
      if (auto* index = GetIndex<TextIndex>(active_field); index)
        indices = {index};
      else
        return IndexResult{};
    } else {
      indices = indices_->GetAllTextIndices();
    }

    auto mapping = [&node, this](TextIndex* index) {
      if constexpr (T == TagType::PREFIX)
        return CollectMatches(index, node.affix, &TextIndex::MatchPrefix);
      else if constexpr (T == TagType::SUFFIX)
        return CollectMatches(index, node.affix, &TextIndex::MatchSuffix);
      else if constexpr (T == TagType::INFIX)
        return CollectMatches(index, node.affix, &TextIndex::MatchInfix);
      else
        return vector<DocId>{};
    };
    return UnifyResults(GetSubResults(indices, mapping), LogicOp::OR);
  }

  // "term": access field's text index or unify results from all text indices if no field is set
  IndexResult Search(const AstAffixNode<TagType::REGULAR> node, string_view active_field) {
    std::string term = node.affix;
    bool strip_whitespace = true;

    if (auto synonyms = indices_->GetSynonyms(); synonyms) {
      if (auto group_id = synonyms->GetGroupToken(term); group_id) {
        term = *group_id;
        strip_whitespace = false;
      }
    }

    if (!active_field.empty()) {
      if (auto* index = GetIndex<TextIndex>(active_field); index)
        return IndexResult{index->Matching(term, strip_whitespace)};
      return IndexResult{};
    }

    vector<TextIndex*> selected_indices = indices_->GetAllTextIndices();
    auto mapping = [&term, strip_whitespace](TextIndex* index) {
      return index->Matching(term, strip_whitespace);
    };

    return UnifyResults(GetSubResults(selected_indices, mapping), LogicOp::OR);
  }

  // [range]: access field's numeric index
  IndexResult Search(const AstRangeNode& node, string_view active_field) {
    DCHECK(!active_field.empty());
    if (auto* index = GetIndex<NumericIndex>(active_field); index) {
      return IndexResult{index->Range(node.lo, node.hi)};
    }
    return IndexResult{};
  }

  IndexResult Search(const AstGeoNode& node, string_view active_field) {
    DCHECK(!active_field.empty());
    if (auto* index = GetIndex<GeoIndex>(active_field); index) {
      return IndexResult{index->RadiusSearch(node.lon, node.lat, node.radius, node.unit)};
    }
    return IndexResult{};
  }

  // negate -(*subquery*): explicitly compute result complement. Needs further optimizations
  IndexResult Search(const AstNegateNode& node, string_view active_field) {
    auto matched = SearchGeneric(*node.node, active_field).Take().first;
    vector<DocId> all = indices_->GetAllDocs();

    // To negate a result, we have to find the complement of matched to all documents,
    // so we remove all matched documents from the set of all documents.
    auto pred = [&matched](DocId doc) {
      return binary_search(matched.begin(), matched.end(), doc);
    };
    all.erase(remove_if(all.begin(), all.end(), pred), all.end());
    return IndexResult{std::move(all)};
  }

  // logical query: unify all sub results
  IndexResult Search(const AstLogicalNode& node, string_view active_field) {
    auto mapping = [&](auto& node) { return SearchGeneric(node, active_field); };
    return UnifyResults(GetSubResults(node.nodes, mapping), node.op);
  }

  // @field: set active field for sub tree
  IndexResult Search(const AstFieldNode& node, string_view active_field) {
    DCHECK(active_field.empty());
    DCHECK(node.node);
    return SearchGeneric(*node.node, node.field);
  }

  // {tags | ...}: Unify results for all tags
  IndexResult Search(const AstTagsNode& node, string_view active_field) {
    auto* tag_index = GetIndex<TagIndex>(active_field);
    if (!tag_index)
      return IndexResult{};

    Overloaded ov{[tag_index](const AstTermNode& term) -> IndexResult {
                    return IndexResult{tag_index->Matching(term.affix)};
                  },
                  [tag_index, this](const AstPrefixNode& prefix) {
                    return CollectMatches(tag_index, prefix.affix, &TagIndex::MatchPrefix);
                  },
                  [tag_index, this](const AstSuffixNode& suffix) {
                    return CollectMatches(tag_index, suffix.affix, &TagIndex::MatchSuffix);
                  },
                  [tag_index, this](const AstInfixNode& infix) {
                    return CollectMatches(tag_index, infix.affix, &TagIndex::MatchInfix);
                  }};
    auto mapping = [ov](const auto& tag) { return visit(ov, tag); };
    return UnifyResults(GetSubResults(node.tags, mapping), LogicOp::OR);
  }

  void SearchKnnFlat(FlatVectorIndex* vec_index, const AstKnnNode& knn, IndexResult&& sub_results) {
    knn_distances_.reserve(sub_results.ApproximateSize());
    auto cb = [&](auto* set) {
      auto [dim, sim] = vec_index->Info();
      for (DocId matched_doc : *set) {
        float dist = VectorDistance(knn.vec.first.get(), vec_index->Get(matched_doc), dim, sim);
        knn_distances_.emplace_back(dist, matched_doc);
      }
    };
    visit(cb, sub_results.Borrowed());

    size_t prefix_size = min(knn.limit, knn_distances_.size());
    partial_sort(knn_distances_.begin(), knn_distances_.begin() + prefix_size,
                 knn_distances_.end());
    knn_distances_.resize(prefix_size);
  }

  // [KNN limit @field vec]: Compute distance from `vec` to all vectors keep closest `limit`
  IndexResult Search(const AstKnnNode& knn, string_view active_field) {
    DCHECK(active_field.empty());
    auto sub_results = SearchGeneric(*knn.filter, active_field);

    auto* vec_index = GetIndex<BaseVectorIndex>(knn.field);
    if (!vec_index)
      return IndexResult{};

    // If vector dimension is 0, treat as placeholder/invalid - return empty results
    // This allows tests to use dummy vector values like "<your_vector_blob>"
    if (knn.vec.second == 0)
      return IndexResult{};

    if (auto [dim, _] = vec_index->Info(); dim != knn.vec.second) {
      error_ =
          absl::StrCat("Wrong vector index dimensions, got: ", knn.vec.second, ", expected: ", dim);
      return IndexResult{};
    }

    knn_scores_.clear();

    if (auto flat_index = dynamic_cast<FlatVectorIndex*>(vec_index); flat_index)
      SearchKnnFlat(dynamic_cast<FlatVectorIndex*>(vec_index), knn, std::move(sub_results));

    vector<DocId> out(knn_distances_.size());
    knn_scores_.reserve(knn_distances_.size());

    for (size_t i = 0; i < knn_distances_.size(); i++) {
      knn_scores_.emplace_back(knn_distances_[i].second, knn_distances_[i].first);
      out[i] = knn_distances_[i].second;
    }

    return IndexResult{std::move(out)};
  }

  // Determine node type and call specific search function
  IndexResult SearchGeneric(const AstNode& node, string_view active_field, bool top_level = false) {
    if (!error_.empty())
      return IndexResult{};

    ProfileBuilder::Tp start = profile_builder_ ? profile_builder_->Start() : ProfileBuilder::Tp{};

    auto cb = [this, active_field](const auto& inner) { return Search(inner, active_field); };
    auto result = visit(cb, node.Variant());

    // Top level results don't need to be sorted, because they will be scored, sorted by fields or
    // used by knn
    DCHECK(top_level || holds_alternative<AstKnnNode>(node.Variant()) ||
           holds_alternative<AstGeoNode>(node.Variant()) ||
           visit([](auto* set) { return is_sorted(set->begin(), set->end()); }, result.Borrowed()));

    if (profile_builder_)
      profile_builder_->Finish(start, node, result);

    return result;
  }

  SearchResult Search(const AstNode& query, size_t cuttoff_limit) {
    IndexResult result = SearchGeneric(query, "", true);

    // Extract profile if enabled
    optional<AlgorithmProfile> profile =
        profile_builder_ ? make_optional(profile_builder_->Take()) : nullopt;

    auto [out, total_size] = result.Take(cuttoff_limit);
    return SearchResult{total_size, std::move(out), std::move(knn_scores_), std::move(profile),
                        std::move(error_)};
  }

  const FieldIndices* indices_;

  string error_;
  optional<ProfileBuilder> profile_builder_ = ProfileBuilder{};

  std::vector<pair<DocId, float>> knn_scores_;
  vector<pair<float, DocId>> knn_distances_;
};

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

}  // namespace

AstNode OptionalNumericFilter::Node(std::string field) {
  return AstFieldNode{"@" + field, AstRangeNode(lo_, false, hi_, false)};
}

string_view Schema::LookupAlias(string_view alias) const {
  if (auto it = field_names.find(alias); it != field_names.end())
    return it->second;
  return alias;
}

string_view Schema::LookupIdentifier(string_view identifier) const {
  if (auto it = fields.find(identifier); it != fields.end())
    return it->second.short_name;
  return identifier;
}

IndicesOptions::IndicesOptions() {
  static absl::flat_hash_set<std::string> kDefaultStopwords{
      "a",    "is",    "the",  "an",    "and",   "are",  "as",   "at", "be",  "but",  "by",
      "for",  "if",    "in",   "into",  "it",    "no",   "not",  "of", "on",  "or",   "such",
      "that", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"};

  stopwords = kDefaultStopwords;
}

FieldIndices::FieldIndices(const Schema& schema, const IndicesOptions& options,
                           PMR_NS::memory_resource* mr, const Synonyms* synonyms)
    : schema_{schema}, options_{options}, synonyms_{synonyms} {
  CreateIndices(mr);
  CreateSortIndices(mr);
}

void FieldIndices::CreateIndices(PMR_NS::memory_resource* mr) {
  for (const auto& [field_ident, field_info] : schema_.fields) {
    if ((field_info.flags & SchemaField::NOINDEX) > 0)
      continue;

    switch (field_info.type) {
      case SchemaField::TEXT: {
        const auto& tparams = std::get<SchemaField::TextParams>(field_info.special_params);
        indices_[field_ident] =
            make_unique<TextIndex>(mr, &options_.stopwords, synonyms_, tparams.with_suffixtrie);
        break;
      }
      case SchemaField::NUMERIC: {
        const auto& nparams = std::get<SchemaField::NumericParams>(field_info.special_params);
        indices_[field_ident] = make_unique<NumericIndex>(nparams.block_size, mr);
        break;
      }
      case SchemaField::TAG: {
        const auto& tparams = std::get<SchemaField::TagParams>(field_info.special_params);
        indices_[field_ident] = make_unique<TagIndex>(mr, tparams);
        break;
      }
      case SchemaField::VECTOR: {
        unique_ptr<BaseVectorIndex> vector_index;

        DCHECK(holds_alternative<SchemaField::VectorParams>(field_info.special_params));
        const auto& vparams = std::get<SchemaField::VectorParams>(field_info.special_params);

        // Use global HNSW index
        if (vparams.use_hnsw)
          break;

        vector_index = make_unique<FlatVectorIndex>(vparams, mr);
        indices_[field_ident] = std::move(vector_index);

        break;
      }
      case SchemaField::GEO: {
        indices_[field_ident] = make_unique<GeoIndex>(mr);
        break;
      }
    }
  }
}

void FieldIndices::CreateSortIndices(PMR_NS::memory_resource* mr) {
  for (const auto& [field_ident, field_info] : schema_.fields) {
    if ((field_info.flags & SchemaField::SORTABLE) == 0)
      continue;

    switch (field_info.type) {
      case SchemaField::TAG:
      case SchemaField::TEXT:
        sort_indices_[field_ident] = make_unique<StringSortIndex>(mr);
        break;
      case SchemaField::NUMERIC:
        sort_indices_[field_ident] = make_unique<NumericSortIndex>(mr);
        break;
      case SchemaField::VECTOR:
      case SchemaField::GEO:
        break;
    }
  }
}

bool FieldIndices::Add(DocId doc, const DocumentAccessor& access) {
  bool was_added = true;

  std::vector<std::pair<std::string_view, BaseIndex*>> successfully_added_indices;
  successfully_added_indices.reserve(indices_.size() + sort_indices_.size());

  auto try_add = [&](const auto& indices_container) {
    for (auto& [field, index] : indices_container) {
      if (index->Add(doc, access, field)) {
        successfully_added_indices.emplace_back(field, index.get());
      } else {
        was_added = false;
        break;
      }
    }
  };

  try_add(indices_);

  if (was_added) {
    try_add(sort_indices_);
  }

  if (!was_added) {
    for (auto& [field, index] : successfully_added_indices) {
      index->Remove(doc, access, field);
    }
    return false;
  }

  all_ids_.insert(upper_bound(all_ids_.begin(), all_ids_.end(), doc), doc);
  return true;
}

void FieldIndices::Remove(DocId doc, const DocumentAccessor& access) {
  for (auto& [field, index] : indices_)
    index->Remove(doc, access, field);
  for (auto& [field, sort_index] : sort_indices_)
    sort_index->Remove(doc, access, field);

  auto it = lower_bound(all_ids_.begin(), all_ids_.end(), doc);
  DCHECK(it != all_ids_.end() && *it == doc);
  all_ids_.erase(it);
}

BaseIndex* FieldIndices::GetIndex(string_view field) const {
  auto it = indices_.find(schema_.LookupAlias(field));
  return it != indices_.end() ? it->second.get() : nullptr;
}

BaseSortIndex* FieldIndices::GetSortIndex(string_view field) const {
  auto it = sort_indices_.find(schema_.LookupAlias(field));
  return it != sort_indices_.end() ? it->second.get() : nullptr;
}

std::vector<TextIndex*> FieldIndices::GetAllTextIndices() const {
  vector<TextIndex*> out;
  for (const auto& [field_name, field_info] : schema_.fields) {
    if (field_info.type != SchemaField::TEXT || (field_info.flags & SchemaField::NOINDEX) > 0)
      continue;
    auto* index = dynamic_cast<TextIndex*>(GetIndex(field_name));
    DCHECK(index);
    out.push_back(index);
  }
  return out;
}

const vector<DocId>& FieldIndices::GetAllDocs() const {
  return all_ids_;
}

const Schema& FieldIndices::GetSchema() const {
  return schema_;
}

SortableValue FieldIndices::GetSortIndexValue(DocId doc, std::string_view field_identifier) const {
  auto it = sort_indices_.find(field_identifier);
  DCHECK(it != sort_indices_.end());
  return it->second->Lookup(doc);
}

void FieldIndices::FinalizeInitialization() {
  for (auto& [field, index] : indices_) {
    index->FinalizeInitialization();
  }
}

const Synonyms* FieldIndices::GetSynonyms() const {
  return synonyms_;
}

SearchAlgorithm::SearchAlgorithm() = default;
SearchAlgorithm::~SearchAlgorithm() = default;

bool SearchAlgorithm::Init(string_view query, const QueryParams* params,
                           const OptionalFilters* filters) {
  try {
    query_ = make_unique<AstExpr>(ParseQuery(query, params, filters));
  } catch (const Parser::syntax_error& se) {
    LOG(INFO) << "Failed to parse query \"" << query << "\":" << se.what();
    return false;
  } catch (...) {
    LOG_EVERY_T(INFO, 10) << "Unexpected query parser error \"" << query << "\"";
    return false;
  }

  if (holds_alternative<monostate>(*query_)) {
    LOG_EVERY_T(INFO, 10) << "Empty result after parsing query \"" << query << "\"";
    return false;
  }

  return true;
}

SearchResult SearchAlgorithm::Search(const FieldIndices* index, size_t cuttoff_limit) const {
  DCHECK(query_);

  auto bs = BasicSearch{index};
  if (profiling_enabled_)
    bs.EnableProfiling();
  return bs.Search(*query_, cuttoff_limit);
}

std::optional<KnnScoreSortOption> SearchAlgorithm::GetKnnScoreSortOption() const {
  // HNSW KNN query
  if (knn_hnsw_score_sort_option_) {
    return knn_hnsw_score_sort_option_;
  }

  // FLAT KNN query
  if (auto* knn = get_if<AstKnnNode>(query_.get()); knn)
    return KnnScoreSortOption{string_view{knn->score_alias}, knn->limit};

  return nullopt;
}

bool SearchAlgorithm::IsKnnQuery() const {
  DCHECK(query_);
  return std::holds_alternative<AstKnnNode>(*query_);
}

AstKnnNode* SearchAlgorithm::GetKnnNode() const {
  if (auto* knn = get_if<AstKnnNode>(query_.get()); knn) {
    return knn;
  }
  return nullptr;
}

std::unique_ptr<AstNode> SearchAlgorithm::PopKnnNode() {
  if (auto* knn = get_if<AstKnnNode>(query_.get()); knn) {
    // Save knn score sort option
    knn_hnsw_score_sort_option_ = KnnScoreSortOption{string_view{knn->score_alias}, knn->limit};
    auto node = std::move(query_);
    AstKnnNode* moved_knn_node = reinterpret_cast<AstKnnNode*>(node.get());
    if (!std::holds_alternative<AstStarNode>(*moved_knn_node->filter))
      query_.swap(moved_knn_node->filter);
    return node;
  }
  LOG(DFATAL) << "Should not reach here";
  return nullptr;
}

void SearchAlgorithm::EnableProfiling() {
  profiling_enabled_ = true;
}

}  // namespace dfly::search
