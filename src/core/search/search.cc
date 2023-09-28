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

#include "base/logging.h"
#include "core/overloaded.h"
#include "core/search/ast_expr.h"
#include "core/search/compressed_sorted_set.h"
#include "core/search/indices.h"
#include "core/search/query_driver.h"
#include "core/search/sort_indices.h"
#include "core/search/vector_utils.h"

using namespace std;

namespace dfly::search {

namespace {

AstExpr ParseQuery(std::string_view query, const QueryParams* params) {
  QueryDriver driver{};
  driver.ResetScanner();
  driver.SetParams(params);
  driver.SetInput(std::string{query});
  (void)Parser (&driver)();  // can throw
  return driver.Take();
}

// GCC 12 yields a wrong warning in a deeply inlined call in UnifyResults, only ignoring the whole
// scope solves it
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"

// Represents an either owned or non-owned result set that can be accessed transparently.
struct IndexResult {
  using DocVec = vector<DocId>;
  using BorrowedView = variant<const DocVec*, const CompressedSortedSet*>;

  IndexResult() : value_{DocVec{}} {
  }

  IndexResult(const CompressedSortedSet* css) : value_{css} {
    if (css == nullptr)
      value_ = DocVec{};
  }

  IndexResult(DocVec&& dv) : value_{move(dv)} {
  }

  IndexResult(const DocVec* dv) : value_{dv} {
  }

  size_t Size() const {
    return visit([](auto* set) { return set->size(); }, Borrowed());
  }

  bool IsOwned() const {
    return holds_alternative<DocVec>(value_);
  }

  IndexResult& operator=(DocVec&& entries) {
    if (holds_alternative<DocVec>(value_)) {
      swap(get<DocVec>(value_), entries);  // swap to keep backing array
      entries.clear();
    } else {
      value_ = std::move(entries);
    }
    return *this;
  }

  BorrowedView Borrowed() const {
    auto cb = [](const auto& v) -> BorrowedView {
      if constexpr (is_pointer_v<remove_reference_t<decltype(v)>>)
        return v;
      else
        return &v;
    };
    return visit(cb, value_);
  }

  // Move out of owned or copy borrowed, truncate to limit if set
  DocVec Take(optional<size_t> limit = nullopt) {
    if (IsOwned()) {
      auto out = std::move(get<DocVec>(value_));
      out.resize(min(limit.value_or(out.size()), out.size()));
      return out;
    }

    auto cb = [limit](auto* set) {
      DocVec out(min(limit.value_or(set->size()), set->size()));
      auto it = set->begin();
      for (size_t i = 0; it != set->end() && i < out.size(); ++i, ++it)
        out[i] = *it;
      return out;
    };
    return visit(cb, Borrowed());
  }

 private:
  variant<DocVec /*owned*/, const CompressedSortedSet*, const DocVec*> value_;
};

struct ProfileBuilder {
  string GetNodeInfo(const AstNode& node) {
    Overloaded node_info{
        [](monostate) -> string { return ""s; },
        [](const AstTermNode& n) { return absl::StrCat("Term{", n.term, "}"); },
        [](const AstRangeNode& n) { return absl::StrCat("Range{", n.lo, "<>", n.hi, "}"); },
        [](const AstLogicalNode& n) {
          auto op = n.op == AstLogicalNode::AND ? "and" : "or";
          return absl::StrCat("Logical{n=", n.nodes.size(), ",o=", op, "}");
        },
        [](const AstTagsNode& n) { return absl::StrCat("Tags{", absl::StrJoin(n.tags, ","), "}"); },
        [](const AstFieldNode& n) { return absl::StrCat("Field{", n.field, "}"); },
        [](const AstKnnNode& n) { return absl::StrCat("KNN{l=", n.limit, "}"); },
        [](const AstNegateNode& n) { return absl::StrCat("Negate{}"); },
        [](const AstStarNode& n) { return absl::StrCat("Star{}"); },
        [](const AstSortNode& n) { return absl::StrCat("Sort{f", n.field, "}"); },
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
    profile_.events.push_back({std::move(descr), micros, depth_ - 1, result.Size()});
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

  BasicSearch(const FieldIndices* indices, size_t limit)
      : indices_{indices}, limit_{limit}, tmp_vec_{} {
  }

  void EnableProfiling() {
    profile_builder_ = ProfileBuilder{};
  }

  // Get casted sub index by field
  template <typename T> T* GetIndex(string_view field) {
    static_assert(is_base_of_v<BaseIndex, T>);

    auto index = indices_->GetIndex(field);
    if (!index) {
      error_ = absl::StrCat("Invalid field: ", field);
      return nullptr;
    }

    auto* casted_ptr = dynamic_cast<T*>(index);
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
      sub_results[i] = f(container[i]);
    return sub_results;
  }

  void Merge(IndexResult matched, IndexResult* current_ptr, LogicOp op) {
    IndexResult& current = *current_ptr;
    tmp_vec_.clear();

    if (op == LogicOp::AND) {
      tmp_vec_.reserve(min(matched.Size(), current.Size()));
      auto cb = [this](auto* s1, auto* s2) {
        set_intersection(s1->begin(), s1->end(), s2->begin(), s2->end(), back_inserter(tmp_vec_));
      };
      visit(cb, matched.Borrowed(), current.Borrowed());
    } else {
      tmp_vec_.reserve(matched.Size() + current.Size());
      auto cb = [this](auto* s1, auto* s2) {
        set_union(s1->begin(), s1->end(), s2->begin(), s2->end(), back_inserter(tmp_vec_));
      };
      visit(cb, matched.Borrowed(), current.Borrowed());
    }

    current = move(tmp_vec_);
  }

  // Efficiently unify multiple sub results with specified logical op
  IndexResult UnifyResults(vector<IndexResult>&& sub_results, LogicOp op) {
    if (sub_results.empty())
      return vector<DocId>{};

    // Unifying from smallest to largest is more efficient.
    // AND: the result only shrinks, so starting with the smallest is most optimal.
    // OR: unifying smaller sets first reduces the number of element traversals on average.
    sort(sub_results.begin(), sub_results.end(),
         [](const auto& l, const auto& r) { return l.Size() < r.Size(); });

    IndexResult out{std::move(sub_results[0])};
    for (auto& matched : absl::MakeSpan(sub_results).subspan(1))
      Merge(move(matched), &out, op);
    return out;
  }

  IndexResult Search(monostate, string_view) {
    return vector<DocId>{};
  }

  IndexResult Search(const AstStarNode& node, string_view active_field) {
    DCHECK(active_field.empty());
    return {&indices_->GetAllDocs()};
  }

  // "term": access field's text index or unify results from all text indices if no field is set
  IndexResult Search(const AstTermNode& node, string_view active_field) {
    if (!active_field.empty()) {
      if (auto* index = GetIndex<TextIndex>(active_field); index)
        return index->Matching(node.term);
      return IndexResult{};
    }

    vector<TextIndex*> selected_indices = indices_->GetAllTextIndices();
    auto mapping = [&node](TextIndex* index) { return index->Matching(node.term); };

    return UnifyResults(GetSubResults(selected_indices, mapping), LogicOp::OR);
  }

  // [range]: access field's numeric index
  IndexResult Search(const AstRangeNode& node, string_view active_field) {
    DCHECK(!active_field.empty());
    if (auto* index = GetIndex<NumericIndex>(active_field); index)
      return index->Range(node.lo, node.hi);
    return IndexResult{};
  }

  // negate -(*subquery*): explicitly compute result complement. Needs further optimizations
  IndexResult Search(const AstNegateNode& node, string_view active_field) {
    vector<DocId> matched = SearchGeneric(*node.node, active_field).Take();
    vector<DocId> all = indices_->GetAllDocs();

    // To negate a result, we have to find the complement of matched to all documents,
    // so we remove all matched documents from the set of all documents.
    auto pred = [&matched](DocId doc) {
      return binary_search(matched.begin(), matched.end(), doc);
    };
    all.erase(remove_if(all.begin(), all.end(), pred), all.end());
    return all;
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
    if (auto* tag_index = GetIndex<TagIndex>(active_field); tag_index) {
      auto mapping = [tag_index](string_view tag) { return tag_index->Matching(tag); };
      return UnifyResults(GetSubResults(node.tags, mapping), LogicOp::OR);
    }
    return IndexResult{};
  }

  // SORTBY field [DESC]: Sort by field. Part of params and not "core query".
  IndexResult Search(const AstSortNode& node, string_view active_field) {
    auto sub_results = SearchGeneric(*node.filter, active_field);
    preagg_total_ = sub_results.Size();

    // Skip sorting again for KNN queries
    if (holds_alternative<AstKnnNode>(node.filter->Variant()))
      return sub_results;

    if (auto* sort_index = GetSortIndex(node.field); sort_index) {
      auto ids_vec = sub_results.Take();
      sort_index->Sort(&ids_vec, &scores_, limit_, node.descending);
      return ids_vec;
    }

    return IndexResult{};
  }

  void SearchKnnFlat(FlatVectorIndex* vec_index, const AstKnnNode& knn, IndexResult&& sub_results) {
    knn_distances_.reserve(sub_results.Size());
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

  void SearchKnnHnsw(HnswVectorIndex* vec_index, const AstKnnNode& knn, IndexResult&& sub_results) {
    if (indices_->GetAllDocs().size() == sub_results.Size())
      knn_distances_ = vec_index->Knn(knn.vec.first.get(), knn.limit);
    else
      knn_distances_ = vec_index->Knn(knn.vec.first.get(), knn.limit, sub_results.Take());
  }

  // [KNN limit @field vec]: Compute distance from `vec` to all vectors keep closest `limit`
  IndexResult Search(const AstKnnNode& knn, string_view active_field) {
    DCHECK(active_field.empty());
    auto sub_results = SearchGeneric(*knn.filter, active_field);

    auto* vec_index = GetIndex<BaseVectorIndex>(knn.field);
    if (!vec_index)
      return IndexResult{};

    if (auto [dim, _] = vec_index->Info(); dim != knn.vec.second) {
      error_ =
          absl::StrCat("Wrong vector index dimensions, got: ", knn.vec.second, ", expected: ", dim);
      return IndexResult{};
    }

    preagg_total_ = sub_results.Size();
    scores_.clear();
    if (auto hnsw_index = dynamic_cast<HnswVectorIndex*>(vec_index); hnsw_index)
      SearchKnnHnsw(hnsw_index, knn, std::move(sub_results));
    else
      SearchKnnFlat(dynamic_cast<FlatVectorIndex*>(vec_index), knn, std::move(sub_results));

    vector<DocId> out(knn_distances_.size());
    scores_.reserve(knn_distances_.size());

    for (size_t i = 0; i < knn_distances_.size(); i++) {
      scores_.emplace_back(knn_distances_[i].first);
      out[i] = knn_distances_[i].second;
    }

    return out;
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
    DCHECK(top_level ||
           visit([](auto* set) { return is_sorted(set->begin(), set->end()); }, result.Borrowed()));

    if (profile_builder_)
      profile_builder_->Finish(start, node, result);

    return result;
  }

  SearchResult Search(const AstNode& query) {
    IndexResult result = SearchGeneric(query, "", true);

    // Extract profile if enabled
    optional<AlgorithmProfile> profile =
        profile_builder_ ? make_optional(profile_builder_->Take()) : nullopt;

    size_t total = result.Size();
    return SearchResult{total,
                        max(total, preagg_total_),
                        result.Take(limit_),
                        std::move(scores_),
                        std::move(profile),
                        std::move(error_)};
  }

  const FieldIndices* indices_;
  size_t limit_;

  size_t preagg_total_ = 0;
  string error_;
  optional<ProfileBuilder> profile_builder_ = ProfileBuilder{};

  std::vector<ResultScore> scores_;

  vector<DocId> tmp_vec_;
  vector<pair<float, DocId>> knn_distances_;
};

#pragma GCC diagnostic pop

}  // namespace

FieldIndices::FieldIndices(Schema schema) : schema_{move(schema)}, all_ids_{}, indices_{} {
  CreateIndices();
  CreateSortIndices();
}

void FieldIndices::CreateIndices() {
  for (const auto& [field_ident, field_info] : schema_.fields) {
    if ((field_info.flags & SchemaField::NOINDEX) > 0)
      continue;

    switch (field_info.type) {
      case SchemaField::TAG:
        indices_[field_ident] = make_unique<TagIndex>();
        break;
      case SchemaField::TEXT:
        indices_[field_ident] = make_unique<TextIndex>();
        break;
      case SchemaField::NUMERIC:
        indices_[field_ident] = make_unique<NumericIndex>();
        break;
      case SchemaField::VECTOR:
        unique_ptr<BaseVectorIndex> vector_index;

        DCHECK(holds_alternative<SchemaField::VectorParams>(field_info.special_params));
        const auto& vparams = std::get<SchemaField::VectorParams>(field_info.special_params);

        if (vparams.use_hnsw)
          vector_index = make_unique<HnswVectorIndex>(vparams.dim, vparams.sim, vparams.capacity);
        else
          vector_index = make_unique<FlatVectorIndex>(vparams.dim, vparams.sim);

        indices_[field_ident] = std::move(vector_index);
        break;
    }
  }
}

void FieldIndices::CreateSortIndices() {
  for (const auto& [field_ident, field_info] : schema_.fields) {
    if ((field_info.flags & SchemaField::SORTABLE) == 0)
      continue;

    switch (field_info.type) {
      case SchemaField::TAG:
      case SchemaField::TEXT:
        sort_indices_[field_ident] = make_unique<StringSortIndex>();
        break;
      case SchemaField::NUMERIC:
        sort_indices_[field_ident] = make_unique<NumericSortIndex>();
        break;
      case SchemaField::VECTOR:
        break;
    }
  }
}

void FieldIndices::Add(DocId doc, DocumentAccessor* access) {
  for (auto& [field, index] : indices_)
    index->Add(doc, access, field);
  for (auto& [field, sort_index] : sort_indices_)
    sort_index->Add(doc, access, field);

  all_ids_.insert(upper_bound(all_ids_.begin(), all_ids_.end(), doc), doc);
}

void FieldIndices::Remove(DocId doc, DocumentAccessor* access) {
  for (auto& [field, index] : indices_)
    index->Remove(doc, access, field);
  for (auto& [field, sort_index] : sort_indices_)
    sort_index->Remove(doc, access, field);

  auto it = lower_bound(all_ids_.begin(), all_ids_.end(), doc);
  CHECK(it != all_ids_.end() && *it == doc);
  all_ids_.erase(it);
}

BaseIndex* FieldIndices::GetIndex(string_view field) const {
  // Replace short field name with full identifier
  if (auto it = schema_.field_names.find(field); it != schema_.field_names.end())
    field = it->second;

  auto it = indices_.find(field);
  return it != indices_.end() ? it->second.get() : nullptr;
}

BaseSortIndex* FieldIndices::GetSortIndex(string_view field) const {
  // Replace short field name with full identifier
  if (auto it = schema_.field_names.find(field); it != schema_.field_names.end())
    field = it->second;

  auto it = sort_indices_.find(field);
  return it != sort_indices_.end() ? it->second.get() : nullptr;
}

std::vector<TextIndex*> FieldIndices::GetAllTextIndices() const {
  vector<TextIndex*> out;
  for (auto& [field_name, field_info] : schema_.fields) {
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

SearchAlgorithm::SearchAlgorithm() = default;
SearchAlgorithm::~SearchAlgorithm() = default;

bool SearchAlgorithm::Init(string_view query, const QueryParams* params, const SortOption* sort) {
  try {
    query_ = make_unique<AstExpr>(ParseQuery(query, params));
  } catch (const Parser::syntax_error& se) {
    LOG(INFO) << "Failed to parse query \"" << query << "\":" << se.what();
    return false;
  } catch (...) {
    LOG(INFO) << "Unexpected query parser error";
    return false;
  }

  if (holds_alternative<monostate>(*query_))
    return false;

  if (sort != nullptr)
    query_ = make_unique<AstNode>(AstSortNode{std::move(query_), sort->field, sort->descending});

  return true;
}

SearchResult SearchAlgorithm::Search(const FieldIndices* index, size_t limit) const {
  auto bs = BasicSearch{index, limit};
  if (profiling_enabled_)
    bs.EnableProfiling();
  return bs.Search(*query_);
}

optional<AggregationInfo> SearchAlgorithm::HasAggregation() const {
  DCHECK(query_);
  if (auto* knn = get_if<AstKnnNode>(query_.get()); knn)
    return AggregationInfo{knn->limit, string_view{knn->score_alias}, false};
  if (holds_alternative<AstSortNode>(*query_))
    return AggregationInfo{nullopt, "", get<AstSortNode>(*query_.get()).descending};
  return nullopt;
}

void SearchAlgorithm::EnableProfiling() {
  profiling_enabled_ = true;
}

}  // namespace dfly::search
