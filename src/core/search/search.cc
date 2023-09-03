// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <chrono>
#include <variant>

#include "base/logging.h"
#include "core/overloaded.h"
#include "core/search/ast_expr.h"
#include "core/search/compressed_sorted_set.h"
#include "core/search/indices.h"
#include "core/search/query_driver.h"
#include "core/search/vector.h"

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

// Represents an either owned or non-owned result set that can be accessed transparently.
struct IndexResult {
  using DocVec = vector<DocId>;

  IndexResult() : value_{DocVec{}} {};

  IndexResult(const CompressedSortedSet* css) : value_{css} {
    if (css == nullptr)
      value_ = DocVec{};
  }

  IndexResult(DocVec&& dv) : value_{move(dv)} {
  }

  size_t Size() const {
    if (holds_alternative<DocVec>(value_))
      return get<DocVec>(value_).size();
    return get<const CompressedSortedSet*>(value_)->Size();
  }

  bool IsOwned() const {
    return holds_alternative<DocVec>(value_);
  }

  IndexResult& operator=(DocVec&& entries) {
    if (holds_alternative<DocVec>(value_)) {
      swap(get<DocVec>(value_), entries);  // swap to keep backing array
      entries.clear();
    } else {
      value_ = move(entries);
    }
    return *this;
  }

  variant<const DocVec*, const CompressedSortedSet*> Borrowed() {
    if (holds_alternative<DocVec>(value_))
      return &get<DocVec>(value_);
    return get<const CompressedSortedSet*>(value_);
  }

  // Move out of owned or copy borrowed
  DocVec Take() {
    if (holds_alternative<DocVec>(value_))
      return move(get<DocVec>(value_));

    const CompressedSortedSet* css = get<const CompressedSortedSet*>(value_);
    return DocVec(css->begin(), css->end());
  }

 private:
  variant<DocVec /*owned*/, const CompressedSortedSet* /* borrowed */> value_;
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
    };
    return visit(node_info, static_cast<const NodeVariants&>(node));
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

  BasicSearch(const FieldIndices* indices) : indices_{indices}, tmp_vec_{} {
  }

  void EnableProfiling() {
    profile_builder_ = ProfileBuilder{};
  }

  // Get casted sub index by field
  template <typename T> T* GetIndex(string_view field) {
    static_assert(is_base_of_v<BaseIndex, T>);
    auto index = indices_->GetIndex(field);
    DCHECK(index) << field;  // TODO: handle not existing error
    auto* casted_ptr = dynamic_cast<T*>(index);
    DCHECK(casted_ptr) << field;  // TODO: handle type errors
    return casted_ptr;
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

    IndexResult out{move(sub_results[0])};
    for (auto& matched : absl::MakeSpan(sub_results).subspan(1))
      Merge(move(matched), &out, op);
    return out;
  }

  IndexResult Search(monostate, string_view) {
    return vector<DocId>{};
  }

  IndexResult Search(const AstStarNode& node, string_view active_field) {
    DCHECK(active_field.empty());
    return vector<DocId>{indices_->GetAllDocs()};  // TODO FIX;
  }

  // "term": access field's text index or unify results from all text indices if no field is set
  IndexResult Search(const AstTermNode& node, string_view active_field) {
    if (!active_field.empty()) {
      auto* index = GetIndex<TextIndex>(active_field);
      return index->Matching(node.term);
    }

    vector<TextIndex*> selected_indices = indices_->GetAllTextIndices();
    auto mapping = [&node](TextIndex* index) { return index->Matching(node.term); };

    return UnifyResults(GetSubResults(selected_indices, mapping), LogicOp::OR);
  }

  // [range]: access field's numeric index
  IndexResult Search(const AstRangeNode& node, string_view active_field) {
    DCHECK(!active_field.empty());
    return GetIndex<NumericIndex>(active_field)->Range(node.lo, node.hi);
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
    auto* tag_index = GetIndex<TagIndex>(active_field);
    auto mapping = [tag_index](string_view tag) { return tag_index->Matching(tag); };
    return UnifyResults(GetSubResults(node.tags, mapping), LogicOp::OR);
  }

  // [KNN limit @field vec]: Compute distance from `vec` to all vectors keep closest `limit`
  IndexResult Search(const AstKnnNode& knn, string_view active_field) {
    DCHECK(active_field.empty());
    auto sub_results = SearchGeneric(*knn.filter, active_field);

    auto* vec_index = GetIndex<VectorIndex>(knn.field);

    distances_.reserve(sub_results.Size());
    auto cb = [&](auto* set) {
      for (DocId matched_doc : *set) {
        float dist = VectorDistance(knn.vector, vec_index->Get(matched_doc));
        distances_.emplace_back(dist, matched_doc);
      }
    };
    visit(cb, sub_results.Borrowed());

    sort(distances_.begin(), distances_.end());

    vector<DocId> out(min(knn.limit, distances_.size()));
    for (size_t i = 0; i < out.size(); i++)
      out[i] = distances_[i].second;

    return out;
  }

  // Determine node type and call specific search function
  IndexResult SearchGeneric(const AstNode& node, string_view active_field, bool top_level = false) {
    ProfileBuilder::Tp start = profile_builder_ ? profile_builder_->Start() : ProfileBuilder::Tp{};

    auto cb = [this, active_field](const auto& inner) { return Search(inner, active_field); };
    auto result = visit(cb, static_cast<const NodeVariants&>(node));

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

    // Copy knn distances to be returned
    vector<float> knn_distances;
    if (!distances_.empty()) {
      knn_distances.resize(result.Size());
      for (size_t i = 0; i < knn_distances.size(); i++)
        knn_distances[i] = distances_[i].first;
    }

    // Extract profile if enabled
    optional<AlgorithmProfile> profile =
        profile_builder_ ? make_optional(profile_builder_->Take()) : nullopt;

    return SearchResult{result.Take(), std::move(knn_distances), std::move(profile)};
  }

  const FieldIndices* indices_;

  optional<ProfileBuilder> profile_builder_ = ProfileBuilder{};

  vector<DocId> tmp_vec_;
  vector<pair<float, DocId>> distances_;
};

}  // namespace

FieldIndices::FieldIndices(Schema schema) : schema_{move(schema)}, all_ids_{}, indices_{} {
  for (const auto& [field_ident, field_info] : schema_.fields) {
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
        indices_[field_ident] = make_unique<VectorIndex>();
        break;
    }
  }
}

void FieldIndices::Add(DocId doc, DocumentAccessor* access) {
  for (auto& [field, index] : indices_) {
    index->Add(doc, access, field);
  }
  all_ids_.insert(upper_bound(all_ids_.begin(), all_ids_.end(), doc), doc);
}

void FieldIndices::Remove(DocId doc, DocumentAccessor* access) {
  for (auto& [field, index] : indices_) {
    index->Remove(doc, access, field);
  }
  auto it = lower_bound(all_ids_.begin(), all_ids_.end(), doc);
  CHECK(it != all_ids_.end() && *it == doc);
  all_ids_.erase(it);
}

BaseIndex* FieldIndices::GetIndex(string_view field) const {
  // Replace short field name with full ident
  if (auto it = schema_.field_names.find(field); it != schema_.field_names.end())
    field = it->second;

  auto it = indices_.find(field);
  return it != indices_.end() ? it->second.get() : nullptr;
}

std::vector<TextIndex*> FieldIndices::GetAllTextIndices() const {
  vector<TextIndex*> out;
  for (auto& [field_name, field_info] : schema_.fields) {
    if (field_info.type != SchemaField::TEXT)
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

SearchAlgorithm::SearchAlgorithm() = default;
SearchAlgorithm::~SearchAlgorithm() = default;

bool SearchAlgorithm::Init(string_view query, const QueryParams* params) {
  try {
    query_ = make_unique<AstExpr>(ParseQuery(query, params));
    return !holds_alternative<monostate>(*query_);
  } catch (const Parser::syntax_error& se) {
    LOG(INFO) << "Failed to parse query \"" << query << "\":" << se.what();
    return false;
  } catch (...) {
    LOG(INFO) << "Unexpected query parser error";
    return false;
  }
}

SearchResult SearchAlgorithm::Search(const FieldIndices* index) const {
  auto bs = BasicSearch{index};
  if (profiling_enabled_)
    bs.EnableProfiling();
  return bs.Search(*query_);
}

optional<size_t> SearchAlgorithm::HasKnn() const {
  DCHECK(query_);
  if (holds_alternative<AstKnnNode>(*query_))
    return get<AstKnnNode>(*query_).limit;
  return nullopt;
}

void SearchAlgorithm::EnableProfiling() {
  profiling_enabled_ = true;
}

}  // namespace dfly::search
