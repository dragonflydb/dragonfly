// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/strings/numbers.h>

#include <variant>

#include "base/logging.h"
#include "core/search/ast_expr.h"
#include "core/search/indices.h"
#include "core/search/query_driver.h"

using namespace std;

namespace dfly::search {

namespace {

AstExpr ParseQuery(std::string_view query) {
  QueryDriver driver{};
  driver.ResetScanner();
  driver.SetInput(std::string{query});
  (void)Parser (&driver)();  // can throw
  return driver.Take();
}

// Represents an either owned or non-owned result set that can be accessed transparently.
struct IndexResult {
  using DocVec = vector<DocId>;

  IndexResult() : value{DocVec{}} {};

  IndexResult(const DocVec* dv) : value{dv} {
    if (dv == nullptr)
      value = DocVec{};
  }

  IndexResult(DocVec&& dv) : value{move(dv)} {
  }

  // Transparent const access to underlying value
  const DocVec* operator->() const {
    return holds_alternative<DocVec>(value) ? &get<DocVec>(value) : get<const DocVec*>(value);
  }

  // Move out of owned or copy borrowed
  DocVec Take() {
    if (holds_alternative<DocVec>(value))
      return move(get<DocVec>(value));
    return *get<const DocVec*>(value);
  }

  // Move out of owned or return empty
  DocVec TakeOwnedOrEmpty() {
    if (holds_alternative<DocVec>(value))
      return move(get<DocVec>(value));
    return DocVec{};
  }

  static bool BySize(const IndexResult& l, const IndexResult& r) {
    return l->size() < r->size();
  }

 private:
  variant<DocVec /*owned*/, const DocVec* /*pointer to borrowed*/> value;
};

// Unify (OR) matched and current
IndexResult MergeOR(IndexResult&& matched, IndexResult&& current, vector<DocId>* tmp) {
  tmp->clear();
  tmp->reserve(matched->size() + current->size());

  merge(matched->begin(), matched->end(), current->begin(), current->end(), back_inserter(*tmp));
  tmp->erase(unique(tmp->begin(), tmp->end()), tmp->end());

  IndexResult result{move(*tmp)};
  *tmp = current.TakeOwnedOrEmpty();  // If current has a backing array, keep it

  return result;
}

// Merge (AND) matched and current
IndexResult MergeAND(IndexResult&& matched, IndexResult&& current, vector<DocId>* tmp) {
  tmp->clear();
  tmp->reserve(min(matched->size(), current->size()));

  set_intersection(matched->begin(), matched->end(), current->begin(), current->end(),
                   back_inserter(*tmp));

  IndexResult result{move(*tmp)};
  *tmp = current.TakeOwnedOrEmpty();  // If current has a backing array, keep it

  return result;
}

struct BasicSearch {
  BasicSearch(const FieldIndices* indices) : indices{indices}, tmp_vec{} {
  }

  // Get casted sub index by field
  template <typename T> T* GetIndex(string_view field) {
    static_assert(is_base_of_v<BaseIndex, T>);
    auto index = indices->GetIndex(field);
    DCHECK(index);  // TODO: handle not existing erorr
    auto* casted_ptr = dynamic_cast<T*>(index);
    DCHECK(casted_ptr);  // TODO: handle type errors
    return casted_ptr;
  }

  // Collect all index results from F(C[i]) and sort by size
  template <typename C, typename F> vector<IndexResult> GetSubResults(C&& container, F&& f) {
    vector<IndexResult> sub_results(container.size());
    for (size_t i = 0; i < container.size(); i++)
      sub_results[i] = f(container[i]);
    return sub_results;
  }

  using LogicOp = AstLogicalNode::LogicOp;

  // Efficiently unify multiple sub results with specified logical op
  IndexResult UnifyResults(vector<IndexResult>&& sub_results, LogicOp op) {
    if (sub_results.empty())
      return vector<DocId>{};

    auto f = op == AstLogicalNode::AND ? MergeAND : MergeOR;

    // Unifying from smallest to largest is more efficient.
    // For AND: the result set only shrinks, so start with the smallest.
    // For OR: unifying smaller sets first reduces the number of element traversals on average.
    sort(sub_results.begin(), sub_results.end(), IndexResult::BySize);

    IndexResult out{move(sub_results[0])};
    for (auto& matched : absl::MakeSpan(sub_results).subspan(1))
      out = f(move(matched), move(out), &tmp_vec);
    return out;
  }

  IndexResult Search(monostate, string_view) {
    return vector<DocId>{};
  }

  // "term": access field's text index or unify results from all text indices if no field is set
  IndexResult Search(const AstTermNode& node, string_view active_field) {
    if (!active_field.empty()) {
      auto* index = GetIndex<TextIndex>(active_field);
      return index->Matching(node.term);
    }

    vector<TextIndex*> selected_indices = indices->GetAllTextIndices();
    auto mapping = [&node](TextIndex* index) { return index->Matching(node.term); };

    return UnifyResults(GetSubResults(selected_indices, mapping), LogicOp::OR);
  }

  // [range]: access field's numeric index
  IndexResult Search(const AstRangeNode& node, string_view active_field) {
    DCHECK(!active_field.empty());
    return GetIndex<NumericIndex>(active_field)->Range(node.lo, node.hi);
  }

  // negate -(*subquery*): explicity compute result complement. Needs further optimizations
  IndexResult Search(const AstNegateNode& node, string_view active_field) {
    vector<DocId> matched = SearchGeneric(*node.node, active_field).Take();
    vector<DocId> all = indices->GetAllDocs();

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

  // Determine node type and call specific search function
  IndexResult SearchGeneric(const AstNode& node, string_view active_field) {
    auto cb = [this, active_field](const auto& inner) { return Search(inner, active_field); };
    auto result = visit(cb, static_cast<const NodeVariants&>(node));
    DCHECK(is_sorted(result->begin(), result->end()));
    return result;
  }

  static vector<DocId> Search(const FieldIndices* indices, const AstNode& query) {
    return BasicSearch{indices}.SearchGeneric(query, "").Take();
  }

  const FieldIndices* indices;
  vector<DocId> tmp_vec;
};

}  // namespace

FieldIndices::FieldIndices(Schema schema) : schema_{move(schema)}, all_ids_{}, indices_{} {
  for (auto& [field, type] : schema_.fields) {
    switch (type) {
      case Schema::TAG:
        indices_[field] = make_unique<TagIndex>();
        break;
      case Schema::TEXT:
        indices_[field] = make_unique<TextIndex>();
        break;
      case Schema::NUMERIC:
        indices_[field] = make_unique<NumericIndex>();
        break;
    }
  }
}

void FieldIndices::Add(DocId doc, DocumentAccessor* access) {
  for (auto& [field, index] : indices_) {
    index->Add(doc, access->Get(field));
  }
  all_ids_.push_back(doc);
  sort(all_ids_.begin(), all_ids_.end());
}

BaseIndex* FieldIndices::GetIndex(string_view field) const {
  auto it = indices_.find(field);
  return it != indices_.end() ? it->second.get() : nullptr;
}

std::vector<TextIndex*> FieldIndices::GetAllTextIndices() const {
  vector<TextIndex*> out;
  for (auto& [field, type] : schema_.fields) {
    if (type != Schema::TEXT)
      continue;
    auto* index = dynamic_cast<TextIndex*>(GetIndex(field));
    DCHECK(index);
    out.push_back(index);
  }
  return out;
}

vector<DocId> FieldIndices::GetAllDocs() const {
  return all_ids_;
}

SearchAlgorithm::SearchAlgorithm() = default;
SearchAlgorithm::~SearchAlgorithm() = default;

bool SearchAlgorithm::Init(string_view query) {
  try {
    query_ = make_unique<AstExpr>(ParseQuery(query));
    return !holds_alternative<monostate>(*query_);
  } catch (const Parser::syntax_error& se) {
    LOG(INFO) << "Failed to parse query \"" << query << "\":" << se.what();
    return false;
  } catch (...) {
    LOG(INFO) << "Unexpected query parser error";
    return false;
  }
}

vector<DocId> SearchAlgorithm::Search(const FieldIndices* index) const {
  return BasicSearch::Search(index, *query_);
}

}  // namespace dfly::search
