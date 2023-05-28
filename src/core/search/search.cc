// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/strings/numbers.h>

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

// Add results from matched to current, keep sorted and remove duplicates.
void UnifyResults(vector<DocId>&& matched, vector<DocId>* current, vector<DocId>* tmp) {
  swap(*current, *tmp);  // Move current to tmp so we can directly write into current
  current->clear();
  current->reserve(matched.size() + tmp->size());

  merge(matched.begin(), matched.end(), tmp->begin(), tmp->end(), back_inserter(*current));
  current->erase(unique(current->begin(), current->end()), current->end());
}

// Merge results from matched with current, keep sorted.
void MergeResults(vector<DocId>&& matched, vector<DocId>* current, vector<DocId>* tmp) {
  swap(*current, *tmp);  // Move current to tmp so we can directly write into current
  current->clear();
  current->reserve(matched.size() + tmp->size());

  set_intersection(matched.begin(), matched.end(), tmp->begin(), tmp->end(),
                   back_inserter(*current));
}

struct BasicSearch {
  // Get casted sub index by field
  template <typename T> T* GetIndex(string_view field) {
    static_assert(is_base_of_v<BaseIndex, T>);
    auto index = indices->GetIndex(field);
    DCHECK(index);  // TODO: handle not existing erorr
    auto* casted_ptr = dynamic_cast<T*>(index);
    DCHECK(casted_ptr);  // TODO: handle type errors
    return casted_ptr;
  }

  vector<DocId> Search(monostate, string_view) {
    return {};
  }

  vector<DocId> Search(const AstTermNode& node, string_view active_field) {
    // Select active indices: search in all text indices if none is selected
    vector<TextIndex*> selected_indices;
    if (active_field.empty())
      selected_indices = indices->GetAllTextIndices();
    else
      selected_indices = {GetIndex<TextIndex>(active_field)};

    // Unify results from all indices
    vector<DocId> out, tmp;
    for (auto* index : selected_indices)
      UnifyResults(index->Matching(node.term), &out, &tmp);

    return out;
  }

  vector<DocId> Search(const AstRangeNode& node, string_view active_field) {
    DCHECK(!active_field.empty());
    return GetIndex<NumericIndex>(active_field)->Range(node.lo, node.hi);
  }

  vector<DocId> Search(const AstNegateNode& node, string_view active_field) {
    // To negate a result, we have to find the complement of matched to all documents.
    auto matched = SearchGeneric(*node.node, active_field);
    auto out = indices->GetAllDocs();
    auto pred = [&matched](DocId doc) {
      return binary_search(matched.begin(), matched.end(), doc);
    };
    out.erase(remove_if(out.begin(), out.end(), pred), out.end());
    return out;
  }

  vector<DocId> Search(const AstLogicalNode& node, string_view active_field) {
    auto merge_func = node.op == AstLogicalNode::AND ? MergeResults : UnifyResults;

    bool first = true;
    vector<DocId> out, tmp;
    for (auto& subnode : node.nodes) {
      auto matched = SearchGeneric(subnode, active_field);
      if (first) {
        out = matched;
        first = false;
      } else {
        merge_func(move(matched), &out, &tmp);
      }
    }
    return out;
  }

  vector<DocId> Search(const AstFieldNode& node, string_view active_field) {
    DCHECK(active_field.empty());
    return SearchGeneric(*node.node, node.field);
  }

  vector<DocId> SearchGeneric(const AstNode& node, string_view active_field) {
    auto cb = [this, active_field](const auto& inner) { return Search(inner, active_field); };
    auto result = visit(cb, static_cast<const NodeVariants&>(node));
    DCHECK(is_sorted(result.begin(), result.end()));
    return result;
  }

  static vector<DocId> Search(FieldIndices* indices, const AstNode& query) {
    return BasicSearch{indices}.SearchGeneric(query, "");
  }

  FieldIndices* indices;
};

}  // namespace

FieldIndices::FieldIndices(Schema schema) : schema_{move(schema)}, all_ids_{}, indices_{} {
  for (auto& [field, type] : schema_.fields) {
    switch (type) {
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

BaseIndex* FieldIndices::GetIndex(string_view field) {
  auto it = indices_.find(field);
  return it != indices_.end() ? it->second.get() : nullptr;
}

std::vector<TextIndex*> FieldIndices::GetAllTextIndices() {
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

vector<DocId> SearchAlgorithm::Search(FieldIndices* index) const {
  return BasicSearch::Search(index, *query_);
}

}  // namespace dfly::search
