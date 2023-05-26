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

struct BasicSearch {
  vector<DocId> Search(monostate, string_view) {
    return {};
  }

  vector<DocId> Search(const AstTermNode& node, string_view active_field) {
    vector<TextIndex*> selected_indices;
    if (active_field.empty()) {
      selected_indices = indices->GetAllTextIndices();
    } else {
      auto index = indices->GetIndex(active_field);
      DCHECK(index);  // TODO: Handle not existing index error
      auto* text_index = dynamic_cast<TextIndex*>(*index);
      DCHECK(text_index);  // TODO: Handle wrong type error
      selected_indices.push_back(text_index);
    }

    vector<DocId> out, current;
    for (auto* index : selected_indices) {
      auto matched = index->Matching(node.term);
      current = move(out);
      merge(matched.begin(), matched.end(), current.begin(), current.end(), back_inserter(out));
      out.erase(unique(out.begin(), out.end()), out.end());
    }

    return out;
  }

  vector<DocId> Search(const AstRangeNode& node, string_view active_field) {
    DCHECK(!active_field.empty());
    auto index = indices->GetIndex(active_field);
    DCHECK(index);  // TODO: Handle not existing index error
    auto* numeric_index = dynamic_cast<NumericIndex*>(*index);
    DCHECK(numeric_index);  // TODO: Handle wrong type error
    return numeric_index->Range(node.lo, node.hi);
  }

  vector<DocId> Search(const AstNegateNode& node, string_view active_field) {
    auto matched = SearchGeneric(*node.node, active_field);
    auto all = indices->GetAllDocs();

    vector<DocId> out;
    for (auto doc : all) {
      if (!binary_search(matched.begin(), matched.end(), doc))
        out.push_back(doc);
    }

    return out;
  }

  vector<DocId> Search(const AstLogicalNode& node, string_view active_field) {
    bool first = true;
    vector<DocId> out, current;

    for (auto& subnode : node.nodes) {
      auto matched = SearchGeneric(subnode, active_field);

      if (first) {
        out = move(matched);
        first = false;
        continue;
      }

      current = move(out);
      if (node.op == AstLogicalNode::AND) {
        // Intersect sorted results sets
        set_intersection(matched.begin(), matched.end(), current.begin(), current.end(),
                         back_inserter(out));
      } else {
        DCHECK_EQ(node.op, AstLogicalNode::OR);
        // Merge sorted result sets and remove duplicates
        merge(matched.begin(), matched.end(), current.begin(), current.end(), back_inserter(out));
        out.erase(unique(out.begin(), out.end()), out.end());
      }
    }
    return out;
  }

  vector<DocId> Search(const AstFieldNode& node, string_view active_field) {
    DCHECK_EQ(active_field.size(), 0u);
    return SearchGeneric(*node.node, node.field);
  }

  vector<DocId> SearchGeneric(const AstNode& node, string_view active_field) {
    auto cb = [this, active_field](const auto& inner) { return Search(inner, active_field); };
    auto result = visit(cb, static_cast<const NodeVariants&>(node));
    DCHECK(is_sorted(result.begin(), result.end()));
    return result;
  }

  static vector<DocId> Search(FieldIndices* indices, const AstNode& query) {
    BasicSearch search{indices};
    return search.SearchGeneric(query, "");
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
      case Schema::TAG:
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

optional<BaseIndex*> FieldIndices::GetIndex(string_view field) {
  auto it = indices_.find(field);
  return it != indices_.end() ? make_optional(it->second.get()) : nullopt;
}

std::vector<TextIndex*> FieldIndices::GetAllTextIndices() {
  vector<TextIndex*> out;
  for (auto& [field, type] : schema_.fields) {
    if (type != Schema::TEXT)
      continue;
    auto index = GetIndex(field);
    DCHECK(index);
    auto* text_index = dynamic_cast<TextIndex*>(*index);
    DCHECK(text_index);
    out.push_back(text_index);
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
