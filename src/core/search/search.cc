// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/search.h"

#include <absl/strings/numbers.h>

#include "base/logging.h"
#include "core/search/ast_expr.h"
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
  bool Check(monostate, string_view) {
    return false;
  }

  bool Check(const AstTermNode& node, string_view active_field) {
    auto cb = [&node](string_view str) {
      return regex_search(str.begin(), str.begin() + str.size(), node.pattern);
    };
    return doc->Check(cb, active_field);
  }

  bool Check(const AstRangeNode& node, string_view active_field) {
    auto cb = [&node](string_view str) {
      int64_t v;
      if (!absl::SimpleAtoi(str, &v))
        return false;
      return node.lo <= v && v <= node.hi;
    };
    return doc->Check(cb, active_field);
  }

  bool Check(const AstNegateNode& node, string_view active_field) {
    return !Check(*node.node, active_field);
  }

  bool Check(const AstLogicalNode& node, string_view active_field) {
    auto pred = [this, active_field](const AstNode& sub) { return Check(sub, active_field); };
    return node.op == AstLogicalNode::AND ? all_of(node.nodes.begin(), node.nodes.end(), pred)
                                          : any_of(node.nodes.begin(), node.nodes.end(), pred);
  }

  bool Check(const AstFieldNode& node, string_view active_field) {
    DCHECK(active_field.empty());
    return Check(*node.node, node.field);
  }

  bool Check(const AstNode& node, string_view active_field) {
    auto cb = [this, active_field](const auto& inner) { return Check(inner, active_field); };
    return visit(cb, (const NodeVariants&)node);
  }

  static bool Check(DocumentAccessor* doc, const AstNode& query) {
    BasicSearch search{doc};
    return search.Check(query, "");
  }

  DocumentAccessor* doc;
};

};  // namespace

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

bool SearchAlgorithm::Check(DocumentAccessor* doc) const {
  return BasicSearch::Check(doc, *query_);
}

}  // namespace dfly::search
