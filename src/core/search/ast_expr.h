// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <ostream>
#include <variant>
#include <vector>

#include "core/search/base.h"

namespace dfly {

namespace search {

struct AstNode;

// Matches all documents
struct AstStarNode {};

// Matches terms in text fields
struct AstTermNode {
  explicit AstTermNode(std::string term);

  std::string term;
};

struct AstPrefixNode {
  explicit AstPrefixNode(std::string prefix);

  std::string prefix;
};

// Matches numeric range
struct AstRangeNode {
  AstRangeNode(double lo, bool lo_excl, double hi, bool hi_excl);

  double lo, hi;
};

// Negates subtree
struct AstNegateNode {
  AstNegateNode(AstNode&& node);

  std::unique_ptr<AstNode> node;
};

// Applies logical operation to results of all sub-nodes
struct AstLogicalNode {
  enum LogicOp { AND, OR };

  // If either node is already a logical node with the same op, it'll be re-used.
  AstLogicalNode(AstNode&& l, AstNode&& r, LogicOp op);

  LogicOp op;
  std::vector<AstNode> nodes;
};

// Selects specific field for subtree
struct AstFieldNode {
  AstFieldNode(std::string field, AstNode&& node);

  std::string field;
  std::unique_ptr<AstNode> node;
};

// Stores a list of tags for a tag query
struct AstTagsNode {
  using TagValue = std::variant<AstTermNode, AstPrefixNode>;

  struct TagValueProxy
      : public AstTagsNode::TagValue {  // bison needs it to be default constructible
    TagValueProxy() : AstTagsNode::TagValue(AstTermNode("")) {
    }
    TagValueProxy(AstPrefixNode tv) : AstTagsNode::TagValue(std::move(tv)) {
    }
    TagValueProxy(AstTermNode tv) : AstTagsNode::TagValue(std::move(tv)) {
    }
  };

  AstTagsNode(TagValue);
  AstTagsNode(AstNode&& l, TagValue);

  std::vector<TagValue> tags;
};

// Applies nearest neighbor search to the final result set
struct AstKnnNode {
  AstKnnNode() = default;
  AstKnnNode(uint32_t limit, std::string_view field, OwnedFtVector vec,
             std::string_view score_alias, std::optional<size_t> ef_runtime);

  AstKnnNode(AstNode&& sub, AstKnnNode&& self);

  friend std::ostream& operator<<(std::ostream& stream, const AstKnnNode& matrix) {
    return stream;
  }

  std::unique_ptr<AstNode> filter;
  size_t limit;
  std::string field;
  OwnedFtVector vec;
  std::string score_alias;
  std::optional<float> ef_runtime;
};

struct AstSortNode {
  std::unique_ptr<AstNode> filter;
  std::string field;
  bool descending = false;
};

using NodeVariants =
    std::variant<std::monostate, AstStarNode, AstTermNode, AstPrefixNode, AstRangeNode,
                 AstNegateNode, AstLogicalNode, AstFieldNode, AstTagsNode, AstKnnNode, AstSortNode>;

struct AstNode : public NodeVariants {
  using variant::variant;

  friend std::ostream& operator<<(std::ostream& stream, const AstNode& matrix) {
    return stream;
  }

  const NodeVariants& Variant() const& {
    return *this;
  }
};

using AstExpr = AstNode;

}  // namespace search
}  // namespace dfly

namespace std {
ostream& operator<<(ostream& os, optional<size_t> o);
ostream& operator<<(ostream& os, dfly::search::AstTagsNode::TagValueProxy o);
}  // namespace std
