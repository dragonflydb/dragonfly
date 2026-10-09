// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/macros.h>

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "core/search/base.h"
#include "core/search/tag_types.h"

namespace dfly {

namespace search {

struct AstNode;

// Matches all documents
struct AstStarNode {};

// Matches all documents where this field has a non-null value
struct AstStarFieldNode {};

template <TagType T> struct AstAffixNode {
  explicit AstAffixNode(std::string affix) : affix{std::move(affix)} {
  }

  std::string affix;
};

using AstTermNode = AstAffixNode<TagType::REGULAR>;
using AstPrefixNode = AstAffixNode<TagType::PREFIX>;
using AstSuffixNode = AstAffixNode<TagType::SUFFIX>;
using AstInfixNode = AstAffixNode<TagType::INFIX>;

// Glob pattern from `w'...'` syntax. `affix` holds the verbatim pattern: `*` matches any run of
// characters, `?` matches exactly one, `\` escapes the next character to a literal.
using AstWildcardNode = AstAffixNode<TagType::WILDCARD>;

// Quoted multi-word phrase. `raw` is the verbatim content between quotes; the
// executor runs the shared text tokenizer over it and matches the resulting
// tokens against posting-list positions.
// `slop` = max intervening tokens allowed between consecutive phrase terms
// (in order). slop=0 = exact adjacency; slop>0 comes from `"..."~N` syntax.
struct AstPhraseNode {
  explicit AstPhraseNode(std::string raw, uint32_t slop = 0) : raw{std::move(raw)}, slop{slop} {
  }

  std::string raw;
  uint32_t slop = 0;
};

// Matches numeric range
struct AstRangeNode {
  AstRangeNode(double lo, bool lo_excl, double hi, bool hi_excl);

  double lo, hi;
};

struct AstGeoNode {
  AstGeoNode(double lon, double lat, double radius, std::string unit);
  double lon, lat;
  double radius;
  std::string unit;
};

// ~subquery: returns all docs, boosts score of those matched by subquery
struct AstOptionalNode {
  explicit AstOptionalNode(AstExpr node) : node{std::move(node)} {
  }

  AstExpr node;
};

// Negates subtree
struct AstNegateNode {
  explicit AstNegateNode(AstExpr node) : node{std::move(node)} {
  }

  AstExpr node;
};

// Applies query attributes to a subtree.
struct AstAttributeNode {
  AstAttributeNode(AstExpr node, double weight) : node{std::move(node)}, weight{weight} {
  }

  AstExpr node;
  double weight = 1.0;
};

// Applies logical operation to results of all sub-nodes
struct AstLogicalNode {
  enum LogicOp { AND, OR };

  AstLogicalNode(AstExpr l, AstExpr r, LogicOp op);

  // Reuses either operand if it already has the same logical operation.
  static AstExpr Combine(AstExpr l, AstExpr r, LogicOp op);

  LogicOp op;
  std::vector<AstExpr> nodes;
};

// Selects specific field for subtree
struct AstFieldNode {
  AstFieldNode(std::string field, AstExpr node);

  std::string field;
  AstExpr node;
};

// Stores a list of tags for a tag query
struct AstTagsNode {
  struct TagValue {
    TagType type = TagType::REGULAR;
    std::string affix;

    friend std::ostream& operator<<(std::ostream& os, const TagValue&) {
      return os;  // Required by bison debug traces.
    }
  };

  explicit AstTagsNode(TagValue tag) {
    tags.push_back(std::move(tag));
  }

  std::vector<TagValue> tags;
};

// Applies nearest neighbor search to the final result set
struct AstKnnNode {
  AstKnnNode() = default;
  AstKnnNode(uint32_t limit, std::string_view field, std::string blob, std::string_view score_alias,
             std::optional<uint32_t> ef_runtime);

  AstKnnNode(const AstKnnNode&) = delete;
  AstKnnNode& operator=(const AstKnnNode&) = delete;

  AstKnnNode(AstKnnNode&&) noexcept = default;
  AstKnnNode& operator=(AstKnnNode&&) noexcept = default;

  friend std::ostream& operator<<(std::ostream& stream, const AstKnnNode& matrix) {
    return stream;
  }

  AstExpr filter;
  size_t limit;
  std::string field;
  std::string blob;  // raw query-vector bytes, decoded at search time using the field dtype
  std::string score_alias;
  std::optional<uint32_t> ef_runtime;

  bool HasPreFilter() const;
};

// Applies vector range search: returns all docs with distance(vec, doc_vec) <= radius
struct AstVectorRangeNode {
  AstVectorRangeNode() = default;
  AstVectorRangeNode(std::string field, double radius, std::string blob, std::string score_alias,
                     std::optional<double> epsilon);

  AstVectorRangeNode(const AstVectorRangeNode&) = delete;
  AstVectorRangeNode& operator=(const AstVectorRangeNode&) = delete;

  AstVectorRangeNode(AstVectorRangeNode&&) noexcept = default;
  AstVectorRangeNode& operator=(AstVectorRangeNode&&) noexcept = default;

  friend std::ostream& operator<<(std::ostream& stream, const AstVectorRangeNode& /*node*/) {
    return stream;
  }

  std::string field;
  double radius;
  std::string blob;  // raw query-vector bytes, decoded at search time using the field dtype
  std::string score_alias;
  std::optional<double> epsilon;
};

using NodeVariants =
    std::variant<std::monostate, AstStarNode, AstStarFieldNode, AstTermNode, AstPrefixNode,
                 AstSuffixNode, AstInfixNode, AstWildcardNode, AstPhraseNode, AstRangeNode,
                 AstNegateNode, AstOptionalNode, AstAttributeNode, AstLogicalNode, AstFieldNode,
                 AstTagsNode, AstKnnNode, AstGeoNode, AstVectorRangeNode>;

struct AstNode : public NodeVariants {
  using variant::variant;

  AstNode(const AstNode&) = delete;
  AstNode& operator=(const AstNode&) = delete;

  AstNode(AstNode&&) noexcept = default;
  AstNode& operator=(AstNode&&) noexcept = default;

  // Iterative, allocation-free teardown: deep queries must be safe to destroy even on OOM.
  ~AstNode() noexcept;

  friend std::ostream& operator<<(std::ostream& stream, const AstNode& matrix) {
    return stream;
  }

  template <typename Node> bool Is() const {
    return std::holds_alternative<Node>(*this);
  }

  template <typename Node> Node* As() {
    return std::get_if<Node>(this);
  }

  template <typename Node> const Node* As() const {
    return std::get_if<Node>(this);
  }

  const NodeVariants& Variant() const& {
    return *this;
  }

 private:
  // Only used during destruction to link the traversal and cleanup lists.
  AstNode* teardown_next_ = nullptr;
};

// Builds the variant wrapper while callers transfer ownership through AstExpr.
template <typename Node, typename... Args> AstExpr MakeAstNode(Args&&... args) {
  return std::make_unique<AstNode>(std::in_place_type<Node>, std::forward<Args>(args)...);
}

template <typename Callback> void VisitAst(const AstNode& node, Callback&& callback) {
  std::visit([&](const auto& inner) { callback(inner); }, node.Variant());
}

// Invokes cb(child, field) for each direct child. Field nodes override the inherited field.
// Children transferred out of the tree are skipped.
template <typename F>
void ForEachChild(const AstNode& node, std::string_view active_field, F&& cb) {
  VisitAst(node, [&](const auto& inner) {
    if constexpr (std::is_same_v<std::decay_t<decltype(inner)>, AstFieldNode>) {
      if (inner.node)
        cb(*inner.node, std::string_view{inner.field});
    } else if constexpr (requires { inner.node; }) {
      if (inner.node)
        cb(*inner.node, active_field);
    } else if constexpr (requires { inner.filter; }) {
      if (inner.filter)
        cb(*inner.filter, active_field);
    } else if constexpr (requires { inner.nodes; }) {
      for (const auto& child : inner.nodes) {
        if (child)
          cb(*child, active_field);
      }
    }
  });
}

}  // namespace search
}  // namespace dfly

namespace std {
ostream& operator<<(ostream& os, optional<uint32_t> o);
}  // namespace std
