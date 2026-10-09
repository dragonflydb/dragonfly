// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/macros.h>

#include <iosfwd>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/search/base.h"
#include "core/search/tag_types.h"

namespace dfly::search {

template <TagType T> struct AstAffixNode;
using AstTermNode = AstAffixNode<TagType::REGULAR>;
using AstPrefixNode = AstAffixNode<TagType::PREFIX>;
using AstSuffixNode = AstAffixNode<TagType::SUFFIX>;
using AstInfixNode = AstAffixNode<TagType::INFIX>;

// Glob pattern from `w'...'` syntax. `affix` holds the verbatim pattern: `*` matches any run of
// characters, `?` matches exactly one, `\` escapes the next character to a literal.
using AstWildcardNode = AstAffixNode<TagType::WILDCARD>;

struct AstStarNode;
struct AstStarFieldNode;
struct AstPhraseNode;
struct AstRangeNode;
struct AstGeoNode;
struct AstNegateNode;
struct AstOptionalNode;
struct AstAttributeNode;
struct AstLogicalNode;
struct AstFieldNode;
struct AstTagsNode;
struct AstKnnNode;
struct AstVectorRangeNode;

// A visitor handles each concrete node; AstNode::Visit dispatches through the node's vtable.
struct AstVisitor {
  virtual ~AstVisitor() = default;

  virtual void Visit(const AstStarNode& node) = 0;
  virtual void Visit(const AstStarFieldNode& node) = 0;
  virtual void Visit(const AstTermNode& node) = 0;
  virtual void Visit(const AstPrefixNode& node) = 0;
  virtual void Visit(const AstSuffixNode& node) = 0;
  virtual void Visit(const AstInfixNode& node) = 0;
  virtual void Visit(const AstWildcardNode& node) = 0;
  virtual void Visit(const AstPhraseNode& node) = 0;
  virtual void Visit(const AstRangeNode& node) = 0;
  virtual void Visit(const AstGeoNode& node) = 0;
  virtual void Visit(const AstNegateNode& node) = 0;
  virtual void Visit(const AstOptionalNode& node) = 0;
  virtual void Visit(const AstAttributeNode& node) = 0;
  virtual void Visit(const AstLogicalNode& node) = 0;
  virtual void Visit(const AstFieldNode& node) = 0;
  virtual void Visit(const AstTagsNode& node) = 0;
  virtual void Visit(const AstKnnNode& node) = 0;
  virtual void Visit(const AstVectorRangeNode& node) = 0;
};

// Nodes own their children through AstExpr. A null expression denotes an empty/failed parse.
struct AstNode {
  enum class Type {
    STAR,
    STAR_FIELD,
    TERM,
    PREFIX,
    SUFFIX,
    INFIX,
    WILDCARD,
    PHRASE,
    RANGE,
    GEO,
    NEGATE,
    OPTIONAL,
    ATTRIBUTE,
    LOGICAL,
    FIELD,
    TAGS,
    KNN,
    VECTOR_RANGE,
  };

  virtual ~AstNode() = default;
  AstNode(const AstNode&) = delete;
  AstNode& operator=(const AstNode&) = delete;

  template <typename Node> bool Is() const {
    return type_ == Node::kType;
  }

  template <typename Node> Node* As() {
    return Is<Node>() ? static_cast<Node*>(this) : nullptr;
  }

  template <typename Node> const Node* As() const {
    return Is<Node>() ? static_cast<const Node*>(this) : nullptr;
  }

  virtual void Visit(AstVisitor& visitor) const = 0;

 protected:
  explicit AstNode(Type type) : type_{type} {
  }

  static void EnqueueChild(AstExpr& child, AstNode*& pending) noexcept {
    if (auto* node = child.release()) {
      node->teardown_next_ = pending;
      pending = node;
    }
  }

 private:
  friend struct AstNodeDeleter;
  virtual void ReleaseChildren(AstNode*& pending) noexcept = 0;

  const Type type_;
  AstNode* teardown_next_ = nullptr;
};

template <typename Derived, AstNode::Type type> struct TypedAstNode : AstNode {
  static constexpr Type kType = type;

  void Visit(AstVisitor& visitor) const final {
    visitor.Visit(static_cast<const Derived&>(*this));
  }

 protected:
  TypedAstNode() : AstNode(type) {
  }

 private:
  void ReleaseChildren(AstNode*& pending) noexcept final {
    auto& self = static_cast<Derived&>(*this);
    if constexpr (requires { self.node; }) {
      EnqueueChild(self.node, pending);
    } else if constexpr (requires { self.filter; }) {
      EnqueueChild(self.filter, pending);
    } else if constexpr (requires { self.nodes; }) {
      for (auto& child : self.nodes)
        EnqueueChild(child, pending);
    }
  }
};

// Adapts a callback to the virtual visitor without allocating.
template <typename Callback> struct AstVisitorAdapter final : AstVisitor {
  explicit AstVisitorAdapter(Callback& callback) : callback_{callback} {
  }

  void Visit(const AstStarNode& node) override {
    callback_(node);
  }
  void Visit(const AstStarFieldNode& node) override {
    callback_(node);
  }
  void Visit(const AstTermNode& node) override {
    callback_(node);
  }
  void Visit(const AstPrefixNode& node) override {
    callback_(node);
  }
  void Visit(const AstSuffixNode& node) override {
    callback_(node);
  }
  void Visit(const AstInfixNode& node) override {
    callback_(node);
  }
  void Visit(const AstWildcardNode& node) override {
    callback_(node);
  }
  void Visit(const AstPhraseNode& node) override {
    callback_(node);
  }
  void Visit(const AstRangeNode& node) override {
    callback_(node);
  }
  void Visit(const AstGeoNode& node) override {
    callback_(node);
  }
  void Visit(const AstNegateNode& node) override {
    callback_(node);
  }
  void Visit(const AstOptionalNode& node) override {
    callback_(node);
  }
  void Visit(const AstAttributeNode& node) override {
    callback_(node);
  }
  void Visit(const AstLogicalNode& node) override {
    callback_(node);
  }
  void Visit(const AstFieldNode& node) override {
    callback_(node);
  }
  void Visit(const AstTagsNode& node) override {
    callback_(node);
  }
  void Visit(const AstKnnNode& node) override {
    callback_(node);
  }
  void Visit(const AstVectorRangeNode& node) override {
    callback_(node);
  }

 private:
  Callback& callback_;
};

template <typename Callback> void VisitAst(const AstNode& node, Callback&& callback) {
  AstVisitorAdapter visitor{callback};
  node.Visit(visitor);
}

// Matches all documents
struct AstStarNode final : TypedAstNode<AstStarNode, AstNode::Type::STAR> {};

// Matches all documents where this field has a non-null value
struct AstStarFieldNode final : TypedAstNode<AstStarFieldNode, AstNode::Type::STAR_FIELD> {};

constexpr AstNode::Type AstAffixType(TagType type) {
  switch (type) {
    case TagType::REGULAR:
      return AstNode::Type::TERM;
    case TagType::PREFIX:
      return AstNode::Type::PREFIX;
    case TagType::SUFFIX:
      return AstNode::Type::SUFFIX;
    case TagType::INFIX:
      return AstNode::Type::INFIX;
    case TagType::WILDCARD:
      return AstNode::Type::WILDCARD;
  }
  ABSL_UNREACHABLE();
}

template <TagType T> struct AstAffixNode final : TypedAstNode<AstAffixNode<T>, AstAffixType(T)> {
  explicit AstAffixNode(std::string affix) : affix{std::move(affix)} {
  }

  std::string affix;
};

// Quoted multi-word phrase. `raw` is the verbatim content between quotes; the
// executor runs the shared text tokenizer over it and matches the resulting
// tokens against posting-list positions.
// `slop` = max intervening tokens allowed between consecutive phrase terms
// (in order). slop=0 = exact adjacency; slop>0 comes from `"..."~N` syntax.
struct AstPhraseNode final : TypedAstNode<AstPhraseNode, AstNode::Type::PHRASE> {
  explicit AstPhraseNode(std::string raw, uint32_t slop = 0) : raw{std::move(raw)}, slop{slop} {
  }

  std::string raw;
  uint32_t slop = 0;
};

// Matches numeric range
struct AstRangeNode final : TypedAstNode<AstRangeNode, AstNode::Type::RANGE> {
  AstRangeNode(double lo, bool lo_excl, double hi, bool hi_excl);

  double lo, hi;
};

struct AstGeoNode final : TypedAstNode<AstGeoNode, AstNode::Type::GEO> {
  AstGeoNode(double lon, double lat, double radius, std::string unit);
  double lon, lat;
  double radius;
  std::string unit;
};

// ~subquery: returns all docs, boosts score of those matched by subquery
struct AstOptionalNode final : TypedAstNode<AstOptionalNode, AstNode::Type::OPTIONAL> {
  explicit AstOptionalNode(AstExpr node) : node{std::move(node)} {
  }

  AstExpr node;
};

// Negates subtree
struct AstNegateNode final : TypedAstNode<AstNegateNode, AstNode::Type::NEGATE> {
  explicit AstNegateNode(AstExpr node) : node{std::move(node)} {
  }

  AstExpr node;
};

// Applies query attributes to a subtree.
struct AstAttributeNode final : TypedAstNode<AstAttributeNode, AstNode::Type::ATTRIBUTE> {
  AstAttributeNode(AstExpr node, double weight) : node{std::move(node)}, weight{weight} {
  }

  AstExpr node;
  double weight = 1.0;
};

// Applies logical operation to results of all sub-nodes
struct AstLogicalNode final : TypedAstNode<AstLogicalNode, AstNode::Type::LOGICAL> {
  enum LogicOp { AND, OR };

  AstLogicalNode(AstExpr l, AstExpr r, LogicOp op);

  // Reuses either operand if it already has the same logical operation.
  static AstExpr Combine(AstExpr l, AstExpr r, LogicOp op);

  LogicOp op;
  std::vector<AstExpr> nodes;
};

// Selects specific field for subtree
struct AstFieldNode final : TypedAstNode<AstFieldNode, AstNode::Type::FIELD> {
  AstFieldNode(std::string field, AstExpr node);

  std::string field;
  AstExpr node;
};

// Stores a list of tags for a tag query
struct AstTagsNode final : TypedAstNode<AstTagsNode, AstNode::Type::TAGS> {
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
struct AstKnnNode final : TypedAstNode<AstKnnNode, AstNode::Type::KNN> {
  AstKnnNode(uint32_t limit, std::string_view field, std::string blob, std::string_view score_alias,
             std::optional<uint32_t> ef_runtime);

  AstExpr filter;
  size_t limit;
  std::string field;
  std::string blob;  // raw query-vector bytes, decoded at search time using the field dtype
  std::string score_alias;
  std::optional<uint32_t> ef_runtime;

  bool HasPreFilter() const;
};

// Applies vector range search: returns all docs with distance(vec, doc_vec) <= radius
struct AstVectorRangeNode final : TypedAstNode<AstVectorRangeNode, AstNode::Type::VECTOR_RANGE> {
  AstVectorRangeNode(std::string field, double radius, std::string blob, std::string score_alias,
                     std::optional<double> epsilon);

  std::string field;
  double radius;
  std::string blob;  // raw query-vector bytes, decoded at search time using the field dtype
  std::string score_alias;
  std::optional<double> epsilon;
};

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

}  // namespace dfly::search

namespace std {
ostream& operator<<(ostream& os, optional<uint32_t> o);
}  // namespace std
