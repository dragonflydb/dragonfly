// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <variant>
#include <vector>

#include "core/search/ast_expr.h"
#include "core/search/block_list.h"
#include "core/search/range_tree.h"

namespace dfly::search {

// Represents an either owned or non-owned result set that can be accessed and merged transparently.
class IndexResult {
 private:
  using DocVec = std::vector<DocId>;
  using Variant =
      std::variant<DocVec /*owned*/, const DocVec*, const BlockList<CompressedSortedSet>*,
                   const BlockList<SortedVector<DocId>>*, RangeResult>;

  template <typename... Ts> using VariantOfConstPtrs = std::variant<const Ts*...>;
  using BorrowedView =
      VariantOfConstPtrs<DocVec, BlockList<CompressedSortedSet>, BlockList<SortedVector<DocId>>,
                         SingleBlockRangeResult, TwoBlocksRangeResult>;

 public:
  IndexResult() = default;

  explicit IndexResult(Variant value);

  template <typename Container> explicit IndexResult(const Container* container = nullptr);

  /* It will return approximate size of the result set.
     Actual result can be smaller than the size returned by this method. */
  size_t ApproximateSize() const;

  BorrowedView Borrowed() const;

  // Move out of owned or copy borrowed
  DocVec Take();

 private:
  bool IsOwned() const;

  Variant value_;
};

std::vector<DocId> MergeIndexResults(const IndexResult& left, const IndexResult& right,
                                     AstLogicalNode::LogicOp op);

// Implementation
/******************************************************************/
inline IndexResult::IndexResult(Variant value) : value_{std::move(value)} {
}

template <typename Container>
IndexResult::IndexResult(const Container* container) : value_{container} {
  if (container == nullptr) {
    value_ = DocVec{};
  }
}

inline size_t IndexResult::ApproximateSize() const {
  return std::visit([](auto* set) { return set->size(); }, Borrowed());
}

inline IndexResult::BorrowedView IndexResult::Borrowed() const {
  auto cb = [](const auto& v) -> BorrowedView {
    using T = std::decay_t<decltype(v)>;
    if constexpr (std::is_pointer_v<std::remove_reference_t<decltype(v)>>) {
      return v;
    } else if constexpr (std::is_same_v<T, RangeResult>) {
      auto range_cb = [](const auto& set) -> BorrowedView { return &set; };
      return std::visit(range_cb, v.GetResult());
    } else {
      return &v;
    }
  };
  return std::visit(cb, value_);
}

inline IndexResult::DocVec IndexResult::Take() {
  if (IsOwned()) {
    return std::move(std::get<DocVec>(value_));
  }

  auto cb = [](auto* set) -> DocVec {
    DocVec out;
    out.reserve(set->size());
    for (auto it = set->begin(); it != set->end(); ++it) {
      out.push_back(*it);
    }
    return out;
  };
  return std::visit(cb, Borrowed());
}

inline bool IndexResult::IsOwned() const {
  return std::holds_alternative<DocVec>(value_);
}

namespace details {
using BackInserter = std::back_insert_iterator<std::vector<DocId>>;

// First - MergeableIterator, Second - non-MergeableIterator
template <typename FirstIterator, typename SecondIterator,
          typename = std::enable_if_t<std::is_base_of<MergeableIterator, FirstIterator>::value>>
std::enable_if_t<std::is_base_of<MergeableIterator, FirstIterator>::value &&
                     !std::is_base_of<MergeableIterator, SecondIterator>::value,
                 void>
SetIntersection(FirstIterator first_begin, FirstIterator first_end, SecondIterator second_begin,
                SecondIterator second_end, BackInserter out) {
  auto l_it = first_begin;
  auto r_it = second_begin;
  while (l_it != first_end && r_it != second_end) {
    DocId l_value = *l_it;
    DocId r_value = *r_it;

    if (l_value == r_value) {
      *out++ = l_value;
      ++r_it;
      if (r_it != second_end) {
        l_it.SeakGE(*r_it);  // Move to the next value in the first iterator
      }
    } else if (l_value < r_value) {
      l_it.SeakGE(r_value);  // Move to the next value in the first iterator
    } else {
      DCHECK(l_value > r_value);
      while (r_it != second_end && *r_it < l_value) {
        ++r_it;  // Move to the next value in the second iterator
      }
    }
  }
}

// First - non-MergeableIterator, Second - MergeableIterator
template <typename FirstIterator, typename SecondIterator,
          typename = std::enable_if_t<std::is_base_of<MergeableIterator, SecondIterator>::value>>
std::enable_if_t<!std::is_base_of<MergeableIterator, FirstIterator>::value &&
                     std::is_base_of<MergeableIterator, SecondIterator>::value,
                 void>
SetIntersection(FirstIterator first_begin, FirstIterator first_end, SecondIterator second_begin,
                SecondIterator second_end, BackInserter out) {
  SetIntersection(second_begin, second_end, first_begin, first_end, out);
}

// Specialization for both iterators being MergeableIterator
template <typename FirstIterator, typename SecondIterator,
          typename = std::enable_if_t<std::is_base_of<MergeableIterator, FirstIterator>::value &&
                                      std::is_base_of<MergeableIterator, SecondIterator>::value>>
std::enable_if_t<std::is_base_of<MergeableIterator, FirstIterator>::value &&
                     std::is_base_of<MergeableIterator, SecondIterator>::value,
                 void>
SetIntersection(FirstIterator first_begin, FirstIterator first_end, SecondIterator second_begin,
                SecondIterator second_end, BackInserter out) {
  auto l_it = first_begin;
  auto r_it = second_begin;
  while (l_it != first_end && r_it != second_end) {
    DocId l_value = *l_it;
    DocId r_value = *r_it;

    if (l_value == r_value) {
      *out++ = l_value;
      ++l_it;
      ++r_it;
    } else if (l_value < r_value) {
      l_it.SeakGE(r_value);  // Move to the next value in the first iterator
    } else {
      DCHECK(l_value > r_value);
      r_it.SeakGE(l_value);  // Move to the next value in the second iterator
    }
  }
}

// Default case for iterators that are not MergeableIterator
template <typename FirstIterator, typename SecondIterator>
std::enable_if_t<!std::is_base_of<MergeableIterator, FirstIterator>::value &&
                     !std::is_base_of<MergeableIterator, SecondIterator>::value,
                 void>
SetIntersection(FirstIterator first_begin, FirstIterator first_end, SecondIterator second_begin,
                SecondIterator second_end, BackInserter out) {
  std::set_intersection(first_begin, first_end, second_begin, second_end, out);
}

}  // namespace details

inline std::vector<DocId> MergeIndexResults(const IndexResult& left, const IndexResult& right,
                                            AstLogicalNode::LogicOp op) {
  std::vector<DocId> result;

  if (op == AstLogicalNode::LogicOp::AND) {
    result.reserve(std::min(left.ApproximateSize(), right.ApproximateSize()));
    auto cb = [&result](auto* s1, auto* s2) {
      details::SetIntersection(s1->begin(), s1->end(), s2->begin(), s2->end(),
                               std::back_inserter(result));
    };
    std::visit(cb, left.Borrowed(), right.Borrowed());
  } else {
    result.reserve(std::max(left.ApproximateSize(), right.ApproximateSize()));
    auto cb = [&result](auto* s1, auto* s2) {
      std::set_union(s1->begin(), s1->end(), s2->begin(), s2->end(), std::back_inserter(result));
    };
    std::visit(cb, left.Borrowed(), right.Borrowed());
  }

  return result;
}

}  // namespace dfly::search
