// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "base/pmr/memory_resource.h"
#include "core/string_map.h"

namespace dfly::search {

using DocId = uint32_t;
using GlobalDocId = uint64_t;
using ShardId = uint16_t;

inline GlobalDocId CreateGlobalDocId(ShardId shard_id, DocId local_doc_id) {
  return ((uint64_t)shard_id << 32) | local_doc_id;
}

inline std::pair<ShardId, DocId> DecomposeGlobalDocId(GlobalDocId id) {
  return {(id >> 32), (id)&0xFFFF};
}

enum class VectorSimilarity { L2, IP, COSINE };

using OwnedFtVector = std::pair<std::unique_ptr<float[]>, size_t /* dimension (size) */>;

// Query params represent named parameters for queries supplied via PARAMS.
struct QueryParams {
  std::string_view operator[](std::string_view name) const;
  std::string& operator[](std::string_view k);

  size_t Size() const {
    return params.size();
  }

 private:
  absl::flat_hash_map<std::string, std::string> params;
};

// Base class for optional search filters

struct AstNode;

struct OptionalFilterBase {
  virtual bool IsEmpty() const = 0;
  virtual AstNode Node(std::string field) = 0;
  virtual ~OptionalFilterBase() = default;
};

using OptionalFilters =
    absl::flat_hash_map<std::string /*field*/, std::unique_ptr<OptionalFilterBase> /* filter */>;

// Values are either sortable as doubles or strings, or not sortable at all.
using SortableValue = std::variant<std::monostate, double, std::string>;

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  using VectorInfo = search::OwnedFtVector;
  using StringList = absl::InlinedVector<std::string_view, 1>;
  using NumsList = absl::InlinedVector<double, 1>;

  virtual ~DocumentAccessor() = default;

  /* Returns nullopt if the specified field is not a list of strings */
  virtual std::optional<StringList> GetStrings(std::string_view active_field) const = 0;

  /* Returns nullopt if the specified field is not a vector */
  virtual std::optional<VectorInfo> GetVector(std::string_view active_field) const = 0;

  /* Return nullopt if the specified field is not a list of doubles */
  virtual std::optional<NumsList> GetNumbers(std::string_view active_field) const = 0;

  /* Same as GetStrings, but also supports boolean values */
  virtual std::optional<StringList> GetTags(std::string_view active_field) const = 0;
};

// Base class for type-specific indices.
//
// Queries should be done directly on subclasses with their distinc
// query functions. All results for all index types should be sorted.
struct BaseIndex {
  virtual ~BaseIndex() = default;

  // Returns true if the document was added / indexed
  virtual bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) = 0;
  virtual void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) = 0;

  // Returns documents that have non-null values for this field (used for @field:* queries)
  // Result must be sorted
  virtual std::vector<DocId> GetAllDocsWithNonNullValues() const = 0;

  /* Called at the end of indexes rebuilding after all initial Add calls are done.
     Some indices may need to finalize internal structures. See RangeTree for example. */
  virtual void FinalizeInitialization() {
  }
};

// Base class for type-specific sorting indices.
struct BaseSortIndex : BaseIndex {
  virtual SortableValue Lookup(DocId doc) const = 0;
  virtual std::vector<SortableValue> Sort(std::vector<DocId>* ids, size_t limit,
                                          bool desc) const = 0;
};

/* Used in iterators of inverse indices.
   It is used to mark iterators that can be seeked to doc id that is greater than or equal to
   the specified value (method name is SeekGE(DocId min_doc_id)).
   This is used to optimize merging of results from different indices.
   See index_result.h for more details. */
struct SeekableTag {};

template <typename Iterator> void BasicSeekGE(DocId min_doc_id, const Iterator& end, Iterator* it);

/* Used for converting field values to double. Returns std::nullopt if the conversion fails */
std::optional<double> ParseNumericField(std::string_view value);

/* Temporary method to create an empty std::optional<InlinedVector> in DocumentAccessor::GetString
   and DocumentAccessor::GetNumbers methods. The problem is that due to internal implementation
   details of absl::InlineVector, we are getting a -Wmaybe-uninitialized compiler warning. To
   suppress this false warning, we temporarily disable it around this block of code using GCC
   diagnostic directives. */
template <typename InlinedVector> std::optional<InlinedVector> EmptyAccessResult() {
#if !defined(__clang__)
  // GCC 13.1 throws spurious warnings around this code.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

  return InlinedVector{};

#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
}

// Implementation
/******************************************************************/
namespace details {
inline size_t GetHighestPowerOfTwo(size_t n) {
  static constexpr size_t kBitsNumber = sizeof(size_t) * 8;
  return size_t(1) << (kBitsNumber - 1 - __builtin_clzl(n));
}
}  // namespace details

template <typename Iterator> void BasicSeekGE(DocId min_doc_id, const Iterator& end, Iterator* it) {
  using Category = typename std::iterator_traits<Iterator>::iterator_category;

  auto extract_doc_id = [](const auto& value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, DocId>) {
      return value;
    } else {
      return value.first;
    }
  };

  if constexpr (std::is_base_of_v<std::random_access_iterator_tag, Category>) {
    size_t length = std::distance(*it, end);
    for (size_t step = details::GetHighestPowerOfTwo(length); step > 0; step >>= 1) {
      if (step < length) {
        auto next_it = *it + step;
        if (extract_doc_id(*next_it) < min_doc_id) {
          *it = next_it;
          length -= step;
        }
      }
    }
  }

  while (*it != end && extract_doc_id(**it) < min_doc_id) {
    ++(*it);
  }
}

}  // namespace dfly::search
