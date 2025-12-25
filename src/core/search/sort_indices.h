// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/detail/stateless_allocator.h"
#include "core/search/base.h"

namespace dfly::search {

using StatelessString = std::basic_string<char, std::char_traits<char>, StatelessAllocator<char>>;
static_assert(sizeof(StatelessString) == sizeof(std::string));

template <typename T> using StatelessVector = std::vector<T, StatelessAllocator<T>>;
static_assert(sizeof(StatelessVector<StatelessString>) == sizeof(std::vector<std::string>));

template <typename T> struct SimpleValueSortIndex : BaseSortIndex {
 protected:
  struct ParsedSortValue {
    bool HasValue() const;
    bool IsNullValue() const;

    // std::monostate - no value was found.
    // std::nullopt - found value is null.
    // T - found value.
    std::variant<std::monostate, std::nullopt_t, T> value;
  };

 public:
  SortableValue Lookup(DocId doc) const override;
  std::vector<SortableValue> Sort(std::vector<DocId>* ids, size_t limit, bool desc) const override;

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

  // Override GetAllResults to return all documents with non-null values
  std::vector<DocId> GetAllDocsWithNonNullValues() const override;

 protected:
  virtual ParsedSortValue Get(const DocumentAccessor& doc, std::string_view field_value) = 0;

 private:
  StatelessVector<T> values_;
  StatelessVector<bool> occupied_;  // instead of optional<T> in values to avoid memory overhead
};

struct NumericSortIndex : SimpleValueSortIndex<double> {
  ParsedSortValue Get(const DocumentAccessor& doc, std::string_view field) override;
};

// TODO: Map tags to integers for fast sort
struct StringSortIndex : SimpleValueSortIndex<StatelessString> {
  ParsedSortValue Get(const DocumentAccessor& doc, std::string_view field) override;
};

}  // namespace dfly::search
