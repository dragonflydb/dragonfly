// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "base/logging.h"
#include "base/pmr/memory_resource.h"
#include "core/search/base.h"
#include "core/search/compressed_sorted_set.h"

namespace dfly::search {

template <typename T> struct SimpleValueSortIndex : public BaseSortIndex {
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
  SimpleValueSortIndex(PMR_NS::memory_resource* mr);

  SortableValue Lookup(DocId doc) const override;
  std::vector<ResultScore> Sort(std::vector<DocId>* ids, size_t limit, bool desc) const override;

  bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) override;
  void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) override;

 protected:
  virtual ParsedSortValue Get(const DocumentAccessor& doc, std::string_view field_value) = 0;

  PMR_NS::memory_resource* GetMemRes() const;

 private:
  PMR_NS::vector<T> values_;
  absl::flat_hash_set<DocId> null_values_;
};

struct NumericSortIndex : public SimpleValueSortIndex<double> {
  NumericSortIndex(PMR_NS::memory_resource* mr) : SimpleValueSortIndex{mr} {};

  ParsedSortValue Get(const DocumentAccessor& doc, std::string_view field) override;
};

// TODO: Map tags to integers for fast sort
struct StringSortIndex : public SimpleValueSortIndex<PMR_NS::string> {
  StringSortIndex(PMR_NS::memory_resource* mr) : SimpleValueSortIndex{mr} {};

  ParsedSortValue Get(const DocumentAccessor& doc, std::string_view field) override;
};

}  // namespace dfly::search
