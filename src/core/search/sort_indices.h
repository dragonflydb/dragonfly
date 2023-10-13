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

template <typename T> struct SimpleValueSortIndex : BaseSortIndex {
  SimpleValueSortIndex(PMR_NS::memory_resource* mr);

  std::vector<ResultScore> Sort(std::vector<DocId>* ids, size_t limit, bool desc) const override;

  virtual void Add(DocId id, DocumentAccessor* doc, std::string_view field);
  virtual void Remove(DocId id, DocumentAccessor* doc, std::string_view field);

 protected:
  virtual T Get(DocId id, DocumentAccessor* doc, std::string_view field) = 0;

  PMR_NS::memory_resource* GetMemRes() const;

 private:
  PMR_NS::vector<T> values_;
};

struct NumericSortIndex : public SimpleValueSortIndex<double> {
  NumericSortIndex(PMR_NS::memory_resource* mr) : SimpleValueSortIndex{mr} {};

  double Get(DocId id, DocumentAccessor* doc, std::string_view field) override;
};

// TODO: Map tags to integers for fast sort
struct StringSortIndex : public SimpleValueSortIndex<PMR_NS::string> {
  StringSortIndex(PMR_NS::memory_resource* mr) : SimpleValueSortIndex{mr} {};

  PMR_NS::string Get(DocId id, DocumentAccessor* doc, std::string_view field) override;
};

}  // namespace dfly::search
