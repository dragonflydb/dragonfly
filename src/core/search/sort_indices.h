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
#include "core/search/base.h"
#include "core/search/compressed_sorted_set.h"

namespace dfly::search {

template <typename T> struct SimpleValueSortIndex : BaseSortIndex {
  void Sort(std::vector<DocId>* entries, std::vector<ResultScore>* out, size_t limit,
            bool desc) const override;

  virtual void Add(DocId id, DocumentAccessor* doc, std::string_view field);
  virtual void Remove(DocId id, DocumentAccessor* doc, std::string_view field);

 protected:
  virtual T Get(DocId id, DocumentAccessor* doc, std::string_view field) = 0;

 private:
  std::vector<T> values_;
};

struct NumericSortIndex : public SimpleValueSortIndex<int64_t> {
  int64_t Get(DocId id, DocumentAccessor* doc, std::string_view field) override;
};

// TODO: Map tags to integers for fast sort
struct StringSortIndex : public SimpleValueSortIndex<std::string> {
  std::string Get(DocId id, DocumentAccessor* doc, std::string_view field) override;
};

}  // namespace dfly::search
