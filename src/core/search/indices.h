// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

#include <map>
#include <optional>
#include <vector>

#include "core/search/base.h"

namespace dfly::search {

// Index for integer fields.
// Range bounds are queried in logarithmic time, iteration is constant.
struct NumericIndex : public BaseIndex {
  void Add(DocId doc, std::string_view value) override;
  void Remove(DocId doc, std::string_view value) override;

  std::vector<DocId> Range(int64_t l, int64_t r) const;

 private:
  absl::btree_set<std::pair<int64_t, DocId>> entries_;
};

// Base index for string based indices.
struct BaseStringIndex : public BaseIndex {
  // Pointer is valid as long as index is not mutated. Nullptr if not found
  const std::vector<DocId>* Matching(std::string_view str) const;

 protected:
  absl::flat_hash_map<std::string, std::vector<DocId>> entries_;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TextIndex : public BaseStringIndex {
  void Add(DocId doc, std::string_view value) override;
  void Remove(DocId doc, std::string_view value) override;
};

// Index for text fields.
// Hashmap based lookup per word.
struct TagIndex : public BaseStringIndex {
  void Add(DocId doc, std::string_view value) override;
  void Remove(DocId doc, std::string_view value) override;
};

}  // namespace dfly::search
