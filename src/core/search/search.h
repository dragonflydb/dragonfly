// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "core/search/base.h"

namespace dfly::search {

struct AstNode;
struct TextIndex;

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  virtual ~DocumentAccessor() = default;
  virtual std::string_view Get(std::string_view active_field) const = 0;
};

struct Schema {
  enum FieldType { TEXT, NUMERIC };

  std::unordered_map<std::string, FieldType> fields;
};

// Collection of indices for all fields in schema
struct FieldIndices {
  // Create indices based on schema
  FieldIndices(Schema schema);

  void Add(DocId doc, DocumentAccessor* access);

  BaseIndex* GetIndex(std::string_view field);
  std::vector<TextIndex*> GetAllTextIndices();
  std::vector<DocId> GetAllDocs() const;

 private:
  Schema schema_;
  std::vector<DocId> all_ids_;
  absl::flat_hash_map<std::string, std::unique_ptr<BaseIndex>> indices_;
};

// SearchAlgorithm allows searching field indices with a query
class SearchAlgorithm {
 public:
  SearchAlgorithm();
  ~SearchAlgorithm();

  // Init with query and return true if successful.
  bool Init(std::string_view query);

  std::vector<DocId> Search(FieldIndices* index) const;

 private:
  std::unique_ptr<AstNode> query_;
};

}  // namespace dfly::search
