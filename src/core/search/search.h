// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <stddef.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/search/base.h"

namespace dfly::search {

struct AstNode;
struct TextIndex;

struct Schema {
  enum FieldType { TAG, TEXT, NUMERIC, VECTOR };

  absl::flat_hash_map<std::string, FieldType> fields;
};

// Collection of indices for all fields in schema
class FieldIndices {
 public:
  // Create indices based on schema
  FieldIndices(Schema schema);

  void Add(DocId doc, DocumentAccessor* access);
  void Remove(DocId doc, DocumentAccessor* access);

  BaseIndex* GetIndex(std::string_view field) const;
  std::vector<TextIndex*> GetAllTextIndices() const;
  const std::vector<DocId>& GetAllDocs() const;

 private:
  Schema schema_;
  std::vector<DocId> all_ids_;
  absl::flat_hash_map<std::string, std::unique_ptr<BaseIndex>> indices_;
};

// Represents a search result returned from the search algorithm.
struct SearchResult {
  std::vector<DocId> ids;

  // If a KNN-query is present, distances for doc ids are returned as well
  // and sorted from smallest to largest.
  std::vector<float> knn_distances;
};

// SearchAlgorithm allows searching field indices with a query
class SearchAlgorithm {
 public:
  SearchAlgorithm();
  ~SearchAlgorithm();

  // Init with query and return true if successful.
  bool Init(std::string_view query, const QueryParams& params);

  SearchResult Search(const FieldIndices* index) const;

  // Return KNN limit if it is enabled
  std::optional<size_t> HasKnn() const;

 private:
  std::unique_ptr<AstNode> query_;
};

}  // namespace dfly::search
