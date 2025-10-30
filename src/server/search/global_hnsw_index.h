// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "core/search/base.h"
#include "core/search/indices.h"
#include "core/search/search.h"
#include "server/search/doc_index.h"
#include "server/tx_base.h"

namespace dfly {
class GlobalHnswIndexRegistry {
 public:
  static GlobalHnswIndexRegistry& Instance();

  bool Create(std::string_view index_name, std::string_view field_name,
              const search::SchemaField::VectorParams& params);

  bool Remove(std::string_view index_name, std::string_view field_name);

  std::shared_ptr<search::HnswVectorIndex> Get(std::string_view index_name,
                                               std::string_view field_name) const;

  bool Exist(std::string_view index_name, std::string_view field_name) const;

  void Reset();

 private:
  GlobalHnswIndexRegistry() = default;
  std::string MakeKey(std::string_view index_name, std::string_view field_name) const;

  mutable std::shared_mutex registry_mutex_;
  absl::flat_hash_map<std::string, std::shared_ptr<search::HnswVectorIndex>> indices_;
};

}  // namespace dfly
