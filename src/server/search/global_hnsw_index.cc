// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/global_hnsw_index.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/search/ast_expr.h"
#include "core/search/base.h"
#include "core/search/index_result.h"
#include "core/search/indices.h"
#include "core/search/vector_utils.h"
#include "server/engine_shard.h"
#include "server/engine_shard_set.h"
#include "server/search/doc_accessors.h"
#include "server/search/doc_index.h"
#include "server/transaction.h"
#include "server/tx_base.h"

namespace dfly {

// Global index registry implementation

GlobalHnswIndexRegistry& GlobalHnswIndexRegistry::Instance() {
  static GlobalHnswIndexRegistry instance;
  return instance;
}

bool GlobalHnswIndexRegistry::Create(std::string_view index_name, std::string_view field_name,
                                     const search::SchemaField::VectorParams& params) {
  std::string key = MakeKey(index_name, field_name);

  std::unique_lock<std::shared_mutex> lock(registry_mutex_);

  auto it = indices_.find(key);

  if (it != indices_.end())
    return false;

  indices_[key] = std::make_shared<search::HnswVectorIndex>(params);

  return true;
}

bool GlobalHnswIndexRegistry::Remove(std::string_view index_name, std::string_view field_name) {
  std::string key = MakeKey(index_name, field_name);

  std::unique_lock<std::shared_mutex> lock(registry_mutex_);

  auto it = indices_.find(key);

  if (it != indices_.end()) {
    indices_.erase(it);
    return true;
  }

  return false;
}

std::shared_ptr<search::HnswVectorIndex> GlobalHnswIndexRegistry::Get(
    std::string_view index_name, std::string_view field_name) const {
  std::string key = MakeKey(index_name, field_name);
  std::shared_lock<std::shared_mutex> lock(registry_mutex_);
  auto it = indices_.find(key);
  return it != indices_.end() ? it->second : nullptr;
}

bool GlobalHnswIndexRegistry::Exist(std::string_view index_name,
                                    std::string_view field_name) const {
  std::string key = MakeKey(index_name, field_name);
  std::shared_lock<std::shared_mutex> lock(registry_mutex_);
  return indices_.find(key) != indices_.end();
}

void GlobalHnswIndexRegistry::Reset() {
  std::unique_lock<std::shared_mutex> lock(registry_mutex_);
  indices_.clear();
}

std::string GlobalHnswIndexRegistry::MakeKey(std::string_view index_name,
                                             std::string_view field_name) const {
  return absl::StrCat(index_name, ":", field_name);
}

}  // namespace dfly
