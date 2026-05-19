// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/functional/function_ref.h>

#include <span>
#include <string>

#include "core/search/base.h"
#include "server/db_slice.h"

namespace dfly {

class RdbSerializer;

struct SearchSerializer {
  static void Serialize(RdbSerializer* serializer, DbSlice* db_slice,
                        absl::FunctionRef<void()> push_fun) {
#ifdef WITH_SEARCH
    SearchSerializer ss{serializer, db_slice, push_fun};
    ss.SerializeIndexMappings();
    ss.SerializeGlobalHnswIndices();
#endif
  }

#ifdef WITH_SEARCH
  std::error_code SerializeIndexMapping(
      uint32_t shard_id, std::string_view index_name,
      std::span<const std::pair<std::string, search::DocId>> mappings) const;

  // Serialize ShardDocIndex key-to-DocId mappings for all search indices on this shard
  void SerializeIndexMappings() const;

  // Serialize HNSW global indices for shard 0 only
  void SerializeGlobalHnswIndices() const;

  RdbSerializer* serializer_;
  DbSlice* db_slice_;
  absl::FunctionRef<void()> push_fun_;
#endif
};

}  // namespace dfly
