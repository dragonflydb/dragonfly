// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#ifndef WITH_SEARCH
#include "core/page_usage/page_usage_stats.h"
#include "core/search/base.h"
#include "server/search/doc_index.h"

namespace dfly {

using namespace std;

ShardDocIndices::ShardDocIndices() : local_mr_(nullptr) {
}

void ShardDocIndices::AddDoc(std::string_view key, const DbContext& db_cnt, const PrimeValue& pv) {
}
void ShardDocIndices::RemoveDoc(std::string_view key, const DbContext& db_cnt,
                                const PrimeValue& pv) {
}

void ShardDocIndices::DropAllIndices() {
}
void ShardDocIndices::RebuildAllIndices(const OpArgs& op_args) {
}

size_t ShardDocIndices::GetUsedMemory() const {
  return 0;
}
SearchStats ShardDocIndices::GetStats() const {
  return {};
}

search::DefragmentResult ShardDocIndices::Defragment(PageUsage*) {
  return search::DefragmentResult{};
}

}  // namespace dfly
#endif
