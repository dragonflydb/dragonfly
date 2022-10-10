// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/memory_cmd.h"

#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include "facade/error.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

using namespace std;
using namespace facade;

namespace dfly {

namespace {

void MiStatsCallback(const char* msg, void* arg) {
  string* str = (string*)arg;
  absl::StrAppend(str, msg);
}

// blocksize, reserved, commited, used.
using BlockKey = std::tuple<size_t, size_t, size_t, size_t>;
using BlockMap = absl::flat_hash_map<BlockKey, uint64_t>;

bool MiArenaVisit(const mi_heap_t* heap, const mi_heap_area_t* area, void* block, size_t block_size,
                  void* arg) {
  BlockMap* bmap = (BlockMap*)arg;
  BlockKey bkey{block_size, area->reserved, area->committed, area->used * block_size};
  (*bmap)[bkey]++;

  return true;
};

}  // namespace

MemoryCmd::MemoryCmd(ServerFamily* owner, ConnectionContext* cntx) : sf_(*owner), cntx_(cntx) {
}

void MemoryCmd::Run(CmdArgList args) {
  string_view sub_cmd = ArgS(args, 1);
  if (sub_cmd == "USAGE") {
    // dummy output, in practice not implemented yet.
    return (*cntx_)->SendLong(1);
  } else if (sub_cmd == "MALLOC-STATS") {
    string res = shard_set->pool()->at(0)->AwaitBrief([this] { return MallocStats(); });
    return (*cntx_)->SendBulkString(res);
  }

  string err = UnknownSubCmd(sub_cmd, "MEMORY");
  return (*cntx_)->SendError(err, kSyntaxErrType);
}

string MemoryCmd::MallocStats() {
  string str;
  absl::StrAppend(&str, "___ Begin mimalloc statistics ___\n");
  mi_stats_print_out(MiStatsCallback, &str);

  absl::StrAppend(&str, "\nArena statistics from a single thread:\n");
  absl::StrAppend(&str, "Count BlockSize Reserved Committed Used\n");

  mi_heap_t* data_heap = ServerState::tlocal()->data_heap();
  BlockMap block_map;

  mi_heap_visit_blocks(data_heap, false /* visit all blocks*/, MiArenaVisit, &block_map);

  for (const auto& k_v : block_map) {
    absl::StrAppend(&str, k_v.second, " ", get<0>(k_v.first), " ", get<1>(k_v.first), " ",
                    get<2>(k_v.first), " ", get<3>(k_v.first), "\n");
  }

  absl::StrAppend(&str, "--- End mimalloc statistics ---\n");

  return str;
}

}  // namespace dfly
