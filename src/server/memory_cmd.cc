// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/memory_cmd.h"

#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include "facade/error.h"
#include "server/server_state.h"

using namespace std;
using namespace facade;

namespace dfly {

namespace {

void MiStatsCallback(const char* msg, void* arg) {
  string* str = (string*)arg;
  absl::StrAppend(str, msg);
}

bool MiArenaVisit(const mi_heap_t* heap, const mi_heap_area_t* area, void* block, size_t block_size,
                  void* arg) {
  string* str = (string*)arg;

  absl::StrAppend(str, "block_size ", block_size, ", reserved ",
                  area->reserved, ", comitted ", area->committed,
                  ", used: ", area->used * block_size, "\n");

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
    return MallocStats();
  }

  string err = UnknownSubCmd(sub_cmd, "MEMORY");
  return (*cntx_)->SendError(err, kSyntaxErrType);
}

void MemoryCmd::MallocStats() {
  string str;
  absl::StrAppend(&str, "___ Begin mimalloc statistics ___\n");
  mi_stats_print_out(MiStatsCallback, &str);

  absl::StrAppend(&str, "\nArena statistics from a single thread:\n");
  mi_heap_t* data_heap = ServerState::tlocal()->data_heap();
  mi_heap_visit_blocks(data_heap, false /* visit all blocks*/, MiArenaVisit, &str);

  absl::StrAppend(&str, "--- End mimalloc statistics ---\n");

  return (*cntx_)->SendBulkString(str);
}

}  // namespace dfly
