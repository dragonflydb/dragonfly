// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// clang-format off
#include <mimalloc.h>
#include <mimalloc-types.h>

// clang-format on

// This is an internal struct in mimalloc.
// To declare it, we must include mimalloc-types.h which is an internal header in the lib.
// Being such it does not interact well with some other c++ headers, therefore we
// use a clean cc file to extract data from this record.
extern "C" mi_stats_t _mi_stats_main;

namespace dfly {

int64_t GetMallocCurrentCommitted() {
  return _mi_stats_main.committed.current;
}

}  // namespace dfly
