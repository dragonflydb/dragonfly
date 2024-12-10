// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/cmd_serializer.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "core/string_set.h"
#include "server/test_utils.h"

extern "C" {
#include "redis/zmalloc.h"
}

using namespace testing;
using namespace std;
using benchmark::DoNotOptimize;

namespace dfly {

static void SetUpTestSuite() {
  InitRedisTables();  // to initialize server struct.

  auto* tlh = mi_heap_get_backing();
  init_zmalloc_threadlocal(tlh);
  SmallString::InitThreadLocal(tlh);
}

static void TearDownTestSuite() {
  mi_heap_collect(mi_heap_get_backing(), true);
}

// 2 args: threshold and value size
void BM_SerializerThresholdSet(benchmark::State& state) {
  ServerState::Init(0, 1, nullptr);
  SetUpTestSuite();
  MiMemoryResource mi(mi_heap_get_backing());
  CompactObj::InitThreadLocal(&mi);

  auto threshold = state.range(0);
  auto val_size = state.range(1);
  string_view k = "key";
  CmdSerializer serializer([](std::string s) { DoNotOptimize(s); }, threshold);
  absl::InsecureBitGen eng;

  {
    // Allocations must be done on the heap using mimalloc, as we use mimalloc API in many places
    auto* key = CompactObj::AllocateMR<PrimeValue>(k);
    auto* value = CompactObj::AllocateMR<PrimeValue>();

    StringSet* s = CompactObj::AllocateMR<StringSet>();
    for (unsigned int i = 0; i < val_size; ++i) {
      s->Add(GetRandomHex(eng, 100));
    }
    value->InitRobj(OBJ_SET, kEncodingStrMap2, s);

    while (state.KeepRunning()) {
      serializer.SerializeEntry(k, *key, *value, 0);
    }

    CompactObj::DeleteMR<PrimeValue>(value);
    CompactObj::DeleteMR<PrimeValue>(key);
  }

  TearDownTestSuite();
  ServerState::Destroy();
}

BENCHMARK(BM_SerializerThresholdSet)
    ->Args({0, 1})
    ->Args({100, 1})
    ->Args({0, 10})
    ->Args({1000, 10})
    ->Args({0, 100})
    ->Args({10'000, 100})
    ->Args({0, 1'000})
    ->Args({100'000, 1'000})
    ->Args({0, 1'000'000})
    ->Args({100'000, 1'000'000});

}  // namespace dfly
