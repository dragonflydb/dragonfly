// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// Times full walks over one set built from the oah_set_bench corpus: with iterators, as RDB
// saving does, or with Scan cursors, as SSCAN does. It reuses the harness by including it, so it
// accepts the harness flags; see README.md for the CMake target.
#pragma GCC diagnostic push
// Unlike main, the renamed harness main must return a value.
#pragma GCC diagnostic ignored "-Wreturn-type"
#define main harness_main
#include "core/oah_set_bench.cc"
#undef main
#pragma GCC diagnostic pop

ABSL_FLAG(std::string, set, "oah", "oah, ss or std");
ABSL_FLAG(uint32_t, passes, 0, "Full walks to time");
ABSL_FLAG(bool, scan, false, "Walk with Scan cursors instead of iterators");

namespace dfly {
namespace {

template <typename Set> uint64_t ScanAll(Set& set) {
  uint64_t checksum = 0;
  uint32_t cursor = 0;
  do {
    if constexpr (std::is_same_v<Set, StringSet>) {
      cursor = set.Scan(
          cursor, [&](sds key) { checksum += KeyChecksum(std::string_view(key, sdslen(key))); });
    } else {
      cursor = set.Scan(cursor, [&](std::string_view key) { checksum += KeyChecksum(key); });
    }
  } while (cursor != 0);
  return checksum;
}

template <typename Set> void Walk(const Corpus& corpus, uint32_t passes) {
  Set set;
  for (const auto& key : corpus.present)
    CHECK(Add(set, key));
  uint64_t checksum = 0;
  const double ns = Measure(corpus.present.size() * std::max(passes, 1u), [&] {
    for (uint32_t pass = 0; pass < passes; ++pass) {
      std::atomic_signal_fence(std::memory_order_seq_cst);
      if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
        checksum += Iterate(set);
      } else {
        checksum += absl::GetFlag(FLAGS_scan) ? ScanAll(set) : Iterate(set);
      }
    }
  });
  CHECK_EQ(checksum, corpus.checksum * passes);
  std::printf("%.2f ns per member\n", ns);
}

}  // namespace
}  // namespace dfly

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);
  auto* heap = mi_heap_get_backing();
  init_zmalloc_threadlocal(heap);
  dfly::MiMemoryResource resource(heap);
  dfly::InitTLStatelessAllocMR(&resource);
  const dfly::Corpus corpus =
      dfly::MakeCorpus(absl::GetFlag(FLAGS_count), absl::GetFlag(FLAGS_key_size),
                       absl::GetFlag(FLAGS_binary), absl::GetFlag(FLAGS_seed));
  const std::string set = absl::GetFlag(FLAGS_set);
  const uint32_t passes = absl::GetFlag(FLAGS_passes);
  if (set == "oah")
    dfly::Walk<dfly::OAHSet>(corpus, passes);
  else if (set == "ss")
    dfly::Walk<dfly::StringSet>(corpus, passes);
  else
    dfly::Walk<dfly::StdUnorderedSet>(corpus, passes);
  dfly::CleanupStatelessAllocMR();
}
