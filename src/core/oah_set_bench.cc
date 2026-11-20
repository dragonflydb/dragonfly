// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

// Compare owning string sets using the same allocator. Only this executable overrides new/delete.
#include <mimalloc-new-delete.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <numeric>
#include <random>
#include <string>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "absl/flags/flag.h"
#include "base/init.h"
#include "base/logging.h"
#include "core/mi_memory_resource.h"
#include "core/oah_set.h"
#include "core/string_set.h"

ABSL_FLAG(uint32_t, count, 100000, "Number of unique strings (less than 2^47)");
ABSL_FLAG(uint32_t, key_size, 32, "String length in bytes, at least 8");
ABSL_FLAG(uint32_t, rounds, 7, "Measured rounds after one unreported warm-up round");
ABSL_FLAG(uint32_t, lookup_passes, 5, "Full passes for each read operation");
ABSL_FLAG(uint64_t, seed, 20261116, "Deterministic corpus and shuffle seed");
ABSL_FLAG(bool, binary, false, "Generate non-ASCII strings instead of ASCII strings");
ABSL_FLAG(bool, reserve, false, "Reserve hash table capacity before timing insertion");

namespace dfly {
namespace {

using Clock = std::chrono::steady_clock;

// The allocator is empty, preserving libstdc++'s normal node and string layouts. Nodes use the
// same tracked resource as Dragonfly's bucket arrays; std::string buffers use mimalloc new/delete.
using StdUnorderedSet =
    std::unordered_set<std::string, std::hash<std::string>, std::equal_to<std::string>,
                       StatelessAllocator<std::string>>;
static_assert(sizeof(StdUnorderedSet) == sizeof(std::unordered_set<std::string>));

struct Corpus {
  std::vector<std::string> present;
  std::vector<std::string> absent;
  std::vector<size_t> read_order;
  uint64_t checksum = 0;
};

struct Sample {
  double insert_ns;
  double hit_ns;
  double miss_ns;
  double duplicate_ns;
  double iterate_ns;
  double erase_ns;
  size_t heap_bytes;
  size_t structure_bytes;
  size_t key_bytes;
  size_t object_bytes;
  size_t buckets;
};

uint64_t KeyChecksum(std::string_view key) {
  return key.size() + static_cast<unsigned char>(key.front());
}

Corpus MakeCorpus(size_t count, size_t key_size, bool binary, uint64_t seed) {
  constexpr std::string_view kAlphabet =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_";
  std::mt19937_64 rng(seed);
  Corpus corpus;
  corpus.present.reserve(count);
  corpus.absent.reserve(count);
  corpus.read_order.resize(count);

  for (size_t i = 0; i < count * 2; ++i) {
    std::string key(key_size, '\0');

    // An odd multiplier permutes 48-bit IDs. Eight base-64 characters make the two key sets
    // provably unique and disjoint, including when key_size == 8.
    uint64_t id = (i * 0x5DEECE66DULL + seed) & ((1ULL << 48) - 1);
    for (size_t j = 0; j < 8; ++j) {
      key[j] = static_cast<char>(kAlphabet[id & 63] | (binary ? 0x80 : 0));
      id >>= 6;
    }
    for (size_t j = 8; j < key_size; ++j) {
      uint64_t random = rng();
      key[j] = binary ? static_cast<char>(random & 255) : kAlphabet[random & 63];
    }
    if (i < count) {
      corpus.checksum += KeyChecksum(key);
      corpus.present.push_back(std::move(key));
    } else {
      corpus.absent.push_back(std::move(key));
    }
  }

  std::shuffle(corpus.present.begin(), corpus.present.end(), rng);
  std::shuffle(corpus.absent.begin(), corpus.absent.end(), rng);
  std::iota(corpus.read_order.begin(), corpus.read_order.end(), 0);
  std::shuffle(corpus.read_order.begin(), corpus.read_order.end(), rng);
  return corpus;
}

template <typename Fn> double Measure(size_t operations, Fn&& fn) {
  const auto start = Clock::now();
  fn();
  const auto stop = Clock::now();
  return std::chrono::duration<double, std::nano>(stop - start).count() / operations;
}

template <typename Set> bool Add(Set& set, const std::string& key) {
  if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
    return set.insert(key).second;
  } else {
    return set.Add(key);
  }
}

template <typename Set> bool Contains(Set& set, const std::string& key) {
  if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
    return set.find(key) != set.end();
  } else {
    return set.Find(key) != set.end();
  }
}

template <typename Set> bool Erase(Set& set, const std::string& key) {
  if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
    return set.erase(key) != 0;
  } else {
    return set.Erase(key);
  }
}

template <typename Set> size_t Size(Set& set) {
  if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
    return set.size();
  } else {
    return set.UpperBoundSize();
  }
}

template <typename Set> uint64_t Iterate(Set& set) {
  uint64_t checksum = 0;
  for (auto it = set.begin(); it != set.end(); ++it) {
    if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
      checksum += KeyChecksum(*it);
    } else {
      auto key = Key(it);
      checksum += KeyChecksum(GetKeyView(key));
    }
  }
  return checksum;
}

size_t StringAllocSize(const std::string& key) {
  const uintptr_t data = reinterpret_cast<uintptr_t>(key.data());
  const uintptr_t object = reinterpret_cast<uintptr_t>(&key);

  // SSO bytes are already included in the hash node allocation. Only heap-backed strings
  // contribute a second allocation. Read usable size from the allocation, not string capacity.
  return data >= object && data < object + sizeof(key) ? 0 : mi_usable_size(key.data());
}

template <typename Set>
Sample Run(const Corpus& corpus, uint32_t passes, bool reserve, MiMemoryResource* resource) {
  CHECK_EQ(resource->used(), 0u);
  const size_t z_before = zmalloc_used_memory_tl;
  const size_t count = corpus.present.size();
  Sample result{};
  {
    Set set;
    if (reserve) {
      if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
        set.reserve(count);
      } else {
        set.Reserve(count);
      }
    }

    size_t added = 0;
    result.insert_ns = Measure(count, [&] {
      for (const auto& key : corpus.present)
        added += Add(set, key);
    });
    CHECK_EQ(added, count);
    CHECK_EQ(Size(set), count);

    result.object_bytes = sizeof(Set);
    if constexpr (std::is_same_v<Set, StdUnorderedSet>) {
      for (const auto& key : set)
        result.key_bytes += StringAllocSize(key);
      result.structure_bytes = resource->used();
      result.buckets = set.bucket_count();
      CHECK_EQ(set.max_load_factor(), 1.0f);
    } else {
      result.key_bytes = set.ObjMallocUsed();
      result.structure_bytes =
          resource->used() + zmalloc_used_memory_tl - z_before - result.key_bytes;
      result.buckets = set.BucketCount();
    }
    result.heap_bytes = result.structure_bytes + result.key_bytes;

    // Pre-touch both query streams outside timing. Every lookup below consumes its result.
    for (const auto& key : corpus.present)
      CHECK(Contains(set, key));
    for (const auto& key : corpus.absent)
      CHECK(!Contains(set, key));

    const size_t reads = count * passes;
    size_t hits = 0;
    result.hit_ns = Measure(reads, [&] {
      for (uint32_t pass = 0; pass < passes; ++pass) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
        for (size_t index : corpus.read_order)
          hits += Contains(set, corpus.present[index]);
      }
    });
    CHECK_EQ(hits, reads);

    size_t misses = 0;
    result.miss_ns = Measure(reads, [&] {
      for (uint32_t pass = 0; pass < passes; ++pass) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
        for (size_t index : corpus.read_order)
          misses += !Contains(set, corpus.absent[index]);
      }
    });
    CHECK_EQ(misses, reads);

    added = 0;
    result.duplicate_ns = Measure(count, [&] {
      for (size_t index : corpus.read_order)
        added += Add(set, corpus.present[index]);
    });
    CHECK_EQ(added, 0u);
    CHECK_EQ(Size(set), count);

    uint64_t checksum = 0;
    result.iterate_ns = Measure(reads, [&] {
      for (uint32_t pass = 0; pass < passes; ++pass) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
        checksum += Iterate(set);
      }
    });
    CHECK_EQ(checksum, corpus.checksum * passes);

    size_t erased = 0;
    result.erase_ns = Measure(count, [&] {
      for (size_t index : corpus.read_order)
        erased += Erase(set, corpus.present[index]);
    });
    CHECK_EQ(erased, count);
    CHECK_EQ(Size(set), 0u);
  }
  CHECK_EQ(resource->used(), 0u);
  CHECK_EQ(zmalloc_used_memory_tl, z_before);
  return result;
}

void Print(const char* name, size_t count, size_t key_size, bool binary, bool reserve,
           uint32_t round, const Sample& result) {
  std::printf("%s,%zu,%zu,%s,%u,%u,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%zu,%zu,%zu,%zu,%zu\n", name,
              count, key_size, binary ? "binary" : "ascii", unsigned(reserve), round,
              result.insert_ns, result.hit_ns, result.miss_ns, result.duplicate_ns,
              result.iterate_ns, result.erase_ns, result.heap_bytes, result.structure_bytes,
              result.key_bytes, result.object_bytes, result.buckets);
  std::fflush(stdout);
}

}  // namespace
}  // namespace dfly

int main(int argc, char** argv) {
  MainInitGuard guard(&argc, &argv);
  const size_t count = absl::GetFlag(FLAGS_count);
  const size_t key_size = absl::GetFlag(FLAGS_key_size);
  const uint32_t rounds = absl::GetFlag(FLAGS_rounds);
  const uint32_t passes = absl::GetFlag(FLAGS_lookup_passes);
  const bool binary = absl::GetFlag(FLAGS_binary);
  const bool reserve = absl::GetFlag(FLAGS_reserve);
  CHECK_GT(count, 0u);
  CHECK_GE(key_size, 8u);
  CHECK_GT(rounds, 0u);
  CHECK_GT(passes, 0u);

  auto* heap = mi_heap_get_backing();
  init_zmalloc_threadlocal(heap);
  dfly::MiMemoryResource resource(heap);
  dfly::InitTLStatelessAllocMR(&resource);
  const dfly::Corpus corpus = dfly::MakeCorpus(count, key_size, binary, absl::GetFlag(FLAGS_seed));

  std::printf(
      "container,count,key_size,encoding,reserved,round,insert_ns,hit_ns,miss_ns,"
      "duplicate_ns,iterate_ns,erase_ns,heap_bytes,structure_bytes,key_bytes,object_bytes,"
      "buckets\n");

  for (uint32_t round = 0; round <= rounds; ++round) {
    for (uint32_t slot = 0; slot < 3; ++slot) {
      dfly::Sample result;
      const char* name = nullptr;

      // Rotate the container order across rounds to reduce systematic order bias.
      switch ((round + slot) % 3) {
        case 0:
          name = "OAHSet";
          result = dfly::Run<dfly::OAHSet>(corpus, passes, reserve, &resource);
          break;
        case 1:
          name = "StringSet";
          result = dfly::Run<dfly::StringSet>(corpus, passes, reserve, &resource);
          break;
        default:
          name = "std::unordered_set";
          result = dfly::Run<dfly::StdUnorderedSet>(corpus, passes, reserve, &resource);
          break;
      }
      if (round != 0)
        dfly::Print(name, count, key_size, binary, reserve, round, result);
    }
  }
  dfly::CleanupStatelessAllocMR();
}
