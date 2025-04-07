#include <absl/base/internal/cycleclock.h>
#include <absl/strings/str_cat.h>
#include <benchmark/benchmark.h>
#include <mimalloc.h>

#include <iostream>
#include <random>
#include <string>
#include <string_view>

#include "base/histogram.h"
#include "base/init.h"
#include "core/string_set.h"

extern "C" {
#include "redis/zmalloc.h"
}

ABSL_FLAG(std::string, operation, "add",
          "Operation you want to benchmark. Only add, erase and find are allowed.");
ABSL_FLAG(uint32_t, keySize, 100, "Size of each key.");
ABSL_FLAG(uint32_t, nKeys, 10000, "Number of keys you want to run benchmark on.");
ABSL_FLAG(double_t, fraction, 0.5,
          "Fraction of string set you want to erase/find for benchmarking.");

using namespace dfly;

std::mt19937 randGenerator(0);

template <typename RandGen>
std::string GetRandomHex(RandGen& gen, size_t len, size_t len_deviation = 0) {
  static_assert(std::is_same<uint64_t, decltype(gen())>::value);
  if (len_deviation) {
    len += (gen() % len_deviation);
  }

  std::string res(len, '\0');
  size_t indx = 0;

  for (size_t i = 0; i < len / 16; ++i) {  // 2 chars per byte
    absl::numbers_internal::FastHexToBufferZeroPad16(gen(), res.data() + indx);
    indx += 16;
  }

  if (indx < res.size()) {
    char buf[32];
    absl::numbers_internal::FastHexToBufferZeroPad16(gen(), buf);

    for (unsigned j = 0; indx < res.size(); indx++, j++) {
      res[indx] = buf[j];
    }
  }

  return res;
}

size_t memUsed(StringSet& obj) {
  // std::cout<<obj.ObjMallocUsed()<<" "<<obj.SetMallocUsed()<<std::endl;
  return obj.ObjMallocUsed() + obj.SetMallocUsed();
}

void BM_Add(benchmark::State& state) {
  std::vector<std::string> strs;
  StringSet ss;
  auto elems = absl::GetFlag(FLAGS_nKeys);
  auto keySize = absl::GetFlag(FLAGS_keySize);
  for (size_t i = 0; i < elems; ++i) {
    std::string str = GetRandomHex(randGenerator, keySize);
    strs.push_back(str);
  }
  ss.Reserve(elems);
  while (state.KeepRunning()) {
    state.PauseTiming();
    ss.Clear();
    ss.Reserve(elems);
    state.ResumeTiming();
    for (auto& str : strs)
      ss.Add(str);
  }
  state.counters["Memory_Used"] = memUsed(ss);
}
BENCHMARK(BM_Add);

void BM_Erase(benchmark::State& state) {
  std::vector<std::string> strs;
  StringSet ss;
  auto elems = absl::GetFlag(FLAGS_nKeys);
  auto keySize = absl::GetFlag(FLAGS_keySize);
  auto fraction = absl::GetFlag(FLAGS_fraction);
  uint32_t netNKeys = elems * fraction;
  for (size_t i = 0; i < elems; ++i) {
    std::string str = GetRandomHex(randGenerator, keySize);
    strs.push_back(str);
    ss.Add(str);
  }
  state.counters["Memory_Before_Erase"] = memUsed(ss);
  while (state.KeepRunning()) {
    state.PauseTiming();
    netNKeys = elems * fraction;
    ss.Clear();
    for (auto& str : strs) {
      ss.Add(str);
    }
    state.ResumeTiming();
    for (auto str = strs.begin(); str != strs.end() && netNKeys > 0; str++, netNKeys--) {
      ss.Erase(*str);
    }
  }
  state.counters["Memory_After_Erase"] = memUsed(ss);
}
BENCHMARK(BM_Erase);

void BM_Get(benchmark::State& state) {
  std::vector<std::string> strs;
  StringSet ss;
  auto elems = absl::GetFlag(FLAGS_nKeys);
  auto keySize = absl::GetFlag(FLAGS_keySize);
  auto fraction = absl::GetFlag(FLAGS_fraction);
  uint32_t netNKeys = elems * fraction;
  for (size_t i = 0; i < elems; ++i) {
    std::string str = GetRandomHex(randGenerator, keySize);
    strs.push_back(str);
    ss.Add(str);
  }
  while (state.KeepRunning()) {
    state.PauseTiming();
    netNKeys = elems * fraction;
    state.ResumeTiming();
    for (auto str = strs.begin(); str != strs.end() && netNKeys > 0; str++, netNKeys--) {
      ss.Find(*str);
    }
  }
}

BENCHMARK(BM_Get);

int main(int argc, char* argv[]) {
  benchmark::Initialize(&argc, argv);
  MainInitGuard guard(&argc, &argv);

  init_zmalloc_threadlocal(mi_heap_get_backing());

  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
