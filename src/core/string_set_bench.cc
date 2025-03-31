#include <absl/base/internal/cycleclock.h>
#include <mimalloc.h>

#include <random>
#include <string>
#include <string_view>
#include <unordered_set>

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
#define USE_TIME 1

int64_t GetNow() {
#if USE_TIME
  return absl::GetCurrentTimeNanos();
#else
  return absl::base_internal::CycleClock::Now();
#endif
}

size_t memUsed(StringSet& obj) {
  return obj.ObjMallocUsed() + obj.SetMallocUsed();
}

#if defined(__i386__) || defined(__amd64__)
#define LFENCE __asm__ __volatile__("lfence")
#else
#define LFENCE __asm__ __volatile__("ISB")
#endif

std::mt19937 randGenerator(0);

static std::string random_string(unsigned len) {
  const std::string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  std::string ret;
  ret.reserve(len);

  for (size_t i = 0; i < len; ++i) {
    ret += alpanum[randGenerator() % alpanum.size()];
  }

  return ret;
}

// Modifies string in one place.
// Created so that we don't have to create string multiple times to get same size string.
static void random_string_modifier(std::string& str) {
  const std::string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  uint8_t slen = str.length();
  str[randGenerator() % slen] = alpanum[randGenerator() % alpanum.size()];
}

StringSet sset;
base::Histogram perfHist;
time_t BenchStringSetAdd(uint32_t nKeys, uint32_t keySize) {
  auto str = random_string(keySize);
  time_t total_time = 0;
  while (sset.UpperBoundSize() < nKeys) {
    random_string_modifier(str);
    // If string set already contains str, we will not consider it for benchmarks.
    if (sset.Contains(str)) {
      continue;
    }
    auto start = GetNow();
    sset.Add(str);
    LFENCE;
    auto end = GetNow();
    perfHist.Add((end - start) / 100);
    total_time += (end - start);
  }
  return total_time;
}

std::unordered_set<std::string> insertKeys(uint32_t nKeys, uint32_t keySize) {
  std::unordered_set<std::string> temp;
  auto str = random_string(keySize);
  while (sset.UpperBoundSize() < nKeys) {
    random_string_modifier(str);
    temp.insert(str);
    sset.Add(str);
  }
  return temp;
}

// Testing erasing a fraction of keys from given sset
time_t BenchStringSetErase(std::unordered_set<std::string> tempSet, uint32_t nKeys,
                           double_t fraction) {
  uint32_t netNKeys = nKeys * fraction;
  time_t total_time = 0;

  for (auto i = tempSet.begin(); i != tempSet.end() && netNKeys > 0; ++i, netNKeys--) {
    auto str = *i;
    auto start = GetNow();
    sset.Erase(str);
    LFENCE;
    auto end = GetNow();
    perfHist.Add((end - start) / 100);
    total_time += (end - start);
  }
  return total_time;
}

// Similar to Erase testing, finding fraction of keys from sset.
time_t BenchStringSetGet(std::unordered_set<std::string> tempSet, uint32_t nKeys,
                         double_t fraction) {
  uint32_t netNKeys = nKeys * fraction;
  time_t total_time = 0;

  for (auto i = tempSet.begin(); i != tempSet.end() && netNKeys > 0; ++i, netNKeys--) {
    auto str = *i;
    auto start = GetNow();
    sset.Find(str);
    LFENCE;
    auto end = GetNow();
    perfHist.Add((end - start) / 100);
    total_time += (end - start);
  }
  return total_time;
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  init_zmalloc_threadlocal(mi_heap_get_backing());

  std::string operation = absl::GetFlag(FLAGS_operation);
  uint32_t keySize = absl::GetFlag(FLAGS_keySize);
  uint32_t nKeys = absl::GetFlag(FLAGS_nKeys);
  double_t fraction = absl::GetFlag(FLAGS_fraction);

  // Little Safeguarding against fractions greater than 1 or less than 0
  if (fraction > 1)
    fraction = 1;
  else if (fraction < 0)
    fraction = 0;

  uint64_t start = absl::GetCurrentTimeNanos();

  if (operation == "add") {
    auto time_taken = BenchStringSetAdd(nKeys, keySize);
    auto memory_used = memUsed(sset);
    CONSOLE_INFO << "Total time taken to insert " << nKeys << " keys with each key is size of "
                 << keySize << " is: " << time_taken / 1000000 << " ms";
    CONSOLE_INFO << "Memory consumed after performing insert operation is : " << memory_used
                 << " Bytes";
    CONSOLE_INFO << "String Set individual insert latencies histogram (jiffies, 100ns):\n"
                 << perfHist.ToString();
  } else if (operation == "erase") {
    auto tempSet = insertKeys(nKeys, keySize);
    auto memory_before_erase = memUsed(sset);
    auto time_taken = BenchStringSetErase(tempSet, nKeys, fraction);
    auto memory_after_erase = memUsed(sset);

    CONSOLE_INFO << "Total time taken to Erase " << nKeys * fraction
                 << " keys with each key is size of " << keySize << " is: " << time_taken / 1000000
                 << " ms";
    CONSOLE_INFO << "Memory consumed before erasing " << fraction * 100 << "% of string set is "
                 << memory_before_erase << " Bytes and after performing erase operation is "
                 << memory_after_erase << " Bytes";
    CONSOLE_INFO << "String Set individual erase latencies histogram (jiffies, 100ns):\n"
                 << perfHist.ToString();
  } else if (operation == "find") {
    auto tempSet = insertKeys(nKeys, keySize);
    auto time_taken = BenchStringSetGet(tempSet, nKeys, fraction);

    CONSOLE_INFO << "Total time taken to Find " << nKeys * fraction
                 << " keys with each key is size of " << keySize << " is: " << time_taken / 1000000
                 << " ms";
    CONSOLE_INFO << "String Set individual find latencies histogram (jiffies, 100ns):\n"
                 << perfHist.ToString();
  } else {
    LOG(FATAL) << "Unknown operation " << operation;
  }

  uint64_t delta = (absl::GetCurrentTimeNanos() - start) / 1000000;
  CONSOLE_INFO << "Took " << delta << " ms";

  return 0;
}
