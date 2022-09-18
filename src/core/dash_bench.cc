// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <mimalloc.h>

#include <absl/base/internal/cycleclock.h>
#include <absl/container/flat_hash_map.h>

#include "base/hash.h"
#include "base/histogram.h"
#include "base/init.h"
#include "core/dash.h"

extern "C" {
#include "redis/dict.h"
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

using namespace std;

ABSL_FLAG(uint32_t, n, 100000, "num items");
ABSL_FLAG(string, type, "dash", "");
ABSL_FLAG(bool, sds, false, "If true, uses sds as primary key");

namespace dfly {

static uint64_t dictSdsHash(const void* key) {
  return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

static int dictSdsKeyCompare(dict*, const void* key1, const void* key2) {
  int l1, l2;

  l1 = sdslen((sds)key1);
  l2 = sdslen((sds)key2);
  if (l1 != l2)
    return 0;
  return memcmp(key1, key2, l1) == 0;
}

static dictType SdsDict = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    NULL,
    // dictSdsDestructor, /* key destructor */
    NULL, /* val destructor */
    NULL,
};

struct UInt64Policy : public BasicDashPolicy {
  static uint64_t HashFn(uint64_t v) {
    return XXH3_64bits(&v, sizeof(v));
  }
};

struct SdsDashPolicy {
  enum { kSlotNum = 14, kBucketNum = 56, kStashBucketNum = 4 };
  static constexpr bool kUseVersion = false;

  static uint64_t HashFn(sds u) {
    return XXH3_64bits(reinterpret_cast<const uint8_t*>(u), sdslen(u));
  }

  static uint64_t HashFn(std::string_view u) {
    return XXH3_64bits(u.data(), u.size());
  }

  static void DestroyKey(sds s) {
    sdsfree(s);
  }

  static void DestroyValue(uint64_t) {
  }

  static bool Equal(sds u1, sds u2) {
    return dictSdsKeyCompare(nullptr, u1, u2) == 0;
  }

  static bool Equal(sds u1, std::string_view u2) {
    return u2 == std::string_view{u1, sdslen(u1)};
  }
};

using Dash64 = DashTable<uint64_t, uint64_t, UInt64Policy>;
using DashSds = DashTable<sds, uint64_t, SdsDashPolicy>;

using absl::GetFlag;

inline void Sample(int64_t start, int64_t end, base::Histogram* hist) {
  hist->Add((end - start) / 100);
}

Dash64 udt;
DashSds sds_dt;
base::Histogram hist;

#define USE_TIME 1

int64_t GetNow() {
  #if USE_TIME
    return absl::GetCurrentTimeNanos();
#else
    return absl::base_internal::CycleClock::Now();
#endif
}

#if defined(__i386__) || defined(__amd64__)
  #define LFENCE __asm__ __volatile__("lfence")
#else
  #define LFENCE __asm__ __volatile__("ISB")
#endif

absl::flat_hash_map<uint64_t, uint64_t> mymap;

void BenchFlat(uint64_t num) {
  for (uint64_t i = 0; i < num; ++i) {
    time_t start = GetNow();
    mymap.emplace(i, 0);
    LFENCE;

    time_t end = GetNow();
    Sample(start, end, &hist);
  }
}

void BenchDash(uint64_t num) {
  for (uint64_t i = 0; i < num; ++i) {
    time_t start = GetNow();
    udt.Insert(i, 0);
    LFENCE;

    time_t end = GetNow();
    Sample(start, end, &hist);
  }
}

inline sds Prefix() {
  return sdsnew("xxxxxxxxxxxxxxxxxxxxxxx");
}

void BenchDashSds(uint64_t num) {
  sds key = sdscatsds(Prefix(), sdsfromlonglong(0));
  for (uint64_t i = 0; i < num; ++i) {
    time_t start = GetNow();
    sds_dt.Insert(key, 0);
    time_t end = GetNow();
    Sample(start, end, &hist);

    key = sdscatsds(Prefix(), sdsfromlonglong(i + 1));
  }
}

static uint64_t callbackHash(const void* key) {
  return XXH64(&key, sizeof(key), 0);
}

static dictType IntDict = {callbackHash, NULL, NULL, NULL, NULL, NULL, NULL};

dict* redis_dict = nullptr;

void BenchDict(uint64_t num) {
  redis_dict = dictCreate(&IntDict);

  for (uint64_t i = 0; i < num; ++i) {
    time_t start = GetNow();
    dictAdd(redis_dict, (void*)i, nullptr);
    LFENCE;
    time_t end = GetNow();
    Sample(start, end, &hist);
  }
}

void BenchDictSds() {
  uint64_t num = GetFlag(FLAGS_n);

  sds key = sdscat(Prefix(), sdsfromlonglong(0));
  redis_dict = dictCreate(&SdsDict);

  for (uint64_t i = 0; i < num; ++i) {
    time_t start = GetNow();
    dictAdd(redis_dict, key, nullptr);
    time_t end = GetNow();
    Sample(start, end, &hist);

    key = sdscatsds(Prefix(), sdsfromlonglong(i + 1));
  }
}

}  // namespace dfly

using namespace dfly;

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  init_zmalloc_threadlocal(mi_heap_get_backing());

  string table_type = GetFlag(FLAGS_type);

  bool is_sds = GetFlag(FLAGS_sds);
  uint64_t start = absl::GetCurrentTimeNanos();
  uint64_t num = GetFlag(FLAGS_n);

  if (table_type == "dash") {
    if (is_sds) {
      BenchDashSds(num);
    } else {
      BenchDash(num);
    }
  } else if (table_type == "dict") {
    if (is_sds) {
      BenchDictSds();
    } else {
      BenchDict(num);
    }
  } else if (table_type == "flat") {
    BenchFlat(num);
  } else {
    LOG(FATAL) << "Unknown type " << table_type;
  }

  CONSOLE_INFO << "latencies histogram (jiffies, 100ns):\n" << hist.ToString();
  uint64_t delta = (absl::GetCurrentTimeNanos() - start) / 1000000;
  CONSOLE_INFO << "Took " << delta << " ms";

  return 0;
}
