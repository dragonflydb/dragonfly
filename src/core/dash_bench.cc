// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <mimalloc.h>

#include "base/hash.h"
#include "base/histogram.h"
#include "base/init.h"
#include "core/dash.h"

extern "C" {
#include "redis/dict.h"
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

ABSL_FLAG(uint32_t, n, 100000, "num items");
ABSL_FLAG(bool, dash, true, "");
ABSL_FLAG(bool, sds, true, "");

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

void Sample(int64_t start, int64_t end, base::Histogram* hist) {
  hist->Add((end - start) / 1000);
}

void BenchDash() {
  uint64_t num = GetFlag(FLAGS_n);

  Dash64 dt;
  base::Histogram hist;

  time_t start = absl::GetCurrentTimeNanos();
  for (uint64_t i = 0; i < num; ++i) {
    dt.Insert(i, 0);
    time_t end = absl::GetCurrentTimeNanos();
    Sample(start, end, &hist);
    start = end;
  }

  CONSOLE_INFO << hist.ToString();
}

inline sds Prefix() {
  return sdsnew("xxxxxxxxxxxxxxxxxxxxxxx");
}

void BenchDashSds() {
  uint64_t num = GetFlag(FLAGS_n);

  base::Histogram hist;
  sds key = sdscatsds(Prefix(), sdsfromlonglong(0));
  DashSds dt;

  time_t start = absl::GetCurrentTimeNanos();
  for (uint64_t i = 0; i < num; ++i) {
    dt.Insert(key, 0);
    time_t end = absl::GetCurrentTimeNanos();
    Sample(start, end, &hist);

    key = sdscatsds(Prefix(), sdsfromlonglong(i + 1));
    start = absl::GetCurrentTimeNanos();
  }

  CONSOLE_INFO << hist.ToString();
}

static uint64_t callbackHash(const void* key) {
  return XXH64(&key, sizeof(key), 0);
}

static dictType IntDict = {callbackHash, NULL, NULL, NULL, NULL, NULL, NULL};

void BenchDict() {
  uint64_t num = GetFlag(FLAGS_n);

  base::Histogram hist;

  time_t start = absl::GetCurrentTimeNanos();
  dict* d = dictCreate(&IntDict);
  for (uint64_t i = 0; i < num; ++i) {
    dictAdd(d, (void*)i, nullptr);
    time_t end = absl::GetCurrentTimeNanos();
    Sample(start, end, &hist);
    start = end;
  }
  dictRelease(d);

  CONSOLE_INFO << hist.ToString();
}

void BenchDictSds() {
  uint64_t num = GetFlag(FLAGS_n);

  base::Histogram hist;
  sds key = sdscat(Prefix(), sdsfromlonglong(0));
  dict* d = dictCreate(&SdsDict);

  time_t start = absl::GetCurrentTimeNanos();
  for (uint64_t i = 0; i < num; ++i) {
    dictAdd(d, key, nullptr);
    time_t end = absl::GetCurrentTimeNanos();
    Sample(start, end, &hist);
    key = sdscatsds(Prefix(), sdsfromlonglong(i + 1));
    start = absl::GetCurrentTimeNanos();
  }
  dictRelease(d);

  CONSOLE_INFO << hist.ToString();
}

}  // namespace dfly

using namespace dfly;
int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  init_zmalloc_threadlocal(mi_heap_get_backing());

  bool is_dash = GetFlag(FLAGS_dash);
  bool is_sds = GetFlag(FLAGS_sds);

  if (is_dash) {
    if (is_sds) {
      BenchDashSds();
    } else {
      BenchDash();
    }
  } else {
    if (is_sds) {
      BenchDictSds();
    } else {
      BenchDict();
    }
  }

  return 0;
}
