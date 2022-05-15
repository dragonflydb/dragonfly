// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#define ENABLE_DASH_STATS

#include "core/dash.h"

#include <absl/container/flat_hash_map.h>
#include <mimalloc.h>

#include <functional>
#include <set>

#include "base/gtest.h"
#include "base/hash.h"
#include "base/logging.h"
#include "base/zipf_gen.h"

extern "C" {
#include "redis/dict.h"
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

namespace dfly {

static uint64_t callbackHash(const void* key) {
  return XXH64(&key, sizeof(key), 0);
}

static dictType IntDict = {callbackHash, NULL, NULL, NULL, NULL, NULL, NULL};

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

using namespace std;
struct Buf24 {
  char buf[20];
  uint32_t index;

  Buf24(uint32_t i = 0) : index(i) {
  }
};

struct UInt64Policy : public BasicDashPolicy {
  static uint64_t HashFn(uint64_t v) {
    return XXH3_64bits(&v, sizeof(v));
  }
};

class CappedResource final : public std::pmr::memory_resource {
 public:
  explicit CappedResource(size_t cap) : cap_(cap) {
  }

  size_t used() const {
    return used_;
  }

 private:
  void* do_allocate(std::size_t size, std::size_t align) {
    if (used_ + size > cap_)
      throw std::bad_alloc{};

    void* res = pmr::get_default_resource()->allocate(size, align);
    used_ += size;

    return res;
  }

  void do_deallocate(void* ptr, std::size_t size, std::size_t align) {
    used_ -= size;
    pmr::get_default_resource()->deallocate(ptr, size, align);
  }

  bool do_is_equal(const std::pmr::memory_resource& o) const noexcept {
    return this == &o;
  }

  size_t cap_;
  size_t used_ = 0;
};

using Segment = detail::Segment<uint64_t, Buf24>;
using Dash64 = DashTable<uint64_t, uint64_t, UInt64Policy>;

constexpr auto kSegTax = Segment::kTaxSize;
constexpr size_t kMaxSize = Segment::kMaxSize;
constexpr size_t kSegSize = sizeof(Segment);
constexpr size_t foo = Segment::kBucketSz;

class DashTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    init_zmalloc_threadlocal(mi_heap_get_backing());
  }

  DashTest() : segment_(1) {
  }

  bool Find(Segment::Key_t key, Segment::Value_t* val) const {
    uint64_t hash = dt_.DoHash(key);

    std::equal_to<Segment::Key_t> eq;
    auto it = segment_.FindIt(key, hash, eq);
    if (!it.found())
      return false;
    *val = segment_.Value(it.index, it.slot);
    return true;
  }

  bool Contains(Segment::Key_t key) const {
    uint64_t hash = dt_.DoHash(key);

    std::equal_to<Segment::Key_t> eq;
    auto it = segment_.FindIt(key, hash, eq);
    return it.found();
  }

  set<Segment::Key_t> FillSegment(unsigned bid);

  Segment segment_;
  Dash64 dt_;
};

set<Segment::Key_t> DashTest::FillSegment(unsigned bid) {
  std::set<Segment::Key_t> keys;

  std::equal_to<Segment::Key_t> eq;
  for (Segment::Key_t key = 0; key < 1000000u; ++key) {
    uint64_t hash = dt_.DoHash(key);
    unsigned bi = (hash >> 8) % Segment::kNumBuckets;
    if (bi != bid)
      continue;
    uint8_t fp = hash & 0xFF;
    if (fp > 2)  // limit fps considerably to find interesting cases.
      continue;
    auto [it, success] = segment_.Insert(key, 0, hash, eq);
    if (!success) {
      LOG(INFO) << "Stopped at " << key;
      break;
    }
    CHECK(it.found());
    keys.insert(key);
  }

  return keys;
}

TEST_F(DashTest, Hash) {
  for (uint64_t i = 0; i < 100; ++i) {
    uint64_t hash = dt_.DoHash(i);
    if (hash >> 63) {
      VLOG(1) << "i " << i << ", Hash " << hash;
    }
  }
}

TEST_F(DashTest, SlotBitmap) {
  detail::SlotBitmap<14> slot;
  slot.SetSlot(1, true);
  slot.SetSlot(5, false);
  EXPECT_EQ(34, slot.GetBusy());
  EXPECT_EQ(2, slot.GetProbe(true));
}

TEST_F(DashTest, Basic) {
  Segment::Key_t key = 0;
  Segment::Value_t val = 0;
  uint64_t hash = dt_.DoHash(key);
  std::equal_to<Segment::Key_t> eq;

  EXPECT_TRUE(segment_.Insert(key, val, hash, eq).second);
  auto [it, res] = segment_.Insert(key, val, hash, eq);
  EXPECT_TRUE(!res && it.found());

  EXPECT_TRUE(Find(key, &val));
  EXPECT_EQ(0, val.index);

  EXPECT_FALSE(Find(1, &val));
  EXPECT_EQ(1, segment_.SlowSize());

  unsigned has_called = 0;
  auto cb = [&](const auto& it) { ++has_called; };

  auto hfun = &UInt64Policy::HashFn;

  auto cursor = segment_.TraverseLogicalBucket((hash >> 8) % Segment::kNumBuckets, hfun, cb);
  ASSERT_EQ(1, has_called);
  ASSERT_EQ(0, segment_.TraverseLogicalBucket(cursor, hfun, cb));
  ASSERT_EQ(1, has_called);
  EXPECT_EQ(0, segment_.GetVersion(0));
}

TEST_F(DashTest, Segment) {
  std::unique_ptr<Segment> seg(new Segment(1));
  LOG(INFO) << "Segment size " << sizeof(Segment)
            << " malloc size: " << malloc_usable_size(seg.get());

  set<Segment::Key_t> keys = FillSegment(0);

  EXPECT_TRUE(segment_.GetBucket(0).IsFull() && segment_.GetBucket(1).IsFull());
  for (size_t i = 2; i < Segment::kNumBuckets; ++i) {
    EXPECT_EQ(0, segment_.GetBucket(i).Size());
  }
  EXPECT_EQ(4 * Segment::kNumSlots, keys.size());
  EXPECT_EQ(4 * Segment::kNumSlots, segment_.SlowSize());

  auto hfun = &UInt64Policy::HashFn;
  unsigned has_called = 0;

  auto cb = [&](const Segment::Iterator& it) {
    ++has_called;
    ASSERT_EQ(1, keys.count(segment_.Key(it.index, it.slot)));
  };

  segment_.TraverseAll(cb);
  ASSERT_EQ(keys.size(), has_called);

  ASSERT_TRUE(segment_.GetBucket(Segment::kNumBuckets).IsFull());
  std::array<uint64_t, Segment::kNumSlots * 2> arr;
  uint64_t* next = arr.begin();
  for (unsigned i = Segment::kNumBuckets; i < Segment::kNumBuckets + 2; ++i) {
    const auto* k = &segment_.Key(i, 0);
    next = std::copy(k, k + Segment::kNumSlots, next);
  }
  std::equal_to<Segment::Key_t> eq;
  for (auto k : arr) {
    auto hash = hfun(k);
    auto it = segment_.FindIt(k, hash, eq);
    ASSERT_TRUE(it.found());
    segment_.Delete(it, hash);
  }
  EXPECT_EQ(2 * Segment::kNumSlots, segment_.SlowSize());
  ASSERT_FALSE(Contains(arr.front()));
}

TEST_F(DashTest, SegmentFull) {
  std::equal_to<Segment::Key_t> eq;

  for (Segment::Key_t key = 8000; key < 15000u; ++key) {
    uint64_t hash = dt_.DoHash(key);
    bool res = segment_.Insert(key, 0, hash, eq).second;
    if (!res) {
      LOG(INFO) << "Stopped at " << key;
      break;
    }
  }
  EXPECT_GT(segment_.SlowSize(), Segment::capacity() * 0.85);

  LOG(INFO) << "Utilization " << double(segment_.SlowSize()) / Segment::capacity()
            << " num probing buckets: " << segment_.NumProbingBuckets();

  LOG(INFO) << "NB: " << segment_.stats.neighbour_probes << " SP: " << segment_.stats.stash_probes
            << " SOP: " << segment_.stats.stash_overflow_probes;
  segment_.stats.neighbour_probes = segment_.stats.stash_overflow_probes =
      segment_.stats.stash_probes = 0;
  for (Segment::Key_t key = 0; key < 10000u; ++key) {
    Contains(key);
  }
  LOG(INFO) << segment_.stats.neighbour_probes << " " << segment_.stats.stash_probes << " "
            << segment_.stats.stash_overflow_probes;

  uint32_t busy = segment_.GetBucket(0).GetBusy();
  uint32_t probe = segment_.GetBucket(0).GetProbe(true);

  EXPECT_EQ((1 << 12) - 1, busy);  // Size 12
  EXPECT_EQ(539, probe);           // verified by running since the test is determenistic.

  unsigned keys[12] = {8045, 8085, 8217, 8330, 8337, 8381, 8432, 8506, 8587, 8605, 8612, 8725};
  for (unsigned i = 0; i < 12; ++i) {
    ASSERT_EQ(keys[i], segment_.Key(0, i));
  }
}

TEST_F(DashTest, Split) {
  set<Segment::Key_t> keys = FillSegment(0);
  Segment::Value_t val;
  Segment s2{2};

  segment_.Split(&UInt64Policy::HashFn, &s2);
  unsigned sum[2] = {0};
  std::equal_to<Segment::Key_t> eq;
  for (auto key : keys) {
    auto it1 = segment_.FindIt(key, dt_.DoHash(key), eq);
    auto it2 = s2.FindIt(key, dt_.DoHash(key), eq);
    ASSERT_NE(it1.found(), it2.found()) << key;

    sum[0] += it1.found();
    sum[1] += it2.found();
  }

  ASSERT_EQ(segment_.SlowSize(), sum[0]);
  EXPECT_EQ(s2.SlowSize(), sum[1]);
  EXPECT_EQ(keys.size(), sum[0] + sum[1]);
  EXPECT_EQ(4 * Segment::kNumSlots, keys.size());
}

TEST_F(DashTest, BumpUp) {
  set<Segment::Key_t> keys = FillSegment(0);
  constexpr unsigned kFirstStashId = Segment::kNumBuckets;
  constexpr unsigned kSecondStashId = Segment::kNumBuckets + 1;
  constexpr unsigned kNumSlots = Segment::kNumSlots;

  EXPECT_TRUE(segment_.GetBucket(0).IsFull());
  EXPECT_TRUE(segment_.GetBucket(1).IsFull());
  EXPECT_TRUE(segment_.GetBucket(kFirstStashId).IsFull());
  EXPECT_TRUE(segment_.GetBucket(kSecondStashId).IsFull());

  // Segment::Iterator it{kFirstStashId, 1};
  Segment::Key_t key = segment_.Key(1, 2);  // key at bucket 1, slot 2
  uint8_t touched_bid[3];

  uint64_t hash = dt_.DoHash(key);

  segment_.Delete(Segment::Iterator{1, 2}, hash);
  EXPECT_FALSE(segment_.GetBucket(1).IsFull());

  segment_.SetVersion(kFirstStashId, 1);
  key = segment_.Key(kFirstStashId, 5);
  hash = dt_.DoHash(key);

  EXPECT_EQ(1, segment_.CVCOnBump(1, kFirstStashId, 5, hash, touched_bid));
  EXPECT_EQ(touched_bid[0], 1);

  // Bump up
  segment_.BumpUp(kFirstStashId, 5, hash);

  // expect the key to move
  EXPECT_TRUE(segment_.GetBucket(1).IsFull());
  EXPECT_FALSE(segment_.GetBucket(kFirstStashId).IsFull());
  EXPECT_EQ(segment_.Key(1, 2), key);
  EXPECT_TRUE(Contains(key));

  // 9 is just a random slot id.
  key = segment_.Key(kSecondStashId, 9);
  hash = dt_.DoHash(key);

  EXPECT_EQ(1, segment_.CVCOnBump(2, kSecondStashId, 9, hash, touched_bid));
  EXPECT_EQ(touched_bid[0], kSecondStashId);

  segment_.BumpUp(kSecondStashId, 9, hash);
  ASSERT_TRUE(key == segment_.Key(0, kNumSlots - 1) || key == segment_.Key(1, kNumSlots - 1));
  EXPECT_TRUE(segment_.GetBucket(kSecondStashId).IsFull());
  EXPECT_TRUE(Contains(key));
  EXPECT_TRUE(segment_.Key(kSecondStashId, 9));
}

TEST_F(DashTest, Insert2) {
  uint64_t k = 1191;
  ASSERT_EQ(2019837007031366716, UInt64Policy::HashFn(k));

  Dash64 dt;
  for (unsigned i = 0; i < 2000; ++i) {
    dt.Insert(i, 0);
  }
}

TEST_F(DashTest, InsertOOM) {
  CappedResource resource(1 << 15);
  Dash64 dt{1, UInt64Policy{}, &resource};

  ASSERT_THROW(
      {
        for (size_t i = 0; i < (1 << 14); ++i) {
          dt.Insert(i, 0);
        }
      },
      bad_alloc);
}

struct Item {
  char buf[24];
};

constexpr size_t ItemAlign = alignof(Item);

struct MyBucket : public detail::BucketBase<16, 4> {
  Item key[14];
};

constexpr size_t kMySz = sizeof(MyBucket);
constexpr size_t kBBSz = sizeof(detail::BucketBase<16, 4>);

TEST_F(DashTest, Custom) {
  using ItemSegment = detail::Segment<Item, uint64_t>;
  constexpr double kTax = ItemSegment::kTaxSize;
  constexpr size_t kMaxSize = ItemSegment::kMaxSize;
  constexpr size_t kSegSize = sizeof(ItemSegment);
  constexpr size_t kBuckSz = ItemSegment::kBucketSz;
  (void)kTax;
  (void)kMaxSize;
  (void)kSegSize;
  (void)kBuckSz;

  ItemSegment seg{2};
  auto cb = [](auto v, auto u) { return v.buf[0] == u.buf[0] && v.buf[1] == u.buf[1]; };

  auto it = seg.FindIt(Item{1, 1}, 42, cb);
  ASSERT_FALSE(it.found());
}

TEST_F(DashTest, Reserve) {
  unsigned bc = dt_.bucket_count();
  for (unsigned i = 0; i <= bc * 2; ++i) {
    dt_.Reserve(i);
    ASSERT_GE((1 << dt_.depth()) * Dash64::kSegCapacity, i);
  }
}

TEST_F(DashTest, Insert) {
  constexpr size_t kNumItems = 10000;
  double sum = 0;
  for (size_t i = 0; i < kNumItems; ++i) {
    dt_.Insert(i, i);
    double u = (dt_.size() * 100.0) / (dt_.unique_segments() * Segment::capacity());

    sum += u;
    VLOG(1) << "Num items " << dt_.size() << ", load factor " << u << ", size per entry "
            << double(dt_.mem_usage()) / dt_.size();
  }
  EXPECT_EQ(kNumItems, dt_.size());
  LOG(INFO) << "Average load factor is " << sum / kNumItems;

  for (size_t i = 0; i < kNumItems; ++i) {
    Dash64::const_iterator it = dt_.Find(i);
    ASSERT_TRUE(it != dt_.end());

    ASSERT_EQ(it->second, i);
    ASSERT_LE(dt_.load_factor(), 1) << i;
  }

  for (size_t i = kNumItems; i < kNumItems * 10; ++i) {
    Dash64::const_iterator it = dt_.Find(i);
    ASSERT_TRUE(it == dt_.end());
  }

  EXPECT_EQ(kNumItems, dt_.size());
  EXPECT_EQ(1, dt_.Erase(0));
  EXPECT_EQ(0, dt_.Erase(0));
  EXPECT_EQ(kNumItems - 1, dt_.size());

  auto it = dt_.begin();
  ASSERT_FALSE(it.is_done());
  auto some_val = it->second;
  dt_.Erase(it);
  ASSERT_TRUE(dt_.Find(some_val).is_done());
}

TEST_F(DashTest, Traverse) {
  constexpr auto kNumItems = 50;
  for (size_t i = 0; i < kNumItems; ++i) {
    dt_.Insert(i, i);
  }

  Dash64::cursor cursor;
  vector<unsigned> nums;
  auto tr_cb = [&](Dash64::iterator it) {
    nums.push_back(it->first);
    VLOG(1) << it.bucket_id() << " " << it.slot_id() << " " << it->first;
  };

  do {
    cursor = dt_.Traverse(cursor, tr_cb);
  } while (cursor);
  sort(nums.begin(), nums.end());
  nums.resize(unique(nums.begin(), nums.end()) - nums.begin());
  ASSERT_EQ(kNumItems, nums.size());
  EXPECT_EQ(0, nums[0]);
  EXPECT_EQ(kNumItems - 1, nums.back());
}

TEST_F(DashTest, Bucket) {
  constexpr auto kNumItems = 250;
  for (size_t i = 0; i < kNumItems; ++i) {
    dt_.Insert(i, 0);
  }
  std::vector<uint64_t> s;
  auto it = dt_.begin();
  auto bucket_it = Dash64::bucket_it(it);

  dt_.TraverseBucket(it, [&](auto i) { s.push_back(i->first); });

  unsigned num_items = 0;
  while (!bucket_it.is_done()) {
    ASSERT_TRUE(find(s.begin(), s.end(), bucket_it->first) != s.end());
    ++bucket_it;
    ++num_items;
  }
  EXPECT_EQ(s.size(), num_items);
}

struct TestEvictionPolicy {
  static constexpr bool can_evict = true;
  static constexpr bool can_gc = false;

  explicit TestEvictionPolicy(unsigned max_cap) : max_capacity(max_cap) {
  }

  bool CanGrow(const Dash64& tbl) const {
    return tbl.bucket_count() < max_capacity;
  }

  void RecordSplit(Dash64::Segment_t*) {
  }

  unsigned Evict(const Dash64::EvictionBuckets& eb, Dash64* me) const {
    if (!evict_enabled)
      return 0;

    auto it = eb.iter[0];
    unsigned res = 0;
    for (; !it.is_done(); ++it) {
      LOG(INFO) << "Deleting " << it->first;
      me->Erase(it);
      ++res;
    }

    return res;
  }

  bool evict_enabled = false;
  unsigned max_capacity;
};

TEST_F(DashTest, Eviction) {
  TestEvictionPolicy ev(1500);

  size_t i = 0;
  auto loop = [&] {
    for (; i < 5000; ++i) {
      dt_.Insert(i, 0, ev);
    }
  };

  ASSERT_THROW(loop(), bad_alloc);
  ASSERT_LT(i, 5000);
  EXPECT_LT(dt_.size(), ev.max_capacity);
  LOG(INFO) << "size is " << dt_.size();

  ev.evict_enabled = true;
  unsigned bucket_cnt = dt_.bucket_count();
  auto [it, res] = dt_.Insert(i, 0, ev);
  EXPECT_TRUE(res);
  EXPECT_EQ(bucket_cnt, dt_.bucket_count());
}

struct VersionPolicy : public BasicDashPolicy {
  static constexpr bool kUseVersion = true;

  static uint64_t HashFn(int v) {
    return XXH3_64bits(&v, sizeof(v));
  }
};

TEST_F(DashTest, Version) {
  DashTable<int, int, VersionPolicy> dt;
  auto [it, inserted] = dt.Insert(1, 1);

  EXPECT_EQ(0, it.GetVersion());
  it.SetVersion(5);
  EXPECT_EQ(5, it.GetVersion());

  dt.Clear();
  constexpr int kNum = 68000;
  for (int i = 0; i < kNum; ++i) {
    auto it = dt.Insert(i, 0).first;
    it.SetVersion(i + 65000);
    if (i) {
      auto p = dt.Find(i - 1);
      ASSERT_GE(p.GetVersion(), i - 1 + 65000) << i;
    }
  }

  unsigned items = 0;
  for (auto it = dt.begin(); it != dt.end(); ++it) {
    ASSERT_FALSE(it.is_done());
    ASSERT_GE(it.GetVersion(), it->first + 65000)
        << it.segment_id() << " " << it.bucket_id() << " " << it.slot_id();
    ++items;
  }
  ASSERT_EQ(kNum, items);
}

struct A {
  int a = 0;
  unsigned moved = 0;

  A(int i = 0) : a(i) {
  }
  A(const A&) = delete;
  A(A&& o) : a(o.a), moved(o.moved + 1) {
    o.a = -1;
  }

  A& operator=(const A&) = delete;
  A& operator=(A&& o) {
    o.moved = o.moved + 1;
    a = o.a;
    o.a = -1;
    return *this;
  }

  bool operator==(const A& o) const {
    return o.a == a;
  }
};

struct ADashPolicy : public BasicDashPolicy {
  static uint64_t HashFn(const A& a) {
    auto val = XXH3_64bits(&a.a, sizeof(a.a));
    return val;
  }
};

TEST_F(DashTest, Moveable) {
  using DType = DashTable<A, A, ADashPolicy>;

  DType table{1};
  ASSERT_TRUE(table.Insert(A{1}, A{2}).second);
  ASSERT_FALSE(table.Insert(A{1}, A{3}).second);
  EXPECT_EQ(1, table.size());
  table.Clear();
  EXPECT_EQ(0, table.size());
}

struct SdsDashPolicy {
  enum { kSlotNum = 12, kBucketNum = 64, kStashBucketNum = 2 };
  static constexpr bool kUseVersion = false;

  static uint64_t HashFn(sds u) {
    return XXH3_64bits(reinterpret_cast<const uint8_t*>(u), sdslen(u));
  }

  static uint64_t HashFn(std::string_view u) {
    return XXH3_64bits(u.data(), u.size());
  }

  static void DestroyValue(uint64_t) {
  }
  static void DestroyKey(sds s) {
    sdsfree(s);
  }

  static bool Equal(sds u1, sds u2) {
    return dictSdsKeyCompare(nullptr, u1, u2) == 0;
  }

  static bool Equal(sds u1, std::string_view u2) {
    return u2 == std::string_view{u1, sdslen(u1)};
  }
};

TEST_F(DashTest, Sds) {
  DashTable<sds, uint64_t, SdsDashPolicy> dt;

  sds foo = sdscatlen(sdsempty(), "foo", 3);
  dt.Insert(foo, 0);
  // dt.Insert(std::string_view{"bar"}, 1);
}

/**
 ______     _      _   _               _______        _
|  ____|   (_)    | | (_)             |__   __|      | |
| |____   ___  ___| |_ _  ___  _ __      | | ___  ___| |_ ___
|  __\ \ / / |/ __| __| |/ _ \| '_ \     | |/ _ \/ __| __/ __|
| |___\ V /| | (__| |_| | (_) | | | |    | |  __/\__ \ |_\__ \
|______\_/ |_|\___|\__|_|\___/|_| |_|    |_|\___||___/\__|___/
 *
 */
struct EvictParams {
  bool use_bumpups;
  double zipf_param;

  string PrintTo() const {
    string name = absl::StrCat(use_bumpups ? "" : "no", "bumps");
    absl::StrAppend(&name, unsigned(zipf_param * 1000));

    return name;
  }
};

string PrintParams(const testing::TestParamInfo<EvictParams>& info) {
  return info.param.PrintTo();
}

struct U64DashPolicy {
  enum { kSlotNum = 14, kBucketNum = 64, kStashBucketNum = 4 };
  static constexpr bool kUseVersion = false;

  static void DestroyValue(uint64_t) {
  }
  static void DestroyKey(uint64_t) {
  }

  static bool Equal(uint64_t u, uint64_t v) {
    return u == v;
  }

  static uint64_t HashFn(uint64_t v) {
    return XXH3_64bits(&v, sizeof(v));
  }
};

using U64Dash = DashTable<uint64_t, unsigned, U64DashPolicy>;

struct SimpleEvictPolicy {
  static constexpr bool can_gc = false;
  static constexpr bool can_evict = true;

  bool CanGrow(const U64Dash& tbl) {
    return tbl.bucket_count() + U64Dash::kSegCapacity < max_capacity;
  }

  void RecordSplit(U64Dash::Segment_t* segment) {
  }

  // Required interface in case can_gc is true
  // returns number of items evicted from the table.
  // 0 means - nothing has been evicted.
  unsigned Evict(const U64Dash::EvictionBuckets& eb, U64Dash* me) {
    // kBucketSize - number of slots in the bucket.
    constexpr size_t kNumSlots = ABSL_ARRAYSIZE(eb.iter) * U64Dash::kBucketSize;

    unsigned slot_index = std::uniform_int_distribution<uint32_t>{0, kNumSlots - 1}(rand_eng_);
    auto it = eb.iter[slot_index / U64Dash::kBucketSize];
    it += (slot_index % Dash64::kBucketSize);

    DCHECK(!it.is_done());
    me->Erase(it);

    return 1;
  }

  size_t max_capacity = SIZE_MAX;
  default_random_engine rand_eng_{42};
};

struct ShiftRightPolicy {
  absl::flat_hash_map<uint64_t, unsigned> evicted;
  unsigned index = 0;
  size_t max_capacity = SIZE_MAX;

  static constexpr bool can_gc = false;
  static constexpr bool can_evict = true;

  bool CanGrow(const U64Dash& tbl) {
    return tbl.bucket_count() + U64Dash::kSegCapacity < max_capacity;
  }

  void RecordSplit(U64Dash::Segment_t* segment) {
  }

  unsigned Evict(const U64Dash::EvictionBuckets& eb, U64Dash* me) {
    constexpr unsigned kBucketNum = ABSL_ARRAYSIZE(eb.iter);
    constexpr unsigned kNumStashBuckets = kBucketNum - 2;

    unsigned stash_pos = index++ % kNumStashBuckets;
    auto stash_it = eb.iter[2 + stash_pos];
    stash_it += (U64Dash::kBucketSize - 1);  // go to the last slot.

    uint64_t k = stash_it->first;
    DVLOG(1) << "Deleting key " << k << " from " << stash_it.bucket_id() << "/"
             << stash_it.slot_id();
    evicted[k]++;

    return me->ShiftRight(stash_it);
  };
};

class EvictionPolicyTest : public testing::TestWithParam<EvictParams> {
 protected:
  template <typename Policy> void FillUniform(unsigned max_range, Policy& policy);

  uint64_t Rand() {
    return zipf_ ? zipf_->Next(rand_eng_) : udist_(rand_eng_);
  }

  void SetUp() final {
    if (GetParam().zipf_param > 0)
      zipf_.emplace(0, 15000, GetParam().zipf_param);
    else {
      uniform_int_distribution<uint64_t>::param_type p{0, 15000};
      udist_.param(p);
    }
  }

  default_random_engine rand_eng_{42};
  U64Dash dt_;
  std::optional<base::ZipfianGenerator> zipf_;
  uniform_int_distribution<uint64_t> udist_;
};

template <typename Policy>
void EvictionPolicyTest::FillUniform(unsigned max_range, Policy& policy) {
  std::uniform_int_distribution<uint64_t> dist(0, max_range - 1);
  for (unsigned i = 0; i < 100000; ++i) {
    auto [it, res] = dt_.Insert(dist(rand_eng_), 0, policy);
    if (!res && it.is_done())  // filled up till the capacity limit
      break;
  }
  LOG(INFO) << dt_.size();
}

TEST_P(EvictionPolicyTest, HitRate) {
  CHECK_LT(GetParam().zipf_param, 1);
  SimpleEvictPolicy ev_policy;
  ev_policy.max_capacity = 3000;
  FillUniform(15000, ev_policy);

  unsigned hits = 0;
  for (unsigned i = 0; i < 150000; ++i) {
    auto [it, res] = dt_.Insert(Rand(), 0, ev_policy);
    CHECK(!it.is_done());
    if (!res) {
      ++hits;
    }
  }
  LOG(INFO) << "Zipf: " << GetParam().zipf_param << ", hits " << hits << " evictions "
            << dt_.evicted();
}

TEST_P(EvictionPolicyTest, HitRateZipf) {
  base::ZipfianGenerator gen(1, 15000, 0.9);
  SimpleEvictPolicy ev_policy;
  ev_policy.max_capacity = 3000;

  FillUniform(15000, ev_policy);

  bool use_bumps = GetParam().use_bumpups;

  unsigned hits = 0;
  for (unsigned i = 0; i < 150000; ++i) {
    uint64_t key = Rand();
    auto [it, res] = dt_.Insert(key, 0, ev_policy);
    CHECK(!it.is_done());
    if (res) {
      DVLOG(1) << "Inserted new key " << key << " to bucket " << it.bucket_id() << " slot "
               << it.slot_id();
    } else {
      if (use_bumps)
        dt_.BumpUp(it);
      ++hits;
    }
  }
  LOG(INFO) << "Zipf: " << GetParam().PrintTo() << " hits " << hits << " evictions "
            << dt_.evicted();
}

TEST_P(EvictionPolicyTest, HitRateZipfShr) {
  ShiftRightPolicy ev_policy;
  ev_policy.max_capacity = 3000;

  FillUniform(15000, ev_policy);

  unsigned hits = 0;
  unsigned inserted_evicted = 0;
  bool use_bumps = GetParam().use_bumpups;
  for (unsigned i = 0; i < 150000; ++i) {
    unsigned key = Rand();

    auto [it, res] = dt_.Insert(key, 0, ev_policy);
    if (!it.is_done()) {
      if (res) {
        DVLOG(1) << "Inserted new key " << key << " to bucket " << it.bucket_id() << " slot "
                 << it.slot_id();
        if (ev_policy.evicted.contains(key)) {
          ++inserted_evicted;
        }
      } else {
        if (use_bumps) {
          dt_.BumpUp(it);
          DVLOG(1) << "Bump up key " << key << " " << it.bucket_id() << " slot " << it.slot_id();
        } else {
          DVLOG(1) << "Hit on key " << key;
        }
        ++hits;
      }
    }
  }

  vector<pair<unsigned, uint64_t>> freq_evicted;
  for (const auto& k_v : ev_policy.evicted) {
    freq_evicted.emplace_back(k_v.second, k_v.first);
  }
  sort(freq_evicted.rbegin(), freq_evicted.rend());

  LOG(INFO) << "Params " << GetParam().PrintTo() << " hits " << hits << " evictions "
            << dt_.evicted() << " "
            << "reinserted " << inserted_evicted;
  unsigned num_outs = 0;
  for (const auto& k_v : freq_evicted) {
    LOG(INFO) << "Evicted " << k_v.first << " : " << k_v.second;
    if (++num_outs > 100 || k_v.first < 5)
      break;
  }
}

INSTANTIATE_TEST_SUITE_P(Eviction, EvictionPolicyTest,
                         testing::Values(EvictParams{false, 0}, EvictParams{false, 0.9},
                                         EvictParams{true, 0.9}),
                         PrintParams);

// Benchmarks
static void BM_Insert(benchmark::State& state) {
  unsigned count = state.range(0);

  size_t next = 0;
  while (state.KeepRunning()) {
    Dash64 dt;

    for (unsigned i = 0; i < count; ++i) {
      dt.Insert(next++, 0);
    }
  }
}
BENCHMARK(BM_Insert)->Arg(1000)->Arg(10000)->Arg(100000);

struct NoDestroySdsPolicy : public SdsDashPolicy {
  static void DestroyKey(sds s) {
  }
};

static void BM_StringInsert(benchmark::State& state) {
  unsigned count = state.range(0);

  std::vector<sds> strs(count);
  for (unsigned i = 0; i < count; ++i) {
    strs[i] = sdscatprintf(sdsempty(), "key__%x", 100 + i);
  }

  while (state.KeepRunning()) {
    DashTable<sds, uint64_t, NoDestroySdsPolicy> dt;

    for (unsigned i = 0; i < count; ++i) {
      dt.Insert(strs[i], 0);
    }
  }

  for (sds s : strs) {
    sdsfree(s);
  }
}
BENCHMARK(BM_StringInsert)->Arg(1000)->Arg(10000)->Arg(100000);

static void BM_FindExisting(benchmark::State& state) {
  unsigned count = state.range(0);

  Dash64 dt;
  for (unsigned i = 0; i < count; ++i) {
    dt.Insert(i, 0);
  }

  size_t next = 0;
  while (state.KeepRunning()) {
    for (unsigned i = 0; i < 100; ++i) {
      dt.Find(next++);
    }
  }
}
BENCHMARK(BM_FindExisting)->Arg(1000000)->Arg(2000000);

// dict memory usage is in [32*n + 8*n, 32*n + 16*n], or
// per entry usage is [40, 48].
static void BM_RedisDictFind(benchmark::State& state) {
  unsigned count = state.range(0);
  dict* d = dictCreate(&IntDict);

  for (unsigned i = 0; i < count; ++i) {
    size_t key = i;
    dictAdd(d, (void*)key, nullptr);
  }

  size_t next = 0;
  while (state.KeepRunning()) {
    for (size_t i = 0; i < 100; ++i) {
      size_t k = next++;
      dictFind(d, (void*)k);
    }
  }
  dictRelease(d);
}
BENCHMARK(BM_RedisDictFind)->Arg(1000000)->Arg(2000000);

// dict memory usage is in [32*n + 8*n, 32*n + 16*n], or
// per entry usage is [40, 48].
static void BM_RedisDictInsert(benchmark::State& state) {
  unsigned count = state.range(0);
  size_t next = 0;
  while (state.KeepRunning()) {
    dict* d = dictCreate(&IntDict);
    for (unsigned i = 0; i < count; ++i) {
      dictAdd(d, (void*)next, nullptr);
      ++next;
    }
    dictRelease(d);
  }
}
BENCHMARK(BM_RedisDictInsert)->Arg(1000)->Arg(10000)->Arg(100000);

static void BM_RedisStringInsert(benchmark::State& state) {
  unsigned count = state.range(0);
  std::vector<sds> strs(count);
  for (unsigned i = 0; i < count; ++i) {
    strs[i] = sdscatprintf(sdsempty(), "key__%x", 100 + i);
  }

  while (state.KeepRunning()) {
    dict* d = dictCreate(&SdsDict);
    for (unsigned i = 0; i < count; ++i) {
      dictAdd(d, strs[i], nullptr);
    }
    dictRelease(d);
  }

  for (sds s : strs) {
    sdsfree(s);
  }
}
BENCHMARK(BM_RedisStringInsert)->Arg(1000)->Arg(10000)->Arg(100000);

}  // namespace dfly
