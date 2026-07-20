// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_map.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <mimalloc.h>

#include <cstring>
#include <random>
#include <string>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/detail/bitpacking.h"
#include "core/mi_memory_resource.h"
#include "core/page_usage/page_usage_stats.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;

// Encodes a logical key the way OAHMap does, then serializes it into a pair blob.
static uint64_t CreatePair(string_view key, string_view value, uint32_t expiry = UINT32_MAX) {
  const ASCIIStr ek(key);
  return OAHPair::Create(ek.content(), ek.len(), value, expiry);
}

// Decodes a pair's logical key (production does this at the table level, via OAHMap::DecodeKey).
static string KeyOf(OAHPair p) {
  const oah::key::Decoded key = OAHMap::DecodeKey(p);
  return string{key.view()};
}

// Mirrors string_map_test.cc for the OAHMap (OAHTable<OAHPair>) container. The StringMap SdsPair
// iterator (it->first/it->second) maps to the OAHPair accessor (KeyOf(*it)/it->Value()), and
// AddOrExchange/Extract return an OwnedOAHPair (RAII, frees on destruction) instead of an SdsEntry.
class OAHMapTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    auto* tlh = mi_heap_get_backing();
    init_zmalloc_threadlocal(tlh);
    InitTLStatelessAllocMR(PMR_NS::get_default_resource());
  }

  static void TearDownTestSuite() {
  }

  void SetUp() override {
    m_ = new OAHMap;
    generator_.seed(0);
  }

  void TearDown() override {
    delete m_;
    EXPECT_EQ(zmalloc_used_memory_tl, 0);
  }

  OAHMap* m_;
  mt19937 generator_;
};

static string random_string(mt19937& rand, unsigned len) {
  const string_view alpanum = "1234567890abcdefghijklmnopqrstuvwxyz";
  string ret;
  ret.reserve(len);
  for (size_t i = 0; i < len; ++i)
    ret += alpanum[rand() % alpanum.size()];
  return ret;
}

static string sized_string(size_t len, char fill) {
  string result(len, fill);
  if (len > 2)
    result[len / 2] = '\0';
  if (len)
    result.back() = static_cast<char>(fill + 1);
  return result;
}

class SelectivePageUsage : public PageUsage {
 public:
  explicit SelectivePageUsage(void* target, void* second_target = nullptr)
      : PageUsage(CollectPageStats::NO, 0), target_(target), second_target_(second_target) {
  }

  bool IsPageForObjectUnderUtilized(void* object) override {
    return object == target_ || object == second_target_;
  }

 private:
  void* target_;
  void* second_target_;
};

static size_t SumEntryAlloc(OAHMap& map) {
  size_t result = 0;
  for (OAHPair pair : map)
    result += pair.AllocSize();
  return result;
}

TEST_F(OAHMapTest, Basic) {
  EXPECT_TRUE(m_->AddOrUpdate("foo", "bar"));
  EXPECT_TRUE(m_->Contains("foo"));
  auto it = m_->Find("foo");
  EXPECT_EQ(it->Value(), "bar"sv);

  it = m_->begin();
  EXPECT_EQ(KeyOf(*it), "foo"sv);
  EXPECT_EQ(it->Value(), "bar"sv);
  ++it;
  EXPECT_TRUE(it == m_->end());

  for (auto e : *m_) {
    EXPECT_EQ(KeyOf(e), "foo"sv);
    EXPECT_EQ(e.Value(), "bar"sv);
  }

  size_t sz = m_->ObjMallocUsed();
  EXPECT_FALSE(m_->AddOrUpdate("foo", "baraaaaaaaaaaaa2"));
  EXPECT_GT(m_->ObjMallocUsed(), sz);
  EXPECT_EQ(m_->Find("foo")->Value(), "baraaaaaaaaaaaa2"sv);

  EXPECT_FALSE(m_->AddOrSkip("foo", "bar2"));
  EXPECT_EQ(m_->Find("foo")->Value(), "baraaaaaaaaaaaa2"sv);

  auto value = m_->GetValue("foo");
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, "baraaaaaaaaaaaa2"sv);
  EXPECT_FALSE(m_->GetValue("missing").has_value());
}

TEST_F(OAHMapTest, EmptyKeyAndValue) {
  // Empty members arrive as non-null empty views (""sv), never a null std::string_view{} -- see
  // CmdArgParser::SafeSV. OAHPair::Create relies on that invariant (DCHECK on the data pointers).
  uint64_t bits = CreatePair(""sv, ""sv);
  OAHPair pair(bits);
  EXPECT_TRUE(KeyOf(pair).empty());
  EXPECT_TRUE(pair.Value().empty());
  OAHPair::Destroy(pair.Release());

  EXPECT_TRUE(m_->AddOrUpdate("", ""));
  auto value = m_->GetValue("");
  ASSERT_TRUE(value.has_value());
  EXPECT_TRUE(value->empty());
  auto it = m_->Find("");
  ASSERT_NE(it, m_->end());
  EXPECT_TRUE(KeyOf(*it).empty());
  EXPECT_TRUE(it->Value().empty());
  EXPECT_TRUE(m_->Erase(""));
  EXPECT_FALSE(m_->GetValue("").has_value());
}

TEST_F(OAHMapTest, OAHPairSizeEncoding) {
  for (uint32_t key_size : {0u, 1u, 31u, 32u, 63u, 64u, 255u, 8191u, 8192u, 100000u}) {
    const string key = sized_string(key_size, 'k');
    for (uint32_t value_size : {0u, 1u, 31u, 32u, 63u, 64u, 255u, 8191u, 8192u, 100000u}) {
      const string value = sized_string(value_size, 'v');
      for (uint32_t expiry : {UINT32_MAX, 7u}) {
        const size_t used_before = zmalloc_used_memory_tl;
        uint64_t bits = CreatePair(key, value, expiry);
        OAHPair pair(bits);

        EXPECT_EQ(KeyOf(pair), key);
        EXPECT_EQ(pair.Value(), value);
        EXPECT_EQ(pair.HasExpiry(), expiry != UINT32_MAX);
        EXPECT_EQ(pair.GetExpiry(), expiry);
        EXPECT_EQ(pair.AllocSize(), zmalloc_used_memory_tl - used_before);

        // Decode the fields directly from the blob:
        //   [expiry?, key_header, value_size, value_ptr:8B, key_content]
        const char* key_header = pair.Raw() + (expiry != UINT32_MAX) * sizeof(expiry);
        const oah::key::Header kh = oah::key::ReadHeader(key_header);
        EXPECT_EQ(kh.len, key_size);
        EXPECT_EQ(kh.encoded, ASCIIStr(key).encoded());
        const char* value_size_field = key_header + kh.field_size;
        const oah::size::Decoded decoded_value_size = oah::size::Read(value_size_field);
        const char* value_ptr_field = value_size_field + decoded_value_size.field_size;
        EXPECT_EQ(decoded_value_size.size, value_size);

        char* stored_value = nullptr;
        std::memcpy(&stored_value, value_ptr_field, sizeof(stored_value));
        EXPECT_EQ(stored_value, pair.Value().data());

        // Key content (ascii-packed or raw) is stored right after the value pointer.
        const char* key_content = value_ptr_field + sizeof(stored_value);
        EXPECT_EQ(pair.KeyContent().data(), key_content);
        EXPECT_EQ(pair.KeyContent().size(), ASCIIStr(key).content().size());

        const uintptr_t raw = reinterpret_cast<uintptr_t>(pair.Raw());
        const uintptr_t raw_end = raw + zmalloc_usable_size(pair.Raw());
        const uintptr_t value_data = reinterpret_cast<uintptr_t>(pair.Value().data());
        if (value_size == 0) {
          EXPECT_EQ(pair.Value().data(), nullptr);  // empty value: no buffer
        } else {
          EXPECT_TRUE(value_data < raw || value_data >= raw_end);  // outside the blob
          EXPECT_EQ(value_data % 8, 0u);  // separate zmalloc buffer is >=8-byte aligned
        }

        OAHPair::Destroy(pair.Release());
        EXPECT_EQ(zmalloc_used_memory_tl, used_before);
      }
    }
  }
}

TEST_F(OAHMapTest, AsciiEncoding) {
  string ascii_key(80, 'a');                // 80 ascii chars -> 70 packed bytes
  string boundary(128, 'b');                // largest ascii-encodable field
  string too_long(129, 'c');                // one over -> raw
  string non_ascii(80, 'd');                //
  non_ascii[40] = static_cast<char>(0xC3);  // a non-ascii byte -> raw
  const string value = "some-value";

  for (string* k : {&ascii_key, &boundary, &too_long, &non_ascii})
    EXPECT_TRUE(m_->AddOrUpdate(*k, value)) << k->size();

  // Encodable fields are packed; the others stay raw. The value is never encoded.
  auto it = m_->Find(ascii_key);
  ASSERT_NE(it, m_->end());
  EXPECT_EQ(it->KeyContent().size(), detail::binpacked_len(80));
  EXPECT_EQ(it->Value(), value);
  EXPECT_EQ(m_->Find(boundary)->KeyContent().size(), detail::binpacked_len(128));
  EXPECT_EQ(m_->Find(too_long)->KeyContent().size(), too_long.size());
  EXPECT_EQ(m_->Find(non_ascii)->KeyContent().size(), non_ascii.size());

  // Updating an existing encoded field works, and lookups respect the logical length.
  EXPECT_FALSE(m_->AddOrUpdate(ascii_key, "v2"));
  EXPECT_EQ(m_->Find(ascii_key)->Value(), "v2"sv);
  EXPECT_FALSE(m_->Contains(string(79, 'a')));
  EXPECT_FALSE(m_->Contains(string(81, 'a')));

  // Iteration reconstructs the logical fields.
  vector<string> keys;
  for (auto e = m_->begin(); e != m_->end(); ++e)
    keys.push_back(string{KeyOf(*e)});
  EXPECT_EQ(keys.size(), 4u);
  auto has = [&](const string& k) {
    for (const string& x : keys)
      if (x == k)
        return true;
    return false;
  };
  for (string* k : {&ascii_key, &boundary, &too_long, &non_ascii})
    EXPECT_TRUE(has(*k)) << k->size();

  for (string* k : {&ascii_key, &boundary, &too_long, &non_ascii})
    EXPECT_TRUE(m_->Extract(*k)) << k->size();
}

TEST_F(OAHMapTest, OAHSizeFieldEncoding) {
  for (auto [size, expected_field_size] :
       {pair{0u, 1u}, pair{63u, 1u}, pair{64u, 2u}, pair{8191u, 2u}, pair{8192u, 4u},
        pair{100000u, 4u}, pair{oah::size::kMaxSize, 4u}}) {
    char field[oah::size::kLargeFieldSize] = {};
    EXPECT_EQ(oah::size::FieldSize(size), expected_field_size);
    oah::size::Write(size, expected_field_size, field);
    const oah::size::Decoded decoded = oah::size::Read(field);
    EXPECT_EQ(decoded.size, size);
    EXPECT_EQ(decoded.field_size, expected_field_size);
  }
}

TEST_F(OAHMapTest, ExternalValueOwnership) {
  const string key = sized_string(8192, 'k');
  const string inline_value = sized_string(8191, 'a');
  const string external_value = sized_string(8192, 'b');
  const string replacement = sized_string(8192, 'c');

  EXPECT_TRUE(m_->AddOrUpdate(key, inline_value));
  ASSERT_NE(m_->Find(key), m_->end());
  EXPECT_EQ(m_->ObjMallocUsed(), m_->Find(key)->AllocSize());

  const size_t used_before_skip = zmalloc_used_memory_tl;
  const size_t obj_before_skip = m_->ObjMallocUsed();
  EXPECT_FALSE(m_->AddOrSkip(key, external_value));
  EXPECT_EQ(zmalloc_used_memory_tl, used_before_skip);
  EXPECT_EQ(m_->ObjMallocUsed(), obj_before_skip);

  size_t used_with_owner = 0;
  size_t owner_alloc = 0;
  {
    auto previous = m_->AddOrExchange(key, external_value);
    ASSERT_TRUE(previous);
    EXPECT_EQ(previous.pair().Value(), inline_value);
    EXPECT_EQ(m_->Find(key)->Value(), external_value);
    EXPECT_EQ(m_->ObjMallocUsed(), m_->Find(key)->AllocSize());
    owner_alloc = previous.pair().AllocSize();
    used_with_owner = zmalloc_used_memory_tl;
  }
  EXPECT_EQ(zmalloc_used_memory_tl, used_with_owner - owner_alloc);

  EXPECT_FALSE(m_->AddOrUpdate(key, replacement, 100));
  EXPECT_EQ(m_->ObjMallocUsed(), m_->Find(key)->AllocSize());

  {
    auto previous = m_->AddOrExchange(key, inline_value, 200);
    ASSERT_TRUE(previous);
    EXPECT_EQ(previous.pair().Value(), replacement);
    EXPECT_EQ(previous.pair().GetExpiry(), 100u);
    EXPECT_EQ(m_->Find(key)->Value(), inline_value);
    EXPECT_EQ(m_->Find(key).ExpiryTime(), 200u);
    EXPECT_EQ(m_->ObjMallocUsed(), m_->Find(key)->AllocSize());
  }

  EXPECT_FALSE(m_->AddOrUpdate(key, replacement));

  const size_t used_before_extract = zmalloc_used_memory_tl;
  size_t extracted_alloc = 0;
  {
    auto extracted = m_->Extract(key);
    ASSERT_TRUE(extracted);
    EXPECT_EQ(KeyOf(extracted.pair()), key);
    EXPECT_EQ(extracted.pair().Value(), replacement);
    EXPECT_EQ(m_->ObjMallocUsed(), 0u);
    EXPECT_EQ(zmalloc_used_memory_tl, used_before_extract);
    extracted_alloc = extracted.pair().AllocSize();
  }
  EXPECT_EQ(zmalloc_used_memory_tl, used_before_extract - extracted_alloc);
}

TEST_F(OAHMapTest, ExtractExpiredExternalValue) {
  const string key = sized_string(8192, 'k');
  const string value = sized_string(8192, 'v');
  EXPECT_TRUE(m_->AddOrUpdate(key, value, 1));
  const size_t pair_alloc = m_->Find(key)->AllocSize();
  const size_t used_before = zmalloc_used_memory_tl;

  m_->set_time(1);
  EXPECT_FALSE(m_->Extract(key));
  EXPECT_EQ(m_->UpperBoundSize(), 0u);
  EXPECT_EQ(m_->ObjMallocUsed(), 0u);
  EXPECT_EQ(zmalloc_used_memory_tl, used_before - pair_alloc);
}

TEST_F(OAHMapTest, ExternalValueExpiryRebuild) {
  const string key = sized_string(8192, 'k');
  const string value = sized_string(8192, 'v');
  EXPECT_TRUE(m_->AddOrUpdate(key, value));

  auto it = m_->Find(key);
  ASSERT_NE(it, m_->end());
  char* old_raw = it->Raw();
  const char* old_value = it->Value().data();
  const uint64_t old_hash = it->GetHash();

  it.SetExpiryTime(7);
  it = m_->Find(key);
  ASSERT_NE(it, m_->end());
  EXPECT_NE(it->Raw(), old_raw);
  EXPECT_EQ(it->Value().data(), old_value);
  EXPECT_EQ(KeyOf(*it), key);
  EXPECT_EQ(it->Value(), value);
  EXPECT_EQ(it->GetHash(), old_hash);
  EXPECT_EQ(it.ExpiryTime(), 7u);
  EXPECT_EQ(m_->ObjMallocUsed(), it->AllocSize());

  old_raw = it->Raw();
  old_value = it->Value().data();
  it.SetExpiryTime(9);
  EXPECT_EQ(it->Raw(), old_raw);
  EXPECT_EQ(it->Value().data(), old_value);
  EXPECT_EQ(it.ExpiryTime(), 9u);

  m_->set_time(9);
  EXPECT_FALSE(m_->Contains(key));
  EXPECT_EQ(m_->UpperBoundSize(), 0u);
  EXPECT_EQ(m_->ObjMallocUsed(), 0u);
}

TEST_F(OAHMapTest, ExternalValueSelectiveRealloc) {
  const string key = sized_string(8192, 'k');
  const string value = sized_string(8192, 'v');
  uint64_t bits = CreatePair(key, value, 11);
  OAHPair pair(bits);
  pair.SetExtHash(123);

  // Only the value's page is flagged: the value buffer is relocated, the blob stays put.
  char* old_raw = pair.Raw();
  const char* old_value = pair.Value().data();
  size_t old_alloc = pair.AllocSize();
  SelectivePageUsage value_only(const_cast<char*>(old_value));
  bool realloced = false;
  ssize_t delta = pair.ReallocIfNeeded(&value_only, &realloced);
  EXPECT_TRUE(realloced);
  EXPECT_EQ(pair.Raw(), old_raw);
  EXPECT_NE(pair.Value().data(), old_value);
  EXPECT_EQ(delta, static_cast<ssize_t>(pair.AllocSize()) - static_cast<ssize_t>(old_alloc));
  EXPECT_EQ(KeyOf(pair), key);
  EXPECT_EQ(pair.Value(), value);
  EXPECT_EQ(pair.GetExpiry(), 11u);
  EXPECT_EQ(pair.GetHash(), 123u);

  // Only the blob's page is flagged: the blob is rebuilt, the value buffer is transferred
  // unchanged.
  old_raw = pair.Raw();
  old_value = pair.Value().data();
  old_alloc = pair.AllocSize();
  SelectivePageUsage raw_only(old_raw);
  delta = pair.ReallocIfNeeded(&raw_only, &realloced);
  EXPECT_TRUE(realloced);
  EXPECT_NE(pair.Raw(), old_raw);
  EXPECT_EQ(pair.Value().data(), old_value);
  EXPECT_EQ(delta, static_cast<ssize_t>(pair.AllocSize()) - static_cast<ssize_t>(old_alloc));
  EXPECT_EQ(KeyOf(pair), key);
  EXPECT_EQ(pair.Value(), value);
  EXPECT_EQ(pair.GetExpiry(), 11u);
  EXPECT_EQ(pair.GetHash(), 123u);

  // Both pages flagged: the blob and the value buffer both move.
  old_raw = pair.Raw();
  old_value = pair.Value().data();
  old_alloc = pair.AllocSize();
  SelectivePageUsage both(old_raw, const_cast<char*>(old_value));
  delta = pair.ReallocIfNeeded(&both, &realloced);
  EXPECT_TRUE(realloced);
  EXPECT_NE(pair.Raw(), old_raw);
  EXPECT_NE(pair.Value().data(), old_value);
  EXPECT_EQ(delta, static_cast<ssize_t>(pair.AllocSize()) - static_cast<ssize_t>(old_alloc));
  EXPECT_EQ(KeyOf(pair), key);
  EXPECT_EQ(pair.Value(), value);
  EXPECT_EQ(pair.GetExpiry(), 11u);
  EXPECT_EQ(pair.GetHash(), 123u);

  OAHPair::Destroy(pair.Release());
}

TEST_F(OAHMapTest, ExternalValueMapReallocAccounting) {
  vector<pair<string, string>> entries;
  for (unsigned i = 0; i < 8; ++i) {
    string key = sized_string(8192, static_cast<char>('a' + i));
    key.replace(0, 1, to_string(i));
    string value = sized_string(8192, static_cast<char>('k' + i));
    entries.emplace_back(std::move(key), std::move(value));
    EXPECT_TRUE(m_->AddOrUpdate(entries.back().first, entries.back().second, 100 + i));
  }
  EXPECT_EQ(m_->ObjMallocUsed(), SumEntryAlloc(*m_));

  auto first = m_->Find(entries.front().first);
  ASSERT_NE(first, m_->end());
  char* first_raw = first->Raw();
  const char* first_value = first->Value().data();
  // Flagging only the value's page relocates the value buffer; the blob stays put.
  SelectivePageUsage value_only(const_cast<char*>(first_value));
  EXPECT_TRUE(first.ReallocIfNeeded(&value_only));
  first = m_->Find(entries.front().first);
  ASSERT_NE(first, m_->end());
  EXPECT_EQ(first->Raw(), first_raw);
  EXPECT_NE(first->Value().data(), first_value);
  EXPECT_EQ(m_->ObjMallocUsed(), SumEntryAlloc(*m_));

  PageUsage page_usage{CollectPageStats::NO, 0.9};
  page_usage.SetForceReallocate(true);
  for (auto it = m_->begin(); it != m_->end(); ++it)
    EXPECT_TRUE(it.ReallocIfNeeded(&page_usage));

  EXPECT_EQ(m_->ObjMallocUsed(), SumEntryAlloc(*m_));
  for (unsigned i = 0; i < entries.size(); ++i) {
    auto it = m_->Find(entries[i].first);
    ASSERT_NE(it, m_->end());
    EXPECT_EQ(it->Value(), entries[i].second);
    EXPECT_EQ(it.ExpiryTime(), 100u + i);
  }

  for (unsigned i = 0; i < entries.size(); i += 2)
    EXPECT_TRUE(m_->Erase(entries[i].first));
  EXPECT_EQ(m_->ObjMallocUsed(), SumEntryAlloc(*m_));

  m_->Clear();
  EXPECT_EQ(m_->ObjMallocUsed(), 0u);
}

TEST_F(OAHMapTest, EmptyFind) {
  EXPECT_TRUE(m_->Find("bar") == m_->end());
}

TEST_F(OAHMapTest, Ttl) {
  EXPECT_TRUE(m_->AddOrUpdate("bla", "val1", 1));
  EXPECT_FALSE(m_->AddOrUpdate("bla", "val2", 1));
  m_->set_time(1);
  EXPECT_TRUE(m_->AddOrUpdate("bla", "val2", 1));
  EXPECT_EQ(1u, m_->UpperBoundSize());

  EXPECT_FALSE(m_->AddOrSkip("bla", "val3", 2));

  // set ttl to 2, meaning that the key will expire at time 3.
  EXPECT_TRUE(m_->AddOrSkip("bla2", "val3", 2));
  EXPECT_TRUE(m_->Contains("bla2"));

  m_->set_time(3);
  auto it = m_->begin();
  EXPECT_TRUE(it == m_->end());
}

TEST_F(OAHMapTest, SetFieldExpire) {
  // Field created with an expiry: SetExpiryTime overwrites it.
  EXPECT_TRUE(m_->AddOrUpdate("k1", "v1", 5));
  auto k1 = m_->Find("k1");
  EXPECT_TRUE(k1.HasExpiry());
  EXPECT_EQ(k1.ExpiryTime(), 5);
  k1.SetExpiryTime(1);
  EXPECT_TRUE(k1.HasExpiry());
  EXPECT_EQ(k1.ExpiryTime(), 1);

  // Field created without an expiry: SetExpiryTime adds one.
  EXPECT_TRUE(m_->AddOrUpdate("k2", "v2"));
  auto k2 = m_->Find("k2");
  EXPECT_FALSE(k2.HasExpiry());
  k2.SetExpiryTime(1);
  EXPECT_TRUE(k2.HasExpiry());
  EXPECT_EQ(k2.ExpiryTime(), 1);
}

TEST_F(OAHMapTest, ManyFieldsSetExpiry) {
  for (unsigned i = 0; i < 8; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "val"));
  for (unsigned i = 0; i < 8; i++) {
    auto k = m_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
  for (unsigned i = 100; i < 1000; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "val"));

  // make sure the first 8 keys have expiry set
  for (unsigned i = 0; i < 8; i++) {
    auto k = m_->Find(to_string(i));
    ASSERT_TRUE(k.HasExpiry());
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
}

TEST_F(OAHMapTest, UpdateAfterSetExpiry) {
  for (unsigned i = 0; i < 6; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "val"));
  for (unsigned i = 0; i < 6; i++) {
    auto k = m_->Find(to_string(i));
    ASSERT_FALSE(k.HasExpiry());
    k.SetExpiryTime(1);
    EXPECT_EQ(k.ExpiryTime(), 1);
  }
  for (unsigned i = 0; i < 6; i++)
    EXPECT_FALSE(m_->AddOrUpdate(to_string(i), "val"));
}

TEST_F(OAHMapTest, ExpiryChangesSize) {
  // Value sized so the +4 expiry bytes push the blob across a mimalloc size class (48 -> 64),
  // making the growth observable in ObjMallocUsed.
  const string value(38, 'x');
  m_->AddOrUpdate("field", value);
  const size_t old_size = m_->ObjMallocUsed();

  auto it = m_->Find("field");
  it.SetExpiryTime(1);

  const size_t new_size = m_->ObjMallocUsed();
  EXPECT_LT(old_size, new_size);

  m_->AddOrUpdate("field", value, 1);
  EXPECT_EQ(new_size, m_->ObjMallocUsed());
}

TEST_F(OAHMapTest, ExpiryWithMaxAndKeepTTL) {
  m_->AddOrUpdate("field", "value", 100);
  auto k = m_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 100);

  // ttl is copied from prev. if max value is supplied
  m_->AddOrUpdate("field", "value", UINT32_MAX, true);
  k = m_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 100);

  // max ttl value results in no expiry without keepttl
  m_->AddOrUpdate("field", "value", UINT32_MAX);
  EXPECT_FALSE(m_->Find("field").HasExpiry());

  // No prev. expiry, supplied ttl_sec value is used
  m_->AddOrUpdate("field", "value", 10, true);
  k = m_->Find("field");
  EXPECT_TRUE(k.HasExpiry());
  EXPECT_EQ(k.ExpiryTime(), 10);

  // object removed while adding due to expiry
  m_->set_time(11);
  m_->AddOrUpdate("field", "value", UINT32_MAX, true);
  k = m_->Find("field");
  EXPECT_FALSE(k.HasExpiry());
}

TEST_F(OAHMapTest, AddOrExchange) {
  // New field: empty owner, no previous entry.
  auto added = m_->AddOrExchange("f1", "v1");
  EXPECT_FALSE(added);
  EXPECT_TRUE(m_->Contains("f1"));
  EXPECT_EQ(m_->Find("f1")->Value(), "v1"sv);
  EXPECT_EQ(m_->UpperBoundSize(), 1u);

  // Replace: the owner holds the previous value (freed on scope exit).
  auto replaced = m_->AddOrExchange("f1", "new_value");
  ASSERT_TRUE(replaced);
  EXPECT_EQ(replaced.pair().Value(), "v1"sv);
  EXPECT_EQ(m_->Find("f1")->Value(), "new_value"sv);
  EXPECT_EQ(m_->UpperBoundSize(), 1u);

  // Replace with a TTL: the new expiry is applied.
  auto with_ttl = m_->AddOrExchange("f1", "v2", 200);
  ASSERT_TRUE(with_ttl);
  EXPECT_EQ(with_ttl.pair().Value(), "new_value"sv);
  auto it = m_->Find("f1");
  EXPECT_EQ(it->Value(), "v2"sv);
  EXPECT_TRUE(it.HasExpiry());
  EXPECT_EQ(it.ExpiryTime(), 200u);
}

TEST_F(OAHMapTest, Extract) {
  // Non-existing key: empty owner, map unchanged.
  m_->AddOrUpdate("f1", "v1");
  EXPECT_FALSE(m_->Extract("no_such_key"));
  EXPECT_EQ(m_->UpperBoundSize(), 1u);

  // Existing key: the owner holds the removed pair; other keys remain.
  m_->AddOrUpdate("f2", "v2");
  {
    auto entry = m_->Extract("f1");
    ASSERT_TRUE(entry);
    EXPECT_EQ(KeyOf(entry.pair()), "f1"sv);
    EXPECT_EQ(entry.pair().Value(), "v1"sv);
  }
  EXPECT_EQ(m_->UpperBoundSize(), 1u);
  EXPECT_FALSE(m_->Contains("f1"));
  EXPECT_TRUE(m_->Contains("f2"));

  // Extract half of many entries (exercises rehash-sized tables).
  m_->Clear();
  for (unsigned i = 0; i < 20; i++)
    m_->AddOrUpdate(to_string(i), "val" + to_string(i));
  EXPECT_EQ(m_->UpperBoundSize(), 20u);
  for (unsigned i = 0; i < 20; i += 2) {
    auto entry = m_->Extract(to_string(i));
    ASSERT_TRUE(entry);
    EXPECT_EQ(entry.pair().Value(), ("val" + to_string(i)));
  }
  EXPECT_EQ(m_->UpperBoundSize(), 10u);
  for (unsigned i = 1; i < 20; i += 2)
    EXPECT_TRUE(m_->Contains(to_string(i)));
}

TEST_F(OAHMapTest, RandomPairs) {
  m_->Reserve(1024);
  for (unsigned i = 0; i < 20; i++)
    EXPECT_TRUE(m_->AddOrUpdate(to_string(i), "val" + to_string(i)));
  EXPECT_FALSE(m_->ExpirationUsed());

  // With values requested, keys and values are returned in matching order.
  vector<string> keys, vals;
  m_->RandomPairsUnique(5, keys, vals, true);
  ASSERT_EQ(keys.size(), 5u);
  ASSERT_EQ(vals.size(), 5u);
  for (size_t i = 0; i < keys.size(); ++i)
    EXPECT_EQ(vals[i], "val" + keys[i]);

  // Expired fields are skipped after setting expiry on half and advancing time.
  for (unsigned i = 0; i < 10; i++) {
    auto it = m_->Find(to_string(i));
    ASSERT_FALSE(it.HasExpiry());
    it.SetExpiryTime(1);
  }
  EXPECT_TRUE(m_->ExpirationUsed());
  m_->set_time(2);

  keys.clear();
  vals.clear();
  m_->RandomPairsUnique(20, keys, vals, false);
  EXPECT_EQ(keys.size(), 10u);
}

TEST_F(OAHMapTest, ReallocIfNeeded) {
  auto build_str = [](size_t i) { return to_string(i) + string(131, 'a'); };

  static unsigned total_wasted_memory = 0;
  auto count_waste = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                        size_t block_size, void* arg) {
    size_t used = block_size * area->used;
    total_wasted_memory += area->committed - used;
    return true;
  };

  for (size_t i = 0; i < 10'000; i++)
    m_->AddOrUpdate(build_str(i), build_str(i + 1), i * 10 + 1);

  for (size_t i = 0; i < 10'000; i++) {
    if (i % 10 == 0)
      continue;
    m_->Erase(build_str(i));
  }

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_before = total_wasted_memory;

  PageUsage page_usage{CollectPageStats::NO, 0.9};
  for (auto it = m_->begin(); it != m_->end(); ++it)
    it.ReallocIfNeeded(&page_usage);

  total_wasted_memory = 0;
  mi_heap_collect(mi_heap_get_backing(), true);
  mi_heap_visit_blocks(mi_heap_get_backing(), false, count_waste, nullptr);
  size_t wasted_after = total_wasted_memory;

  EXPECT_GT(wasted_before, wasted_after * 2);

  EXPECT_EQ(m_->UpperBoundSize(), 1000);
  for (size_t i = 0; i < 1000; i++)
    EXPECT_EQ(m_->Find(build_str(i * 10))->Value(), build_str(i * 10 + 1));
}

// Benchmarks, mirroring oah_set_test.cc adapted to key+value map operations.

static size_t MemUsed(OAHMap& obj) {
  return obj.ObjMallocUsed() + obj.SetMallocUsed();
}

void BM_Clone(benchmark::State& state) {
  mt19937 generator(0);
  OAHMap src, dst;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    src.AddOrUpdate(random_string(generator, keySize), random_string(generator, keySize));
  dst.Reserve(src.UpperBoundSize());
  while (state.KeepRunning()) {
    for (auto e : src)
      dst.AddOrUpdate(KeyOf(e), e.Value());
    state.PauseTiming();
    dst.Clear();
    dst.Reserve(src.UpperBoundSize());
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Clone)->ArgNames({"elements", "KeySize"})->ArgsProduct({{32000}, {10, 100, 1000}});

void BM_Clear(benchmark::State& state) {
  unsigned elems = state.range(0);
  unsigned key_size = state.range(1);
  mt19937 generator(0);
  OAHMap m;
  while (state.KeepRunning()) {
    state.PauseTiming();
    for (size_t i = 0; i < elems; ++i)
      m.AddOrUpdate(random_string(generator, key_size), random_string(generator, key_size));
    state.ResumeTiming();
    m.Clear();
  }
}
BENCHMARK(BM_Clear)->ArgNames({"elements", "KeySize"})->ArgsProduct({{32000}, {10, 100, 1000}});

void BM_AddOrUpdate(benchmark::State& state) {
  vector<pair<string, string>> kv;
  mt19937 generator(0);
  OAHMap m;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    kv.emplace_back(random_string(generator, keySize), random_string(generator, keySize));
  m.Reserve(elems);
  size_t mem_used = 0;
  while (state.KeepRunning()) {
    for (auto& [k, v] : kv)
      m.AddOrUpdate(k, v);
    state.PauseTiming();
    mem_used += MemUsed(m);
    m.Clear();
    m.Reserve(elems);
    state.ResumeTiming();
  }
  state.counters["Memory_Used"] = mem_used / state.iterations();
}
BENCHMARK(BM_AddOrUpdate)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Erase(benchmark::State& state) {
  vector<pair<string, string>> kv;
  mt19937 generator(0);
  OAHMap m;
  auto elems = state.range(0);
  auto keySize = state.range(1);
  for (long int i = 0; i < elems; ++i) {
    kv.emplace_back(random_string(generator, keySize), random_string(generator, keySize));
    m.AddOrUpdate(kv.back().first, kv.back().second);
  }
  while (state.KeepRunning()) {
    for (auto& [k, v] : kv)
      m.Erase(k);
    state.PauseTiming();
    for (auto& [k, v] : kv)
      m.AddOrUpdate(k, v);
    state.ResumeTiming();
  }
}
BENCHMARK(BM_Erase)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Get(benchmark::State& state) {
  vector<string> keys;
  mt19937 generator(0);
  OAHMap m;
  auto elems = state.range(0);
  auto keySize = state.range(1);
  for (long int i = 0; i < elems; ++i) {
    string k = random_string(generator, keySize);
    keys.push_back(k);
    m.AddOrUpdate(k, random_string(generator, keySize));
  }
  while (state.KeepRunning()) {
    for (auto& k : keys)
      benchmark::DoNotOptimize(m.Find(k));
  }
}
BENCHMARK(BM_Get)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_GetRandomMember(benchmark::State& state) {
  mt19937 generator(0);
  OAHMap m;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    m.AddOrUpdate(random_string(generator, keySize), random_string(generator, keySize));

  while (state.KeepRunning())
    benchmark::DoNotOptimize(m.GetRandomMember());
}
BENCHMARK(BM_GetRandomMember)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Scan(benchmark::State& state) {
  mt19937 generator(0);
  OAHMap m;
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  for (size_t i = 0; i < elems; ++i)
    m.AddOrUpdate(random_string(generator, keySize), random_string(generator, keySize));

  while (state.KeepRunning()) {
    uint32_t cursor = 0;
    size_t seen = 0;
    do {
      cursor = m.Scan(cursor, [&](const auto& key) {
        benchmark::DoNotOptimize(key.size());
        ++seen;
      });
    } while (cursor != 0);
    benchmark::DoNotOptimize(seen);
  }
}
BENCHMARK(BM_Scan)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

void BM_Shrink(benchmark::State& state) {
  mt19937 generator(0);
  unsigned elems = state.range(0);
  unsigned keySize = state.range(1);
  vector<pair<string, string>> kv;
  for (size_t i = 0; i < elems; ++i)
    kv.emplace_back(random_string(generator, keySize), random_string(generator, keySize));

  size_t kShrinkTo = absl::bit_ceil(size_t(elems));
  size_t kGrowTo = kShrinkTo * 4;
  OAHMap m;
  while (state.KeepRunning()) {
    state.PauseTiming();
    m.Clear();
    m.Reserve(kGrowTo);
    for (auto& [k, v] : kv)
      m.AddOrUpdate(k, v);
    CHECK_EQ(m.BucketCount(), kGrowTo);
    state.ResumeTiming();
    m.Shrink(kShrinkTo);
  }
}
BENCHMARK(BM_Shrink)
    ->ArgNames({"elements", "KeySize"})
    ->ArgsProduct({{1000, 10000, 100000}, {10, 100, 1000}});

}  // namespace dfly
