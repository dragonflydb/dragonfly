// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <gmock/gmock.h>

extern "C" {
#include "redis/crc64.h"
#include "redis/redis_aux.h"
#include "redis/zmalloc.h"
}

#include <absl/flags/reflection.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "core/bloom.h"
#include "facade/facade_test.h"  // needed to find operator== for RespExpr.
#include "io/file.h"
#include "server/engine_shard_set.h"
#include "server/rdb_extensions.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/serializer_commons.h"
#include "server/test_utils.h"

namespace rng = std::ranges;

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(int32, list_compress_depth);
ABSL_DECLARE_FLAG(int32, list_max_listpack_size);
ABSL_DECLARE_FLAG(dfly::CompressionMode, compression_mode);
ABSL_DECLARE_FLAG(bool, rdb_ignore_expiry);
ABSL_DECLARE_FLAG(uint32_t, num_shards);
ABSL_DECLARE_FLAG(bool, rdb_sbf_chunked);
ABSL_DECLARE_FLAG(bool, serialize_hnsw_index);
ABSL_DECLARE_FLAG(bool, deserialize_hnsw_index);

namespace dfly {

static const auto kMatchNil = ArgType(RespExpr::NIL);

class RdbTest : public BaseFamilyTest {
 protected:
  void SetUp();

  io::FileSource GetSource(string name);

  std::error_code LoadRdb(const string& filename) {
    return pp_->at(0)->Await([&] {
      io::FileSource fs = GetSource(filename);

      RdbLoadContext load_context;
      RdbLoader loader(service_.get(), &load_context);
      return loader.Load(&fs);
    });
  }
};

void RdbTest::SetUp() {
  // Setting max_memory_limit must be before calling  InitWithDbFilename
  max_memory_limit = 40000000;
  absl::SetFlag(&FLAGS_serialize_hnsw_index, true);
  absl::SetFlag(&FLAGS_deserialize_hnsw_index, true);
  InitWithDbFilename();
  CHECK_EQ(zmalloc_used_memory_tl, 0);
}

inline const uint8_t* to_byte(const void* s) {
  return reinterpret_cast<const uint8_t*>(s);
}

io::FileSource RdbTest::GetSource(string name) {
  string rdb_file = base::ProgramRunfile("testdata/" + name);
  auto open_res = io::OpenRead(rdb_file, io::ReadonlyFile::Options{});
  CHECK(open_res) << rdb_file;

  return io::FileSource(*open_res);
}

static string FloatToBytes(float f) {
  return string(reinterpret_cast<const char*>(&f), sizeof(float));
}

TEST_F(RdbTest, SnapshotIdTest) {
  absl::SetFlag(&FLAGS_num_shards, num_threads_);
  ResetService();

  EXPECT_EQ(Run({"mset", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"}), "OK");

  Run({"save", "df", "test_dump"});

  absl::SetFlag(&FLAGS_num_shards, num_threads_ - 1);
  ResetService();

  EXPECT_EQ(Run({"mset", "test1", "val1", "test2", "val2"}), "OK");

  Run({"save", "df", "test_dump"});

  ResetService();

  EXPECT_EQ(Run({"dfly", "load", "test_dump-summary.dfs"}), "OK");

  auto resp = Run({"keys", "*"});
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("test1", "test2"));
}

TEST_F(RdbTest, Crc) {
  std::string_view s{"TEST"};

  uint64_t c = crc64(0, to_byte(s.data()), s.size());
  ASSERT_NE(c, 0);

  uint64_t c2 = crc64(c, to_byte(s.data()), s.size());
  EXPECT_NE(c, c2);

  uint64_t c3 = crc64(c, to_byte(&c), sizeof(c));
  EXPECT_EQ(c3, 0);

  s = "COOLTEST";
  c = crc64(0, to_byte(s.data()), 8);
  c2 = crc64(0, to_byte(s.data()), 4);
  c3 = crc64(c2, to_byte(s.data() + 4), 4);
  EXPECT_EQ(c, c3);

  c2 = crc64(0, to_byte(s.data() + 4), 4);
  c3 = crc64(c2, to_byte(s.data()), 4);
  EXPECT_NE(c, c3);
}

TEST_F(RdbTest, LoadEmpty) {
  auto ec = LoadRdb("empty.rdb");
  ASSERT_FALSE(ec) << ec;
}

TEST_F(RdbTest, LoadSmall6) {
  // The rdb file contians keys that already expired, we want to continue loading them in this test.
  absl::FlagSaver fs;
  SetTestFlag("rdb_ignore_expiry", "true");

  auto ec = LoadRdb("redis6_small.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"scan", "0"});

  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(StrArray(resp.GetVec()[1]),
              UnorderedElementsAre("list1", "hset_zl", "list2", "zset_sl", "intset", "set1",
                                   "zset_zl", "hset_ht", "intkey", "strkey"));
  EXPECT_THAT(Run({"get", "intkey"}), "1234567");
  EXPECT_THAT(Run({"get", "strkey"}), "abcdefghjjjjjjjjjj");

  resp = Run({"smembers", "intset"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(),
              UnorderedElementsAre("111", "222", "1234", "3333", "4444", "67899", "76554"));

  // TODO: when we implement PEXPIRETIME we will be able to do it directly.
  int ttl = CheckedInt({"ttl", "set1"});    // should expire at 1747008000.
  EXPECT_GT(ttl + time(NULL), 1747007000);  // left 1000 seconds margin in case the clock is off.

  Run({"select", "1"});
  ASSERT_EQ(10, CheckedInt({"dbsize"}));
  ASSERT_EQ(128, CheckedInt({"strlen", "longggggggggggggggkeyyyyyyyyyyyyy:9"}));
  resp = Run({"script", "exists", "4ca238f611c9d0ae4e9a75a5dbac22aedc379801",
              "282297a0228f48cd3fc6a55de6316f31422f5d17"});
  ASSERT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre(IntArg(1), IntArg(1)));
}

TEST_F(RdbTest, Stream) {
  auto ec = LoadRdb("redis6_stream.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"type", "key:10"});
  EXPECT_EQ(resp, "stream");

  resp = Run({"xinfo", "groups", "key:0"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0],
              RespElementsAre("name", "g1", "consumers", 0, "pending", 0, "last-delivered-id",
                              "1655444851524-3", "entries-read", 128, "lag", 0));
  EXPECT_THAT(resp.GetVec()[1],
              RespElementsAre("name", "g2", "consumers", 1, "pending", 0, "last-delivered-id",
                              "1655444851523-1", "entries-read", kMatchNil, "lag", kMatchNil));

  resp = Run({"xinfo", "groups", "key:1"});  // test dereferences array of size 1
  EXPECT_THAT(resp, RespElementsAre("name", "g2", "consumers", IntArg(0), "pending", IntArg(0),
                                    "last-delivered-id", "1655444851523-1", "entries-read",
                                    kMatchNil, "lag", kMatchNil));

  resp = Run({"xinfo", "groups", "key:2"});
  EXPECT_THAT(resp, ArrLen(0));

  Run({"save"});
}

TEST_F(RdbTest, ComressionModeSaveDragonflyAndReload) {
  Run({"debug", "populate", "50000"});
  ASSERT_EQ(50000, CheckedInt({"dbsize"}));
  // Check keys inserted are lower than 50,000.
  auto resp = Run({"keys", "key:[5-9][0-9][0-9][0-9][0-9]*"});
  EXPECT_EQ(resp.GetVec().size(), 0);

  for (auto mode : {CompressionMode::NONE, CompressionMode::SINGLE_ENTRY,
                    CompressionMode::MULTI_ENTRY_ZSTD, CompressionMode::MULTI_ENTRY_LZ4}) {
    SetFlag(&FLAGS_compression_mode, mode);
    RespExpr resp = Run({"save", "df"});
    ASSERT_EQ(resp, "OK");

    if (mode == CompressionMode::MULTI_ENTRY_ZSTD || mode == CompressionMode::MULTI_ENTRY_LZ4) {
      EXPECT_GE(GetMetrics().coordinator_stats.compressed_blobs, 1);
    }

    auto save_info = service_->server_family().GetLastSaveInfo();
    resp = Run({"dfly", "load", save_info.file_name});
    ASSERT_EQ(resp, "OK");
    ASSERT_EQ(50000, CheckedInt({"dbsize"}));
  }
}

TEST_F(RdbTest, RdbLoaderOnReadCompressedDataShouldNotEnterEnsureReadFlow) {
  SetFlag(&FLAGS_compression_mode, CompressionMode::MULTI_ENTRY_ZSTD);
  for (int i = 0; i < 1000; ++i) {
    Run({"set", StrCat(i), "1"});
  }
  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");
}

TEST_F(RdbTest, SaveLoadSticky) {
  Run({"set", "a", "1"});
  Run({"set", "b", "2"});
  Run({"set", "c", "3"});
  Run({"stick", "a", "b"});
  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"debug", "reload"});
  ASSERT_EQ(resp, "OK");
  EXPECT_THAT(Run({"get", "a"}), "1");
  EXPECT_THAT(Run({"get", "b"}), "2");
  EXPECT_THAT(Run({"get", "c"}), "3");
  EXPECT_THAT(Run({"stick", "a", "b"}), IntArg(0));
  EXPECT_THAT(Run({"stick", "c"}), IntArg(1));
}

TEST_F(RdbTest, ReloadSetSmallStringBug) {
  auto str = absl::StrCat(std::string(32, 'X'));
  Run({"set", "small_key", str});
  auto resp = Run({"debug", "reload"});
  ASSERT_EQ(resp, "OK");
}

TEST_F(RdbTest, Reload) {
  absl::FlagSaver fs;

  SetFlag(&FLAGS_list_compress_depth, 1);
  SetFlag(&FLAGS_list_max_listpack_size, 1);  // limit listpack to a single element.

  Run({"set", "string_key", "val"});
  Run({"set", "large_key", string(511, 'L')});
  Run({"set", "huge_key", string((1 << 17) - 10, 'H')});

  Run({"sadd", "set_key1", "val1", "val2"});
  Run({"sadd", "intset_key", "1", "2", "3"});
  Run({"hset", "small_hset", "field1", "val1", "field2", "val2"});
  Run({"hset", "large_hset", "field1", string(510, 'V'), string(120, 'F'), "val2"});

  Run({"rpush", "list_key1", "val", "val2"});
  Run({"rpush", "list_key2", "head", string(511, 'a'), string(500, 'b'), "tail"});

  Run({"zadd", "zs1", "1.1", "a", "-1.1", "b"});
  Run({"zadd", "zs2", "1.1", string(510, 'a'), "-1.1", string(502, 'b')});

  Run({"hset", "large_keyname", string(240, 'X'), "-5"});
  Run({"hset", "large_keyname", string(240, 'Y'), "-500"});
  Run({"hset", "large_keyname", string(240, 'Z'), "-50000"});

  auto resp = Run({"debug", "reload"});
  ASSERT_EQ(resp, "OK");

  EXPECT_EQ(2, CheckedInt({"scard", "set_key1"}));
  EXPECT_EQ(3, CheckedInt({"scard", "intset_key"}));
  EXPECT_EQ(2, CheckedInt({"hlen", "small_hset"}));
  EXPECT_EQ(2, CheckedInt({"hlen", "large_hset"}));
  EXPECT_EQ(4, CheckedInt({"LLEN", "list_key2"}));
  EXPECT_EQ(2, CheckedInt({"ZCARD", "zs1"}));
  EXPECT_EQ(2, CheckedInt({"ZCARD", "zs2"}));

  EXPECT_EQ(-5, CheckedInt({"hget", "large_keyname", string(240, 'X')}));
  EXPECT_EQ(-500, CheckedInt({"hget", "large_keyname", string(240, 'Y')}));
  EXPECT_EQ(-50000, CheckedInt({"hget", "large_keyname", string(240, 'Z')}));
}

TEST_F(RdbTest, ReloadTtl) {
  Run({"set", "key", "val"});
  Run({"expire", "key", "1000"});
  Run({"debug", "reload"});
  EXPECT_LT(990, CheckedInt({"ttl", "key"}));
}

TEST_F(RdbTest, ReloadExpired) {
  Run({"set", "key", "val"});
  Run({"expire", "key", "2"});
  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");
  auto save_info = service_->server_family().GetLastSaveInfo();
  AdvanceTime(2000);
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");
  resp = Run({"get", "key"});
  ASSERT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(RdbTest, HashmapExpiry) {
  // Add non-expiring elements
  Run({"hset", "key", "key1", "val1", "key2", "val2"});
  Run({"debug", "reload"});
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2")));

  // Add expiring elements
  Run({"hsetex", "key", "5", "key3", "val3", "key4", "val4"});
  Run({"debug", "reload"});  // Reload before expiration
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2", "key3", "val3", "key4",
                                             "val4")));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2")));

  Run({"hsetex", "key", "5", "key5", "val5", "key6", "val6"});
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2", "key5", "val5", "key6",
                                             "val6")));
  AdvanceTime(10'000);
  Run({"debug", "reload"});  // Reload after expiration
  EXPECT_THAT(Run({"hgetall", "key"}),
              RespArray(UnorderedElementsAre("key1", "val1", "key2", "val2")));
}

TEST_F(RdbTest, SaveLoadExpiredValuesHmap) {
  // Add expiring elements
  Run({"hsetex", "hkey", "1", "key3", "val3", "key4", "val4"});

  RespExpr resp = Run({"TYPE", "hkey"});
  ASSERT_EQ(resp, "hash");

  AdvanceTime(10'000);
  resp = Run({"save", "RDB"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"TYPE", "hkey"});
  ASSERT_EQ(resp, "hash");

  Run({"debug", "reload"});

  resp = Run({"TYPE", "hkey"});
  ASSERT_EQ(resp, "none");
}

TEST_F(RdbTest, SaveLoadExpiredValuesHugeHmap) {
  constexpr auto keys_num = 10000;
  for (int i = 0; i < keys_num; ++i) {
    Run({"hsetex", "hkey", "1", absl::StrCat("key", i), "val"});
  }

  ASSERT_EQ(keys_num, CheckedInt({"hlen", "hkey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(Run({"TYPE", "hkey"}), "none");

  // with one value that isn't expired
  for (int i = 0; i < keys_num; ++i) {
    Run({"hsetex", "hkey", "1", absl::StrCat("key", i), "val"});
  }

  Run({"hset", "hkey", base::RandStr(20), "val"});

  ASSERT_EQ(keys_num + 1, CheckedInt({"hlen", "hkey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(1, CheckedInt({"hlen", "hkey"}));
}

TEST_F(RdbTest, SaveLoadExpiredValuesSSet) {
  // Add expiring elements
  Run({"saddex", "skey", "1", "key3", "key4"});

  RespExpr resp = Run({"TYPE", "skey"});
  ASSERT_EQ(resp, "set");

  AdvanceTime(10'000);
  resp = Run({"save", "RDB"});
  ASSERT_EQ(resp, "OK");

  resp = Run({"TYPE", "skey"});
  ASSERT_EQ(resp, "set");

  Run({"debug", "reload"});

  resp = Run({"TYPE", "skey"});
  ASSERT_EQ(resp, "none");
}

TEST_F(RdbTest, SaveLoadExpiredValuesHugeSet) {
  constexpr auto keys_num = 10000;
  for (int i = 0; i < keys_num; ++i) {
    Run({"saddex", "skey", "1", absl::StrCat("key", i)});
  }

  ASSERT_EQ(keys_num, CheckedInt({"scard", "skey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(Run({"TYPE", "skey"}), "none");

  // with one value that isn't expired
  for (int i = 0; i < keys_num; ++i) {
    Run({"saddex", "skey", "1", absl::StrCat("key", i)});
  }
  Run({"sadd", "skey", base::RandStr(20)});

  ASSERT_EQ(keys_num + 1, CheckedInt({"scard", "skey"}));

  AdvanceTime(10'000);

  Run({"debug", "reload"});

  ASSERT_EQ(1, CheckedInt({"scard", "skey"}));
}

TEST_F(RdbTest, SetExpiry) {
  // Add non-expiring elements
  Run({"sadd", "key", "key1", "key2"});
  Run({"debug", "reload"});
  EXPECT_THAT(Run({"smembers", "key"}), RespArray(UnorderedElementsAre("key1", "key2")));

  // Add expiring elements
  Run({"saddex", "key", "5", "key3", "key4"});
  Run({"debug", "reload"});  // Reload before expiration
  EXPECT_THAT(Run({"smembers", "key"}),
              RespArray(UnorderedElementsAre("key1", "key2", "key3", "key4")));
  AdvanceTime(10'000);
  EXPECT_THAT(Run({"smembers", "key"}), RespArray(UnorderedElementsAre("key1", "key2")));

  Run({"saddex", "key", "5", "key5", "key6"});
  EXPECT_THAT(Run({"smembers", "key"}),
              RespArray(UnorderedElementsAre("key1", "key2", "key5", "key6")));
  AdvanceTime(10'000);
  Run({"debug", "reload"});  // Reload after expiration
  EXPECT_THAT(Run({"smembers", "key"}), RespArray(UnorderedElementsAre("key1", "key2")));
}

// Tests that integer elements in sets with expiry are not corrupted during RDB load.
// This test covers the bug where ToSV() internal buffer was being reused,
// causing string corruption when loading integer elements.
TEST_F(RdbTest, SetExpiryInteger) {
  // Add integer elements with expiry - integers trigger ToSV() buffer reuse
  Run({"saddex", "s1", "10", "1", "2", "3", "12345", "67890"});

  // Verify elements are added correctly
  EXPECT_EQ(5, CheckedInt({"scard", "s1"}));
  EXPECT_THAT(Run({"smembers", "s1"}),
              RespArray(UnorderedElementsAre("1", "2", "3", "12345", "67890")));

  // Reload from RDB - this would trigger the corruption bug
  Run({"debug", "reload"});

  // Verify integers were loaded correctly without corruption
  EXPECT_EQ(5, CheckedInt({"scard", "s1"}));
  EXPECT_THAT(Run({"smembers", "s1"}),
              RespArray(UnorderedElementsAre("1", "2", "3", "12345", "67890")));

  // Verify all elements are actually in the set (no duplicates from corruption)
  EXPECT_THAT(Run({"sismember", "s1", "1"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "2"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "3"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "12345"}), IntArg(1));
  EXPECT_THAT(Run({"sismember", "s1", "67890"}), IntArg(1));
}

TEST_F(RdbTest, SaveFlush) {
  Run({"debug", "populate", "500000"});

  auto save_fb = pp_->at(1)->LaunchFiber([&] {
    RespExpr resp = Run({"save"});
    ASSERT_EQ(resp, "OK");
  });

  do {
    usleep(10);
  } while (!service_->server_family().TEST_IsSaving());

  Run({"flushdb"});
  save_fb.Join();
  auto save_info = service_->server_family().GetLastSaveInfo();
  ASSERT_EQ(1, save_info.freq_map.size());
  auto& k_v = save_info.freq_map.front();
  EXPECT_EQ("string", k_v.first);
  EXPECT_EQ(500000, k_v.second);
}

TEST_F(RdbTest, SaveManyDbs) {
  Run({"debug", "populate", "50000"});
  pp_->at(1)->Await([&] {
    Run({"select", "1"});
    Run({"debug", "populate", "10000"});
  });

  auto metrics = GetMetrics();
  ASSERT_EQ(2, metrics.db_stats.size());
  EXPECT_EQ(50000, metrics.db_stats[0].key_count);
  EXPECT_EQ(10000, metrics.db_stats[1].key_count);

  auto save_fb = pp_->at(0)->LaunchFiber([&] {
    RespExpr resp = Run({"save"});
    ASSERT_EQ(resp, "OK");
  });

  do {
    usleep(10);
  } while (!service_->server_family().TEST_IsSaving());

  pp_->at(1)->Await([&] {
    Run({"select", "1"});
    for (unsigned i = 0; i < 1000; ++i) {
      Run({"set", StrCat("abc", i), "bar"});
    }
  });

  save_fb.Join();

  auto save_info = service_->server_family().GetLastSaveInfo();
  ASSERT_EQ(1, save_info.freq_map.size());
  auto& k_v = save_info.freq_map.front();

  EXPECT_EQ("string", k_v.first);
  EXPECT_EQ(60000, k_v.second);
  auto resp = Run({"debug", "reload", "NOSAVE"});
  EXPECT_EQ(resp, "OK");

  metrics = GetMetrics();
  ASSERT_EQ(2, metrics.db_stats.size());
  EXPECT_EQ(50000, metrics.db_stats[0].key_count);
  EXPECT_EQ(10000, metrics.db_stats[1].key_count);
  if (metrics.db_stats[1].key_count != 10000) {
    Run({"select", "1"});
    resp = Run({"scan", "0", "match", "ab*"});
    StringVec vec = StrArray(resp.GetVec()[1]);
    for (const auto& s : vec) {
      LOG(ERROR) << "Bad key: " << s;
    }
  }
}

TEST_F(RdbTest, HMapBugs) {
  // Force kEncodingStrMap2 encoding.
  server.max_map_field_len = 0;
  Run({"hset", "hmap1", "key1", "val", "key2", "val2"});
  Run({"hset", "hmap2", "key1", string(690557, 'a')});

  server.max_map_field_len = 32;
  Run({"debug", "reload"});
  EXPECT_EQ(2, CheckedInt({"hlen", "hmap1"}));
}

TEST_F(RdbTest, Issue1305) {
  /***************
   * The code below crashes because of the weird listpack API that assumes that lpInsert
   * pointers are null then it should do deletion :(. See lpInsert comments for more info.

     uint8_t* lp = lpNew(128);
     lpAppend(lp, NULL, 0);
     lpFree(lp);

  */

  // Force kEncodingStrMap2 encoding.
  server.max_map_field_len = 0;
  Run({"hset", "hmap", "key1", "val", "key2", ""});

  server.max_map_field_len = 32;
  Run({"debug", "reload"});
  EXPECT_EQ(2, CheckedInt({"hlen", "hmap"}));
}

TEST_F(RdbTest, JsonTest) {
  string_view data[] = {
      R"({"a":1})"sv,                          //
      R"([1,2,3,4,5,6])"sv,                    //
      R"({"a":1.0,"b":[1,2],"c":"value"})"sv,  //
      R"({"a":{"a":{"a":{"a":1}}}})"sv         //
  };

  for (auto test : data) {
    Run({"json.set", "doc", "$", test});
    auto dump = Run({"dump", "doc"});
    Run({"del", "doc"});
    Run({"restore", "doc", "0", facade::ToSV(dump.GetBuf())});
    auto res = Run({"json.get", "doc"});
    ASSERT_EQ(res, test);
  }
}

// hll.rdb has 2 keys: "key-dense" and "key-sparse", both are HLL with a single added value "1".
class HllRdbTest : public RdbTest, public testing::WithParamInterface<string> {};

TEST_P(HllRdbTest, Hll) {
  LOG(INFO) << " max memory: " << max_memory_limit
            << " used_mem_current: " << used_mem_current.load();
  auto ec = LoadRdb("hll.rdb");

  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(CheckedInt({"pfcount", GetParam()}), 1);

  EXPECT_EQ(CheckedInt({"pfcount", GetParam(), "non-existing"}), 1);

  EXPECT_EQ(CheckedInt({"pfadd", "key2", "2"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", GetParam(), "key2"}), 2);

  EXPECT_EQ(CheckedInt({"pfadd", GetParam(), "2"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", GetParam()}), 2);

  EXPECT_EQ(Run({"pfmerge", "key3", GetParam(), "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 2);
}

INSTANTIATE_TEST_SUITE_P(HllRdbTest, HllRdbTest, Values("key-sparse", "key-dense"));

TEST_F(RdbTest, LoadSmall7) {
  // Contains 3 keys
  // 1. A list called my-list encoded as RDB_TYPE_LIST_QUICKLIST_2
  // 2. A hashtable called my-hset encoded as RDB_TYPE_HASH_LISTPACK
  // 3. A set called my-set encoded as RDB_TYPE_SET_LISTPACK
  // 4. A zset called my-zset encoded as RDB_TYPE_ZSET_LISTPACK
  auto ec = LoadRdb("redis7_small.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"scan", "0"});

  ASSERT_THAT(resp, ArrLen(2));

  EXPECT_THAT(StrArray(resp.GetVec()[1]),
              UnorderedElementsAre("my-set", "my-hset", "my-list", "zset"));

  resp = Run({"smembers", "my-set"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("redis", "acme"));

  resp = Run({"hgetall", "my-hset"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("acme", "44", "field", "22"));

  resp = Run({"lrange", "my-list", "0", "-1"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("list1", "list2"));

  resp = Run({"zrange", "zset", "0", "-1"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  EXPECT_THAT(resp.GetVec(), ElementsAre("einstein", "schrodinger"));
}

TEST_F(RdbTest, RedisJson) {
  // RDB file generated via:
  // ./redis-server --save "" --appendonly no --loadmodule ../lib/rejson.so
  // and then:
  // JSON.SET json-str $ '"hello"'
  // JSON.SET json-arr $ "[1, true, \"hello\", 3.14]"
  // JSON.SET json-obj $
  // '{"company":"DragonflyDB","product":"Dragonfly","website":"https://dragondlydb.io","years-active":[2021,2022,2023,2024,"and
  // more!"]}'
  auto ec = LoadRdb("redis_json.rdb");

  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"JSON.GET", "json-str"}), "\"hello\"");
  EXPECT_EQ(Run({"JSON.GET", "json-arr"}), "[1,true,\"hello\",3.14]");
  EXPECT_EQ(Run({"JSON.GET", "json-obj"}),
            "{\"company\":\"DragonflyDB\",\"product\":\"Dragonfly\",\"website\":\"https://"
            "dragondlydb.io\",\"years-active\":[2021,2022,2023,2024,\"and more!\"]}");
}

TEST_F(RdbTest, SBF) {
  EXPECT_THAT(Run({"BF.ADD", "k", "1"}), IntArg(1));
  Run({"debug", "reload"});
  EXPECT_EQ(Run({"type", "k"}), "MBbloom--");
  EXPECT_THAT(Run({"BF.EXISTS", "k", "1"}), IntArg(1));
}

TEST_F(RdbTest, SBFLargeFilterChunking) {
  absl::SetFlag(&FLAGS_rdb_sbf_chunked, true);
  max_memory_limit = 200000000;

  // Using this set of parameters for the BF.RESERVE command resulted in a
  // filter size large enough to require chunking (> 64 MB).
  const double error_rate = 0.001;
  const size_t capacity = 50'000'000;
  const size_t num_items = 100;

  size_t collisions = 0;

  Run({"BF.RESERVE", "large_key", std::to_string(error_rate), std::to_string(capacity)});
  for (size_t i = 0; i < num_items; i++) {
    auto res = Run({"BF.ADD", "large_key", absl::StrCat("item", i)});
    if (*res.GetInt() == 0)
      collisions++;
  }
  EXPECT_LT(static_cast<double>(collisions) / num_items, error_rate);

  Run({"debug", "reload"});
  EXPECT_EQ(Run({"type", "large_key"}), "MBbloom--");

  for (size_t i = 0; i < num_items; i++) {
    EXPECT_THAT(Run({"BF.EXISTS", "large_key", absl::StrCat("item", i)}), IntArg(1));
  }
}

TEST_F(RdbTest, RestoreSearchIndexNameStartingWithColon) {
  // Create an index with a name that starts with ':' and add a sample document
  EXPECT_EQ(Run({"FT.CREATE", ":Order:index", "ON", "HASH", "PREFIX", "1", ":Order:", "SCHEMA",
                 "customer_name", "AS", "customer_name", "TEXT", "status", "AS", "status", "TAG"}),
            "OK");

  EXPECT_THAT(Run({"HSET", ":Order:1", "customer_name", "John", "status", "new"}), IntArg(2));

  // Save and reload to ensure the index definition is persisted and restored
  EXPECT_EQ(Run({"save", "df"}), "OK");
  EXPECT_EQ(Run({"debug", "reload"}), "OK");

  // Verify a basic search works on the restored index
  auto search = Run({"FT.SEARCH", ":Order:index", "John"});
  ASSERT_THAT(search, ArgType(RespExpr::ARRAY));
  const auto& v = search.GetVec();
  ASSERT_FALSE(v.empty());
  EXPECT_THAT(v.front(), IntArg(1));
}

// Parametrized test for RestoreVectorSearchIndexHnsw with varying document counts
class HnswRestoreTest : public RdbTest, public testing::WithParamInterface<int> {};

TEST_P(HnswRestoreTest, RestoreVectorSearchIndexHnsw) {
  int num_docs = GetParam();

  EXPECT_EQ(
      Run({"FT.CREATE", "only_vec_idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA", "embedding",
           "VECTOR", "HNSW", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2"}),
      "OK");

  EXPECT_EQ(Run({"FT.CREATE", "vec_idx", "ON",   "HASH",      "PREFIX",          "1",    "doc:",
                 "SCHEMA",    "name",    "TEXT", "embedding", "VECTOR",          "HNSW", "6",
                 "TYPE",      "FLOAT32", "DIM",  "2",         "DISTANCE_METRIC", "L2"}),
            "OK");

  // Insert documents with incrementing vectors
  for (int i = 1; i <= num_docs; ++i) {
    float x = static_cast<float>(i * 2 - 1);
    float y = static_cast<float>(i * 2);
    Run({"HSET", StrCat("doc:", i), "name", StrCat("doc", i), "embedding",
         StrCat(FloatToBytes(x), FloatToBytes(y))});
  }

  LOG(INFO) << "Created " << num_docs << " documents with vector embeddings";

  EXPECT_EQ(Run({"save", "df"}), "OK");
  auto save_info = service_->server_family().GetLastSaveInfo();

  // Reload from the saved file - this should restore the HNSW index, not rebuild it
  // Look for "Restored HNSW index" in logs to verify restoration vs rebuild
  LOG(INFO) << "Reloading from " << save_info.file_name << " - expecting HNSW index restoration";
  EXPECT_EQ(Run({"dfly", "load", save_info.file_name}), "OK");

  // Wait for async index building to complete on both indices
  auto is_indexing_done = [this](string_view idx_name) {
    auto resp = Run({"FT.INFO", idx_name});
    auto arr = resp.GetVec();
    auto it = rng::find_if(arr, [](const auto& e) { return e == "indexing"; });
    return it != arr.end() && (++it)->GetInt() == 0;
  };

  ASSERT_TRUE(WaitUntilCondition([&] { return is_indexing_done("vec_idx"); },
                                 std::chrono::milliseconds(10000)));
  ASSERT_TRUE(WaitUntilCondition([&] { return is_indexing_done("only_vec_idx"); },
                                 std::chrono::milliseconds(10000)));

  // Verify text search still works on the restored index
  auto search = Run({"FT.SEARCH", "vec_idx", "doc1"});
  ASSERT_THAT(search, ArgType(RespExpr::ARRAY));
  const auto& v = search.GetVec();
  ASSERT_FALSE(v.empty());
  EXPECT_THAT(v.front(), IntArg(1));

  // Verify KNN vector search works on the restored index
  // Query vector close to (1.0, 2.0) should find doc:1 as nearest
  string query_vec = StrCat(FloatToBytes(1.1f), FloatToBytes(2.1f));
  auto knn_search = Run({"FT.SEARCH", "vec_idx", "*=>[KNN 2 @embedding $vec]", "PARAMS", "2", "vec",
                         query_vec, "RETURN", "1", "name"});
  ASSERT_THAT(knn_search, ArgType(RespExpr::ARRAY));
  EXPECT_GE(knn_search.GetVec().front().GetInt(), 1);

  // The same check for another index with only vector field
  knn_search = Run({"FT.SEARCH", "only_vec_idx", "*=>[KNN 2 @embedding $vec]", "PARAMS", "2", "vec",
                    query_vec, "RETURN", "1", "name"});
  ASSERT_THAT(knn_search, ArgType(RespExpr::ARRAY));
  EXPECT_GE(knn_search.GetVec().front().GetInt(), 1);

  // Verify total document count matches
  EXPECT_EQ(CheckedInt({"dbsize"}), num_docs);

  LOG(INFO) << "Successfully verified HNSW index restoration with " << num_docs << " documents";
}

INSTANTIATE_TEST_SUITE_P(HnswRestoreTest, HnswRestoreTest, Values(5, 50, 500, 1000),
                         [](const testing::TestParamInfo<int>& info) {
                           return StrCat("Docs", info.param);
                         });

TEST_F(RdbTest, DflyLoadAppend) {
  // Create an RDB with (k1,1) value in it saved as `filename`
  EXPECT_EQ(Run({"set", "k1", "1"}), "OK");
  EXPECT_EQ(Run({"save", "df"}), "OK");
  string filename = service_->server_family().GetLastSaveInfo().file_name;

  // Without APPEND option - db should be flushed
  EXPECT_EQ(Run({"set", "k1", "TO-BE-FLUSHED"}), "OK");
  EXPECT_EQ(Run({"set", "k2", "TO-BE-FLUSHED"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", filename}), "OK");
  EXPECT_THAT(Run({"dbsize"}), IntArg(1));
  EXPECT_EQ(Run({"get", "k1"}), "1");

  // With APPEND option - db shouldn't be flushed, but k1 should be overridden
  EXPECT_EQ(Run({"set", "k1", "TO-BE-OVERRIDDEN"}), "OK");
  EXPECT_EQ(Run({"set", "k2", "2"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", filename, "append"}), "OK");
  EXPECT_THAT(Run({"dbsize"}), IntArg(2));
  EXPECT_EQ(Run({"get", "k1"}), "1");
  EXPECT_EQ(Run({"get", "k2"}), "2");
}

// Tests loading a huge set, where the set is loaded in multiple partial reads.
TEST_F(RdbTest, LoadHugeSet) {
  // Add 2 sets with 100k elements each (note must have more than kMaxBlobLen
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "set", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"scard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"scard", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"scard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"scard", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 24'000'000u);
}

// Tests loading a huge hmap, where the map is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeHMap) {
  // Add 2 sets with 100k elements each (note must have more than kMaxBlobLen
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "hash", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"hlen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"hlen", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"hlen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"hlen", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 29'000'000u);
}

// Tests loading a huge zset, where the zset is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeZSet) {
  // Add 2 sets with 100k elements each (note must have more than kMaxBlobLen
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "zset", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"zcard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"zcard", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"zcard", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"zcard", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 26'000'000u);
}

// Tests loading a huge list, where the list is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeList) {
  // Add 2 lists with 100k elements each (note must have more than 512*8Kb
  // elements to test partial reads).
  Run({"debug", "populate", "2", "test", "100", "rand", "type", "list", "elements", "100000"});
  ASSERT_EQ(100000, CheckedInt({"llen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"llen", "test:1"}));

  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(100000, CheckedInt({"llen", "test:0"}));
  ASSERT_EQ(100000, CheckedInt({"llen", "test:1"}));
  auto metrics = GetMetrics();
  EXPECT_GT(metrics.db_stats[0].obj_memory_usage, 20'000'000u);
}

// Tests loading a huge stream, where the stream is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeStream) {
  TEST_current_time_ms = 1000;

  // Add a huge stream (test:0) with 2000 entries, and 4 1k elements per entry
  // (note must be more than 512*4kb elements to test partial reads).
  // We add 2000 entries to the stream to ensure that the stream, because populate stream
  // adds only a single entry at a time, with multiple elements in it.

  Run({"debug", "populate", "1", "test", "2000", "rand", "type", "stream", "elements", "8000"});

  ASSERT_EQ(2000, CheckedInt({"xlen", "test:0"}));
  Run({"XGROUP", "CREATE", "test:0", "grp1", "0"});
  Run({"XGROUP", "CREATE", "test:0", "grp2", "0"});
  Run({"XREADGROUP", "GROUP", "grp1", "Alice", "COUNT", "1", "STREAMS", "test:0", ">"});
  Run({"XREADGROUP", "GROUP", "grp2", "Alice", "COUNT", "1", "STREAMS", "test:0", ">"});

  auto resp = Run({"xinfo", "stream", "test:0"});

  EXPECT_THAT(
      resp, RespElementsAre("length", 2000, "radix-tree-keys", 2000, "radix-tree-nodes", 2010,
                            "last-generated-id", "1000-1999", "max-deleted-entry-id", "0-0",
                            "entries-added", 2000, "recorded-first-entry-id", "1000-0", "groups", 2,
                            "first-entry", ArrLen(2), "last-entry", ArrLen(2)));

  resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"dfly", "load", save_info.file_name});
  ASSERT_EQ(resp, "OK");

  ASSERT_EQ(2000, CheckedInt({"xlen", "test:0"}));
  resp = Run({"xinfo", "stream", "test:0"});
  EXPECT_THAT(
      resp, RespElementsAre("length", 2000, "radix-tree-keys", 2000, "radix-tree-nodes", 2010,
                            "last-generated-id", "1000-1999", "max-deleted-entry-id", "0-0",
                            "entries-added", 2000, "recorded-first-entry-id", "1000-0", "groups", 2,
                            "first-entry", ArrLen(2), "last-entry", ArrLen(2)));
  resp = Run({"xinfo", "groups", "test:0"});
  EXPECT_THAT(resp, RespElementsAre(RespElementsAre("name", "grp1", "consumers", 1, "pending", 1,
                                                    "last-delivered-id", "1000-0", "entries-read",
                                                    1, "lag", 1999),
                                    _));
}

TEST_F(RdbTest, LoadStream2) {
  auto ec = LoadRdb("RDB_TYPE_STREAM_LISTPACKS_2.rdb");
  ASSERT_FALSE(ec) << ec.message();
  auto res = Run({"XINFO", "STREAM", "mystream"});
  ASSERT_THAT(res.GetVec(),
              ElementsAre("length", 2, "radix-tree-keys", 1, "radix-tree-nodes", 2,
                          "last-generated-id", "1732613360686-0", "max-deleted-entry-id", "0-0",
                          "entries-added", 2, "recorded-first-entry-id", "1732613352350-0",
                          "groups", 1, "first-entry", RespElementsAre("1732613352350-0", _),
                          "last-entry", RespElementsAre("1732613360686-0", _)));
}

TEST_F(RdbTest, LoadStream3) {
  auto ec = LoadRdb("RDB_TYPE_STREAM_LISTPACKS_3.rdb");
  ASSERT_FALSE(ec) << ec.message();
  auto res = Run({"XINFO", "STREAM", "mystream"});
  ASSERT_THAT(
      res.GetVec(),
      ElementsAre("length", 2, "radix-tree-keys", 1, "radix-tree-nodes", 2, "last-generated-id",
                  "1732614679549-0", "max-deleted-entry-id", "0-0", "entries-added", 2,
                  "recorded-first-entry-id", "1732614676541-0", "groups", 1, "first-entry",
                  ArgType(RespExpr::ARRAY), "last-entry", ArgType(RespExpr::ARRAY)));
}

TEST_F(RdbTest, SnapshotTooBig) {
  // Run({"debug", "populate", "10000", "foo", "1000"});
  //  usleep(5000);  // let the stats to sync
  max_memory_limit = 100000;
  used_mem_current = 1000000;
  auto resp = Run({"debug", "reload"});
  ASSERT_THAT(resp, ErrArg("Out of memory"));
}

TEST_F(RdbTest, HugeKeyIssue4497) {
  absl::FlagSaver fs;
  SetTestFlag("cache_mode", "true");
  ResetService();

  EXPECT_EQ(Run({"flushall"}), "OK");
  EXPECT_EQ(Run({"debug", "populate", "1", "k", "1000", "rand", "type", "set", "elements", "5000"}),
            "OK");
  EXPECT_EQ(Run({"save", "rdb", "hugekey.rdb"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", "hugekey.rdb"}), "OK");
  EXPECT_EQ(Run({"flushall"}), "OK");
}

TEST_F(RdbTest, HugeKeyIssue4554) {
  absl::FlagSaver fs;
  SetTestFlag("cache_mode", "true");
  // We need to stress one flow/shard such that the others finish early. Lock on hashtags allows
  // that.
  SetTestFlag("lock_on_hashtags", "true");
  ResetService();

  EXPECT_EQ(
      Run({"debug", "populate", "20", "{tmp}", "20", "rand", "type", "set", "elements", "10000"}),
      "OK");
  EXPECT_EQ(Run({"save", "df", "hugekey"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", "hugekey-summary.dfs"}), "OK");
  EXPECT_EQ(Run({"flushall"}), "OK");
}

// ignore_expiry.rdb contains 2 keys which are expired keys
// this test case verifies wheather rdb_ignore_expiry flag is working as expected.
TEST_F(RdbTest, RDBIgnoreExpiryFlag) {
  absl::FlagSaver fs;

  SetTestFlag("rdb_ignore_expiry", "true");
  auto ec = LoadRdb("ignore_expiry.rdb");

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"scan", "0"});

  ASSERT_THAT(resp, ArrLen(2));

  EXPECT_THAT(StrArray(resp.GetVec()[1]), UnorderedElementsAre("test", "test2"));

  EXPECT_THAT(Run({"get", "test"}), "expkey");
  EXPECT_THAT(Run({"get", "test2"}), "expkey");

  int ttl = CheckedInt({"ttl", "test"});  // should ignore expiry for key
  EXPECT_EQ(ttl, -1);

  int ttl2 = CheckedInt({"ttl", "test2"});  // should ignore expiry for key
  EXPECT_EQ(ttl2, -1);
}

TEST_F(RdbTest, CmsSerialization) {
  Run("cms.initbydim cms 1000 5");
  Run("cms.incrby cms foo 5 bar 3 baz 9");

  auto resp = Run("cms.query cms foo bar baz");
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(5), IntArg(3), IntArg(9))));

  Run("save df cms");
  Run("flushall");
  EXPECT_EQ(Run("dfly load cms-summary.dfs"), "OK");

  resp = Run("cms.query cms foo bar baz");
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(5), IntArg(3), IntArg(9))));
}

// Tests basic TOPK save/load: verifies that top-k heap items are correctly serialized
// and restored, maintaining their frequency-based ordering.
// Uses TOPK.INCRBY with large increments to ensure deterministic counts despite
// the stochastic HeavyKeeper decay (decay^count ≈ 0 for large counts).
TEST_F(RdbTest, TopkSerializationBasic) {
  Run({"TOPK.RESERVE", "topk_small", "3", "50", "7", "0.9"});
  Run({"TOPK.INCRBY", "topk_small", "foo", "300", "bar", "200", "baz", "400"});

  auto resp = Run({"TOPK.LIST", "topk_small"});
  EXPECT_THAT(resp, RespArray(ElementsAre("baz", "foo", "bar")));

  Run({"debug", "reload"});

  resp = Run({"TOPK.LIST", "topk_small"});
  EXPECT_THAT(resp, RespArray(ElementsAre("baz", "foo", "bar")));
}

// Tests that the Count-Min Sketch counter array is correctly serialized:
// verifies that existing counters suppress colliding items correctly after load.
TEST_F(RdbTest, TopkSerializationCounterArrayIntegrity) {
  Run({"TOPK.RESERVE", "topk_counters", "5", "100", "5", "0.9"});
  Run({"TOPK.INCRBY", "topk_counters", "alpha", "300", "beta", "200"});

  Run({"debug", "reload"});

  // Verify counts are preserved via TOPK.COUNT, which reads the counter array directly.
  // If counters weren't restored, these would return 0 (or wrong values).
  // TOPK.COUNT returns an array with one element per queried item.
  auto counts = Run({"TOPK.COUNT", "topk_counters", "alpha", "beta"});
  ASSERT_THAT(counts, ArrLen(2));
  int64_t alpha_count = counts.GetVec()[0].GetInt().value_or(0);
  int64_t beta_count = counts.GetVec()[1].GetInt().value_or(0);
  EXPECT_GE(alpha_count, 1);
  EXPECT_GE(beta_count, 1);
  EXPECT_GT(alpha_count, beta_count);

  // Also verify items are still in the heap (heap restoration).
  EXPECT_THAT(Run({"TOPK.QUERY", "topk_counters", "alpha", "beta"}),
              RespArray(ElementsAre(IntArg(1), IntArg(1))));

  auto resp = Run({"TOPK.LIST", "topk_counters"});
  EXPECT_THAT(resp, RespArray(ElementsAre("alpha", "beta")));
}

// Tests that K parameter (max heap size) is preserved after serialization:
// verifies list size stays at K=3 and eviction works correctly after load.
TEST_F(RdbTest, TopkSerializationParametersPreserved) {
  Run({"TOPK.RESERVE", "topk_params", "3", "64", "4", "0.95"});
  Run({"TOPK.INCRBY", "topk_params", "a", "100", "b", "200", "c", "300"});

  Run({"debug", "reload"});

  auto before = Run({"TOPK.LIST", "topk_params"});
  ASSERT_THAT(before, ArrLen(3));  // K=3 must be enforced

  // Add a new item heavily. It should evict the lowest item, maintaining K=3.
  Run({"TOPK.INCRBY", "topk_params", "z", "1000"});

  auto after = Run({"TOPK.LIST", "topk_params"});
  ASSERT_THAT(after, ArrLen(3));
  EXPECT_EQ(after.GetVec().front(), "z");  // 'z' should be the new king
}

// Tests serialization of heap-allocated strings (bypass SSO) to verify correct
// memory handling for string pointers in the min-heap.
TEST_F(RdbTest, TopkSerializationExtensive) {
  Run({"TOPK.RESERVE", "topk_large", "10", "128", "5", "0.9"});

  // Bypass SSO (Small String Optimization) to test memory pointers
  std::string long_str1(50, 'A');
  std::string long_str2(60, 'B');
  std::string long_str3(70, 'C');

  // Use INCRBY with large values to ensure deterministic counts
  Run({"TOPK.INCRBY", "topk_large", long_str1, "500"});
  Run({"TOPK.INCRBY", "topk_large", long_str2, "300"});
  Run({"TOPK.INCRBY", "topk_large", long_str3, "700"});

  Run({"debug", "reload"});

  auto resp = Run({"TOPK.LIST", "topk_large"});
  EXPECT_THAT(resp, RespArray(ElementsAre(long_str3, long_str1, long_str2)));
}

// Tests that empty TOPK (zero items in heap) can be saved and loaded correctly:
// validates TagAllowsEmptyValue() and ensures structure remains functional after load.
TEST_F(RdbTest, TopkSerializationEmptyEdgeCase) {
  Run({"TOPK.RESERVE", "topk_empty", "5", "50", "3", "0.9"});

  Run({"debug", "reload"});

  auto resp = Run({"TOPK.LIST", "topk_empty"});
  EXPECT_THAT(resp, ArrLen(0));

  // After loading an empty TOPK, adding items must work correctly.
  Run({"TOPK.INCRBY", "topk_empty", "new_item", "100"});
  resp = Run({"TOPK.LIST", "topk_empty"});
  // Run() unwraps single-element arrays to a scalar string
  EXPECT_EQ(resp, "new_item");
}

// Tests that the decay parameter (double) is correctly serialized using SaveBinaryDouble/
// FetchBinaryDouble: critical test for the strict aliasing fix (no reinterpret_cast).
TEST_F(RdbTest, TopkSerializationDecayParameter) {
  // Create TOPK with extreme decay values to ensure the double serialization works
  Run({"TOPK.RESERVE", "topk_decay_low", "5", "50", "3", "0.1"});     // Very aggressive decay
  Run({"TOPK.RESERVE", "topk_decay_high", "5", "50", "3", "0.999"});  // Minimal decay

  // Use INCRBY with large values to ensure deterministic counts
  Run({"TOPK.INCRBY", "topk_decay_low", "item1", "500", "item2", "300"});
  Run({"TOPK.INCRBY", "topk_decay_high", "item3", "500", "item4", "300"});

  Run({"debug", "reload"});

  // Verify both TOPKs loaded successfully and maintain their items
  auto resp1 = Run({"TOPK.LIST", "topk_decay_low"});
  EXPECT_THAT(resp1, RespArray(ElementsAre("item1", "item2")));

  auto resp2 = Run({"TOPK.LIST", "topk_decay_high"});
  EXPECT_THAT(resp2, RespArray(ElementsAre("item3", "item4")));
}

void AssertTaggedData(std::string_view blob, std::string_view expected, uint32_t expected_id = 1) {
  using namespace absl::little_endian;

  ASSERT_EQ(blob.size(), MemBufController::kHeaderSize + expected.size());
  EXPECT_EQ(static_cast<uint8_t>(blob[0]), RDB_OPCODE_TAGGED_CHUNK);

  auto id = Load32(reinterpret_cast<const uint8_t*>(blob.data()) + 1);
  auto len = Load32(reinterpret_cast<const uint8_t*>(blob.data()) + 5);

  EXPECT_EQ(id, expected_id);
  EXPECT_EQ(len, expected.size());
  EXPECT_EQ(blob.substr(MemBufController::kHeaderSize), expected);
}

class MemBufControllerTest : public Test {
 protected:
  MemBufController controller_;

  bool HasSplitEntries() const {
    return !controller_.split_entries_.empty();
  }

  std::string Flush() {
    auto current = controller_.CurrentBuffer()->InputBuffer();
    const auto blob = controller_.BuildBlob(current);
    EXPECT_EQ(controller_.FlushableSize(), 0);
    return blob;
  }

  void Write(std::string_view s) {
    controller_.CurrentBuffer()->WriteAndCommit(s.data(), s.size());
  }

  void AssertDefaultState() const {
    EXPECT_EQ(controller_.active_id_, 0u);
    EXPECT_EQ(controller_.CurrentBuffer(), &controller_.default_buffer_);
  }

  void MarkMidFlush() {
    controller_.MarkEntrySplit();
    EXPECT_TRUE(controller_.split_entries_.contains(controller_.active_id_));
  }

  MemBufController::EntryId SplitAndSuspend(std::string_view payload, uint32_t expected_id) {
    controller_.StartEntry();
    EXPECT_EQ(controller_.active_id_, expected_id);
    Write(payload);
    MarkMidFlush();
    AssertTaggedData(Flush(), payload, expected_id);

    const auto saved_id = controller_.SaveStateBeforeConsume();
    EXPECT_EQ(saved_id, expected_id);
    AssertDefaultState();
    EXPECT_EQ(controller_.FlushableSize(), 0);
    return saved_id;
  }

  void Restore(MemBufController::EntryId id) {
    controller_.RestoreStateAfterConsume(id);
    EXPECT_EQ(controller_.active_id_, id);
    EXPECT_EQ(controller_.CurrentBuffer(), &controller_.entry_buffer_);
  }
};

TEST_F(MemBufControllerTest, TaggedData) {
  controller_.SetTagEntries(true);

  const std::string_view data = "a_a_a_";
  const auto saved_id = SplitAndSuspend(data, 1);
  EXPECT_TRUE(HasSplitEntries());

  Write("a");
  Restore(saved_id);
  EXPECT_EQ(controller_.FlushableSize(), 1);

  Write("b");
  EXPECT_EQ(controller_.FlushableSize(), 2);
  controller_.FinishEntry();
  EXPECT_FALSE(HasSplitEntries());

  const std::string blob = Flush();

  EXPECT_EQ(blob.size(), MemBufController::kHeaderSize + 2);
  EXPECT_EQ(blob[0], 'a');
  AssertTaggedData(blob.substr(1), "b");
}

TEST_F(MemBufControllerTest, NestedInterleaving) {
  controller_.SetTagEntries(true);

  const auto saved_id_a = SplitAndSuspend("aaa", 1);
  const auto saved_id_b = SplitAndSuspend("bbb", 2);

  controller_.StartEntry();
  Write("ccc");
  controller_.FinishEntry();
  AssertDefaultState();

  EXPECT_EQ(controller_.FlushableSize(), 3);

  EXPECT_EQ(Flush(), "ccc");

  Restore(saved_id_b);
  Write("x");
  controller_.FinishEntry();

  AssertTaggedData(Flush(), "x", 2);

  Restore(saved_id_a);
  Write("y");
  controller_.FinishEntry();
  EXPECT_FALSE(HasSplitEntries());

  AssertTaggedData(Flush(), "y");
}

TEST_F(MemBufControllerTest, BuildBlobEdgeCases) {
  controller_.SetTagEntries(true);

  Write("p");
  controller_.StartEntry();
  Write("x");
  MarkMidFlush();

  const std::string blob = Flush();
  ASSERT_FALSE(blob.empty());
  EXPECT_EQ(blob[0], 'p');
  AssertTaggedData(blob.substr(1), "x");

  controller_.FinishEntry();
  AssertDefaultState();
}

TEST_F(MemBufControllerTest, UnsplitEntry) {
  controller_.SetTagEntries(true);

  controller_.StartEntry();
  Write("hello");
  controller_.FinishEntry();
  AssertDefaultState();

  EXPECT_EQ(controller_.FlushableSize(), 5);
  EXPECT_EQ(Flush(), "hello");
}

TEST_F(MemBufControllerTest, TaggingDisabled) {
  controller_.StartEntry();
  Write("abc");
  MarkMidFlush();

  EXPECT_EQ(Flush(), "abc");

  const auto saved_id = controller_.SaveStateBeforeConsume();
  Restore(saved_id);

  Write("def");
  controller_.FinishEntry();

  EXPECT_EQ(Flush(), "def");
}

namespace {

// Wraps string in rdb version, eof, checksum, etc so it can be fed to a loader
std::string WrapInRdb(std::string_view body) {
  std::string out = absl::StrFormat("REDIS%04d", RDB_SER_VERSION);
  out.append(body);
  out.push_back(static_cast<char>(RDB_OPCODE_EOF));
  constexpr uint8_t checksum[8] = {};
  out.append(reinterpret_cast<const char*>(checksum), sizeof(checksum));
  return out;
}

std::error_code LoadRdbData(Service* service, const std::string& rdb,
                            std::optional<uint64_t> journal_offset = std::nullopt) {
  io::BytesSource src{io::Buffer(rdb)};
  RdbLoadContext load_context;
  RdbLoader loader(service, &load_context);
  auto ec = loader.Load(&src);
  EXPECT_EQ(loader.journal_offset(), journal_offset);
  return ec;
}

void AppendLen(std::string* out, uint64_t len) {
  uint8_t buf[9];
  const auto sz = WritePackedUInt(len, {buf, sizeof(buf)});
  out->append(reinterpret_cast<const char*>(buf), sz);
}

void AppendString(std::string* out, std::string_view s) {
  AppendLen(out, s.size());
  out->append(s);
}

void AddKV(std::string* out, std::string_view key, std::string_view val) {
  AppendString(out, key);
  AppendString(out, val);
}

std::string MakeTaggedChunk(uint32_t id, std::string_view payload) {
  std::string out;
  out.push_back(static_cast<char>(RDB_OPCODE_TAGGED_CHUNK));

  uint8_t header[8];
  absl::little_endian::Store32(header, id);
  absl::little_endian::Store32(header + 4, payload.size());
  out.append(reinterpret_cast<const char*>(header), sizeof(header));

  out.append(payload);
  return out;
}

void AppendBinaryDouble(std::string* out, double val) {
  uint64_t bits;
  memcpy(&bits, &val, sizeof(bits));

  uint8_t buf[8];
  absl::little_endian::Store64(buf, bits);
  out->append(reinterpret_cast<const char*>(buf), sizeof(buf));
}

}  // namespace

// The following are tests that directly feed byte data to loader to exercise chunk loading.
// Some of these will become redundant once the saver starts sending chunked data, so instead of
// hand-crafting data we will be able to load from the db directly.

TEST_F(RdbTest, LoadTwoChunks) {
  std::string first;
  first.push_back(RDB_TYPE_HASH);
  AppendString(&first, "h");
  AppendLen(&first, 2);
  AddKV(&first, "f1", "v1");

  std::string second;
  AddKV(&second, "f2", "v2");

  std::string body;
  // hash is split across two tagged chunks
  body += MakeTaggedChunk(1, first);
  body += MakeTaggedChunk(1, second);

  const auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"HGET", "h", "f1"}), "v1");
  EXPECT_EQ(Run({"HGET", "h", "f2"}), "v2");
}

TEST_F(RdbTest, InterleavedLoad) {
  // must have >1 shards for non inlined path check. find a key that lands in shard 1 by hashing, to
  // test non inlined obj. creation
  ASSERT_GT(shard_set->size(), 1u);
  std::string key;
  for (unsigned i = 0; i < 1000; ++i) {
    key = StrCat("x", i);
    if (Shard(key, shard_set->size()) == 1)
      break;
  }
  ASSERT_EQ(Shard(key, shard_set->size()), 1u);

  std::string a1;
  // hash chunk 1
  a1.push_back(RDB_TYPE_HASH);
  AppendString(&a1, key);
  AppendLen(&a1, 2);
  AddKV(&a1, "f1", "v1");

  // string
  std::string b;
  b.push_back(RDB_TYPE_STRING);
  AppendString(&b, "b");
  AppendString(&b, "plain");

  // hash chunk 2
  std::string a2;
  AddKV(&a2, "f2", "v2");

  std::string body;
  // chunk for db 0
  body += MakeTaggedChunk(1, a1);
  // simple string b=plain
  body += b;
  body.push_back(static_cast<char>(RDB_OPCODE_SELECTDB));
  // switch to db 1
  AppendLen(&body, 1);
  // back to chunk for db 0
  body += MakeTaggedChunk(1, a2);

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"SELECT", "0"}), "OK");
  EXPECT_EQ(Run({"HGET", key, "f1"}), "v1");
  EXPECT_EQ(Run({"HGET", key, "f2"}), "v2");
  EXPECT_EQ(Run({"GET", "b"}), "plain");

  EXPECT_EQ(Run({"SELECT", "1"}), "OK");
  EXPECT_THAT(Run({"EXISTS", key}), IntArg(0));
  EXPECT_EQ(Run({"SELECT", "0"}), "OK");
}

TEST_F(RdbTest, ChunksAroundJournalOffset) {
  std::string a1;
  a1.push_back(RDB_TYPE_HASH);
  AppendString(&a1, "a");
  AppendLen(&a1, 2);
  AddKV(&a1, "f1", "v1");

  std::string a2;
  AddKV(&a2, "f2", "v2");

  std::string body;
  body += MakeTaggedChunk(1, a1);

  // put the journal offset in the middle
  body.push_back(static_cast<char>(RDB_OPCODE_JOURNAL_OFFSET));
  uint8_t offset_bytes[8];
  absl::little_endian::Store64(offset_bytes, 1234);
  body.append(reinterpret_cast<const char*>(offset_bytes), sizeof(offset_bytes));

  body += MakeTaggedChunk(1, a2);

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body), 1234); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"HGET", "a", "f1"}), "v1");
  EXPECT_EQ(Run({"HGET", "a", "f2"}), "v2");
}

TEST_F(RdbTest, SplitSBF) {
  // this test creates two filter SBF, then splits one of the filters. Since in sbf loading there
  // are two layers of possible splits, intra-filter and inter-filter, this test exercises both
  // splits. A plain string is also added between the split filter.

  // Creates filter in db to copy the fields from
  auto resp = Run({"BF.RESERVE", "bf_src", "0.01", "10"});
  EXPECT_EQ(resp, "OK");
  for (size_t i = 0; i < 50; ++i) {
    resp = Run({"BF.ADD", "bf_src", StrCat("item", i)});
    EXPECT_THAT(resp, AnyOf(0, 1));
  }

  std::string first;
  std::string blob1;

  // split the blob of the second filter into three chunks. this exercises the loader path where we
  // first try to load the incomplete filter, and return early before that finishes
  constexpr size_t kFirstSplit = 17;
  constexpr size_t kSecondSplit = 13;

  pp_->at(0)->Await([&] {
    const DbContext ctx{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()};
    const auto& db = ctx.GetDbSlice(0);
    auto it = db.FindReadOnly(ctx, "bf_src", OBJ_SBF);
    ASSERT_TRUE(it.ok());

    const SBF* sbf = it.value()->second.GetSBF();
    ASSERT_GE(sbf->num_filters(), 2);

    const std::string blob0{sbf->data(0)};

    blob1 = std::string{sbf->data(1)};
    ASSERT_GT(blob1.size(), kFirstSplit + kSecondSplit);

    first.push_back(RDB_TYPE_SBF2);
    // brand new key whose shape is copied off bf_src
    AppendString(&first, "bf_loaded");
    AppendLen(&first, 0);
    AppendBinaryDouble(&first, sbf->grow_factor());
    AppendBinaryDouble(&first, sbf->fp_probability());
    AppendLen(&first, sbf->prev_size());
    AppendLen(&first, sbf->current_size());
    AppendLen(&first, sbf->max_capacity());
    AppendLen(&first, sbf->num_filters());

    AppendLen(&first, sbf->hashfunc_cnt(0));
    // total size of blob0
    AppendLen(&first, blob0.size());
    // this chunk size (all of blob0 is fit in one chunk)
    AppendLen(&first, blob0.size());
    first.append(blob0);

    AppendLen(&first, sbf->hashfunc_cnt(1));
    // total size of blob1
    AppendLen(&first, blob1.size());
    // only 17 bytes from blob1 in this chunk
    AppendLen(&first, kFirstSplit);
    first.append(blob1.data(), kFirstSplit);
  });

  // add this plain string between chunks of blob1 filter
  std::string plain;
  plain.push_back(RDB_TYPE_STRING);
  AppendString(&plain, "plain_key");
  AppendString(&plain, "plain_val");

  // p2 of blob1
  std::string second;
  AppendLen(&second, kSecondSplit);
  second.append(blob1.data() + kFirstSplit, kSecondSplit);

  // p3 of blob1
  std::string third;
  constexpr auto kPrefixConsumed = kFirstSplit + kSecondSplit;
  AppendLen(&third, blob1.size() - kPrefixConsumed);
  third.append(blob1.data() + kPrefixConsumed, blob1.size() - kPrefixConsumed);

  std::string body;
  body += MakeTaggedChunk(1, first);
  body += plain;
  body += MakeTaggedChunk(1, second);
  body += MakeTaggedChunk(1, third);

  EXPECT_EQ(Run({"FLUSHALL"}), "OK");

  auto ec = pp_->at(0)->Await([&] { return LoadRdbData(service_.get(), WrapInRdb(body)); });
  ASSERT_FALSE(ec) << ec.message();

  EXPECT_EQ(Run({"TYPE", "bf_loaded"}), "MBbloom--");
  EXPECT_EQ(Run({"GET", "plain_key"}), "plain_val");

  for (size_t i = 0; i < 50; ++i) {
    EXPECT_THAT(Run({"BF.EXISTS", "bf_loaded", StrCat("item", i)}), IntArg(1));
  }
}

}  // namespace dfly
