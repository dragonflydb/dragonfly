// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <gmock/gmock.h>

extern "C" {
#include "redis/crc64.h"
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/zmalloc.h"
}

#include <absl/flags/reflection.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"  // needed to find operator== for RespExpr.
#include "io/file.h"
#include "server/engine_shard_set.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(int32, list_compress_depth);
ABSL_DECLARE_FLAG(int32, list_max_listpack_size);
ABSL_DECLARE_FLAG(dfly::CompressionMode, compression_mode);

namespace dfly {

static const auto kMatchNil = ArgType(RespExpr::NIL);

class RdbTest : public BaseFamilyTest {
 protected:
  void SetUp();

  io::FileSource GetSource(string name);

  std::error_code LoadRdb(const string& filename) {
    return pp_->at(0)->Await([&] {
      io::FileSource fs = GetSource(filename);

      RdbLoader loader(service_.get());
      return loader.Load(&fs);
    });
  }
};

void RdbTest::SetUp() {
  // Setting max_memory_limit must be before calling  InitWithDbFilename
  max_memory_limit = 40000000;
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
}

// Tests loading a huge stream, where the stream is loaded in multiple partial
// reads.
TEST_F(RdbTest, LoadHugeStream) {
  TEST_current_time_ms = 1000;

  // Add a huge stream (test:0) with 2000 entries, and 4 1k elements per entry
  // (note must be more than 512*4kb elements to test partial reads).
  // We add 2000 entries to the stream to ensure that the stream, because populate strream
  // adds only a single entry at a time, with multiple elements in it.
  for (unsigned i = 0; i < 2000; i++) {
    Run({"debug", "populate", "1", "test", "2000", "rand", "type", "stream", "elements", "4"});
  }
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
  SetTestFlag("cache_mode", "true");
  ResetService();

  EXPECT_EQ(Run({"flushall"}), "OK");
  EXPECT_EQ(Run({"debug", "populate", "1", "k", "1000", "rand", "type", "set", "elements", "5000"}),
            "OK");
  EXPECT_EQ(Run({"save", "rdb", "hugekey.rdb"}), "OK");
  EXPECT_EQ(Run({"dfly", "load", "hugekey.rdb"}), "OK");
  EXPECT_EQ(Run({"flushall"}), "OK");
}

}  // namespace dfly
