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
#include "io/file_util.h"
#include "server/engine_shard_set.h"
#include "server/rdb_load.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(int32, list_compress_depth);
ABSL_DECLARE_FLAG(int32, list_max_listpack_size);
ABSL_DECLARE_FLAG(int, compression_mode);
ABSL_DECLARE_FLAG(string, dbfilename);

namespace dfly {

class RdbTest : public BaseFamilyTest {
 protected:
  void TearDown();
  void SetUp();

  io::FileSource GetSource(string name);
};

void RdbTest::SetUp() {
  SetFlag(&FLAGS_dbfilename, "rdbtestdump");
  BaseFamilyTest::SetUp();
}

void RdbTest::TearDown() {
  // Disable save on shutdown
  SetFlag(&FLAGS_dbfilename, "");

  auto rdb_files = io::StatFiles("rdbtestdump*");
  CHECK(rdb_files);
  for (const auto& fl : *rdb_files) {
    unlink(fl.name.c_str());
  }
  BaseFamilyTest::TearDown();
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
  io::FileSource fs = GetSource("empty.rdb");
  RdbLoader loader(NULL);
  auto ec = loader.Load(&fs);
  CHECK(!ec);
}

TEST_F(RdbTest, LoadSmall6) {
  io::FileSource fs = GetSource("redis6_small.rdb");
  RdbLoader loader{service_.get()};

  // must run in proactor thread in order to avoid polluting the serverstate
  // in the main, testing thread.
  auto ec = pp_->at(0)->Await([&] { return loader.Load(&fs); });

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
  io::FileSource fs = GetSource("redis6_stream.rdb");
  RdbLoader loader{service_.get()};

  // must run in proactor thread in order to avoid polluting the serverstate
  // in the main, testing thread.
  auto ec = pp_->at(0)->Await([&] { return loader.Load(&fs); });

  ASSERT_FALSE(ec) << ec.message();

  auto resp = Run({"type", "key:10"});
  EXPECT_EQ(resp, "stream");
  resp = Run({"xinfo", "groups", "key:0"});
  EXPECT_THAT(resp, ArrLen(2));

  resp = Run({"xinfo", "groups", "key:1"});  // test dereferences array of size 1
  EXPECT_THAT(resp, ArrLen(8));
  EXPECT_THAT(resp.GetVec(), ElementsAre("name", "g2", "consumers", "0", "pending", "0",
                                         "last-delivered-id", "1655444851523-1"));

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

  for (int i = 0; i <= 3; ++i) {
    SetFlag(&FLAGS_compression_mode, i);
    RespExpr resp = Run({"save", "df"});
    ASSERT_EQ(resp, "OK");

    auto save_info = service_->server_family().GetLastSaveInfo();
    resp = Run({"debug", "load", save_info->file_name});
    ASSERT_EQ(resp, "OK");
    ASSERT_EQ(50000, CheckedInt({"dbsize"}));
  }
}

TEST_F(RdbTest, RdbLoaderOnReadCompressedDataShouldNotEnterEnsureReadFlow) {
  SetFlag(&FLAGS_compression_mode, 2);
  for (int i = 0; i < 1000; ++i) {
    Run({"set", StrCat(i), "1"});
  }
  RespExpr resp = Run({"save", "df"});
  ASSERT_EQ(resp, "OK");

  auto save_info = service_->server_family().GetLastSaveInfo();
  resp = Run({"debug", "load", save_info->file_name});
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

  auto resp = Run({"debug", "reload"});
  ASSERT_EQ(resp, "OK");

  EXPECT_EQ(2, CheckedInt({"scard", "set_key1"}));
  EXPECT_EQ(3, CheckedInt({"scard", "intset_key"}));
  EXPECT_EQ(2, CheckedInt({"hlen", "small_hset"}));
  EXPECT_EQ(2, CheckedInt({"hlen", "large_hset"}));
  EXPECT_EQ(4, CheckedInt({"LLEN", "list_key2"}));
  EXPECT_EQ(2, CheckedInt({"ZCARD", "zs1"}));
  EXPECT_EQ(2, CheckedInt({"ZCARD", "zs2"}));
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
  sleep(2);
  Run({"debug", "reload"});
  auto resp = Run({"get", "key"});
  ASSERT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(RdbTest, SaveFlush) {
  Run({"debug", "populate", "500000"});

  auto save_fb = pp_->at(1)->LaunchFiber([&] {
    RespExpr resp = Run({"save"});
    ASSERT_EQ(resp, "OK");
  });

  do {
    usleep(10);
  } while (!service_->server_family().IsSaving());

  Run({"flushdb"});
  save_fb.Join();
  auto save_info = service_->server_family().GetLastSaveInfo();
  ASSERT_EQ(1, save_info->freq_map.size());
  auto& k_v = save_info->freq_map.front();
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
  ASSERT_EQ(2, metrics.db.size());
  EXPECT_EQ(50000, metrics.db[0].key_count);
  EXPECT_EQ(10000, metrics.db[1].key_count);

  auto save_fb = pp_->at(0)->LaunchFiber([&] {
    RespExpr resp = Run({"save"});
    ASSERT_EQ(resp, "OK");
  });

  do {
    usleep(10);
  } while (!service_->server_family().IsSaving());

  pp_->at(1)->Await([&] {
    Run({"select", "1"});
    for (unsigned i = 0; i < 1000; ++i) {
      Run({"set", StrCat("abc", i), "bar"});
    }
  });

  save_fb.Join();

  auto save_info = service_->server_family().GetLastSaveInfo();
  ASSERT_EQ(1, save_info->freq_map.size());
  auto& k_v = save_info->freq_map.front();

  EXPECT_EQ("string", k_v.first);
  EXPECT_EQ(60000, k_v.second);
  auto resp = Run({"debug", "reload", "NOSAVE"});
  EXPECT_EQ(resp, "OK");

  metrics = GetMetrics();
  ASSERT_EQ(2, metrics.db.size());
  EXPECT_EQ(50000, metrics.db[0].key_count);
  EXPECT_EQ(10000, metrics.db[1].key_count);
  if (metrics.db[1].key_count != 10000) {
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
  server.hash_max_listpack_value = 0;
  Run({"hset", "hmap1", "key1", "val", "key2", "val2"});
  Run({"hset", "hmap2", "key1", string(690557, 'a')});

  server.hash_max_listpack_value = 32;
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
  server.hash_max_listpack_value = 0;
  Run({"hset", "hmap", "key1", "val", "key2", ""});

  server.hash_max_listpack_value = 32;
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
  io::FileSource fs = GetSource("hll.rdb");
  RdbLoader loader{service_.get()};

  // must run in proactor thread in order to avoid polluting the serverstate
  // in the main, testing thread.
  auto ec = pp_->at(0)->Await([&] { return loader.Load(&fs); });

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

}  // namespace dfly
