// Copyright 2022, Roman Gershman.  All rights reserved.
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
#include "facade/facade_test.h"  // needed to find operator== for RespExpr.
#include "io/file.h"
#include "server/engine_shard_set.h"
#include "server/rdb_load.h"
#include "server/test_utils.h"
#include "util/uring/uring_pool.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(int32, list_compress_depth);
ABSL_DECLARE_FLAG(int32, list_max_listpack_size);

namespace dfly {

class RdbTest : public BaseFamilyTest {
 protected:
 protected:
  io::FileSource GetSource(string name);
};

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
  RdbLoader loader(service_->script_mgr());

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
  RdbLoader loader(service_->script_mgr());

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
  save_fb.join();
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
  auto metrics = service_->server_family().GetMetrics();
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

  save_fb.join();

  auto save_info = service_->server_family().GetLastSaveInfo();
  ASSERT_EQ(1, save_info->freq_map.size());
  auto& k_v = save_info->freq_map.front();

  EXPECT_EQ("string", k_v.first);
  EXPECT_EQ(60000, k_v.second);
  auto resp = Run({"debug", "reload", "NOSAVE"});
  EXPECT_EQ(resp, "OK");

  metrics = service_->server_family().GetMetrics();
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
  // Force OBJ_ENCODING_HT encoding.
  server.hash_max_listpack_value = 0;
  Run({"hset", "hmap1", "key1", "val", "key2", "val2"});
  Run({"hset", "hmap2", "key1", string(690557, 'a')});

  server.hash_max_listpack_value = 32;
  Run({"debug", "reload"});
  EXPECT_EQ(2, CheckedInt({"hlen", "hmap1"}));
}

}  // namespace dfly
