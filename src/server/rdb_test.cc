// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <gmock/gmock.h>

extern "C" {
#include "redis/crc64.h"
#include "redis/zmalloc.h"
}

#include "base/gtest.h"
#include "base/logging.h"
#include "io/file.h"
#include "server/engine_shard_set.h"
#include "server/rdb_load.h"
#include "util/uring/uring_pool.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class RdbTest : public testing::Test {
 protected:
  void SetUp() final;

  void TearDown() final {
    pp_->AwaitFiberOnAll([](auto*) { EngineShard::DestroyThreadLocal(); });
    ess_.reset();
    pp_->Stop();
  }

  static void SetUpTestSuite() {
    crc64_init();
    init_zmalloc_threadlocal();
  }

 protected:
  io::FileSource GetSource(string name);

  unique_ptr<ProactorPool> pp_;
  unique_ptr<EngineShardSet> ess_;
};

inline const uint8_t* to_byte(const void* s) {
  return reinterpret_cast<const uint8_t*>(s);
}

void RdbTest::SetUp() {
  pp_.reset(new uring::UringPool(16, 2));
  pp_->Run();
  ess_.reset(new EngineShardSet(pp_.get()));
  ess_->Init(pp_->size());

  pp_->Await([&](uint32_t index, ProactorBase* pb) { ess_->InitThreadLocal(pb, false); });
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
  RdbLoader loader;
  auto ec = loader.Load(&fs);
  CHECK(!ec);
}

TEST_F(RdbTest, LoadSmall) {
  io::FileSource fs = GetSource("small.rdb");
  RdbLoader loader;
  auto ec = loader.Load(&fs);
  CHECK(!ec);
}

}  // namespace dfly