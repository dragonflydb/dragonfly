// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "facade/facade_test.h"
#include "server/test_utils.h"

namespace dfly {

using testing::ElementsAre;

class BloomFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(BloomFamilyTest, Basic) {
  auto resp = Run({"bf.reserve", "b1", "0.1", "32"});
  EXPECT_EQ(resp, "OK");
  EXPECT_EQ(Run({"type", "b1"}), "MBbloom--");
  EXPECT_THAT(Run({"bf.add", "b1", "a"}), IntArg(1));
  EXPECT_THAT(Run({"bf.add", "b1", "b"}), IntArg(1));
  EXPECT_THAT(Run({"bf.add", "b1", "b"}), IntArg(0));
  EXPECT_THAT(Run({"bf.add", "b2", "b"}), IntArg(1));
  EXPECT_EQ(Run({"type", "b2"}), "MBbloom--");

  EXPECT_THAT(Run({"bf.exists", "b2", "c"}), IntArg(0));
  EXPECT_THAT(Run({"bf.exists", "b3", "c"}), IntArg(0));
  EXPECT_THAT(Run({"bf.exists", "b2", "b"}), IntArg(1));
  Run({"set", "str", "foo"});
  EXPECT_THAT(Run({"bf.exists", "str", "b"}), IntArg(0));
}

TEST_F(BloomFamilyTest, Multiple) {
  auto resp = Run({"bf.mexists", "bf1", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(0), IntArg(0), IntArg(0))));

  Run({"set", "str", "foo"});
  resp = Run({"bf.mexists", "str", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(0), IntArg(0), IntArg(0))));

  resp = Run({"bf.madd", "str", "a"});
  EXPECT_THAT(resp, ErrArg("WRONG"));

  resp = Run({"bf.madd", "bf1", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
  resp = Run({"bf.madd", "bf1", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(0), IntArg(0), IntArg(0))));
  resp = Run({"bf.mexists", "bf1", "a", "b", "c"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(1), IntArg(1), IntArg(1))));
}

TEST_F(BloomFamilyTest, ScanDumpAndLoadChunk) {
  constexpr int kNumItems = 100;

  Run({"bf.reserve", "b1", "0.01", "1000"});
  for (int i = 0; i < kNumItems; ++i) {
    Run({"bf.add", "b1", absl::StrCat("item", i)});
  }

  struct Chunk {
    int64_t cursor;
    std::string data;
  };
  std::vector<Chunk> chunks;

  int64_t cursor = 0;
  do {
    auto resp = Run({"bf.scandump", "b1", std::to_string(cursor)});
    const auto& vec = resp.GetVec();
    ASSERT_EQ(vec.size(), 2u);

    int64_t next_cursor = *vec[0].GetInt();
    ASSERT_TRUE(next_cursor > cursor || next_cursor == 0);

    if (next_cursor != 0) {
      EXPECT_EQ(vec[1].type, RespExpr::STRING);
      EXPECT_FALSE(vec[1].GetBuf().empty());
      chunks.push_back({next_cursor, vec[1].GetString()});
    }
    cursor = next_cursor;
  } while (cursor != 0);

  ASSERT_GE(chunks.size(), 2);

  // Load the header separately
  const auto& [crs, data] = chunks.front();
  EXPECT_EQ(Run({"bf.loadchunk", "b2", std::to_string(crs), data}), "OK");

  // Test that payload > filter size triggers validation failure
  std::string oversized = chunks[1].data;
  oversized.push_back('x');
  EXPECT_THAT(Run({"bf.loadchunk", "b2", std::to_string(chunks[1].cursor), oversized}),
              ErrArg("index out of range"));

  // Load all data chunks into new key
  for (auto it = chunks.begin() + 1; it != chunks.end(); ++it)
    EXPECT_EQ(Run({"bf.loadchunk", "b2", std::to_string(it->cursor), it->data}), "OK")
        << "failed to add cursor " << it->cursor;

  for (int i = 0; i < kNumItems; ++i)
    EXPECT_THAT(Run({"bf.exists", "b2", absl::StrCat("item", i)}), IntArg(1))
        << "missing item " << i;
  for (int i = kNumItems; i < kNumItems + 50; ++i)
    EXPECT_THAT(Run({"bf.exists", "b2", absl::StrCat("item", i)}), IntArg(0))
        << "false positive for item " << i;
}

TEST_F(BloomFamilyTest, ScanDumpPastEnd) {
  Run({"bf.reserve", "b1", "0.01", "100"});
  Run({"bf.add", "b1", "x"});

  const auto resp = Run({"bf.scandump", "b1", "999999"});
  const auto& vec = resp.GetVec();

  ASSERT_EQ(vec.size(), 2u);

  EXPECT_EQ(*vec[0].GetInt(), 0);
  EXPECT_EQ(vec[1].type, RespExpr::STRING);
  EXPECT_TRUE(vec[1].GetBuf().empty());
}

TEST_F(BloomFamilyTest, LoadChunkErrors) {
  Run({"bf.reserve", "b1", "0.01", "100"});
  Run({"bf.add", "b1", "x"});

  EXPECT_THAT(Run({"bf.loadchunk", "b1", "0", "data"}), ErrArg("not an integer"));
  EXPECT_THAT(Run({"bf.loadchunk", "b1", "-1", "data"}), ErrArg("not an integer"));
}

}  // namespace dfly
