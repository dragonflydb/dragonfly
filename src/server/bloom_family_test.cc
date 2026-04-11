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

TEST_F(BloomFamilyTest, ScanDump) {
  Run({"bf.reserve", "b1", "0.01", "1000"});
  for (int i = 0; i < 100; ++i) {
    Run({"bf.add", "b1", absl::StrCat("item", i)});
  }

  auto resp = Run({"bf.scandump", "b1", "0"});
  auto vec = resp.GetVec();

  ASSERT_EQ(vec.size(), 2u);
  int64_t cursor = *vec[0].GetInt();

  EXPECT_EQ(cursor, 1);
  EXPECT_EQ(vec[1].type, RespExpr::STRING);

  int chunk_count = 1;
  while (cursor != 0) {
    resp = Run({"bf.scandump", "b1", std::to_string(cursor)});
    vec = resp.GetVec();
    ASSERT_EQ(vec.size(), 2u);

    const auto next_cursor = *vec[0].GetInt();
    ASSERT_TRUE(next_cursor > cursor || next_cursor == 0);
    cursor = next_cursor;

    EXPECT_EQ(vec[1].type, RespExpr::STRING);
    if (cursor != 0) {
      ++chunk_count;
      EXPECT_FALSE(vec[1].GetBuf().empty());
    } else {
      EXPECT_TRUE(vec[1].GetBuf().empty());
    }
  }

  EXPECT_GE(chunk_count, 1);
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

// This test only asserts that adding to SBF is safe while an iterator is reading it. The data
// produced is inconsistent. The test only asserts that the server does not crash due to growth.
TEST_F(BloomFamilyTest, ScanDumpDuringIteration) {
  auto num_filters = [&](std::string_view key) {
    const auto resp = Run({"bf.scandump", std::string(key), "0"});
    const auto& hdr = resp.GetVec()[1].GetBuf();
    const uint8_t* p = hdr.data();
    return absl::little_endian::Load32(p + 44);
  };

  Run({"bf.reserve", "b1", "0.01", "100"});

  for (int i = 0; i < 200; ++i)
    Run({"bf.add", "b1", absl::StrCat("item", i)});

  auto n = num_filters("b1");

  // The header chunk
  auto resp = Run({"bf.scandump", "b1", "0"});
  auto vec = resp.GetVec();
  ASSERT_EQ(vec.size(), 2u);
  ASSERT_EQ(*vec[0].GetInt(), 1);
  ASSERT_EQ(vec[1].type, RespExpr::STRING);
  ASSERT_FALSE(vec[1].GetBuf().empty());
  int64_t cursor = *vec[0].GetInt();

  // First data chunk
  auto resp2 = Run({"bf.scandump", "b1", std::to_string(cursor)});
  const auto& vec2 = resp2.GetVec();
  ASSERT_EQ(vec2.size(), 2u);
  ASSERT_EQ(vec2[1].type, RespExpr::STRING);
  cursor = *vec2[0].GetInt();
  ASSERT_GT(cursor, 1);

  // add to SBF after header sent. this changes filters and adds new filters
  for (int i = 200; i < 5000; ++i)
    Run({"bf.add", "b1", absl::StrCat("item", i)});

  EXPECT_GT(num_filters("b1"), n);

  constexpr int kMaxSteps = 100;
  bool finished = false;

  // The scanning will still finish and include the new filters but the header is out of sync
  for (int step = 0; step < kMaxSteps; ++step) {
    resp = Run({"bf.scandump", "b1", std::to_string(cursor)});
    vec = resp.GetVec();

    ASSERT_EQ(vec.size(), 2u);
    ASSERT_TRUE(vec[0].GetInt().has_value());
    ASSERT_EQ(vec[1].type, RespExpr::STRING);

    const int64_t next_cursor = *vec[0].GetInt();
    if (next_cursor == 0) {
      EXPECT_TRUE(vec[1].GetBuf().empty());
      finished = true;
      break;
    }

    EXPECT_GT(next_cursor, cursor);
    EXPECT_FALSE(vec[1].GetBuf().empty());
    cursor = next_cursor;
  }

  EXPECT_TRUE(finished) << "SCANDUMP did not finish";
}

}  // namespace dfly
