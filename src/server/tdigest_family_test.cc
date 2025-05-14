// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tdigest_family.h"

#include <absl/flags/flag.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace util;

namespace dfly {

class TDigestFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(TDigestFamilyTest, Basic) {
  // errors
  std::string err = "ERR wrong number of arguments for 'tdigest.create' command";
  ASSERT_THAT(Run({"TDIGEST.CREATE", "k1", "k2"}), ErrArg("ERR syntax error"));
  // Triggers a check in InitByArgs -- a logical error
  // TODO fix this
  // ASSERT_THAT(Run({"TDIGEST.CREATE"}), ErrArg(err));

  auto resp = Run({"TDIGEST.CREATE", "k1"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.CREATE", "k1", "COMPRESSION", "200"});
  ASSERT_THAT(resp, ErrArg("ERR key already exists"));

  resp = Run({"TDIGEST.CREATE", "k2", "COMPRESSION", "200"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.ADD", "k1", "10.0", "20.0"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.ADD", "k2", "30.0", "40.0"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.RESET", "k1"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.RESET", "k2"});
  EXPECT_EQ(resp, "OK");
}

TEST_F(TDigestFamilyTest, Merge) {
  auto resp = Run({"TDIGEST.CREATE", "k1"});
  resp = Run({"TDIGEST.CREATE", "k2"});

  Run({"TDIGEST.ADD", "k1", "10.0", "20.0"});
  Run({"TDIGEST.ADD", "k2", "30.0", "40.0"});

  resp = Run({"TDIGEST.MERGE", "res", "2", "k1", "k2"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.BYRANK", "res", "0", "1", "2", "3", "4"});
  auto results = resp.GetVec();
  ASSERT_THAT(results, ElementsAre(DoubleArg(10), DoubleArg(20), DoubleArg(30), DoubleArg(40),
                                   DoubleArg(INFINITY)));

  resp = Run({"TDIGEST.INFO", "res"});
  results = resp.GetVec();
  ASSERT_THAT(results,
              ElementsAre("Compression", 100, "Capacity", 610, "Merged nodes", 4, "Unmerged nodes",
                          0, "Merged weight", 4, "Unmerged weight", 0, "Observations", 4,
                          "Total compressions", 2, "Memory usage", 9768));

  Run({"TDIGEST.CREATE", "k3"});
  Run({"TDIGEST.CREATE", "k4"});
  Run({"TDIGEST.CREATE", "k5"});
  Run({"TDIGEST.CREATE", "k6"});

  Run({"TDIGEST.ADD", "k3", "11.0", "21.0"});
  Run({"TDIGEST.ADD", "k4", "31.1", "40.1"});
  Run({"TDIGEST.ADD", "k5", "10.0", "20.0"});
  Run({"TDIGEST.ADD", "k6", "32.2", "42.1"});

  // OVERIDE overides the key
  // compression sets the compression level
  resp = Run({"TDIGEST.MERGE", "res", "6", "k1", "k2", "k3", "k4", "k5", "k6", "COMPRESSION", "50",
              "OVERRIDE"});
  EXPECT_EQ(resp, "OK");

  resp = Run({"TDIGEST.INFO", "res"});
  results = resp.GetVec();
  ASSERT_THAT(results,
              ElementsAre("Compression", IntArg(50), "Capacity", IntArg(310), "Merged nodes",
                          IntArg(10), "Unmerged nodes", IntArg(2), "Merged weight", IntArg(10),
                          "Unmerged weight", IntArg(2), "Observations", IntArg(12),
                          "Total compressions", IntArg(5), "Memory usage", IntArg(4968)));

  Run({"SET", "foo", "bar"});
  resp = Run({"TDIGEST.MERGE", "foo", "2", "k1", "k2"});
  ASSERT_THAT(resp, ErrArg("ERR no such key"));
  resp = Run({"TDIGEST.MERGE", "k1", "2", "foo", "k2"});
  ASSERT_THAT(resp, ErrArg("ERR no such key"));
}

TEST_F(TDigestFamilyTest, MinMax) {
  // errors
  std::string min_err = "ERR wrong number of arguments for 'tdigest.min' command";
  std::string max_err = "ERR wrong number of arguments for 'tdigest.max' command";
  ASSERT_THAT(Run({"TDIGEST.MAX", "k1", "k2"}), ErrArg(max_err));
  ASSERT_THAT(Run({"TDIGEST.MAX"}), ErrArg(max_err));
  ASSERT_THAT(Run({"TDIGEST.MIN", "k1", "k2"}), ErrArg(min_err));
  ASSERT_THAT(Run({"TDIGEST.MIN"}), ErrArg(min_err));

  Run({"TDIGEST.CREATE", "k1"});
  Run({"TDIGEST.ADD", "k1", "10.0", "22.0", "33.0", "44.4", "55.5"});

  ASSERT_THAT(Run({"TDIGEST.MIN", "k1"}), DoubleArg(10));
  ASSERT_THAT(Run({"TDIGEST.MAX", "k1"}), DoubleArg(55.5));
}

TEST_F(TDigestFamilyTest, Rank) {
  // errors
  auto error = [](std::string_view msg) {
    std::string err = "ERR wrong number of arguments for ";
    return absl::StrCat(err, "'", msg, "'", " command");
  };
  ASSERT_THAT(Run({"TDIGEST.RANK", "k1"}), ErrArg(error("tdigest.rank")));
  ASSERT_THAT(Run({"TDIGEST.REVRANK", "k1"}), ErrArg(error("tdigest.revrank")));
  ASSERT_THAT(Run({"TDIGEST.BYRANK", "k1"}), ErrArg(error("tdigest.byrank")));
  ASSERT_THAT(Run({"TDIGEST.BYREVRANK", "k1"}), ErrArg(error("tdigest.byrevrank")));

  Run({"TDIGEST.CREATE", "k1"});
  Run({"TDIGEST.ADD", "k1", "10.0", "22.0", "33.0", "44.4", "55.5"});

  auto resp = Run({"TDIGEST.BYRANK", "k1", "0", "1", "2", "3", "4", "5"});
  auto results = resp.GetVec();
  ASSERT_THAT(results, ElementsAre(DoubleArg(10), DoubleArg(22), DoubleArg(33), DoubleArg(44.4),
                                   DoubleArg(55.5), DoubleArg(INFINITY)));

  resp = Run({"TDIGEST.BYREVRANK", "k1", "0", "1", "2", "3", "4", "5"});
  results = resp.GetVec();
  ASSERT_THAT(results, ElementsAre(DoubleArg(55.5), DoubleArg(44.4), DoubleArg(33), DoubleArg(22),
                                   DoubleArg(10), DoubleArg(-INFINITY)));

  ASSERT_THAT(Run({"TDIGEST.RANK", "k1", "1"}), IntArg(-1));
  ASSERT_THAT(Run({"TDIGEST.REVRANK", "k1", "1"}), IntArg(5));

  ASSERT_THAT(Run({"TDIGEST.RANK", "k1", "50"}), IntArg(4));
  ASSERT_THAT(Run({"TDIGEST.REVRANK", "k1", "50"}), IntArg(1));
}

TEST_F(TDigestFamilyTest, Cdf) {
  Run({"TDIGEST.CREATE", "k1"});
  // errors
  std::string err = "ERR wrong number of arguments for 'tdigest.cdf' command";
  ASSERT_THAT(Run({"TDIGEST.CDF", "k1"}), ErrArg(err));

  Run({"TDIGEST.ADD", "k1", "1", "2", "2", "3", "3", "3", "4", "4", "4", "4", "5", "5", "5", "5",
       "5"});

  auto resp = Run({"TDIGEST.CDF", "k1", "0", "1", "2", "3", "4", "5", "6"});

  const auto& results = resp.GetVec();
  ASSERT_THAT(results, ElementsAre(DoubleArg(0), DoubleArg(0.033333333333333333),
                                   DoubleArg(0.13333333333333333), DoubleArg(0.29999999999999999),
                                   DoubleArg(0.53333333333333333), DoubleArg(0.83333333333333337),
                                   DoubleArg(1)));
}

TEST_F(TDigestFamilyTest, Quantile) {
  Run({"TDIGEST.CREATE", "k1"});
  // errors
  std::string err = "ERR wrong number of arguments for 'tdigest.quantile' command";

  ASSERT_THAT(Run({"TDIGEST.QUANTILE", "k1"}), ErrArg(err));

  Run({"TDIGEST.ADD", "k1", "1", "2", "2", "3", "3", "3", "4", "4", "4", "4", "5", "5", "5", "5",
       "5"});

  auto resp = Run({"TDIGEST.QUANTILE", "k1", "0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7",
                   "0.8", "0.9", "1"});

  const auto& results = resp.GetVec();
  ASSERT_THAT(results, ElementsAre(DoubleArg(1), DoubleArg(2), DoubleArg(3), DoubleArg(3),
                                   DoubleArg(4), DoubleArg(4), DoubleArg(4), DoubleArg(5),
                                   DoubleArg(5), DoubleArg(5), DoubleArg(5)));
}

TEST_F(TDigestFamilyTest, TrimmedMean) {
  Run({"TDIGEST.CREATE", "k1", "compression", "1000"});
  // errors
  std::string err = "ERR wrong number of arguments for 'tdigest.trimmed_mean' command";

  ASSERT_THAT(Run({"TDIGEST.TRIMMED_MEAN", "k1"}), ErrArg(err));
  ASSERT_THAT(Run({"TDIGEST.TRIMMED_MEAN", "k1", "0.1"}), ErrArg(err));

  Run({"TDIGEST.ADD", "k1", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"});

  auto resp = Run({"TDIGEST.TRIMMED_MEAN", "k1", "0.1", "0.6"});
  ASSERT_THAT(resp, DoubleArg(4));

  resp = Run({"TDIGEST.TRIMMED_MEAN", "k1", "0.3", "0.9"});
  ASSERT_THAT(resp, DoubleArg(6.5));

  resp = Run({"TDIGEST.TRIMMED_MEAN", "k1", "0", "1"});
  ASSERT_THAT(resp, DoubleArg(5.5));
}
}  // namespace dfly
