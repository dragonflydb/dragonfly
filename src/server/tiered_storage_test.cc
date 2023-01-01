// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_cat.h>
#include <gmock/gmock.h>

#include "base/flags.h"
#include "server/test_utils.h"

using namespace std;
using namespace testing;
using absl::SetFlag;
using absl::StrCat;

ABSL_DECLARE_FLAG(string, spill_file_prefix);

namespace dfly {

class TieredStorageTest : public BaseFamilyTest {
 protected:
  TieredStorageTest() : BaseFamilyTest() {
    num_threads_ = 1;
  }

  void FillExternalKeys(unsigned count);

  static void SetUpTestSuite();
};

void TieredStorageTest::SetUpTestSuite() {
  BaseFamilyTest::SetUpTestSuite();
  SetFlag(&FLAGS_spill_file_prefix, "/tmp/spill");
}

void TieredStorageTest::FillExternalKeys(unsigned count) {
  string val(256, 'a');

  unsigned batch_cnt = count / 50;
  for (unsigned i = 0; i < batch_cnt; ++i) {
    vector<string> cmd;
    cmd.push_back("mset");

    for (unsigned j = 0; j < 50; ++j) {
      string key = StrCat("k", i * 100 + j);
      cmd.push_back(key);
      cmd.push_back(val);
    }
    Run(absl::Span<string>{cmd});
  }

  for (unsigned i = batch_cnt * 50; i < count; ++i) {
    Run({"set", StrCat("k", i), val});
  }
}

TEST_F(TieredStorageTest, Basic) {
  FillExternalKeys(5000);

  EXPECT_EQ(5000, CheckedInt({"dbsize"}));
  Metrics m = GetMetrics();
  EXPECT_GT(m.db[0].tiered_entries, 0u);

  FillExternalKeys(5000);

  m = GetMetrics();
  unsigned tiered_entries = m.db[0].tiered_entries;
  EXPECT_GT(tiered_entries, 0u);
  string resp = CheckedString({"debug", "object", "k1"});
  EXPECT_THAT(resp, HasSubstr("spill_len"));

  Run({"del", "k1"});
  m = GetMetrics();
  EXPECT_EQ(m.db[0].tiered_entries, tiered_entries - 1);
}

}  // namespace dfly
