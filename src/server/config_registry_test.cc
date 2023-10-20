// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/config_registry.h"

#include "base/flags.h"
#include "base/gtest.h"

ABSL_FLAG(std::string, key1, "", "nop flag");

namespace dfly {

std::vector<MutableSlice> StringsToMutableSlice(const std::vector<std::string>& ss) {
  std::vector<MutableSlice> slices;
  for (const auto& s : ss) {
    slices.push_back(MutableSlice{(char*)s.c_str(), s.size()});
  }
  return slices;
}

class ConfigRegistryTest : public ::testing::Test {};

TEST_F(ConfigRegistryTest, SetParamOK) {
  std::string val1;

  ConfigRegistry registry;
  registry.RegisterMutable("key1", [&](const absl::CommandLineFlag& flag) {
    auto res = flag.TryGet<std::string>();
    if (res.has_value()) {
      val1 = res.value();
    }
    return res.has_value();
  });

  std::vector<std::string> args{"key1", "foo"};
  std::vector<MutableSlice> slices = StringsToMutableSlice(args);
  EXPECT_EQ(ConfigRegistry::SetResult::OK, registry.Set(CmdArgList{slices}));

  EXPECT_EQ("foo", registry.Get("key1"));
  EXPECT_EQ("foo", val1);
}

// ... TODO(andydunstall)

}  // namespace dfly
