// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user_registry.h"

#include <string>
#include <string_view>

#include "base/gtest.h"
#include "base/logging.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/user.h"

using namespace testing;

namespace dfly::acl {

class UserRegistryTest : public Test {};

TEST_F(UserRegistryTest, BasicOp) {
  UserRegistry registry;
  const std::string username = "kostas";
  const std::string pass = "mypass";

  User::UpdateRequest req{pass, {}, true};
  registry.MaybeAddAndUpdate(username, std::move(req));
  CHECK_EQ(registry.AuthUser(username, pass), true);
  CHECK_EQ(registry.IsUserActive(username), true);

  CHECK_EQ(registry.GetCredentials(username).acl_categories, NONE);

  using Sign = User::Sign;
  std::vector<std::pair<Sign, uint32_t>> cat = {{Sign::PLUS, LIST}, {Sign::PLUS, SET}};
  req = User::UpdateRequest{{}, std::move(cat), {}};
  registry.MaybeAddAndUpdate(username, std::move(req));
  auto acl_categories = registry.GetCredentials(username).acl_categories;
  uint32_t expected_result = NONE | LIST | SET;
  CHECK_EQ(acl_categories, expected_result);

  cat.push_back({Sign::MINUS, LIST});
  req = User::UpdateRequest{{}, std::move(cat), {}};
  registry.MaybeAddAndUpdate(username, std::move(req));
  acl_categories = registry.GetCredentials(username).acl_categories;
  expected_result = NONE | SET;
  CHECK_EQ(acl_categories, expected_result);
}

}  // namespace dfly::acl
