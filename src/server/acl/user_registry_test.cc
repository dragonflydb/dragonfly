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

namespace dfly {

class UserRegistryTest : public Test {};

TEST_F(UserRegistryTest, BasicOp) {
  UserRegistry registry;
  const std::string username = "kostas";
  const std::string pass = "mypass";

  User::UpdateRequest req{pass, {}, {}, {}};
  registry.MaybeAddAndUpdate(username, std::move(req));
  CHECK_EQ(registry.AuthUser(username, pass), true);
  CHECK_EQ(registry.IsUserActive(username), false);

  CHECK_EQ(registry.GetCredentials(username).acl_categories, AclCategory::NONE);

  const uint32_t set_category = 0 | AclCategory::LIST | AclCategory::SET;
  req = User::UpdateRequest{{}, set_category, {}, {}};
  registry.MaybeAddAndUpdate(username, std::move(req));
  auto acl_categories = registry.GetCredentials(username).acl_categories;
  CHECK_EQ(acl_categories, set_category);

  req = User::UpdateRequest{{}, {}, 0 | AclCategory::LIST, {}};
  registry.MaybeAddAndUpdate(username, std::move(req));
  acl_categories = registry.GetCredentials(username).acl_categories;
  const uint32_t expected_res = 0 | AclCategory::SET;
  CHECK_EQ(acl_categories, expected_res);
}

}  // namespace dfly
