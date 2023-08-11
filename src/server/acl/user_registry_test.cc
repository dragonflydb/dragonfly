// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/user_registry.h"
#include "server/acl/user.h"

#include "base/gtest.h"
#include "base/logging.h"
#include <string_view>
#include <string>

using namespace testing;

namespace dfly {

class UserRegistryTest : public Test {
};

TEST_F(UserRegistryTest, BasicOp) {
  UserRegistry registry;
  const std::string username = "kostas";
  const std::string pass = "mypass";

  User::UpdateRequest req {.password = pass };
  registry.MaybeAddAndUpdate(username, std::move(req));
  CHECK_EQ(registry.AuthUser(username, pass), true);
  CHECK_EQ(registry.IsUserActive(username), false);

  CHECK_EQ(registry.GetCredentials(username).acl_categories, 0);

  const uint32_t set_category = 0 | AclCat::ACL_CATEGORY_LIST | AclCat::ACL_CATEGORY_SET;
  req = User::UpdateRequest{ .plus_acl_categories = set_category };
  registry.MaybeAddAndUpdate(username, std::move(req));
  auto acl_categories = registry.GetCredentials(username).acl_categories;

  CHECK_EQ(acl_categories, set_category);

  req = User::UpdateRequest{ .minus_acl_categories = 0 | AclCat::ACL_CATEGORY_LIST};
  registry.MaybeAddAndUpdate(username, std::move(req));
  acl_categories = registry.GetCredentials(username).acl_categories;
  CHECK_EQ(acl_categories, 0 | AclCat::ACL_CATEGORY_SET);
}

}
