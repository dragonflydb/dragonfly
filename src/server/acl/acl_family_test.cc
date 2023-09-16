// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/acl_family.h"

#include "absl/strings/str_cat.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;

namespace dfly {

class AclFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(AclFamilyTest, AclSetUser) {
  TestInitAclFam();
  auto resp = Run({"ACL", "SETUSER"});
  EXPECT_THAT(resp, ErrArg("ERR wrong number of arguments for 'acl setuser' command"));

  resp = Run({"ACL", "SETUSER", "kostas", "ONN"});
  EXPECT_THAT(resp, ErrArg("ERR Unrecognized parameter ONN"));

  resp = Run({"ACL", "SETUSER", "kostas", "+@nonsense"});
  EXPECT_THAT(resp, ErrArg("ERR Unrecognized parameter +@NONSENSE"));

  resp = Run({"ACL", "SETUSER", "vlad"});
  EXPECT_THAT(resp, "OK");
  resp = Run({"ACL", "LIST"});
  auto vec = resp.GetVec();
  EXPECT_THAT(vec, UnorderedElementsAre("user default on nopass +@ALL +ALL",
                                        "user vlad off nopass +@NONE"));
}

TEST_F(AclFamilyTest, AclDelUser) {
  TestInitAclFam();
  auto resp = Run({"ACL", "DELUSER"});
  EXPECT_THAT(resp, ErrArg("ERR wrong number of arguments for 'acl deluser' command"));

  resp = Run({"ACL", "DELUSER", "NOTEXISTS"});
  EXPECT_THAT(resp, ErrArg("ERR User NOTEXISTS does not exist"));

  resp = Run({"ACL", "SETUSER", "kostas", "ON"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"ACL", "DELUSER", "KOSTAS", "NONSENSE"});
  EXPECT_THAT(resp, ErrArg("ERR wrong number of arguments for 'acl deluser' command"));

  resp = Run({"ACL", "DELUSER", "kostas"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"ACL", "LIST"});
  EXPECT_THAT(resp.GetString(), "user default on nopass +@ALL +ALL");
}

TEST_F(AclFamilyTest, AclList) {
  TestInitAclFam();
  auto resp = Run({"ACL", "LIST", "NONSENSE"});
  EXPECT_THAT(resp, ErrArg("ERR wrong number of arguments for 'acl list' command"));

  resp = Run({"ACL", "SETUSER", "kostas", ">pass", "+@admin"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"ACL", "SETUSER", "adi", ">pass", "+@fast"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"ACL", "LIST"});
  auto vec = resp.GetVec();
  EXPECT_THAT(vec, UnorderedElementsAre("user default on nopass +@ALL +ALL",
                                        "user kostas off d74ff0ee8da3b98 +@ADMIN",
                                        "user adi off d74ff0ee8da3b98 +@FAST"));
}

TEST_F(AclFamilyTest, AclAuth) {
  TestInitAclFam();
  auto resp = Run({"AUTH", "default", R"("")"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"ACL", "SETUSER", "shahar", ">mypass"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"AUTH", "shahar", "wrongpass"});
  EXPECT_THAT(resp, ErrArg("ERR Could not authorize user: shahar"));

  resp = Run({"AUTH", "shahar", "mypass"});
  EXPECT_THAT(resp, ErrArg("ERR Could not authorize user: shahar"));

  // Activate the user
  resp = Run({"ACL", "SETUSER", "shahar", "ON", "+@fast"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"AUTH", "shahar", "mypass"});
  EXPECT_THAT(resp, "OK");
}

TEST_F(AclFamilyTest, AclWhoAmI) {
  TestInitAclFam();
  auto resp = Run({"ACL", "WHOAMI", "WHO"});
  EXPECT_THAT(resp, ErrArg("ERR wrong number of arguments for 'acl whoami' command"));

  resp = Run({"ACL", "SETUSER", "kostas", "ON", ">pass", "+@SLOW"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"AUTH", "kostas", "pass"});
  EXPECT_THAT(resp, "OK");

  resp = Run({"ACL", "WHOAMI"});
  EXPECT_THAT(resp, "User is kostas");
}

TEST_F(AclFamilyTest, TestAllCategories) {
  TestInitAclFam();
  for (auto& cat : acl::REVERSE_CATEGORY_INDEX_TABLE) {
    if (cat != "_RESERVED") {
      auto resp = Run({"ACL", "SETUSER", "kostas", absl::StrCat("+@", cat)});
      EXPECT_THAT(resp, "OK");

      resp = Run({"ACL", "LIST"});
      EXPECT_THAT(resp.GetVec(),
                  UnorderedElementsAre("user default on nopass +@ALL +ALL",
                                       absl::StrCat("user kostas off nopass ", "+@", cat)));

      resp = Run({"ACL", "SETUSER", "kostas", absl::StrCat("-@", cat)});
      EXPECT_THAT(resp, "OK");

      resp = Run({"ACL", "LIST"});
      EXPECT_THAT(resp.GetVec(),
                  UnorderedElementsAre("user default on nopass +@ALL +ALL",
                                       absl::StrCat("user kostas off nopass ", "+@NONE")));

      resp = Run({"ACL", "DELUSER", "kostas"});
      EXPECT_THAT(resp, "OK");
    }
  }

  for (auto& cat : acl::REVERSE_CATEGORY_INDEX_TABLE) {
    if (cat != "_RESERVED") {
      auto resp = Run({"ACL", "SETUSER", "kostas", absl::StrCat("+@", cat)});
      EXPECT_THAT(resp, "OK");
    }
  }
  // This won't work because of __RESERVED
  // TODO(fix this)
  //  auto resp = Run({"ACL", "LIST"});
  //  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("user default on nopass +@ALL",
  //  absl::StrCat("user kostas off nopass ", "+@ALL")));
  //

  // TODO(Bug here fix none/all)
  //  auto resp = Run({"ACL", "SETUSER", "kostas", "+@NONE"});
  //  EXPECT_THAT(resp, "OK");
  //
  //  resp = Run({"ACL", "LIST"});
  //  EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("user default on nopass +@ALL", "user kostas
  //  off nopass +@NONE"));
}

TEST_F(AclFamilyTest, TestAllCommands) {
  TestInitAclFam();
  const auto& rev_indexer = acl::CommandsRevIndexer();
  for (const auto& family : rev_indexer) {
    for (const auto& command_name : family) {
      auto resp = Run({"ACL", "SETUSER", "kostas", absl::StrCat("+", command_name)});
      EXPECT_THAT(resp, "OK");

      resp = Run({"ACL", "LIST"});
      EXPECT_THAT(resp.GetVec(), UnorderedElementsAre("user default on nopass +@ALL +ALL",
                                                      absl::StrCat("user kostas off nopass +@NONE ",
                                                                   "+", command_name)));

      resp = Run({"ACL", "SETUSER", "kostas", absl::StrCat("-", command_name)});

      resp = Run({"ACL", "LIST"});
      EXPECT_THAT(resp.GetVec(),
                  UnorderedElementsAre("user default on nopass +@ALL +ALL",
                                       absl::StrCat("user kostas off nopass ", "+@NONE")));

      resp = Run({"ACL", "DELUSER", "kostas"});
      EXPECT_THAT(resp, "OK");
    }
  }
}

}  // namespace dfly
