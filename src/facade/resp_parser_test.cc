// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_parser.h"

#include <mimalloc.h>

#include "base/gtest.h"
#include "base/logging.h"

using namespace testing;
using namespace std;
namespace facade {

class RESPParserTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    init_zmalloc_threadlocal(mi_heap_get_backing());
  }
};

TEST_F(RESPParserTest, BaseRespTypesTest) {
  using Fields = std::map<std::string, std::string>;
  using Docs = std::map<std::string, Fields>;

  std::string msg1 =
      "*17\r\n:8\r\n$2\r\ns0\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "0\r\n$2\r\ns3\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "3\r\n$2\r\ns7\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "7\r\n$2\r\ns8\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "8\r\n$2\r\ns4\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "4\r\n$2\r\ns9\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest 9\r\n";

  std::string msg2 =
      "$2\r\ns1\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "1\r\n$2\r\ns5\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest 5\r\n";

  RESPParser reader;
  auto reply = reader.Feed(msg1.c_str(), msg1.size());
  ASSERT_TRUE(reply->Empty());

  reply = reader.Feed(msg2.c_str(), msg2.size());
  ASSERT_FALSE(reply->Empty());

  EXPECT_EQ(reply->GetType(), RESPObj::Type::ARRAY);
  auto array = *reply->As<RESPArray>();
  EXPECT_GE(array.Size(), 1);
  EXPECT_EQ(array[0].GetType(), RESPObj::Type::INTEGER);

  Docs search_results;
  for (size_t i = 1; i < array.Size(); i += 2) {
    auto& fields = search_results[*array[i].As<std::string>()];

    auto field_array = *array[i + 1].As<RESPArray>();

    for (size_t j = 0; j < field_array.Size(); j += 2) {
      std::string field_name = *field_array[j].As<std::string>();
      std::string field_value = *field_array[j + 1].As<std::string>();

      fields[field_name] = field_value;
    }
  }

  EXPECT_EQ(search_results.size(), 8);

  EXPECT_EQ(search_results["s0"]["title"], "test 0");
  EXPECT_EQ(search_results["s1"]["title"], "test 1");
  EXPECT_EQ(search_results["s3"]["title"], "test 3");
  EXPECT_EQ(search_results["s4"]["title"], "test 4");
  EXPECT_EQ(search_results["s5"]["title"], "test 5");
  EXPECT_EQ(search_results["s7"]["title"], "test 7");
  EXPECT_EQ(search_results["s8"]["title"], "test 8");
  EXPECT_EQ(search_results["s9"]["title"], "test 9");
}

TEST_F(RESPParserTest, RESPIteratorTest) {
  using Fields = std::map<std::string, std::string>;
  using Docs = std::map<std::string, Fields>;

  std::string msg1 =
      "*17\r\n:8\r\n$2\r\ns0\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "0\r\n$2\r\ns3\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "3\r\n$2\r\ns7\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "7\r\n$2\r\ns8\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "8\r\n$2\r\ns4\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "4\r\n$2\r\ns9\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest 9\r\n";

  std::string msg2 =
      "$2\r\ns1\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest "
      "1\r\n$2\r\ns5\r\n*2\r\n$5\r\ntitle\r\n$6\r\ntest 5\r\n";

  RESPParser reader;
  auto reply = reader.Feed(msg1.c_str(), msg1.size());
  ASSERT_TRUE(reply->Empty());

  reply = reader.Feed(msg2.c_str(), msg2.size());
  ASSERT_FALSE(reply->Empty());

  RESPIterator it(*reply);
  EXPECT_EQ(it.Next<size_t>(), 8);

  Docs search_results;
  while (it.HasNext()) {
    auto [doc_id, field_it] = it.Next<std::string, RESPIterator>();
    auto& fields = search_results[std::move(doc_id)];

    while (field_it.HasNext()) {
      auto [field_name, field_value] = field_it.Next<std::string_view, std::string_view>();
      fields.emplace(field_name, field_value);
    }
  }

  EXPECT_EQ(search_results.size(), 8);

  EXPECT_EQ(search_results["s0"]["title"], "test 0");
  EXPECT_EQ(search_results["s1"]["title"], "test 1");
  EXPECT_EQ(search_results["s3"]["title"], "test 3");
  EXPECT_EQ(search_results["s4"]["title"], "test 4");
  EXPECT_EQ(search_results["s5"]["title"], "test 5");
  EXPECT_EQ(search_results["s7"]["title"], "test 7");
  EXPECT_EQ(search_results["s8"]["title"], "test 8");
  EXPECT_EQ(search_results["s9"]["title"], "test 9");
}

}  // namespace facade
