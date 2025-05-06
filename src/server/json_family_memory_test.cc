// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/json_family.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

ABSL_DECLARE_FLAG(bool, jsonpathv2);

namespace dfly {

class JsonFamilyMemoryTest : public BaseFamilyTest {
 public:
  static dfly::MiMemoryResource* GetMemoryResource() {
    static thread_local mi_heap_t* heap = mi_heap_new();
    static thread_local dfly::MiMemoryResource memory_resource{heap};
    return &memory_resource;
  }

 protected:
  auto GetJsonMemoryUsageFromDb(std::string_view key) {
    return Run({"MEMORY", "USAGE", key, "WITHOUTKEY"});
  }
};

size_t GetMemoryUsage() {
  return static_cast<MiMemoryResource*>(JsonFamilyMemoryTest::GetMemoryResource())->used();
}

size_t GetJsonMemoryUsageFromString(std::string_view json_str) {
  size_t start = GetMemoryUsage();
  auto json = dfly::JsonFromString(json_str, JsonFamilyMemoryTest::GetMemoryResource());
  if (!json) {
    return 0;
  }

  // The same behaviour as in CompactObj
  void* ptr =
      JsonFamilyMemoryTest::GetMemoryResource()->allocate(sizeof(JsonType), alignof(JsonType));
  JsonType* json_on_heap = new (ptr) JsonType(std::move(json).value());
  DCHECK(json_on_heap);

  size_t result = GetMemoryUsage() - start;

  // Free the memory
  json_on_heap->~JsonType();
  JsonFamilyMemoryTest::GetMemoryResource()->deallocate(json_on_heap, sizeof(JsonType),
                                                        alignof(JsonType));
  return result;
}

TEST_F(JsonFamilyMemoryTest, SimpleSet) {
  std::string_view big_json = R"({"a":"some big string asdkasdkasdfkkasjdkfjka"})";
  size_t start_size = GetJsonMemoryUsageFromString(big_json);

  auto resp = Run({"JSON.SET", "j1", "$", big_json});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(start_size));

  std::string_view small_json = R"({"a":" "})";
  size_t next_size = GetJsonMemoryUsageFromString(small_json);

  resp = Run({"JSON.SET", "j1", "$", small_json});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(next_size));

  // Again set big json
  resp = Run({"JSON.SET", "j1", "$", big_json});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(start_size));
}

TEST_F(JsonFamilyMemoryTest, PartialSet) {
  std::string_view start_json = R"({"a":"some text", "b":" "})";
  size_t start_size = GetJsonMemoryUsageFromString(start_json);

  auto resp = Run({"JSON.SET", "j1", "$", start_json});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(start_size));

  std::string_view json_after_set = R"({"a":"some text", "b":"some another text"})";
  size_t size_after_set = GetJsonMemoryUsageFromString(json_after_set);

  resp = Run({"JSON.SET", "j1", "$.b", "\"some another text\""});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(size_after_set));

  // Again set start json
  resp = Run({"JSON.SET", "j1", "$.b", "\" \""});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(start_size));
}

}  // namespace dfly
