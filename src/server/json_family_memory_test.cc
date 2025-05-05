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
};

size_t GetMemoryUsage() {
  return static_cast<MiMemoryResource*>(JsonFamilyMemoryTest::GetMemoryResource())->used();
}

size_t GetJsonMemoryUsage(std::string_view json_str) {
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
  size_t start_size = GetJsonMemoryUsage(big_json);

  auto resp = Run({"JSON.SET", "j1", "$", big_json});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(start_size));

  std::string_view small_json = R"({"a":" "})";
  size_t next_size = GetJsonMemoryUsage(small_json);

  resp = Run({"JSON.SET", "j1", "$", small_json});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(next_size));

  // Again set big json
  resp = Run({"JSON.SET", "j1", "$", big_json});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(start_size));
}

TEST_F(JsonFamilyMemoryTest, PartialSet) {
  std::string_view start_json = R"({"a":"some text", "b":" "})";
  size_t start_size = GetJsonMemoryUsage(start_json);

  auto resp = Run({"JSON.SET", "j1", "$", start_json});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(start_size));

  std::string_view json_after_set = R"({"a":"some text", "b":"some another text"})";
  size_t size_after_set = GetJsonMemoryUsage(json_after_set);

  resp = Run({"JSON.SET", "j1", "$.b", "\"some another text\""});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(size_after_set));

  // Again set start json
  resp = Run({"JSON.SET", "j1", "$.b", "\" \""});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(start_size));
}

/* Tests how works memory usage after deleting json object in jsoncons */
TEST_F(JsonFamilyMemoryTest, JsonConsDelTest) {
  std::string_view start_json = R"({"a":"some text", "b":" "})";

  size_t start = GetMemoryUsage();

  auto json = dfly::JsonFromString(start_json, JsonFamilyMemoryTest::GetMemoryResource());
  void* ptr =
      JsonFamilyMemoryTest::GetMemoryResource()->allocate(sizeof(JsonType), alignof(JsonType));
  JsonType* json_on_heap = new (ptr) JsonType(std::move(json).value());

  size_t memory_usage_before_erase = GetMemoryUsage() - start;

  json_on_heap->erase("a");
  /* To deallocate memory we should use shrink_to_fit */
  json_on_heap->shrink_to_fit();

  size_t memory_usage_after_erase = GetMemoryUsage() - start;

  EXPECT_GT(memory_usage_before_erase, memory_usage_after_erase);
  EXPECT_EQ(memory_usage_after_erase, GetJsonMemoryUsage(R"({"b":" "})"));
}

TEST_F(JsonFamilyMemoryTest, SimpleDel) {
  std::string_view start_json = R"({"a":"some text", "b":" "})";
  size_t start_size = GetJsonMemoryUsage(start_json);

  auto resp = Run({"JSON.SET", "j1", "$", start_json});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(start_size));

  std::string_view json_after_del = R"({"b":" "})";

  resp = Run({"JSON.DEL", "j1", "$.a"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"JSON.GET", "j1"});
  EXPECT_EQ(resp, json_after_del);
  resp = Run({"JSON._MEMUSAGE", "j1"});

  /* We still expect the initial size here, because after deletion we do not call shrink_to_fit on
     the JSON object. As a result, the memory will not be deallocated. Check
     JsonFamilyMemoryTest::JsonConsDelTest for example. */
  EXPECT_THAT(resp, IntArg(start_size));
  // TODO: check if we can use shrink_to_fit here

  // Again set start json
  resp = Run({"JSON.SET", "j1", "$.b", "\" \""});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON._MEMUSAGE", "j1"});
  EXPECT_THAT(resp, IntArg(start_size));
}

}  // namespace dfly
