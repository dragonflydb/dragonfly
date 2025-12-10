// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

ABSL_DECLARE_FLAG(bool, jsonpathv2);

namespace dfly {

class JsonFamilyMemoryTest : public BaseFamilyTest {
 public:
  static MiMemoryResource* GetMemoryResource() {
    thread_local mi_heap_t* heap = mi_heap_new();
    thread_local MiMemoryResource memory_resource{heap};
    return &memory_resource;
  }

 protected:
  void SetUp() override {
    BaseFamilyTest::SetUp();
    // Make the core running the thread use the same resource as the rest of the test. Although
    // BaseFamilyTest initializes the heap on shards serving transactions, the core running the test
    // needs this initialized explicitly.
    InitTLStatelessAllocMR(GetMemoryResource());
  }

  auto GetJsonMemoryUsageFromDb(std::string_view key) {
    return Run({"MEMORY", "USAGE", key, "WITHOUTKEY"});
  }
};

size_t GetMemoryUsage() {
  return JsonFamilyMemoryTest::GetMemoryResource()->used();
}

size_t GetJsonMemoryUsageFromString(std::string_view json_str) {
  size_t start = GetMemoryUsage();
  auto json = ParseJsonUsingShardHeap(json_str);
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

/* Tests how works memory usage after deleting json object in jsoncons */
TEST_F(JsonFamilyMemoryTest, JsonConsDelTest) {
  std::string_view start_json = R"({"a":"some text", "b":" "})";

  size_t start = GetMemoryUsage();

  auto json = ParseJsonUsingShardHeap(start_json);
  void* ptr = GetMemoryResource()->allocate(sizeof(JsonType), alignof(JsonType));
  JsonType* json_on_heap = new (ptr) JsonType(std::move(json).value());

  size_t memory_usage_before_erase = GetMemoryUsage() - start;

  json_on_heap->erase("a");
  /* To deallocate memory we should use shrink_to_fit */
  json_on_heap->shrink_to_fit();

  size_t memory_usage_after_erase = GetMemoryUsage() - start;

  EXPECT_GT(memory_usage_before_erase, memory_usage_after_erase);
  EXPECT_EQ(memory_usage_after_erase, GetJsonMemoryUsageFromString(R"({"b":" "})"));
}

TEST_F(JsonFamilyMemoryTest, SimpleDel) {
  std::string_view start_json = R"({"a":"some text", "b":" "})";
  size_t start_size = GetJsonMemoryUsageFromString(start_json);

  auto resp = Run({"JSON.SET", "j1", "$", start_json});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(start_size));

  std::string_view json_after_del = R"({"b":" "})";
  size_t size_after_del = GetJsonMemoryUsageFromString(json_after_del);

  // Test that raw memory usage is correct
  resp = Run({"JSON.SET", "j2", "$", json_after_del});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j2");
  EXPECT_THAT(resp, IntArg(size_after_del));

  // Test that after deletion memory usage is correct
  resp = Run({"JSON.DEL", "j1", "$.a"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"JSON.GET", "j1"});
  EXPECT_EQ(resp, json_after_del);
  resp = GetJsonMemoryUsageFromDb("j1");

  /* We still expect the initial size here, because after deletion we do not call shrink_to_fit on
     the JSON object. As a result, the memory will not be deallocated. Check
     JsonFamilyMemoryTest::JsonConsDelTest for example. */
  EXPECT_THAT(resp, IntArg(start_size));

  // Again set start json
  resp = Run({"JSON.SET", "j1", "$.a", "\"some text\""});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(start_size));
}

TEST_F(JsonFamilyMemoryTest, JsonShrinking) {
  std::string_view start_json = R"({"a":"some text","b":"some another text","c":" "})";
  size_t start_size = GetJsonMemoryUsageFromString(start_json);

  auto resp = Run({"JSON.SET", "j1", "$", start_json});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j1");
  EXPECT_THAT(resp, IntArg(start_size));

  std::string_view json_after_del = R"({"c":" "})";
  size_t size_after_del = GetJsonMemoryUsageFromString(json_after_del);

  // Test that raw memory usage is correct
  resp = Run({"JSON.SET", "j2", "$", json_after_del});
  EXPECT_EQ(resp, "OK");
  resp = GetJsonMemoryUsageFromDb("j2");
  EXPECT_THAT(resp, IntArg(size_after_del));

  // Test that after deletion memory usage decreases
  resp = Run({"JSON.DEL", "j1", "$.a"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"JSON.DEL", "j1", "$.b"});
  EXPECT_THAT(resp, IntArg(1));
  resp = Run({"JSON.GET", "j1"});
  EXPECT_EQ(resp, json_after_del);
  resp = GetJsonMemoryUsageFromDb("j1");
  // Now we expect the size to be smaller, because shrink_to_fit was called
  EXPECT_THAT(resp, IntArg(size_after_del));

  // Again set start json
  resp = Run({"JSON.SET", "j1", "$.a", "\"some text\""});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.SET", "j1", "$.b", "\"some another text\""});
  EXPECT_EQ(resp, "OK");
  resp = Run({"JSON.GET", "j1"});
  EXPECT_EQ(resp, start_json);
  resp = GetJsonMemoryUsageFromDb("j1");

  // Jsoncons will allocate more memory for the new json that needed.
  // This is totally fine, because we will not call shrink_to_fit.
  // Different compilers may allocate different amounts, so check reasonable range
  auto final_size = get<int64_t>(resp.u);
  EXPECT_GT(final_size, start_size);      // Should be larger than initial
  EXPECT_LT(final_size, start_size * 2);  // But not unreasonably large
}

}  // namespace dfly
