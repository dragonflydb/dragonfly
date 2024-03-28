// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/flatbuffers.h"

#include <absl/strings/escaping.h>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace dfly {
class FlatBuffersTest : public ::testing::Test {
 protected:
};

TEST_F(FlatBuffersTest, Basic) {
  flexbuffers::Builder fbb;
  fbb.Map([&] {
    fbb.String("foo", "bar");
    fbb.Double("bar", 1.5);
    fbb.Vector("strs", [&] {
      fbb.String("hello");
      fbb.String("world");
    });
  });

  fbb.Finish();
  auto buffer = fbb.GetBuffer();
  flexbuffers::Reference ref = flexbuffers::GetRoot(buffer);
  auto map = ref.AsMap();
  EXPECT_EQ("bar", map["foo"].AsString().str());
}

TEST_F(FlatBuffersTest, FlexiParser) {
  flatbuffers::Parser parser;
  const char* json = R"(
    {
      "foo": "bar",
      "bar": 1.5,
      "strs": ["hello", "world"]
    }
  )";
  flexbuffers::Builder fbb;
  ASSERT_TRUE(parser.ParseFlexBuffer(json, nullptr, &fbb));
  fbb.Finish();
  const auto& buffer = fbb.GetBuffer();
  string_view buf_view{reinterpret_cast<const char*>(buffer.data()), buffer.size()};
  LOG(INFO) << "Binary buffer: " << absl::CHexEscape(buf_view);
  flexbuffers::Reference root = flexbuffers::GetRoot(buffer);
  auto map = root.AsMap();
  EXPECT_EQ("bar", map["foo"].AsString().str());
}

TEST_F(FlatBuffersTest, ParseJson) {
  const char* schema = R"(
    namespace dfly;
    table Foo {
      foo: string;
      bar: double;
      strs: [string];
    }
    root_type Foo;
  )";

  flatbuffers::Parser parser;
  ASSERT_TRUE(parser.Parse(schema));
  parser.Serialize();
  flatbuffers::DetachedBuffer bsb = parser.builder_.Release();

  // This schema will always reference bsb.
  auto* fbs_schema = reflection::GetSchema(bsb.data());

  flatbuffers::Verifier verifier(bsb.data(), bsb.size());
  ASSERT_TRUE(fbs_schema->Verify(verifier));

  auto* root_table = fbs_schema->root_table();
  auto* fields = root_table->fields();
  auto* field_foo = fields->LookupByKey("foo");
  ASSERT_EQ(field_foo->type()->base_type(), reflection::String);

  const char* json = R"(
    {
      "foo": "value",
      "bar": 1.5,
      "strs": ["hello", "world"]
    }
  )";

  ASSERT_TRUE(parser.Parse(json));
  size_t buf_size = parser.builder_.GetSize();

  ASSERT_TRUE(
      flatbuffers::Verify(*fbs_schema, *root_table, parser.builder_.GetBufferPointer(), buf_size));
  auto* root_obj = flatbuffers::GetAnyRoot(parser.builder_.GetBufferPointer());

  const flatbuffers::String* value = flatbuffers::GetFieldS(*root_obj, *field_foo);
  EXPECT_EQ("value", value->str());

  // wrong type.
  ASSERT_FALSE(parser.Parse(R"({"foo": 1})"));
}

}  // namespace dfly
