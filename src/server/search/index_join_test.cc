// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/index_join.h"

#include <absl/container/flat_hash_set.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <utility>

#include "base/gtest.h"
#include "base/logging.h"

namespace dfly {

using namespace join;

class IndexJoinTest : public testing::Test {
 protected:
};

struct IndexData {
  struct FieldData {
    std::string_view name;
    JoinableValue value;
  };

  struct KeyData {
    std::string_view key;
    std::vector<FieldData> fields;
  };

  std::string_view index_name;
  std::vector<KeyData> entries;
};

MATCHER_P(IsJoinResultMatcher, expected, "") {
  std::vector<testing::Matcher<std::vector<std::string_view>>> matchers;
  for (const auto& entry : expected) {
    std::vector<std::string_view> keys;
    for (auto field : entry) {
      keys.push_back(field);
    }
    matchers.push_back(testing::ElementsAreArray(keys));
  }

  std::vector<std::vector<std::string_view>> result;
  for (size_t index = 0; index < arg.Size(); ++index) {
    std::vector<std::string_view> entry;
    for (const auto& key : arg[index]) {
      entry.push_back(key);
    }
    result.push_back(std::move(entry));
  }
  return testing::ExplainMatchResult(testing::UnorderedElementsAreArray(matchers), result,
                                     result_listener);
}

template <typename... Args>
auto IsJoinResult(std::vector<std::vector<std::string_view>> joined_data) {
  return IsJoinResultMatcher(std::move(joined_data));
}

std::pair<EntriesPerIndex, KeysPerIndex> BuildIndexesData(std::vector<IndexData> indexes_data) {
  EntriesPerIndex entries;
  KeysPerIndex keys;

  for (const auto& [index_name, index_data] : indexes_data) {
    DCHECK(!entries.contains(index_name)) << "Duplicate index name: " << index_name;

    std::vector<std::string_view> field_names;
    if (!index_data.empty()) {
      for (const auto& field : index_data[0].fields) {
        DCHECK(std::find(field_names.begin(), field_names.end(), field.name) == field_names.end())
            << "Duplicate field name in index: " << field.name;
        field_names.push_back(field.name);
      }
    }

    IndexEntries index_entries(field_names.begin(), field_names.end(), index_data.size());

    absl::InlinedVector<std::string_view, 4> index_keys;
    index_keys.reserve(index_data.size());

    for (size_t i = 0; i < index_data.size(); ++i) {
      const auto& [key, fields] = index_data[i];
      DCHECK(std::find(index_keys.begin(), index_keys.end(), key) == index_keys.end())
          << "Duplicate key in index: " << key;
      index_keys.push_back(key);

      std::vector<std::string_view> fields_set;
      for (const auto& [field_name, field_value] : fields) {
        DCHECK(index_entries[i].Contains(field_name));
        DCHECK(std::find(fields_set.begin(), fields_set.end(), field_name) == fields_set.end())
            << "Duplicate field name in key: " << field_name;

        index_entries[i][field_name] = field_value;
        fields_set.push_back(field_name);
      }
      DCHECK_EQ(fields_set.size(), field_names.size()) << "Not all fields are set for key: " << key;
    }

    entries.insert(index_name, std::move(index_entries));
    keys.insert(index_name, std::move(index_keys));
  }

  return {std::move(entries), std::move(keys)};
}

IndexesJoinExpressions BuildJoinExpressions(
    std::string_view start_index,
    std::initializer_list<std::pair<std::string_view, std::initializer_list<JoinExpression>>>
        data) {
  IndexesJoinExpressions join_expressions;
  join_expressions.insert(start_index, {});

  for (const auto& [index_name, expressions] : data) {
    DCHECK(!join_expressions.contains(index_name))
        << "Duplicate index name in join expressions: " << index_name;
    JoinExpressionsList exprs;
    for (const auto& expr : expressions) {
      DCHECK(join_expressions.contains(expr.foreign_index))
          << "Foreign index not found in join expressions: " << expr.foreign_index;
      exprs.push_back(expr);
    }
    join_expressions.insert(index_name, std::move(exprs));
  }

  return join_expressions;
}

TEST_F(IndexJoinTest, SimpleJoin) {
  auto [entries, keys] = BuildIndexesData({{"index1",
                                            {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                             {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                           {"index2",
                                            {{"key3", {{"field3", 1.0}, {"field4", "value3"}}},
                                             {"key4", {{"field3", 2.0}, {"field4", "value4"}}}}}});

  auto joins = BuildJoinExpressions("index1", {{"index2", {{"field3", "index1", "field1"}}}});

  auto result = JoinAllIndexes(entries, keys, joins);
  EXPECT_THAT(result, IsJoinResult({{"key1", "key3"}, {"key2", "key4"}}));
}

TEST_F(IndexJoinTest, MultipleJoins) {
  auto [entries, keys] = BuildIndexesData({{"index1",
                                            {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                             {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                           {"index2",
                                            {{"key3", {{"field3", 1.0}, {"field4", "value3"}}},
                                             {"key4", {{"field3", 2.0}, {"field4", "value4"}}}}},
                                           {"index3",
                                            {{"key5", {{"field5", 1.0}, {"field6", "value5"}}},
                                             {"key6", {{"field5", 2.0}, {"field6", "value6"}}}}}});

  auto joins = BuildJoinExpressions("index1", {{"index2", {{"field3", "index1", "field1"}}},
                                               {"index3", {{"field5", "index2", "field3"}}}});

  auto result = JoinAllIndexes(entries, keys, joins);
  EXPECT_THAT(result, IsJoinResult({{"key1", "key3", "key5"}, {"key2", "key4", "key6"}}));
}

TEST_F(IndexJoinTest, NoMatches) {
  // Different values
  auto [entries, keys] = BuildIndexesData({{"index1",
                                            {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                             {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                           {"index2",
                                            {{"key3", {{"field3", 3.0}, {"field4", "value3"}}},
                                             {"key4", {{"field3", 4.0}, {"field4", "value4"}}}}}});

  auto joins = BuildJoinExpressions("index1", {{"index2", {{"field3", "index1", "field1"}}}});

  auto result = JoinAllIndexes(entries, keys, joins);
  EXPECT_TRUE(result.Empty());

  // Different types
  auto [entries2, keys2] =
      BuildIndexesData({{"index1",
                         {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                          {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                        {"index2",
                         {{"key3", {{"field3", "value3"}, {"field4", "value4"}}},
                          {"key4", {{"field3", "value5"}, {"field4", "value6"}}}}}});

  result = JoinAllIndexes(entries2, keys2, joins);
  EXPECT_TRUE(result.Empty());
}

TEST_F(IndexJoinTest, JoinWithMultipleFields) {
  auto [entries, keys] = BuildIndexesData({{"index1",
                                            {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                             {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                           {"index2",
                                            {{"key3", {{"field3", 1.0}, {"field4", "value1"}}},
                                             {"key4", {{"field3", 2.0}, {"field4", "value2"}}}}},
                                           {"index3",
                                            {{"key5", {{"field5", 1.0}, {"field6", "value1"}}},
                                             {"key6", {{"field5", 2.0}, {"field6", "value2"}}}}}});

  auto joins = BuildJoinExpressions(
      "index1", {{"index2", {{"field3", "index1", "field1"}, {"field4", "index1", "field2"}}},
                 {"index3", {{"field5", "index2", "field3"}, {"field6", "index2", "field4"}}}});

  auto result = JoinAllIndexes(entries, keys, joins);
  EXPECT_THAT(result, IsJoinResult({{"key1", "key3", "key5"}, {"key2", "key4", "key6"}}));
}

TEST_F(IndexJoinTest, JoinWithSeveralCopiesOfSameKey) {
  auto [entries, keys] =
      BuildIndexesData({{"index1",
                         {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                          {"key2", {{"field1", 2.0}, {"field2", "value2"}}},
                          {"key3", {{"field1", 1.0}, {"field2", "value1"}}},
                          {"key4", {{"field1", 2.0}, {"field2", "value2"}}}}},
                        {"index2",
                         {{"key5", {{"field3", 1.0}, {"field4", "value1"}}},
                          {"key6", {{"field3", 2.0}, {"field4", "value2"}}}}},
                        {"index3",
                         {{"key7", {{"field5", 1.0}, {"field6", "value1"}}},
                          {"key8", {{"field5", 2.0}, {"field6", "value2"}}},
                          {"key9", {{"field5", 1.0}, {"field6", "value1"}}},
                          {"key10", {{"field5", 2.0}, {"field6", "value2"}}},
                          {"key11", {{"field5", 11.0}, {"field6", "value2"}}}}}});

  auto joins = BuildJoinExpressions(
      "index1", {{"index2", {{"field3", "index1", "field1"}, {"field4", "index1", "field2"}}},
                 {"index3", {{"field5", "index2", "field3"}, {"field6", "index2", "field4"}}}});

  auto result = JoinAllIndexes(entries, keys, joins);
  EXPECT_THAT(result, IsJoinResult({{"key1", "key5", "key7"},
                                    {"key2", "key6", "key8"},
                                    {"key3", "key5", "key7"},
                                    {"key4", "key6", "key8"},
                                    {"key1", "key5", "key9"},
                                    {"key2", "key6", "key10"},
                                    {"key3", "key5", "key9"},
                                    {"key4", "key6", "key10"}}));
}

}  // namespace dfly
