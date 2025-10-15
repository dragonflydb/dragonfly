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

struct TestIndexData {
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

struct TestJoinExpression {
  std::string_view field;
  std::string_view foreign_index;
  std::string_view foreign_field;
};

struct PreprocessedIndexData {
  Vector<Vector<Entry>> entries;
  std::unordered_map<std::string_view, size_t> index_name_to_index;
  std::unordered_map<std::string_view, Key> key_name_to_key;
  std::vector<std::unordered_map<std::string_view, size_t>> field_names_to_index;
};

MATCHER_P(IsJoinResultMatcher, expected, "") {
  std::vector<testing::Matcher<std::vector<join::Key>>> matchers;
  for (const auto& entry : expected) {
    std::vector<join::Key> keys;
    for (auto field : entry) {
      keys.push_back(field);
    }
    matchers.push_back(testing::ElementsAreArray(keys));
  }

  std::vector<std::vector<join::Key>> result;
  for (size_t index = 0; index < arg.size(); ++index) {
    std::vector<join::Key> entry;
    for (const auto& key : arg[index]) {
      entry.push_back(key);
    }
    result.push_back(std::move(entry));
  }
  return testing::ExplainMatchResult(testing::UnorderedElementsAreArray(matchers), result,
                                     result_listener);
}

template <typename... Args>
auto IsJoinResult(const PreprocessedIndexData& data,
                  std::vector<std::vector<std::string_view>> joined_data) {
  std::vector<std::vector<join::Key>> joined_keys(joined_data.size());
  for (size_t i = 0; i < joined_data.size(); ++i) {
    for (const auto& entry : joined_data[i]) {
      auto it = data.key_name_to_key.find(entry);
      DCHECK(it != data.key_name_to_key.end()) << "Key not found in index data: " << entry;
      joined_keys[i].push_back(it->second);
    }
  }

  return IsJoinResultMatcher(std::move(joined_keys));
}

PreprocessedIndexData PreprocessIndexesData(std::vector<TestIndexData> indexes_data) {
  PreprocessedIndexData data;

  auto contains = [](const auto& set, const auto& key) { return set.find(key) != set.end(); };

  search::DocId doc_id = 0;
  for (size_t index = 0; index < indexes_data.size(); index++) {
    const auto& [index_name, index_data] = indexes_data[index];
    DCHECK(!contains(data.index_name_to_index, index_name))
        << "Duplicate index name: " << index_name;
    data.index_name_to_index[index_name] = index;

    data.field_names_to_index.emplace_back();
    auto& field_names_map = data.field_names_to_index.back();

    if (!index_data.empty()) {
      for (size_t i = 0; i < index_data[0].fields.size(); ++i) {
        const auto& field = index_data[0].fields[i];
        DCHECK(!contains(field_names_map, field.name))
            << "Duplicate field name in index: " << field.name;
        field_names_map[field.name] = i;
      }
    }

    Vector<Entry> index_entries;
    index_entries.reserve(index_data.size());

    for (size_t i = 0; i < index_data.size(); ++i) {
      const auto& [key, fields] = index_data[i];
      DCHECK(!contains(data.key_name_to_key, key)) << "Duplicate key name in index: " << key;

      Key key_for_join = {0 /*in tests we are using 0 for ShardId*/, doc_id++};
      data.key_name_to_key[key] = key_for_join;

      Entry entry = {key_for_join, Vector<JoinableValue>(field_names_map.size())};
      std::set<std::string_view> fields_set;
      for (const auto& [field_name, field_value] : fields) {
        DCHECK(contains(field_names_map, field_name));
        DCHECK(!contains(fields_set, field_name)) << "Duplicate field name in key: " << field_name;

        entry.second[field_names_map[field_name]] = field_value;
        fields_set.insert(field_name);
      }

      DCHECK_EQ(fields_set.size(), field_names_map.size())
          << "Not all fields are set for key: " << key;

      index_entries.emplace_back(std::move(entry));
    }

    data.entries.emplace_back(std::move(index_entries));
  }

  return data;
}

join::Vector<JoinExpressionsVec> BuildJoinExpressions(
    const PreprocessedIndexData& index_data,
    std::initializer_list<std::pair<std::string_view, std::initializer_list<TestJoinExpression>>>
        data) {
  join::Vector<JoinExpressionsVec> join_expressions(1);

  auto contains = [](const auto& set, const auto& key) { return set.find(key) != set.end(); };

  std::set<std::string_view> index_names_set;
  for (const auto& [index_name, expressions] : data) {
    DCHECK(contains(index_data.index_name_to_index, index_name))
        << "Index not found in join expressions: " << index_name;
    DCHECK(!contains(index_names_set, index_name))
        << "Duplicate index name in join expressions: " << index_name;

    index_names_set.insert(index_name);
    size_t current_index = index_data.index_name_to_index.at(index_name);

    JoinExpressionsVec exprs;
    for (const auto& expr : expressions) {
      DCHECK(contains(index_data.field_names_to_index[current_index], expr.field))
          << "Field not found in index: " << expr.field;
      size_t field_index = index_data.field_names_to_index[current_index].at(expr.field);

      DCHECK(contains(index_data.index_name_to_index, expr.foreign_index))
          << "Foreign index not found in join expressions: " << expr.foreign_index;
      size_t foreign_index = index_data.index_name_to_index.at(expr.foreign_index);

      DCHECK(contains(index_data.field_names_to_index[foreign_index], expr.foreign_field))
          << "Foreign field not found in foreign index: " << expr.foreign_field;
      size_t foreign_field_index =
          index_data.field_names_to_index[foreign_index].at(expr.foreign_field);

      exprs.emplace_back(JoinExpression{field_index, foreign_index, foreign_field_index});
    }

    join_expressions.emplace_back(std::move(exprs));
  }

  return join_expressions;
}

TEST_F(IndexJoinTest, SimpleJoin) {
  auto data = PreprocessIndexesData({{"index1",
                                      {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                       {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                     {"index2",
                                      {{"key3", {{"field3", 1.0}, {"field4", "value3"}}},
                                       {"key4", {{"field3", 2.0}, {"field4", "value4"}}}}}});

  auto joins = BuildJoinExpressions(data, {{"index2", {{"field3", "index1", "field1"}}}});

  auto result = JoinAllIndexes(data.entries, joins);
  EXPECT_THAT(result, IsJoinResult(data, {{"key1", "key3"}, {"key2", "key4"}}));
}

TEST_F(IndexJoinTest, MultipleJoins) {
  auto data = PreprocessIndexesData({{"index1",
                                      {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                       {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                     {"index2",
                                      {{"key3", {{"field3", 1.0}, {"field4", "value3"}}},
                                       {"key4", {{"field3", 2.0}, {"field4", "value4"}}}}},
                                     {"index3",
                                      {{"key5", {{"field5", 1.0}, {"field6", "value5"}}},
                                       {"key6", {{"field5", 2.0}, {"field6", "value6"}}}}}});

  auto joins = BuildJoinExpressions(data, {{"index2", {{"field3", "index1", "field1"}}},
                                           {"index3", {{"field5", "index2", "field3"}}}});

  auto result = JoinAllIndexes(data.entries, joins);
  EXPECT_THAT(result, IsJoinResult(data, {{"key1", "key3", "key5"}, {"key2", "key4", "key6"}}));
}

TEST_F(IndexJoinTest, NoMatches) {
  // Different values
  auto data = PreprocessIndexesData({{"index1",
                                      {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                       {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                     {"index2",
                                      {{"key3", {{"field3", 3.0}, {"field4", "value3"}}},
                                       {"key4", {{"field3", 4.0}, {"field4", "value4"}}}}}});

  auto joins = BuildJoinExpressions(data, {{"index2", {{"field3", "index1", "field1"}}}});

  auto result = JoinAllIndexes(data.entries, joins);
  EXPECT_TRUE(result.empty());

  // Different types
  auto data2 = PreprocessIndexesData({{"index1",
                                       {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                        {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                      {"index2",
                                       {{"key3", {{"field3", "value3"}, {"field4", "value4"}}},
                                        {"key4", {{"field3", "value5"}, {"field4", "value6"}}}}}});

  auto joins2 = BuildJoinExpressions(data2, {{"index2", {{"field3", "index1", "field1"}}}});

  result = JoinAllIndexes(data2.entries, joins2);
  EXPECT_TRUE(result.empty());
}

TEST_F(IndexJoinTest, JoinWithMultipleFields) {
  auto data = PreprocessIndexesData({{"index1",
                                      {{"key1", {{"field1", 1.0}, {"field2", "value1"}}},
                                       {"key2", {{"field1", 2.0}, {"field2", "value2"}}}}},
                                     {"index2",
                                      {{"key3", {{"field3", 1.0}, {"field4", "value1"}}},
                                       {"key4", {{"field3", 2.0}, {"field4", "value2"}}}}},
                                     {"index3",
                                      {{"key5", {{"field5", 1.0}, {"field6", "value1"}}},
                                       {"key6", {{"field5", 2.0}, {"field6", "value2"}}}}}});

  auto joins = BuildJoinExpressions(
      data, {{"index2", {{"field3", "index1", "field1"}, {"field4", "index1", "field2"}}},
             {"index3", {{"field5", "index2", "field3"}, {"field6", "index2", "field4"}}}});

  auto result = JoinAllIndexes(data.entries, joins);
  EXPECT_THAT(result, IsJoinResult(data, {{"key1", "key3", "key5"}, {"key2", "key4", "key6"}}));
}

TEST_F(IndexJoinTest, JoinWithSeveralCopiesOfSameKey) {
  auto data = PreprocessIndexesData({{"index1",
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
      data, {{"index2", {{"field3", "index1", "field1"}, {"field4", "index1", "field2"}}},
             {"index3", {{"field5", "index2", "field3"}, {"field6", "index2", "field4"}}}});

  auto result = JoinAllIndexes(data.entries, joins);
  EXPECT_THAT(result, IsJoinResult(data, {{"key1", "key5", "key7"},
                                          {"key2", "key6", "key8"},
                                          {"key3", "key5", "key7"},
                                          {"key4", "key6", "key8"},
                                          {"key1", "key5", "key9"},
                                          {"key2", "key6", "key10"},
                                          {"key3", "key5", "key9"},
                                          {"key4", "key6", "key10"}}));
}

}  // namespace dfly
