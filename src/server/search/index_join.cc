// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/index_join.h"

namespace dfly::join {

namespace {

using KeyIndex = size_t;
// using KeyIndexes = absl::InlinedVector<KeyIndex, 4>;
using KeyIndexes = std::vector<KeyIndex>;

// Joins joined_entries with new index entries using join_expressions.
// It uses hash joining algorithm to find matching entries.
std::vector<KeyIndexes> JoinWithNewIndex(const EntriesPerIndex& indexes,
                                         const std::vector<KeyIndexes>& joined_entries,
                                         std::string_view new_index,
                                         const JoinExpressionsList& join_expressions) {
  /* We fill join_map with values sets from joined entries.
     In join_map we store {set of field values} to indexes in joined_entries that match this set of
     field values. So, then we can go over new_index entries and match their values with
     joined_entries using this.
     TODO: use hash map for the smallest set (new_index or joined_entries) */
  using ValuesSet = absl::InlinedVector<JoinableValue, 4>;
  using JoinEntriesIndexes = absl::InlinedVector<size_t, 1>;
  absl::flat_hash_map<ValuesSet, JoinEntriesIndexes> join_map;
  join_map.reserve(joined_entries.size());

  // Now we need to initialize join_map with values of joined entries.
  for (size_t i = 0; i < joined_entries.size(); ++i) {
    const auto& joined_entry_keys = joined_entries[i];

    ValuesSet values_set;
    values_set.reserve(join_expressions.size());

    // Go over all join expressions and get field values using foreign index and field.
    for (const auto& join_expression : join_expressions) {
      auto& index_name = join_expression.foreign_index;
      auto& field_name = join_expression.foreign_field;

      // Now we need to get value of this field from joined key in this index
      size_t index = indexes.find_index(index_name);
      KeyIndex key_index = joined_entry_keys[index];
      const JoinableValue& field_value = indexes[index].second[key_index][field_name];

      // Add value to the set
      values_set.push_back(field_value);
    }

    // That means that this set of values corresponds to joined entry i
    join_map[values_set].push_back(i);
  }

  std::vector<KeyIndexes> result;
  result.reserve(join_map.size());

  // Now we store all possible sets of values in joined_entries that match this set.
  // We can iterate over new index and find entries with the same set of values.
  const auto& new_index_entries = indexes[new_index];
  for (size_t i = 0; i < new_index_entries.Size(); ++i) {
    const auto index_entries = new_index_entries[i];

    ValuesSet values_set;
    values_set.reserve(join_expressions.size());
    // Go over all join expressions and get field values for this entry
    for (const auto& join_expression : join_expressions) {
      const JoinableValue& field_value = index_entries[join_expression.field];
      values_set.push_back(field_value);
    }

    // Now we need to find this set in the join_map
    auto it = join_map.find(values_set);
    if (it == join_map.end()) {
      continue;
    }

    // This entry in new index matches some joined entries,
    // we need to go over all entries with the same set of values
    // and add them to the result
    for (size_t joined_entry_index : it->second) {
      result.push_back(joined_entries[joined_entry_index]);
      // Add new index entry to the joined entry
      result.back().push_back(i);
    }
  }

  return result;
}

}  // anonymous namespace

JoinResult JoinAllIndexes(const EntriesPerIndex& indexes_entries, const KeysPerIndex& indexes_keys,
                          const IndexesJoinExpressions& joins) {
  if (indexes_entries.empty()) {
    return {};
  }

  // Will used to initialize joined entries
  auto& first_index_entries = indexes_entries[0].second;

  /* Store current result of joins
     Each entry is vector of indexes, that referce to one key in the index
     For example, {1, 0, 4} means that key with index 1 in the first index,
     key with index 0 in the second index and key with index 4 in the third index were joined to
     single entry. */
  std::vector<KeyIndexes> joined_entries(first_index_entries.Size(), KeyIndexes(1));

  // At the first step all keys from the first index are joined
  for (size_t i = 0; i < first_index_entries.Size(); ++i) {
    joined_entries[i][0] = i;
  }

  /* Now we need to iterate over all indexes and the joins
     Using joins for the new index, we will find matching entries in the current result
     (joined_entries) with the entries in the new index. */
  for (size_t i = 1; i < indexes_entries.size(); ++i) {
    const auto& index_name = indexes_entries[i].first;
    joined_entries =
        JoinWithNewIndex(indexes_entries, joined_entries, index_name, joins[index_name]);
  }

  auto index_names = indexes_entries.keys();
  const size_t indexes_count = index_names.size();
  // Now we have joined entries, we need to build JoinResult
  JoinResult result(index_names.begin(), index_names.end(), joined_entries.size());

  for (size_t i = 0; i < joined_entries.size(); ++i) {
    auto result_entry = result[i];

    for (size_t index = 0; index < indexes_count; ++index) {
      std::string_view index_name = index_names[index];

      // Index of joined key in the current index
      KeyIndex key_index = joined_entries[i][index];
      // Find key name by the key_index
      std::string_view key_name = indexes_keys[index_name][key_index];

      // Add key to the result
      // That means that this key from this index was joined
      result_entry[index_name] = key_name;
    }
  }

  return result;
}

}  // namespace dfly::join
