// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/index_join.h"

namespace dfly::join {

namespace {

using KeyIndex = size_t;
using KeyIndexes = Vector<KeyIndex>;

// Joins joined_entries with new index entries using join_expressions.
// It uses hash joining algorithm to find matching entries.
std::vector<KeyIndexes> JoinWithNewIndex(
    EntriesPerIndex indexes_entries, absl::Span<const KeyIndexes> joined_entries,
    size_t new_index,  // represented as index in indexes_entries
    absl::Span<const JoinExpression> join_expressions) {
  /* We fill join_map with values sets from joined entries.
     In join_map we store {set of field values} to indexes in joined_entries that match this set of
     field values. So, then we can go over new_index entries and match their values with
     joined_entries using this.
     TODO: use hash map for the smallest set (new_index or joined_entries) */
  using ValuesSet = Vector<JoinableValue>;
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
      size_t index = join_expression.foreign_index;
      size_t field_index = join_expression.foreign_field;

      // Now we need to get value of this field from joined key in this index
      DCHECK_LT(index, joined_entry_keys.size()) << "Join order broken, index out of range";
      KeyIndex key_index = joined_entry_keys[index];
      const JoinableValue& field_value = indexes_entries[index][key_index].second[field_index];

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
  const auto& new_index_entries = indexes_entries[new_index];
  for (size_t i = 0; i < new_index_entries.size(); ++i) {
    const auto& index_entries = new_index_entries[i].second;

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

Vector<Vector<Key>> JoinAllIndexes(EntriesPerIndex indexes_entries, IndexesJoinExpressions joins) {
  if (indexes_entries.empty()) {
    return {};
  }

  // Will used to initialize joined entries
  const auto& first_index_entries = indexes_entries[0];

  /* Store current result of joins
     Each entry is vector of indexes, that referce to one key in the index
     For example, {1, 0, 4} means that key with index 1 in the first index,
     key with index 0 in the second index and key with index 4 in the third index were joined to
     single entry. */
  std::vector<KeyIndexes> joined_entries(first_index_entries.size(), KeyIndexes(1));

  // At the first step all keys from the first index are joined
  for (size_t i = 0; i < first_index_entries.size(); ++i) {
    joined_entries[i][0] = i;
  }

  DCHECK(joins[0].empty()) << "Base index must be first and have no joins";

  /* Now we need to iterate over all indexes and the joins
     Using joins for the new index, we will find matching entries in the current result
     (joined_entries) with the entries in the new index. */
  for (size_t i = 1; i < indexes_entries.size(); ++i) {
    joined_entries = JoinWithNewIndex(indexes_entries, joined_entries, i, joins[i]);
  }

  const size_t result_size = joined_entries.size();
  const size_t indexes_count = indexes_entries.size();
  // Now we have joined entries, we need to build JoinResult
  Vector<Vector<Key>> result(result_size, Vector<Key>(indexes_count));

  for (size_t i = 0; i < result_size; ++i) {
    auto& result_entry = result[i];

    for (size_t index = 0; index < indexes_count; ++index) {
      // Index of joined key in the current index
      KeyIndex key_index = joined_entries[i][index];
      // Find key by the key_index
      const auto& key = indexes_entries[index][key_index].first;

      // Add key to the result
      // That means that this key from this index was joined
      result_entry[index] = key;
    }
  }

  return result;
}

}  // namespace dfly::join
