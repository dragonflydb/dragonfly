// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>

#include "base/logging.h"
#include "core/linear_search_map.h"
#include "core/search/base.h"
#include "server/tx_base.h"

namespace dfly::join {

template <typename T> using Vector = absl::InlinedVector<T, 4>;

/* Represents field value.
   Same as search::SortableValue, but do not have monostate and stores string_view instead of
   std::string. */
using JoinableValue = std::variant<double, std::string_view>;

/* Each index has its own set of fields used for joins.
   Additionally, each index contains multiple keys/documents it has indexed, and each document
   includes several fields.

   For example:
    JOIN index2 ON index2.field1 = other_index.field2 AND index2.field3 = other_index.field4

    So, index2 uses field1 and field3 for joins. It also indexed docs key1, key2, key3:
    EntriesPerIndex will store something like:
                            [{"key1", {"field1" : value, "field3" : value}},
                             {"key2", {"field1" : value, "field3" : value}},
                             {"key3", {"field1" : value, "field3" : value}}].
    But to make join algorithm more efficient, we store it as raw vectors,
    instead of field_name as string, we use indexes;
    instead of key names we use shard id and doc id.
*/
using Key = std::pair<ShardId, search::DocId>;
using Entry = std::pair<Key, Vector<JoinableValue> /*fields values of this key*/>;
using EntriesPerIndex = absl::Span<const Vector<Entry> /*one index can store several keys*/>;

// TODO: comments
using OwnedJoinableValue = std::variant<double, std::string>;
using OwnedEntry = std::pair<Key, Vector<OwnedJoinableValue>>;

// Stores data for single join expression,
// e.g. index1.field1 = index2.field2:
// field - "field1", foreign_index - "index2", foreign_field - "field2"
struct JoinExpression {
  size_t field;          // field is represented as index in the Entry.second array
  size_t foreign_index;  // foreign_index is represented as index in the EntriesPerIndex array
  size_t foreign_field;  // foreign_field is too represented as index in the Entry.second array
};

using JoinExpressionsVec = Vector<JoinExpression>;

/* Each index can have several join expressions, e.g.:
   JOIN index1 ON index1.field1 = other_index.field2 AND index1.field3 = other_index.field4
   will result in:
   {"index1", {{"field1", "other_index", "field2"}, {"field3", "other_index", "field4"}}} */
using IndexesJoinExpressions = absl::Span<const JoinExpressionsVec>;

using KeyIndex = size_t;
using KeyIndexes = Vector<KeyIndex>;

/* Joins all indexes in indexes_map using join_expressions.
   Join algorithm is used is hash join. */
Vector<Vector<Key>> JoinAllIndexes(
    EntriesPerIndex indexes_entries, IndexesJoinExpressions joins,
    absl::FunctionRef<void(std::vector<KeyIndexes>*)> aggregate_after_join);

Vector<Vector<Key>> JoinAllIndexes(EntriesPerIndex indexes_entries, IndexesJoinExpressions joins);

}  // namespace dfly::join
