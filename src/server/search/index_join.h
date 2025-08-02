// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>

#include "base/logging.h"
#include "core/linear_search_map.h"
#include "core/maps_list.h"

namespace dfly::join {
// Field value
// Same as search::SortableValue, but do not have monostate and stores string_view instead of
// std::string.
using JoinableValue = std::variant<double, std::string_view>;

/* Each index has its own set of fields used for joins.
   Additionally, each index contains multiple keys/documents it has indexed, and each document
   includes several fields.

   For example:
    JOIN index2 ON index2.field1 = other_index.field2 AND index2.field3 = other_index.field4

    So, index2 uses field1 and field3 for joins. It also indexed docs key1, key2, key3:
    IndexEntries will store: [{"key1", {"field1" : value, "field3" : value}},
                             {"key2", {"field1" : value, "field3" : value}},
                             {"key3", {"field1" : value, "field3" : value}}]
*/
using IndexEntries = MapsList<std::string_view /*field names*/, JoinableValue /*field values*/>;
using EntriesPerIndex = LinearSearchMap<std::string_view /*index name*/, IndexEntries>;
using KeysPerIndex = LinearSearchMap<std::string_view /*index name*/,
                                     absl::InlinedVector<std::string_view, 4> /*keys*/>;

// Stores data for single join expression,
// e.g. index1.field1 = index2.field2:
// field - "field1", foreign_index - "index2", foreign_field - "field2"
struct JoinExpression {
  std::string_view field;
  std::string_view foreign_index;
  std::string_view foreign_field;
};

using JoinExpressionsList = absl::InlinedVector<JoinExpression, 4>;

/* Each index can have several join expressions, e.g.:
   JOIN index1 ON index1.field1 = other_index.field2 AND index1.field3 = other_index.field4
   will result in:
   {"index1", {{"field1", "other_index", "field2"}, {"field3", "other_index", "field4"}}} */
using IndexesJoinExpressions =
    LinearSearchMap<std::string_view /*index name*/, JoinExpressionsList>;

/* TODO: add description */
using JoinResult = MapsList<std::string_view /*index name*/, std::string_view /*key*/>;

// Joins all indexes in indexes_map using join_expressions.
// Join algorithm is used is hash join.
JoinResult JoinAllIndexes(const EntriesPerIndex& indexes_entries, const KeysPerIndex& indexes_keys,
                          const IndexesJoinExpressions& joins);

}  // namespace dfly::join
