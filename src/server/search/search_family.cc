// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/container/flat_hash_map.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>
#include <absl/strings/string_view.h>

#include <atomic>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/search/indices.h"
#include "core/search/query_driver.h"
#include "core/search/search.h"
#include "core/search/vector_utils.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/cluster/cluster_config.h"
#include "server/cluster/coordinator.h"
#include "server/command_registry.h"
#include "server/config_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/search/aggregator.h"
#include "server/search/doc_index.h"
#include "server/search/global_hnsw_index.h"
#include "server/transaction.h"
#include "src/core/overloaded.h"

ABSL_FLAG(bool, search_reject_legacy_field, true, "FT.AGGREGATE: Reject legacy field names.");
ABSL_FLAG(bool, cluster_search, false,
          "Enable search commands for cross-shard search. turned off by default for safety.");

ABSL_FLAG(size_t, MAXSEARCHRESULTS, 1000000, "Maximum number of results from ft.search command");

ABSL_FLAG(size_t, search_query_string_bytes, 10240,
          "Maximum number of bytes in search query string");

namespace dfly {

using namespace std;
using namespace facade;

namespace {
// we use it to find which flags are belong to search
const std::string kCurrentFile = std::filesystem::path(__FILE__).filename().string();

using nonstd::make_unexpected;

template <typename T> using ParseResult = io::Result<T, ErrorReply>;

nonstd::unexpected_type<ErrorReply> CreateSyntaxError(std::string message) {
  return make_unexpected(ErrorReply{std::move(message), kSyntaxErrType});
}

nonstd::unexpected_type<ErrorReply> CreateSyntaxError(std::string_view message) {
  return make_unexpected(ErrorReply{message, kSyntaxErrType});
}

string IndexNotFoundMsg(string_view index_name) {
  return absl::StrCat("Index with name '", index_name, "' not found");
}

// Send error from parser or result
// Returns false if no errors occured
template <typename T>
bool SendErrorIfOccurred(const ParseResult<T>& result, CmdArgParser* parser,
                         SinkReplyBuilder* builder) {
  if (auto err = parser->TakeError(); err || !result) {
    builder->SendError(!result ? result.error() : err.MakeReply());
    return true;
  }

  return false;
}

bool IsValidJsonPath(string_view path) {
  error_code ec;
  MakeJsonPathExpr<TmpJson>(path, ec);
  return !ec;
}

search::SchemaField::VectorParams ParseVectorParams(CmdArgParser* parser) {
  search::SchemaField::VectorParams params{};

  params.use_hnsw = parser->MapNext("HNSW", true, "FLAT", false);
  const size_t num_args = parser->Next<size_t>();

  for (size_t i = 0; i * 2 < num_args; i++) {
    if (parser->Check("DIM", &params.dim)) {
    } else if (parser->Check("DISTANCE_METRIC")) {
      params.sim =
          parser->MapNext("L2", search::VectorSimilarity::L2, "IP", search::VectorSimilarity::IP,
                          "COSINE", search::VectorSimilarity::COSINE);
    } else if (parser->Check("INITIAL_CAP", &params.capacity)) {
    } else if (parser->Check("M", &params.hnsw_m)) {
    } else if (parser->Check("EF_CONSTRUCTION", &params.hnsw_ef_construction)) {
    } else if (parser->Check("EF_RUNTIME")) {
      parser->Next<size_t>();
      LOG(WARNING) << "EF_RUNTIME not supported";
    } else if (parser->Check("EPSILON")) {
      parser->Next<double>();
      LOG(WARNING) << "EPSILON not supported";
    } else {
      parser->Skip(2);
    }
  }

  return params;
}

ParseResult<search::SchemaField::TagParams> ParseTagParams(CmdArgParser* parser) {
  search::SchemaField::TagParams params{};
  while (parser->HasNext()) {
    if (parser->Check("SEPARATOR")) {
      std::string_view separator = parser->NextOrDefault();

      if (separator.size() != 1) {
        return CreateSyntaxError(
            absl::StrCat("Tag separator must be a single character. Got `"sv, separator, "`"sv));
      }

      params.separator = separator.front();
      continue;
    }

    if (parser->Check("CASESENSITIVE")) {
      params.case_sensitive = true;
      continue;
    }

    if (parser->Check("WITHSUFFIXTRIE")) {
      params.with_suffixtrie = true;
      continue;
    }

    break;
  }
  return params;
}

ParseResult<search::SchemaField::TextParams> ParseTextParams(CmdArgParser* parser) {
  search::SchemaField::TextParams params{};
  params.with_suffixtrie = parser->Check("WITHSUFFIXTRIE");
  return params;
}

search::SchemaField::NumericParams ParseNumericParams(CmdArgParser* parser) {
  search::SchemaField::NumericParams params{};
  if (parser->Check("BLOCKSIZE")) {
    params.block_size = parser->Next<size_t>();
  }
  return params;
}

// breaks on ParamsVariant initialization
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

using ParsedSchemaField =
    ParseResult<std::pair<search::SchemaField::FieldType, search::SchemaField::ParamsVariant>>;

// Tag fields include: [separator char] [casesensitive]
ParsedSchemaField ParseTag(CmdArgParser* parser) {
  auto tag_params = ParseTagParams(parser);
  if (!tag_params) {
    return make_unexpected(tag_params.error());
  }
  return std::make_pair(search::SchemaField::TAG, std::move(tag_params).value());
}

ParsedSchemaField ParseText(CmdArgParser* parser) {
  auto text_params = ParseTextParams(parser);
  if (!text_params)
    return make_unexpected(text_params.error());
  return std::make_pair(search::SchemaField::TEXT, std::move(text_params).value());
}

ParsedSchemaField ParseNumeric(CmdArgParser* parser) {
  return std::make_pair(search::SchemaField::NUMERIC, ParseNumericParams(parser));
}

// Vector fields include: {algorithm} num_args args...
ParsedSchemaField ParseVector(CmdArgParser* parser) {
  auto vector_params = ParseVectorParams(parser);

  if (parser->HasError()) {
    auto err = parser->TakeError();
    VLOG(1) << "Could not parse vector param " << err.index;
    return CreateSyntaxError("Parse error of vector parameters"sv);
  }

  if (vector_params.dim == 0) {
    return CreateSyntaxError("Knn vector dimension cannot be zero"sv);
  }
  return std::make_pair(search::SchemaField::VECTOR, vector_params);
}

ParsedSchemaField ParseGeo(CmdArgParser* parser) {
  return std::make_pair(search::SchemaField::GEO, std::monostate{});
}

// ON HASH | JSON
ParseResult<bool> ParseOnOption(CmdArgParser* parser, DocIndex* index) {
  index->type = parser->MapNext("HASH"sv, DocIndex::HASH, "JSON"sv, DocIndex::JSON);
  return true;
}

// PREFIX count prefix [prefix ...]
ParseResult<bool> ParsePrefix(CmdArgParser* parser, DocIndex* index) {
  size_t count = parser->Next<size_t>();
  index->prefixes.reserve(count);
  for (size_t i = 0; i < count; i++) {
    index->prefixes.push_back(parser->Next<std::string>());
  }
  return true;
}

// STOPWORDS count [words...]
ParseResult<bool> ParseStopwords(CmdArgParser* parser, DocIndex* index) {
  index->options.stopwords.clear();
  for (size_t num = parser->Next<size_t>(); num > 0; num--) {
    index->options.stopwords.emplace(parser->Next());
  }
  return true;
}

constexpr std::array<const std::string_view, 4> kIgnoredOptions = {
    "UNF"sv, "NOSTEM"sv, "INDEXMISSING"sv, "INDEXEMPTY"sv};
constexpr std::array<const std::string_view, 3> kIgnoredOptionsWithArg = {"WEIGHT"sv, "PHONETIC"sv};

// SCHEMA field [AS alias] type [flags...]
ParseResult<bool> ParseSchema(CmdArgParser* parser, DocIndex* index) {
  auto& schema = index->schema;

  if (!parser->HasNext()) {
    return CreateSyntaxError("Fields arguments are missing"sv);
  }

  while (parser->HasNext()) {
    string_view field = parser->Next();
    string_view field_alias = field;

    // Verify json path is correct
    if (index->type == DocIndex::JSON && !IsValidJsonPath(field)) {
      return CreateSyntaxError(absl::StrCat("Bad json path: "sv, field));
    }

    // AS [alias]
    parser->Check("AS", &field_alias);

    if (schema.field_names.contains(field_alias)) {
      return CreateSyntaxError(absl::StrCat("Duplicate field in schema - "sv, field_alias));
    }

    // Determine type
    using search::SchemaField;
    auto params_parser =
        parser->TryMapNext("TAG"sv, &ParseTag, "TEXT"sv, &ParseText, "NUMERIC"sv, &ParseNumeric,
                           "VECTOR"sv, &ParseVector, "GEO", &ParseGeo);
    if (!params_parser) {
      return CreateSyntaxError(
          absl::StrCat("Field type "sv, parser->Next(), " is not supported"sv));
    }

    auto parsed_params = params_parser.value()(parser);
    if (!parsed_params) {
      return make_unexpected(parsed_params.error());
    }

    auto [field_type, params] = std::move(parsed_params).value();

    // Flags: check for SORTABLE and NOINDEX
    uint8_t flags = 0;
    while (parser->HasNext()) {
      auto flag = parser->TryMapNext("NOINDEX", search::SchemaField::NOINDEX, "SORTABLE",
                                     search::SchemaField::SORTABLE);
      if (!flag) {
        std::string_view option = parser->Peek();
        if (std::find(kIgnoredOptions.begin(), kIgnoredOptions.end(), option) !=
            kIgnoredOptions.end()) {
          LOG_IF(WARNING, option != "INDEXMISSING"sv && option != "INDEXEMPTY"sv)
              << "Ignoring unsupported field option in FT.CREATE: " << option;
          // Ignore these options
          parser->Skip(1);
          continue;
        }
        if (std::find(kIgnoredOptionsWithArg.begin(), kIgnoredOptionsWithArg.end(), option) !=
            kIgnoredOptionsWithArg.end()) {
          LOG(WARNING) << "Ignoring unsupported field option in FT.CREATE: " << option;
          // Ignore these options with argument
          parser->Skip(2);
          continue;
        }
        break;
      }

      flags |= *flag;
    }

    schema.fields[field] = {field_type, flags, string{field_alias}, params};
    schema.field_names[field_alias] = field;
  }

  return false;
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

ParseResult<DocIndex> CreateDocIndex(CmdArgParser* parser) {
  DocIndex index{};

  while (parser->HasNext()) {
    auto option_parser =
        parser->TryMapNext("ON"sv, &ParseOnOption, "PREFIX"sv, &ParsePrefix, "STOPWORDS"sv,
                           &ParseStopwords, "SCHEMA"sv, &ParseSchema);

    if (!option_parser) {
      // Unsupported parameters are ignored for now
      parser->Skip(1);
      continue;
    }

    auto parse_result = option_parser.value()(parser, &index);
    if (!parse_result) {
      return make_unexpected(parse_result.error());
    }
    if (!parse_result.value()) {
      break;
    }
  }

  return index;
}

std::string_view ParseField(CmdArgParser* parser) {
  std::string_view field = parser->Next();
  if (absl::StartsWith(field, "@"sv)) {
    field.remove_prefix(1);  // remove leading @ if exists
  }
  return field;
}

std::optional<std::string_view> ParseFieldWithAtSign(CmdArgParser* parser) {
  std::string_view field = parser->Next();
  if (absl::StartsWith(field, "@"sv)) {
    field.remove_prefix(1);  // remove leading @
  } else {
    if (absl::GetFlag(FLAGS_search_reject_legacy_field)) {
      return std::nullopt;
    }
  }
  return field;
}

void ParseNumericFilter(CmdArgParser* parser, SearchParams* params) {
  auto field = ParseField(parser);
  size_t lo = parser->Next<size_t>();
  size_t hi = parser->Next<size_t>();
  if (auto it = params->optional_filters.find(field); it != params->optional_filters.end()) {
    search::OptionalNumericFilter* numeric_filter =
        dynamic_cast<search::OptionalNumericFilter*>(it->second.get());
    numeric_filter->AddRange(lo, hi);
  } else {
    params->optional_filters.emplace(field,
                                     std::make_unique<search::OptionalNumericFilter>(lo, hi));
  }
}

std::vector<FieldReference> ParseLoadOrReturnFields(CmdArgParser* parser, bool is_load) {
  // TODO: Change to num_strings. In Redis strings number is expected. For example: LOAD 3 $.a AS a
  std::vector<FieldReference> fields;
  size_t num_fields = parser->Next<size_t>();

  while (parser->HasNext() && num_fields--) {
    string_view field = is_load ? ParseField(parser) : parser->Next();
    string_view alias;
    parser->Check("AS", &alias);
    fields.emplace_back(field, alias);
  }
  return fields;
}

search::QueryParams ParseQueryParams(CmdArgParser* parser) {
  search::QueryParams params;
  size_t num_args = parser->Next<size_t>();
  while (parser->HasNext() && params.Size() * 2 < num_args) {
    auto [k, v] = parser->Next<string_view, string_view>();
    params[k] = v;
  }
  return params;
}

ParseResult<SearchParams> ParseSearchParams(CmdArgParser* parser) {
  SearchParams params;

  const size_t max_results = absl::GetFlag(FLAGS_MAXSEARCHRESULTS);

  while (parser->HasNext()) {
    // [LIMIT offset total]
    if (parser->Check("LIMIT")) {
      params.limit_offset = parser->Next<size_t>();
      params.limit_total = parser->Next<size_t>();
      if (params.limit_total > max_results) {
        return CreateSyntaxError(absl::StrFormat("LIMIT exceeds maximum of %d", max_results));
      }
    } else if (parser->Check("LOAD")) {
      if (params.return_fields) {
        return CreateSyntaxError("LOAD cannot be applied after RETURN"sv);
      }

      params.load_fields = ParseLoadOrReturnFields(parser, true);
    } else if (parser->Check("RETURN")) {
      if (params.load_fields) {
        return CreateSyntaxError("RETURN cannot be applied after LOAD"sv);
      }
      if (!params.return_fields)  // after NOCONTENT it's silently ignored
        params.return_fields = ParseLoadOrReturnFields(parser, false);
    } else if (parser->Check("NOCONTENT")) {  // NOCONTENT
      params.return_fields.emplace();
    } else if (parser->Check("PARAMS")) {  // [PARAMS num(ignored) name(ignored) knn_vector]
      params.query_params = ParseQueryParams(parser);
    } else if (parser->Check("SORTBY")) {
      FieldReference field{ParseField(parser)};
      params.sort_option =
          SearchParams::SortOption{field, parser->Check("DESC") ? SortOrder::DESC : SortOrder::ASC};
    } else if (parser->Check("FILTER")) {
      ParseNumericFilter(parser, &params);
    } else {
      // Unsupported parameters are ignored for now
      parser->Skip(1);
    }
  }

  params.limit_total = std::min(params.limit_total, max_results);

  return params;
}

ParseResult<aggregate::SortParams> ParseAggregatorSortParams(CmdArgParser* parser) {
  size_t strings_num = parser->Next<size_t>();

  aggregate::SortParams sort_params;
  sort_params.fields.reserve(strings_num / 2);

  while (parser->HasNext() && strings_num > 0) {
    std::string_view potential_field =
        parser->Peek();  // Peek to get the field name for potential error message
    std::optional<std::string_view> parsed_field = ParseFieldWithAtSign(parser);
    if (!parsed_field) {
      return CreateSyntaxError(
          absl::StrCat("SORTBY field name '", potential_field, "' must start with '@'"));
    }
    strings_num--;

    SortOrder sord_order = SortOrder::ASC;
    if (strings_num > 0) {
      auto order = parser->TryMapNext("ASC", SortOrder::ASC, "DESC", SortOrder::DESC);
      if (order) {
        sord_order = order.value();
        strings_num--;
      }
    }

    sort_params.fields.emplace_back(*parsed_field, sord_order);
  }

  if (strings_num) {
    return CreateSyntaxError("bad arguments for SORTBY: specified invalid number of strings"sv);
  }

  if (parser->Check("MAX")) {
    sort_params.max = parser->Next<size_t>();
  }

  return sort_params;
}

std::pair<std::string_view, std::string_view> Split(std::string_view s, char delim) {
  return absl::StrSplit(s, absl::MaxSplits(absl::ByChar(delim), 1));
}

// Example: LOAD_FROM index AS alias num_conditions condition [condition ...] [QUERY query]
// condition is in the form index.field=foreign_index.field or foreign_index.field=index.field
ParseResult<AggregateParams::JoinParams> ParseAggregatorJoinParams(
    CmdArgParser* parser, absl::flat_hash_set<std::string>* known_indexes) {
  AggregateParams::JoinParams join_params;
  join_params.index = parser->Next<std::string>();
  if (parser->Check("AS")) {
    join_params.index_alias = parser->Next<std::string>();
  } else {
    join_params.index_alias = join_params.index;
  }

  if (known_indexes->contains(join_params.index_alias)) {
    return CreateSyntaxError(
        absl::StrCat("Duplicate index alias in LOAD_FROM: '", join_params.index_alias, "'"));
  }

  // Validate index name
  known_indexes->insert(join_params.index_alias);

  size_t num_fields = parser->Next<size_t>();
  join_params.conditions.reserve(num_fields);
  // Conditions are in the form index.field=foreign_index.field or foreign_index.field=index.field
  while (parser->HasNext() && num_fields > 0) {
    auto [left, right] = Split(parser->Next(), '=');
    auto [l_index, l_field] = Split(left, '.');
    auto [r_index, r_field] = Split(right, '.');

    if (right.empty() || l_field.empty() || r_field.empty()) {
      return CreateSyntaxError(
          "bad arguments for LOAD_FROM: expected 'index.field=foreign_index.field'"sv);
    }

    if (!known_indexes->contains(l_index) || !known_indexes->contains(r_index)) {
      return CreateSyntaxError(absl::StrCat("bad arguments for LOAD_FROM: unknown index '",
                                            known_indexes->contains(l_index) ? r_index : l_index,
                                            "'"));
    }

    if (l_index == join_params.index_alias) {
      join_params.conditions.emplace_back(l_field, r_index, r_field);
    } else if (r_index == join_params.index_alias) {
      join_params.conditions.emplace_back(r_field, l_index, l_field);
    } else {
      return CreateSyntaxError(absl::StrCat(
          "bad arguments for LOAD_FROM: one of the field must be from the current index '",
          join_params.index_alias, "'. Got '", left, "' and '", right, "'"));
    }

    num_fields--;
  }

  parser->Check("QUERY", &join_params.query);

  return join_params;
}

ParseResult<AggregateParams> ParseAggregatorParams(CmdArgParser* parser) {
  AggregateParams params;
  tie(params.index, params.query) = parser->Next<string_view, string_view>();

  // Parse LOAD count field [field ...]
  // LOAD options are at the beginning of the query, so we need to parse them first
  while (parser->HasNext() && parser->Check("LOAD")) {
    auto fields = ParseLoadOrReturnFields(parser, true);
    if (!params.load_fields.has_value())
      params.load_fields = std::move(fields);
    else
      params.load_fields->insert(params.load_fields->end(), make_move_iterator(fields.begin()),
                                 make_move_iterator(fields.end()));
  }

  // Used for join params
  absl::flat_hash_set<std::string> current_known_indexes;
  current_known_indexes.insert(std::string{params.index});
  while (parser->HasNext() && parser->Check("LOAD_FROM")) {
    auto join_params = ParseAggregatorJoinParams(parser, &current_known_indexes);
    if (!join_params) {
      return make_unexpected(join_params.error());
    }
    params.joins.emplace_back(std::move(join_params).value());
  }
  const bool joining_enabled = !params.joins.empty();

  while (parser->HasNext()) {
    // GROUPBY nargs property [property ...]
    if (parser->Check("GROUPBY")) {
      size_t num_fields = parser->Next<size_t>();

      std::vector<std::string> fields;
      fields.reserve(num_fields);
      while (parser->HasNext() && num_fields > 0) {
        auto parsed_field = ParseFieldWithAtSign(parser);
        if (!parsed_field) {
          return CreateSyntaxError("bad arguments: Field name should start with '@'"sv);
        }

        fields.emplace_back(*parsed_field);
        num_fields--;
      }

      vector<aggregate::Reducer> reducers;
      while (parser->Check("REDUCE")) {
        using RF = aggregate::ReducerFunc;
        auto func_name =
            parser->TryMapNext("COUNT", RF::COUNT, "COUNT_DISTINCT", RF::COUNT_DISTINCT, "SUM",
                               RF::SUM, "AVG", RF::AVG, "MAX", RF::MAX, "MIN", RF::MIN);

        if (!func_name) {
          return CreateSyntaxError(absl::StrCat("reducer function ", parser->Next(), " not found"));
        }

        auto func = aggregate::FindReducerFunc(*func_name);
        auto nargs = parser->Next<size_t>();

        string source_field;
        if (nargs > 0) {
          source_field = ParseField(parser);
        }

        parser->ExpectTag("AS");
        string result_field = parser->Next<string>();

        reducers.push_back(
            aggregate::Reducer{std::move(source_field), std::move(result_field), func});
      }

      params.steps.push_back(aggregate::MakeGroupStep(std::move(fields), std::move(reducers)));
      continue;
    }

    // SORTBY nargs
    if (parser->Check("SORTBY")) {
      auto sort_params = ParseAggregatorSortParams(parser);
      if (!sort_params) {
        return make_unexpected(sort_params.error());  // Propagate the specific error
      }

      if (!joining_enabled || params.join_agg_params.HasValue()) {
        params.steps.push_back(aggregate::MakeSortStep(std::move(sort_params).value()));
      } else {
        params.join_agg_params.sort = std::move(sort_params).value();
      }
      continue;
    }

    // LIMIT
    if (parser->Check("LIMIT")) {
      auto [offset, num] = parser->Next<size_t, size_t>();
      if (!joining_enabled || params.join_agg_params.HasLimit()) {
        params.steps.push_back(aggregate::MakeLimitStep(offset, num));
      } else {
        params.join_agg_params.limit_offset = offset;
        params.join_agg_params.limit_total = num;
      }
      continue;
    }

    // PARAMS
    if (parser->Check("PARAMS")) {
      params.params = ParseQueryParams(parser);
      continue;
    }

    if (parser->Check("LOAD")) {
      return CreateSyntaxError("LOAD cannot be applied after projectors or reducers"sv);
    }

    if (parser->Check("LOAD_FROM")) {
      return CreateSyntaxError("LOAD_FROM cannot be applied after projectors or reducers"sv);
    }

    return CreateSyntaxError(absl::StrCat("Unknown clause: ", parser->Peek()));
  }

  return params;
}

// Data that we need at the first step of join
struct PreprocessedJoinData {
  struct SortParam {
    size_t index;
    size_t field_index;
    SortOrder order;
  };

  explicit PreprocessedJoinData(size_t n)
      : indexes(n), needed_fields(n), joins_per_index(n), fields_to_load_per_index(n) {
  }

  // Index names
  join::Vector<std::string_view> indexes;
  // Maps index alias to its index in the indexes vector
  absl::flat_hash_map<std::string_view, size_t> alias_to_index;

  // For each index we store the fields that are needed for the join
  join::Vector<join::Vector<std::string_view>> needed_fields;
  // For each index we store the join expressions that are used to join this index
  join::Vector<join::JoinExpressionsVec> joins_per_index;
  // For each index we store the fields that should be loaded from the document after the join
  join::Vector<join::Vector<std::string_view>> fields_to_load_per_index;
  // Maps field names to the shard_id and their index in the needed_fields vector
  join::Vector<SortParam> sort_params;
};

io::Result<PreprocessedJoinData, ErrorReply> PreprocessDataForJoin(std::string_view index,
                                                                   const AggregateParams& params) {
  DCHECK(!params.joins.empty());

  const size_t n = params.joins.size();
  PreprocessedJoinData result(n + 1);

  // Collect aliases and initialize result.indexes
  result.alias_to_index.reserve(n);
  result.alias_to_index[index] = 0;
  result.indexes[0] = index;
  for (size_t i = 0; i < n; ++i) {
    result.alias_to_index[params.joins[i].index_alias] = i + 1;
    result.indexes[i + 1] = params.joins[i].index;
  }

  // Collect needed fields for joins for each index
  // needed_fields[i] contains fields needed for index i
  // for each field name we store its index
  // Also collect joins for each index
  std::vector<absl::flat_hash_map<std::string_view, size_t>> needed_fields(n + 1);

  auto insert = [&](std::string_view field, auto* map) -> size_t {
    auto it = map->find(field);
    if (it == map->end()) {
      const size_t field_index = map->size();
      map->emplace(field, field_index);
      return field_index;
    }
    return it->second;
  };

  for (size_t i = 0; i < n; ++i) {
    const auto& join = params.joins[i];
    for (const auto& condition : join.conditions) {
      size_t field_index = insert(condition.field, &needed_fields[i + 1]);

      DCHECK(result.alias_to_index.contains(condition.foreign_field.first))
          << "Unknown foreign index alias: " << condition.foreign_field.first;
      size_t foreign_index = result.alias_to_index[condition.foreign_field.first];
      DCHECK_LE(foreign_index, i) << "Foreign index alias out of range: "
                                  << condition.foreign_field.first;

      size_t foreign_field_index =
          insert(condition.foreign_field.second, &needed_fields[foreign_index]);

      // Update joins for this index
      result.joins_per_index[i + 1].emplace_back(
          join::JoinExpression{field_index, foreign_index, foreign_field_index});
    }
  }

  // Collect fields needed for sorting
  // Max option will be temprorary ignored
  if (params.join_agg_params.sort) {
    for (const auto& sort_field : params.join_agg_params.sort.value().fields) {
      auto [index_alias, field_name] = Split(sort_field.first, '.');

      auto it = result.alias_to_index.find(index_alias);
      if (it == result.alias_to_index.end()) {
        return CreateSyntaxError(absl::StrCat("Unknown index alias '", index_alias,
                                              "' in the SORTBY option. Field: '", field_name, "'"));
      }

      size_t index = it->second;
      size_t field_index = insert(field_name, &needed_fields[index]);
      result.sort_params.push_back(
          PreprocessedJoinData::SortParam{index, field_index, sort_field.second});
    }
  }

  // Map them to the result.needed_fields
  for (size_t i = 0; i <= n; ++i) {
    auto& from = needed_fields[i];
    auto& to = result.needed_fields[i];

    to.resize(from.size());
    for (const auto& [field_name, field_index] : from) {
      to[field_index] = field_name;
    }
  }

  // Initialize fields_to_load_per_index
  for (const auto& field : params.load_fields.value_or(std::vector<FieldReference>{})) {
    auto [index_alias, field_name] = Split(field.Name(), '.');

    auto it = result.alias_to_index.find(index_alias);
    if (it == result.alias_to_index.end()) {
      return CreateSyntaxError(absl::StrCat("Unknown index alias '", index_alias,
                                            "' in the LOAD option. Field: '", field_name, "'"));
    }

    result.fields_to_load_per_index[it->second].emplace_back(field_name);
  }

  return result;
}

// Merge preaggregated results from all shards for each index
join::Vector<join::Vector<join::Entry>> MergePreaggregatedShardJoinData(
    absl::Span<const std::vector<join::Vector<join::OwnedEntry>>> preaggregated_shard_data) {
  if (preaggregated_shard_data.empty()) {
    return {};
  }

  // indexes_entries[i] contains the preaggregated data for index i
  const size_t indexes_count = preaggregated_shard_data[0].size();
  join::Vector<join::Vector<join::Entry>> indexes_entries(indexes_count);
  for (size_t i = 0; i < indexes_count; ++i) {
    auto& entries = indexes_entries[i];

    size_t num_docs = 0;
    for (size_t j = 0; j < shard_set->size(); ++j) {
      num_docs += preaggregated_shard_data[j][i].size();
    }

    entries.reserve(num_docs);
    for (size_t j = 0; j < shard_set->size(); ++j) {
      for (const auto& entry : preaggregated_shard_data[j][i]) {
        join::Vector<join::JoinableValue> field_values;
        field_values.reserve(entry.second.size());

        auto insert_copy = [&field_values](const auto& field_value) {
          field_values.emplace_back(field_value);
        };

        for (const auto& field_value : entry.second) {
          std::visit(insert_copy, field_value);
        }

        entries.emplace_back(entry.first, std::move(field_values));
      }
    }
  }

  return indexes_entries;
}

join::Vector<join::Vector<join::Key>> DoJoin(
    absl::Span<const std::vector<join::Vector<join::OwnedEntry>>> preaggregated_shard_data,
    const AggregateParams& params, const PreprocessedJoinData& join_data) {
  using join::KeyIndexes;

  auto indexes_entries = MergePreaggregatedShardJoinData(preaggregated_shard_data);

  auto sort_and_limit = [&](std::vector<KeyIndexes>* joined_entries) {
    const size_t offset = params.join_agg_params.limit_offset;
    const size_t total = params.join_agg_params.limit_total;
    if (offset >= joined_entries->size()) {
      joined_entries->clear();
      return;
    }

    const auto& sort_params = join_data.sort_params;
    auto comparator = [&](const KeyIndexes& l, const KeyIndexes& r) {
      for (const auto& sort_param : sort_params) {
        size_t index = sort_param.index;
        const join::JoinableValue& l_value =
            indexes_entries[index][l[index]].second[sort_param.field_index];
        const join::JoinableValue& r_value =
            indexes_entries[index][r[index]].second[sort_param.field_index];

        if (l_value == r_value) {
          continue;
        }
        return sort_param.order == SortOrder::ASC ? l_value < r_value : l_value > r_value;
      }
      return false;
    };

    size_t limit = offset + total;
    if (!sort_params.empty()) {
      if (limit >= joined_entries->size()) {
        std::sort(joined_entries->begin(), joined_entries->end(), std::move(comparator));
      } else {
        std::partial_sort(joined_entries->begin(), joined_entries->begin() + limit,
                          joined_entries->end(), std::move(comparator));
        joined_entries->resize(limit);
      }
    }

    size_t new_limit = std::min(limit, joined_entries->size());
    if (offset) {
      for (size_t i = offset; i < new_limit; ++i) {
        auto& dest = (*joined_entries)[i - offset];
        auto& src = (*joined_entries)[i];
        DCHECK(dest.size() == src.size());
        dest = std::move(src);
      }
    }

    size_t new_size = std::min(total, joined_entries->size() - offset);
    joined_entries->resize(new_size);
  };

  return join::JoinAllIndexes(indexes_entries, join_data.joins_per_index, sort_and_limit);
}

std::vector<aggregate::DocValues> MergeJoinedKeysWithData(
    const AggregateParams& agg_params, const PreprocessedJoinData& join_data,
    absl::Span<const join::Vector<join::Key>> joined_entries,
    absl::Span<const std::vector<ShardDocIndex::FieldsValuesPerDocId>> shard_keys_data) {
  std::vector<aggregate::DocValues> merged_data;
  merged_data.reserve(joined_entries.size());

  const size_t indexes_count = join_data.indexes.size();
  const auto& fields_per_index = join_data.fields_to_load_per_index;

  for (const auto& entry : joined_entries) {
    aggregate::DocValues doc_values;

    // First reserve space for the total number of fields
    size_t docs_count = 0;
    for (size_t i = 0; i < indexes_count; ++i) {
      docs_count += fields_per_index[i].size();
    }
    doc_values.reserve(docs_count);

    for (size_t i = 0; i < indexes_count; ++i) {
      std::string_view index_alias =
          (i == 0) ? agg_params.index : agg_params.joins[i - 1].index_alias;

      const auto [shard_id, doc_id] = entry[i];
      const auto& field_values_per_doc_id = shard_keys_data[shard_id][i];

      auto it = field_values_per_doc_id.find(doc_id);
      if (it == field_values_per_doc_id.end()) {
        /* This doc id was joined but not found on the second step. This can happen due to
         * expiration for example. For now, just skip it */
        continue;
      }

      const auto& field_values = it->second;

      for (size_t j = 0; j < fields_per_index[i].size(); ++j) {
        std::string_view field_alias = fields_per_index[i][j];  // tmp alias is identifier
        doc_values.emplace(absl::StrCat(index_alias, "."sv, field_alias), field_values[j]);
      }
    }

    merged_data.push_back(std::move(doc_values));
  }
  return merged_data;
}

auto SortableValueSender(RedisReplyBuilder* rb) {
  return Overloaded{
      [rb](monostate) { rb->SendNull(); },
      [rb](double d) { rb->SendDouble(d); },
      [rb](const string& s) { rb->SendBulkString(s); },
  };
}

void SendSerializedDoc(const SerializedSearchDoc& doc, SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  auto sortable_value_sender = SortableValueSender(rb);

  rb->SendBulkString(doc.key);
  rb->StartCollection(doc.values.size(), RedisReplyBuilder::MAP);
  for (const auto& [k, v] : doc.values) {
    rb->SendBulkString(k);
    visit(sortable_value_sender, v);
  }
}

template <typename T>
void PartialSort(absl::Span<SerializedSearchDoc*> docs, size_t limit, SortOrder order,
                 T SerializedSearchDoc::*field) {
  auto cb = [order, field](SerializedSearchDoc* l, SerializedSearchDoc* r) {
    return order == SortOrder::ASC ? l->*field < r->*field : r->*field < l->*field;
  };
  partial_sort(docs.begin(), docs.begin() + min(limit, docs.size()), docs.end(), cb);
}

void SearchReply(const SearchParams& params,
                 std::optional<search::KnnScoreSortOption> knn_sort_option,
                 absl::Span<SearchResult> results, SinkReplyBuilder* builder) {
  size_t total_hits = 0;
  absl::InlinedVector<SerializedSearchDoc*, 5> docs;
  docs.reserve(results.size());
  for (auto& shard_results : results) {
    total_hits += shard_results.total_hits;
    for (auto& doc : shard_results.docs) {
      docs.push_back(&doc);
    }
  }

  // Reorder and cut KNN results before applying SORT and LIMIT
  optional<string> knn_score_ret_field;
  bool ignore_sort = false;
  if (knn_sort_option) {
    total_hits = min(total_hits, knn_sort_option->limit);
    PartialSort(absl::MakeSpan(docs), total_hits, SortOrder::ASC, &SerializedSearchDoc::knn_score);
    docs.resize(min(docs.size(), knn_sort_option->limit));

    ignore_sort = !params.sort_option || params.sort_option->IsSame(*knn_sort_option);
    if (params.ShouldReturnField(knn_sort_option->score_field_alias))
      knn_score_ret_field = knn_sort_option->score_field_alias;
  }

  // Apply LIMIT
  const size_t offset = std::min(params.limit_offset, docs.size());
  const size_t limit = std::min(docs.size() - offset, params.limit_total);
  const size_t end = offset + limit;

  // Apply SORTBY if its different from the KNN sort
  if (params.sort_option && !ignore_sort)
    PartialSort(absl::MakeSpan(docs), end, params.sort_option->order,
                &SerializedSearchDoc::sort_score);

  const bool reply_with_ids_only = params.IdsOnly();
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  RedisReplyBuilder::ArrayScope scope{rb, reply_with_ids_only ? (limit + 1) : (limit * 2 + 1)};

  rb->SendLong(total_hits);
  for (size_t i = offset; i < end; i++) {
    if (reply_with_ids_only) {
      rb->SendBulkString(docs[i]->key);
      continue;
    }

    if (knn_score_ret_field)
      docs[i]->values[*knn_score_ret_field] = docs[i]->knn_score;

    SendSerializedDoc(*docs[i], builder);
  }
}

// Warms up the query parser to avoid first-call slowness
void WarmupQueryParser() {
  static std::once_flag warmed_up;
  std::call_once(warmed_up, []() {
    search::QueryParams params;
    search::QueryDriver driver{};
    driver.SetParams(&params);
    driver.SetInput(std::string{""});
    (void)search::Parser (&driver)();
  });
}

vector<SearchResult> SearchGlobalHnswIndex(
    const search::AstKnnNode* knn, const shared_ptr<search::HnswVectorIndex>& index,
    const std::string_view index_name,
    const std::optional<search::KnnScoreSortOption>& knn_score_option,
    const std::vector<SearchResult>& sharded_prefilter_docs, const SearchParams& params,
    const CommandContext& cmd_cntx) {
  std::vector<SearchResult> results(1);

  std::optional<std::vector<search::GlobalDocId>> prefilter_global_docs_ids = std::nullopt;

  // Quick lookup to match global id to serialized doc
  std::map<search::GlobalDocId, const SerializedSearchDoc*> prefilter_docs_lookup;

  const bool has_prefilter_docs = knn->HasPreFilter();
  const ShardId shard_size = sharded_prefilter_docs.size();

  // We have pre filter docs so all documents should already be fetched
  if (has_prefilter_docs) {
    std::vector<search::GlobalDocId> global_doc_ids;
    for (size_t shard_id = 0; shard_id < shard_size; shard_id++) {
      for (auto& doc : sharded_prefilter_docs[shard_id].docs) {
        auto global_doc_id = search::CreateGlobalDocId(shard_id, doc.id);
        global_doc_ids.emplace_back(global_doc_id);
        prefilter_docs_lookup[global_doc_id] = &doc;
      }
    }
    prefilter_global_docs_ids = std::move(global_doc_ids);
  }

  // Search HNSW index
  std::vector<std::pair<float, search::GlobalDocId>> knn_results;

  if (prefilter_global_docs_ids)
    knn_results =
        index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime, *prefilter_global_docs_ids);
  else
    knn_results = index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime);

  std::vector<SerializedSearchDoc> knn_search_serialized_docs;
  knn_search_serialized_docs.reserve(knn_results.size());

  // Serialized docs for each shard
  std::vector<std::vector<SerializedSearchDoc>> shard_docs(shard_size);

  for (const auto& [score, global_doc_id] : knn_results) {
    if (has_prefilter_docs) {
      knn_search_serialized_docs.emplace_back(*prefilter_docs_lookup[global_doc_id]);
      knn_search_serialized_docs.back().knn_score = score;
    } else {
      // Create SerializedSearchDoc and fill only knn information
      auto [shard_id, local_doc_id] = search::DecomposeGlobalDocId(global_doc_id);
      SerializedSearchDoc doc;
      doc.id = local_doc_id;
      doc.knn_score = score;
      shard_docs[shard_id].emplace_back(doc);
    }
  }

  // If we have prefilter docs we don't need to fetch docs so can return early
  if (has_prefilter_docs) {
    results[0].total_hits = knn_search_serialized_docs.size();
    results[0].docs = std::move(knn_search_serialized_docs);
    return results;
  }

  // Do we need to set sort score
  bool set_sort_score = params.sort_option && !params.sort_option->IsSame(*knn_score_option);

  // Do we need to remove sort field from response
  bool remove_sort_field = false;

  std::optional<std::vector<FieldReference>> return_fields = params.return_fields;

  // If we don't return all fields
  if (return_fields) {
    // We have sort_option and it's different than knn score
    if (set_sort_score) {
      bool found_sort_return_field = false;
      for (const auto& return_field : *return_fields) {
        if (params.sort_option->field.Name() == return_field.Name()) {
          found_sort_return_field = true;
          break;
        }
      }
      // Sort return field is not found so we need to add it for request and
      // remove this field in response
      if (!found_sort_return_field) {
        (*return_fields).push_back(params.sort_option->field);
        remove_sort_field = true;
      }
    }
  }

  // Indicator if we serialized document on shard
  std::vector<std::vector<bool>> shard_docs_serialized_indicator(shard_size);

  // Fetch all docs from shards
  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(index_name);

    // No index found or no docs on this shard
    if (!index || shard_docs[es->shard_id()].empty()) {
      return OpStatus::OK;
    }

    const auto& schema = index->GetInfo().base_index.schema;

    // Resize shard with default `true` value
    shard_docs_serialized_indicator[es->shard_id()].resize(shard_docs[es->shard_id()].size(), true);

    for (size_t i = 0; i < shard_docs[es->shard_id()].size(); i++) {
      auto& shard_doc = shard_docs[es->shard_id()][i];
      if (auto doc =
              index->SerializeDocWithKey(shard_doc.id, t->GetOpArgs(es), schema, return_fields);
          doc) {
        auto& [key, fields] = *doc;

        // Handle sort_score and remove field if we don't need it
        search::SortableValue sort_score = std::monostate{};
        if (set_sort_score) {
          sort_score = fields[params.sort_option->field.Name()];
          if (remove_sort_field) {
            fields.erase(params.sort_option->field.Name());
          }
        }
        shard_doc.key = std::string{key};
        shard_doc.values = std::move(fields);
        shard_doc.sort_score = sort_score;
      } else {
        // If we couldn't serialize requested doc
        shard_docs_serialized_indicator[es->shard_id()][i] = false;
      }
    }
    return OpStatus::OK;
  });

  // Transform shard results back to
  size_t shard_id = 0;
  std::for_each(shard_docs.begin(), shard_docs.end(),
                [&](const std::vector<SerializedSearchDoc>& shard) {
                  for (size_t doc_index = 0; doc_index < shard.size(); ++doc_index) {
                    // Check if we serialized doc
                    if (shard_docs_serialized_indicator[shard_id][doc_index]) {
                      knn_search_serialized_docs.push_back(shard[doc_index]);
                    }
                  }
                  shard_id++;
                });

  results[0].total_hits = knn_search_serialized_docs.size();
  results[0].docs = std::move(knn_search_serialized_docs);

  return results;
}

}  // namespace

void SearchFamily::FtCreate(CmdArgList args, const CommandContext& cmd_cntx) {
  WarmupQueryParser();

  auto* builder = cmd_cntx.rb;
  if (cmd_cntx.conn_cntx->conn_state.db_index != 0) {
    return builder->SendError("Cannot create index on db != 0"sv);
  }

  CmdArgParser parser{args};
  string_view idx_name = parser.Next();

  bool is_cross_shard = parser.Check("CSS");

  auto parsed_index = CreateDocIndex(&parser);
  if (SendErrorIfOccurred(parsed_index, &parser, builder)) {
    return;
  }

  // Check if index already exists
  atomic_uint exists_cnt = 0;
  cmd_cntx.tx->Execute(
      [idx_name, &exists_cnt](auto* tx, auto* es) {
        if (es->search_indices()->GetIndex(idx_name) != nullptr)
          exists_cnt.fetch_add(1, std::memory_order_relaxed);
        return OpStatus::OK;
      },
      false);

  DCHECK(exists_cnt == 0u || exists_cnt == shard_set->size());

  if (exists_cnt.load(memory_order_relaxed) > 0) {
    cmd_cntx.tx->Conclude();
    return builder->SendError("Index already exists");
  }

  if (absl::GetFlag(FLAGS_cluster_search) && !is_cross_shard && IsClusterEnabled()) {
    std::string args_str = absl::StrJoin(args.subspan(1), " ");
    std::string cmd = absl::StrCat("FT.CREATE ", idx_name, " CSS ", args_str);

    // TODO add processing of the reply to make sure index was created successfully on all shards,
    // and prevent simultaneous creation of the same index.
    cluster::Coordinator::Current().DispatchAll(cmd, [](const facade::RespVec&) {});
  }

  auto idx_ptr = make_shared<DocIndex>(std::move(parsed_index).value());

  for (const auto& [field_ident, field_info] : idx_ptr->schema.fields) {
    if (field_info.type == search::SchemaField::VECTOR &&
        !(field_info.flags & search::SchemaField::NOINDEX)) {
      const auto& vparams = std::get<search::SchemaField::VectorParams>(field_info.special_params);
      if (vparams.use_hnsw &&
          !GlobalHnswIndexRegistry::Instance().Create(idx_name, field_info.short_name, vparams)) {
        cmd_cntx.tx->Conclude();
        return builder->SendError("Index already exists");
      }
    }
  }

  cmd_cntx.tx->Execute(
      [idx_name, idx_ptr](auto* tx, auto* es) {
        es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, idx_ptr);
        if (auto* index = es->search_indices()->GetIndex(idx_name); index) {
          index->RebuildGlobalVectorIndices(idx_name, tx->GetOpArgs(es));
        }
        return OpStatus::OK;
      },
      true);

  builder->SendOk();
}

void SearchFamily::FtAlter(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  string_view idx_name = parser.Next();
  parser.ExpectTag("SCHEMA");
  parser.ExpectTag("ADD");
  auto* builder = cmd_cntx.rb;
  if (auto err = parser.TakeError(); err)
    return builder->SendError(err.MakeReply());

  // First, extract existing index info
  shared_ptr<DocIndex> index_info;
  auto idx_cb = [idx_name, &index_info](auto* tx, EngineShard* es) {
    if (es->shard_id() > 0)  // all shards have the same data, fetch from first
      return OpStatus::OK;

    if (auto* idx = es->search_indices()->GetIndex(idx_name); idx != nullptr)
      index_info = make_shared<DocIndex>(idx->GetInfo().base_index);
    return OpStatus::OK;
  };
  cmd_cntx.tx->Execute(idx_cb, false);

  if (!index_info) {
    cmd_cntx.tx->Conclude();
    return builder->SendError("Index not found");
  }

  // Parse additional schema
  DocIndex new_index{};
  new_index.type = index_info->type;
  auto parse_result = ParseSchema(&parser, &new_index);
  if (SendErrorIfOccurred(parse_result, &parser, builder)) {
    cmd_cntx.tx->Conclude();
    return;
  }

  auto& new_fields = new_index.schema;

  // For logging we copy the whole schema
  // TODO: Use a more efficient way for logging
  LOG(INFO) << "Adding " << DocIndexInfo{.base_index = new_index}.BuildRestoreCommand();

  // Merge schemas
  search::Schema& schema = index_info->schema;
  schema.fields.insert(new_fields.fields.begin(), new_fields.fields.end());
  schema.field_names.insert(new_fields.field_names.begin(), new_fields.field_names.end());

  // Rebuild index
  // TODO: Introduce partial rebuild
  auto upd_cb = [idx_name, index_info](Transaction* tx, EngineShard* es) {
    (void)es->search_indices()->DropIndex(idx_name);
    es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, index_info);
    return OpStatus::OK;
  };
  cmd_cntx.tx->Execute(upd_cb, true);

  builder->SendOk();
}

void SearchFamily::FtDropIndex(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view idx_name = ArgS(args, 0);

  // Parse optional DD (Delete Documents) parameter
  bool delete_docs = args.size() > 1 && absl::EqualsIgnoreCase(args[1], "DD");

  shared_ptr<DocIndex> index_info;
  atomic_uint num_deleted{0};

  auto cb = [&](Transaction* t, EngineShard* es) {
    // Get index info from first shard for global cleanup
    if (es->shard_id() == 0) {
      if (auto* idx = es->search_indices()->GetIndex(idx_name); idx != nullptr) {
        index_info = make_shared<DocIndex>(idx->GetInfo().base_index);
      }
    }
    // Drop the index and get its pointer
    auto index = es->search_indices()->DropIndex(idx_name);
    if (!index)
      return OpStatus::OK;

    num_deleted.fetch_add(1);

    // If DD is set, delete all documents that were in the index
    if (delete_docs) {
      // Get const reference to document keys map (index will be destroyed after this scope)
      const auto& doc_keys = index->key_index().GetDocKeysMap();

      auto op_args = t->GetOpArgs(es);
      auto& db_slice = op_args.GetDbSlice();

      for (const auto& [key, doc_id] : doc_keys) {
        auto it = db_slice.FindMutable(op_args.db_cntx, key).it;
        if (IsValid(it)) {
          db_slice.Del(op_args.db_cntx, it);
        }
      }
    }

    return OpStatus::OK;
  };

  cmd_cntx.tx->Execute(cb, true);

  if (index_info) {
    for (const auto& [field_ident, field_info] : index_info->schema.fields) {
      if (field_info.type == search::SchemaField::VECTOR &&
          !(field_info.flags & search::SchemaField::NOINDEX)) {
        if (GlobalHnswIndexRegistry::Instance().Remove(idx_name, field_info.short_name)) {
          num_deleted.fetch_add(1);
        }
      }
    }
  }

  if (num_deleted == 0u)
    return cmd_cntx.rb->SendError(IndexNotFoundMsg(idx_name));
  return cmd_cntx.rb->SendOk();
}

void SearchFamily::FtInfo(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view idx_name = ArgS(args, 0);

  atomic_uint num_notfound{0};
  vector<DocIndexInfo> infos(shard_set->size());

  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(idx_name);
    if (index == nullptr)
      num_notfound.fetch_add(1);
    else
      infos[es->shard_id()] = index->GetInfo();
    return OpStatus::OK;
  });

  DCHECK(num_notfound == 0u || num_notfound == shard_set->size());
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (num_notfound > 0u)
    return rb->SendError(IndexNotFoundMsg(idx_name));

  DCHECK(infos.front().base_index.schema.fields.size() ==
         infos.back().base_index.schema.fields.size());

  size_t total_num_docs = 0;
  for (const auto& info : infos)
    total_num_docs += info.num_docs;

  const auto& info = infos.front();
  const auto& schema = info.base_index.schema;

  rb->StartCollection(5, RedisReplyBuilder::MAP);

  rb->SendSimpleString("index_name");
  rb->SendSimpleString(idx_name);

  rb->SendSimpleString("index_definition");
  {
    rb->StartCollection(3, RedisReplyBuilder::MAP);
    rb->SendSimpleString("key_type");
    rb->SendSimpleString(info.base_index.type == DocIndex::JSON ? "JSON" : "HASH");
    rb->SendSimpleString("prefixes");
    rb->StartArray(info.base_index.prefixes.size());
    for (const auto& prefix : info.base_index.prefixes) {
      rb->SendBulkString(prefix);
    }
    rb->SendSimpleString("default_score");
    rb->SendLong(1);
  }

  rb->SendSimpleString("index_options");
  rb->SendEmptyArray();

  rb->SendSimpleString("attributes");
  rb->StartArray(schema.fields.size());
  for (const auto& [field_ident, field_info] : schema.fields) {
    vector<string> info;

    string_view base[] = {"identifier"sv, string_view{field_ident},
                          "attribute"sv,  field_info.short_name,
                          "type"sv,       SearchFieldTypeToString(field_info.type)};
    info.insert(info.end(), base, base + ABSL_ARRAYSIZE(base));

    if (field_info.flags & search::SchemaField::NOINDEX)
      info.emplace_back("NOINDEX"sv);

    if (field_info.flags & search::SchemaField::SORTABLE)
      info.emplace_back("SORTABLE"sv);

    if (field_info.type == search::SchemaField::NUMERIC) {
      auto& numeric_params =
          std::get<search::SchemaField::NumericParams>(field_info.special_params);
      info.emplace_back("blocksize"sv);
      info.emplace_back(std::to_string(numeric_params.block_size));
    }

    rb->SendSimpleStrArr(info);
  }

  rb->SendSimpleString("num_docs");
  rb->SendLong(total_num_docs);
}

void SearchFamily::FtList(CmdArgList args, const CommandContext& cmd_cntx) {
  atomic_int first{0};
  vector<string> names;

  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    // Using `first` to assign `names` only once without a race
    if (first.fetch_add(1) == 0)
      names = es->search_indices()->GetIndexNames();
    return OpStatus::OK;
  });
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  rb->SendBulkStrArr(names);
}

static vector<SearchResult> FtSearchCSS(std::string_view idx, std::string_view query,
                                        std::string_view args_str) {
  vector<SearchResult> results;
  std::string cmd = absl::StrCat("FT.SEARCH ", idx, " ", query, " CSS ", args_str);

  // TODO for now we suppose that callback is called synchronously. If not, we need to add
  // synchronization here for results vector modification.
  cluster::Coordinator::Current().DispatchAll(cmd, [&](const facade::RespVec& res) {
    VLOG(3) << "FT.SEARCH CSS reply: " << res;

    if (res.empty()) {
      LOG(ERROR) << "FT.SEARCH CSS reply is empty";
      return;
    }
    if (res[0].type == facade::RespExpr::Type::ERROR) {
      LOG(WARNING) << "FT.SEARCH CSS reply error: " << res[0].GetView();
      return;
    }

    const auto size = res[0].GetInt();
    if (!size.has_value()) {
      LOG(ERROR) << "FT.SEARCH CSS reply unexpected type: " << static_cast<int>(res[0].type);
      return;
    }

    results.emplace_back();
    results.back().total_hits = *size;
    for (size_t i = 2; i < res.size(); i += 2) {
      auto& search_doc = results.back().docs.emplace_back();
      search_doc.key = res[i - 1].GetString();
      if (res[i].type != facade::RespExpr::Type::ARRAY) {
        LOG(ERROR) << "FT.SEARCH CSS reply unexpected type for document data: "
                   << static_cast<int>(res[i].type);
        return;
      }
      const auto& arr_res = res[i].GetVec();
      if (arr_res.size() % 2 != 0) {
        LOG(ERROR) << "FT.SEARCH CSS reply unexpected number of elements for document data: "
                   << arr_res.size();
        return;
      }

      for (size_t j = 0; j < arr_res.size(); j += 2) {
        if (arr_res[j].type != facade::RespExpr::Type::STRING) {
          LOG(ERROR) << "FT.SEARCH CSS reply unexpected type for document data: "
                     << static_cast<int>(arr_res[j].type);
          return;
        }
        if (arr_res[j + 1].type != facade::RespExpr::Type::STRING) {
          LOG(ERROR) << "FT.SEARCH CSS reply unexpected type for document data: "
                     << static_cast<int>(arr_res[j].type);
          return;
        }
        search_doc.values.emplace(arr_res[j].GetString(), arr_res[j + 1].GetString());
      }
    }
  });
  return results;
}

void SearchFamily::FtSearch(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  string_view index_name = parser.Next();
  string_view query_str = parser.Next();

  bool is_cross_shard = parser.Check("CSS");

  auto* builder = cmd_cntx.rb;
  auto params = ParseSearchParams(&parser);
  if (SendErrorIfOccurred(params, &parser, builder))
    return;

  // Check query string length limit
  size_t max_query_bytes = absl::GetFlag(FLAGS_search_query_string_bytes);
  if (query_str.size() > max_query_bytes) {
    return builder->SendError(
        absl::StrCat("Query string is too long, max length is ", max_query_bytes, " bytes"));
  }

  vector<SearchResult> css_docs;
  if (absl::GetFlag(FLAGS_cluster_search) && !is_cross_shard && IsClusterEnabled()) {
    std::string args_str = absl::StrJoin(args.subspan(2), " ");

    css_docs = FtSearchCSS(index_name, query_str, args_str);
  }

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(query_str, &params->query_params, &params->optional_filters))
    return builder->SendError("Query syntax error");

  std::unique_ptr<search::AstNode> knn_node;
  search::AstKnnNode* knn = nullptr;

  if (search_algo.IsKnnQuery()) {
    // Check if it is HNSW node
    if (GlobalHnswIndexRegistry::Instance().Exist(index_name, search_algo.GetKnnNode()->field)) {
      knn_node = search_algo.PopKnnNode();
      knn = std::get_if<search::AstKnnNode>(knn_node.get());
    }
  }

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  atomic<bool> index_not_found{false};
  vector<SearchResult> docs(shard_set->size());

  // If the query does not contain knn component, or it is a hybrid query
  if (!knn || (knn && knn->HasPreFilter())) {
    cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
      if (auto* index = es->search_indices()->GetIndex(index_name); index)
        docs[es->shard_id()] = index->Search(t->GetOpArgs(es), *params, &search_algo);
      else
        index_not_found.store(true, memory_order_relaxed);
      return OpStatus::OK;
    });

    if (index_not_found.load(memory_order_relaxed))
      return builder->SendError(string{index_name} + ": no such index");

    for (const auto& res : docs) {
      if (res.error)
        return builder->SendError(*res.error);
    }
  }

  if (knn_node) {
    auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(index_name, knn->field);
    if (!hnsw_index) {
      return builder->SendError(string{index_name} + ": no such global hnsw index");
    }
    docs = SearchGlobalHnswIndex(knn, hnsw_index, index_name, search_algo.GetKnnScoreSortOption(),
                                 docs, *params, cmd_cntx);
  }

  // TODO add merging of CSS results with local results (SORT, LIMIT, etc)
  docs.insert(docs.end(), std::make_move_iterator(css_docs.begin()),
              std::make_move_iterator(css_docs.end()));

  SearchReply(*params, search_algo.GetKnnScoreSortOption(), absl::MakeSpan(docs), builder);
}

void SearchFamily::FtProfile(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};

  string_view index_name = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (!parser.Check("SEARCH") && !parser.Check("AGGREGATE")) {
    return rb->SendError("no `SEARCH` or `AGGREGATE` provided");
  }

  parser.Check("LIMITED");  // TODO: Implement limited profiling
  parser.ExpectTag("QUERY");

  string_view query_str = parser.Next();

  auto params = ParseSearchParams(&parser);
  if (SendErrorIfOccurred(params, &parser, rb))
    return;

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(query_str, &params->query_params))
    return rb->SendError("query syntax error");

  search_algo.EnableProfiling();

  absl::Time start = absl::Now();
  const size_t shards_count = shard_set->size();

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  std::atomic<bool> index_not_found{false};
  std::vector<SearchResult> search_results(shards_count);
  std::vector<absl::Duration> profile_results(shards_count);

  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(index_name);
    if (!index) {
      index_not_found.store(true, memory_order_relaxed);
      return OpStatus::OK;
    }

    const ShardId shard_id = es->shard_id();

    auto shard_start = absl::Now();
    search_results[shard_id] = index->Search(t->GetOpArgs(es), *params, &search_algo);
    profile_results[shard_id] = {absl::Now() - shard_start};

    return OpStatus::OK;
  });

  if (index_not_found.load())
    return rb->SendError(std::string{index_name} + ": no such index");

  auto took = absl::Now() - start;

  bool result_is_empty = false;
  size_t total_docs = 0;
  size_t total_serialized = 0;
  for (const auto& result : search_results) {
    if (!result.error) {
      total_docs += result.total_hits;
      total_serialized += result.docs.size();
    } else {
      result_is_empty = true;
    }
  }

  // First element -> Result of the search command
  // Second element -> Profile information
  rb->StartArray(2);

  // Result of the search command
  if (!result_is_empty) {
    SearchReply(*params, search_algo.GetKnnScoreSortOption(), absl::MakeSpan(search_results), rb);
  } else {
    rb->StartArray(1);
    rb->SendLong(0);
  }

  // Profile information
  rb->StartArray(shards_count + 1);

  // General stats
  rb->StartCollection(3, RedisReplyBuilder::MAP);
  rb->SendBulkString("took");
  rb->SendLong(absl::ToInt64Microseconds(took));
  rb->SendBulkString("hits");
  rb->SendLong(static_cast<long>(total_docs));
  rb->SendBulkString("serialized");
  rb->SendLong(static_cast<long>(total_serialized));

  // Per-shard stats
  for (size_t shard_id = 0; shard_id < shards_count; shard_id++) {
    rb->StartCollection(2, RedisReplyBuilder::MAP);
    rb->SendBulkString("took");
    rb->SendLong(absl::ToInt64Microseconds(profile_results[shard_id]));
    rb->SendBulkString("tree");

    const auto& search_result = search_results[shard_id];
    if (search_result.error || !search_result.profile || search_result.profile->events.empty()) {
      rb->SendEmptyArray();
      continue;
    }

    const auto& events = search_result.profile->events;
    for (size_t i = 0; i < events.size(); i++) {
      const auto& event = events[i];

      size_t children = 0;
      size_t children_micros = 0;
      for (size_t j = i + 1; j < events.size(); j++) {
        if (events[j].depth == event.depth)
          break;
        if (events[j].depth == event.depth + 1) {
          children++;
          children_micros += events[j].micros;
        }
      }

      rb->StartCollection(4 + (children > 0), RedisReplyBuilder::MAP);
      rb->SendSimpleString("total_time");
      rb->SendLong(event.micros);
      rb->SendSimpleString("operation");
      rb->SendSimpleString(event.descr);
      rb->SendSimpleString("self_time");
      rb->SendLong(event.micros - children_micros);
      rb->SendSimpleString("procecssed");
      rb->SendLong(event.num_processed);

      if (children > 0) {
        rb->SendSimpleString("children");
        rb->StartArray(children);
      }
    }
  }
}

void SearchFamily::FtTagVals(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view index_name = ArgS(args, 0);
  string_view field_name = ArgS(args, 1);
  VLOG(1) << "FtTagVals: " << index_name << " " << field_name;

  vector<io::Result<StringVec, ErrorReply>> shard_results(shard_set->size(), StringVec{});

  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(index_name); index)
      shard_results[es->shard_id()] = index->GetTagVals(field_name);
    else
      shard_results[es->shard_id()] =
          nonstd::make_unexpected(ErrorReply(IndexNotFoundMsg(index_name)));

    return OpStatus::OK;
  });

  absl::flat_hash_set<string> result_set;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  // Check first if either shard had errors. Also merge the results into a single set.
  for (auto& res : shard_results) {
    if (res) {
      result_set.insert(make_move_iterator(res->begin()), make_move_iterator(res->end()));
    } else {
      res.error().kind = facade::kSearchErrType;
      return rb->SendError(res.error());
    }
  }

  shard_results.clear();
  vector<string> vec(result_set.begin(), result_set.end());

  rb->SendBulkStrArr(vec, RedisReplyBuilder::SET);
}

void SearchFamily::FtAggregate(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  auto* builder = cmd_cntx.rb;

  const auto params = ParseAggregatorParams(&parser);
  if (SendErrorIfOccurred(params, &parser, builder))
    return;

  // Check query string length limit
  size_t max_query_bytes = absl::GetFlag(FLAGS_search_query_string_bytes);
  if (params->query.size() > max_query_bytes) {
    return builder->SendError(
        absl::StrCat("Query string is too long, max length is ", max_query_bytes, " bytes"));
  }

  std::vector<aggregate::DocValues> values;

  if (params->joins.empty()) {
    search::SearchAlgorithm search_algo;
    if (!search_algo.Init(params->query, &params->params))
      return builder->SendError("Query syntax error");

    using ResultContainer = decltype(declval<ShardDocIndex>().SearchForAggregator(
        declval<OpArgs>(), params.value(), &search_algo));

    vector<ResultContainer> query_results(shard_set->size());

    cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
      if (auto* index = es->search_indices()->GetIndex(params->index); index) {
        query_results[es->shard_id()] =
            index->SearchForAggregator(t->GetOpArgs(es), params.value(), &search_algo);
      }
      return OpStatus::OK;
    });

    // ResultContainer is absl::flat_hash_map<std::string, search::SortableValue>
    // DocValues is absl::flat_hash_map<std::string_view, SortableValue>
    // Keys of values should point to the keys of the query_results
    size_t total_values = 0;
    for (const auto& sub_results : query_results) {
      total_values += sub_results.size();
    }

    values.reserve(total_values);
    for (auto& sub_results : query_results) {
      for (auto& docs : sub_results) {
        aggregate::DocValues doc_value;
        for (auto& doc : docs) {
          doc_value[doc.first] = std::move(doc.second);
        }
        values.emplace_back(std::move(doc_value));
      }
    }
  } else {
    const size_t indexes_count = params->joins.size() + 1;

    std::vector<search::SearchAlgorithm> search_algos(indexes_count);
    if (!search_algos[0].Init(params->query, &params->params)) {
      return builder->SendError("Query syntax error");
    }

    for (size_t i = 0; i < params->joins.size(); ++i) {
      // Check join query string length limit
      if (params->joins[i].query.size() > max_query_bytes) {
        return builder->SendError(absl::StrCat("Join query string is too long, max length is ",
                                               max_query_bytes, " bytes"));
      }

      search::QueryParams empty_params;
      if (!search_algos[i + 1].Init(params->joins[i].query, &empty_params)) {
        return builder->SendError("Query syntax error in JOIN");
      }
    }

    auto data_for_join = PreprocessDataForJoin(params->index, *params);
    if (!data_for_join) {
      return builder->SendError(data_for_join.error());
    }

    // preaggregated_shard_data is preaggregation results per index per shard
    // preaggregated_shard_data[shard_id][i] is the results of index i on shard shard_id
    using JoinDataVector = join::Vector<join::OwnedEntry>;
    std::vector<std::vector<JoinDataVector>> preaggregated_shard_data(
        shard_set->size(), std::vector<JoinDataVector>(indexes_count));
    cmd_cntx.tx->Execute(
        [&](Transaction* t, EngineShard* es) {
          auto& shard_data = preaggregated_shard_data[es->shard_id()];
          for (size_t i = 0; i < indexes_count; ++i) {
            if (auto* index = es->search_indices()->GetIndex(data_for_join->indexes[i]); index) {
              shard_data[i] = index->PreagregateDataForJoin(
                  t->GetOpArgs(es), data_for_join->needed_fields[i], &search_algos[i]);
            }
          }
          return OpStatus::OK;
        },
        false);

    // Do join
    auto joined_entries = DoJoin(preaggregated_shard_data, *params, *data_for_join);

    // Collect doc_ids per index that were joined
    // Each shard stores set of doc_ids per each index that was joined
    using DocIdsSet = absl::flat_hash_set<search::DocId>;
    std::vector<std::vector<DocIdsSet>> doc_ids_per_shard(shard_set->size(),
                                                          std::vector<DocIdsSet>(indexes_count));
    for (const auto& entry : joined_entries) {
      for (size_t index = 0; index < indexes_count; index++) {
        const auto [shard_id, doc_id] = entry[index];
        doc_ids_per_shard[shard_id][index].insert(doc_id);
      }
    }

    // Load fields for keys that were joined
    std::vector<std::vector<ShardDocIndex::FieldsValuesPerDocId>> shard_keys_data_per_index(
        shard_set->size(), std::vector<ShardDocIndex::FieldsValuesPerDocId>(indexes_count));
    cmd_cntx.tx->Execute(
        [&](Transaction* t, EngineShard* es) {
          const ShardId shard_id = es->shard_id();
          auto& shard_keys_data = shard_keys_data_per_index[shard_id];
          const auto& doc_ids_per_index = doc_ids_per_shard[shard_id];

          for (size_t i = 0; i < indexes_count; ++i) {
            if (auto* index = es->search_indices()->GetIndex(data_for_join->indexes[i]); index) {
              shard_keys_data[i] = index->LoadKeysData(t->GetOpArgs(es), doc_ids_per_index[i],
                                                       data_for_join->fields_to_load_per_index[i]);
            }
          }
          return OpStatus::OK;
        },
        true);

    // Now we have sets of keys that were joined and keys data.
    // We need to build DocValues for each joined set.
    values =
        MergeJoinedKeysWithData(*params, *data_for_join, joined_entries, shard_keys_data_per_index);
  }

  std::vector<std::string_view> load_fields;
  if (params->load_fields) {
    load_fields.reserve(params->load_fields->size());
    for (const auto& field : params->load_fields.value()) {
      load_fields.push_back(field.OutputName());
    }
  }

  auto agg_results = aggregate::Process(std::move(values), load_fields, params->steps);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  auto sortable_value_sender = SortableValueSender(rb);

  const size_t result_size = agg_results.values.size();
  RedisReplyBuilder::ArrayScope scope{rb, result_size + 1};
  rb->SendLong(result_size);

  for (const auto& value : agg_results.values) {
    size_t fields_count = 0;
    for (const auto& field : agg_results.fields_to_print) {
      if (value.find(field) != value.end()) {
        fields_count++;
      }
    }

    rb->StartArray(fields_count * 2);
    for (const auto& field : agg_results.fields_to_print) {
      auto it = value.find(field);
      if (it != value.end()) {
        rb->SendBulkString(field);
        std::visit(sortable_value_sender, it->second);
      }
    }
  }
}

void SearchFamily::FtSynDump(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view index_name = ArgS(args, 0);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  atomic_bool index_not_found{true};
  // Store per-shard synonym data
  vector<absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>> shard_term_groups(
      shard_set->size());

  // Collect synonym data from all shards
  cmd_cntx.tx->Execute(
      [&](Transaction* t, EngineShard* es) {
        auto* index = es->search_indices()->GetIndex(index_name);
        if (!index)
          return OpStatus::OK;

        index_not_found.store(false, std::memory_order_relaxed);

        // Get synonym data from current shard
        const auto& groups = index->GetSynonyms().GetGroups();

        // Build term -> group_ids mapping for this shard
        auto& term_groups = shard_term_groups[es->shard_id()];
        for (const auto& [group_id, group] : groups) {
          for (const auto& term : group) {
            term_groups[term].insert(group_id);
          }
        }

        return OpStatus::OK;
      },
      true);

  if (index_not_found.load(std::memory_order_relaxed))
    return rb->SendError("Unknown index name");

  // Merge data from all shards into a single map
  absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>> merged_term_groups;
  for (auto& shard_groups : shard_term_groups) {
    for (auto& [term, group_ids] : shard_groups) {
      auto& merged_ids = merged_term_groups[term];
      merged_ids.merge(group_ids);
    }
  }

  // Format response according to Redis protocol:
  // Array of term + array of group ids pairs
  rb->StartArray(merged_term_groups.size() * 2);
  for (const auto& [term, group_ids] : merged_term_groups) {
    rb->SendBulkString(term);
    rb->StartArray(group_ids.size());

    // Sort group_ids before sending
    std::vector<std::string> sorted_ids(group_ids.begin(), group_ids.end());
    std::sort(sorted_ids.begin(), sorted_ids.end());

    for (const auto& id : sorted_ids) {
      rb->SendBulkString(id);
    }
  }
}

void FtConfigHelp(CmdArgParser* parser, RedisReplyBuilder* rb) {
  string_view param = parser->Next();

  vector<string> names = config_registry.List(param);
  vector<absl::CommandLineFlag*> res;

  for (const auto& name : names) {
    auto* flag = config_registry.GetFlag(name);
    DCHECK(flag);
    if (flag && flag->Filename().find(kCurrentFile) != std::string::npos) {
      res.push_back(flag);
    }
  }

  rb->StartArray(res.size());
  for (const auto& flag : res) {
    rb->StartArray(5);
    rb->SendBulkString(flag->Name());
    rb->SendBulkString("Description"sv);
    rb->SendBulkString(flag->Help());
    rb->SendBulkString("Value"sv);
    rb->SendBulkString(flag->CurrentValue());
  }
}

void FtConfigGet(CmdArgParser* parser, RedisReplyBuilder* rb) {
  string_view param = parser->Next();
  vector<string> names = config_registry.List(param);

  vector<string> res;

  for (const auto& name : names) {
    auto* flag = config_registry.GetFlag(name);
    DCHECK(flag);
    if (flag && flag->Filename().find(kCurrentFile) != std::string::npos) {
      // Convert internal name (search_query_string_bytes) back to user-facing format
      // (search.query-string-bytes)
      string display_name = DenormalizeConfigName(name);
      res.push_back(display_name);
      res.push_back(flag->CurrentValue());
    }
  }
  return rb->SendBulkStrArr(res, RedisReplyBuilder::MAP);
}

void FtConfigSet(CmdArgParser* parser, RedisReplyBuilder* rb) {
  auto [param, value] = parser->Next<string_view, string_view>();

  if (!parser->Finalize()) {
    rb->SendError(parser->TakeError().MakeReply());
    return;
  }

  vector<string> names = config_registry.List(param);
  if (names.size() != 1 ||
      config_registry.GetFlag(names[0])->Filename().find(kCurrentFile) == std::string::npos) {
    return rb->SendError("Invalid option name");
  }

  ConfigRegistry::SetResult result = config_registry.Set(param, value);

  const char kErrPrefix[] = "FT.CONFIG SET failed (possibly related to argument '";
  switch (result) {
    case ConfigRegistry::SetResult::OK:
      return rb->SendOk();
    case ConfigRegistry::SetResult::UNKNOWN:
      return rb->SendError(
          absl::StrCat("Unknown option or number of arguments for CONFIG SET - '", param, "'"),
          kConfigErrType);

    case ConfigRegistry::SetResult::READONLY:
      return rb->SendError(absl::StrCat(kErrPrefix, param, "') - can't set immutable config"),
                           kConfigErrType);

    case ConfigRegistry::SetResult::INVALID:
      return rb->SendError(absl::StrCat(kErrPrefix, param, "') - argument can not be set"),
                           kConfigErrType);
  }
  ABSL_UNREACHABLE();
}

void SearchFamily::FtConfig(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  auto func = parser.MapNext("GET", &FtConfigGet, "SET", &FtConfigSet, "HELP", &FtConfigHelp);

  if (auto err = parser.TakeError(); err) {
    rb->SendError("Unknown subcommand");
    return;
  }
  func(&parser, rb);
}

void SearchFamily::FtSynUpdate(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};
  auto [index_name, group_id] = parser.Next<string_view, string>();

  // Redis ignores this parameter. Checked on redis_version:6.2.13
  [[maybe_unused]] bool skip_initial_scan = parser.Check("SKIPINITIALSCAN");

  // Collect terms
  std::vector<std::string_view> terms;
  while (parser.HasNext()) {
    terms.emplace_back(parser.Next());
  }

  if (terms.empty()) {
    return cmd_cntx.rb->SendError("No terms specified");
  }

  if (!parser.Finalize()) {
    return cmd_cntx.rb->SendError(parser.TakeError().MakeReply());
  }

  std::atomic_bool index_not_found{true};

  // Update synonym groups in all shards
  cmd_cntx.tx->Execute(
      [&](Transaction* t, EngineShard* es) {
        auto* index = es->search_indices()->GetIndex(index_name);
        if (!index)
          return OpStatus::OK;

        index_not_found.store(false, std::memory_order_relaxed);

        // Rebuild indices only for documents containing terms from the updated group
        index->RebuildForGroup(
            OpArgs{es, nullptr,
                   DbContext{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()}},
            group_id, terms);

        return OpStatus::OK;
      },
      true);

  if (index_not_found.load(std::memory_order_relaxed))
    return cmd_cntx.rb->SendError(string{index_name} + ": no such index");

  cmd_cntx.rb->SendOk();
}

void SearchFamily::FtDebug(CmdArgList args, const CommandContext& cmd_cntx) {
  // FT._DEBUG command stub for test compatibility
  // This command is used by integration tests to control internal behavior
  CmdArgParser parser{args};
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (args.empty() || parser.Check("HELP")) {
    rb->SendSimpleString("FT._DEBUG - Debug command stub (not fully implemented)");
    return;
  }

  // Handle CONTROLLED_VARIABLE subcommand used by tests
  if (parser.Check("CONTROLLED_VARIABLE")) {
    if (parser.Check("SET")) {
      // Consume variable name and value - these are required by the command
      parser.Next();  // variable name
      parser.Next();  // variable value

      if (auto err = parser.TakeError(); err) {
        return rb->SendError(err.MakeReply());
      }

      // Just acknowledge the command
      rb->SendOk();
      return;
    }
  }

  // For any other subcommand, just return OK
  rb->SendOk();
}

#define HFUNC(x) SetHandler(&SearchFamily::x)

// Redis search is a module. Therefore we introduce dragonfly extension search
// to set as the default for the search family of commands. More sensible defaults,
// should also be considered in the future

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  // Disable journaling, because no-key-transactional enables it by default
  const uint32_t kReadOnlyMask =
      CO::NO_KEY_TRANSACTIONAL | CO::NO_KEY_TX_SPAN_ALL | CO::NO_AUTOJOURNAL | CO::IDEMPOTENT;

  registry->StartFamily();
  *registry
      << CI{"FT.CREATE", CO::JOURNALED | CO::GLOBAL_TRANS, -2, 0, 0, acl::FT_SEARCH}.HFUNC(FtCreate)
      << CI{"FT.ALTER", CO::JOURNALED | CO::GLOBAL_TRANS, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtAlter)
      << CI{"FT.DROPINDEX", CO::JOURNALED | CO::GLOBAL_TRANS, -2, 0, 0, acl::FT_SEARCH}.HFUNC(
             FtDropIndex)
      << CI{"FT.INFO", kReadOnlyMask, -2, 0, 0, acl::FT_SEARCH}.HFUNC(FtInfo)
      << CI{"FT.CONFIG", CO::ADMIN | CO::LOADING | CO::DANGEROUS, -3, 0, 0, acl::FT_SEARCH}.HFUNC(
             FtConfig)
      // Underscore same as in RediSearch because it's "temporary" (long time already)
      << CI{"FT._LIST", kReadOnlyMask, 1, 0, 0, acl::FT_SEARCH}.HFUNC(FtList)
      << CI{"FT.SEARCH", kReadOnlyMask, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtSearch)
      << CI{"FT.AGGREGATE", kReadOnlyMask, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtAggregate)
      << CI{"FT.PROFILE", kReadOnlyMask, -4, 0, 0, acl::FT_SEARCH}.HFUNC(FtProfile)
      << CI{"FT.TAGVALS", kReadOnlyMask, 3, 0, 0, acl::FT_SEARCH}.HFUNC(FtTagVals)
      << CI{"FT.SYNDUMP", kReadOnlyMask, 2, 0, 0, acl::FT_SEARCH}.HFUNC(FtSynDump)
      << CI{"FT.SYNUPDATE", CO::JOURNALED | CO::GLOBAL_TRANS, -4, 0, 0, acl::FT_SEARCH}.HFUNC(
             FtSynUpdate)
      << CI{"FT._DEBUG", kReadOnlyMask, -1, 0, 0, acl::FT_SEARCH}.HFUNC(FtDebug);
}

void SearchFamily::Shutdown() {
  GlobalHnswIndexRegistry::Instance().Reset();
}

}  // namespace dfly
