// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/flat_hash_map.h>
#include <absl/flags/flag.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <exception>
#include <functional>
#include <ranges>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/search/indices.h"
#include "core/search/query_driver.h"
#include "core/search/scoring.h"
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
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/namespaces.h"
#include "server/search/aggregator.h"
#include "server/search/doc_index.h"
#include "server/search/global_hnsw_index.h"
#include "server/transaction.h"
#include "src/core/overloaded.h"

namespace rng = std::ranges;

ABSL_FLAG(bool, search_reject_legacy_field, true, "FT.AGGREGATE: Reject legacy field names.");
ABSL_FLAG(bool, cluster_search, false,
          "Enable search commands for cross-shard search. turned off by default for safety.");

ABSL_FLAG(size_t, MAXSEARCHRESULTS, 1000000, "Maximum number of results from ft.search command");

ABSL_FLAG(size_t, search_query_string_bytes, 10240,
          "Maximum number of bytes in search query string");

ABSL_FLAG(size_t, subset_knn_search_threshold, 8192,
          "If prefilter results are below this threshold, we will do exact subset search "
          "instead of HNSW graph search");

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
                         CommandContext* cmd_cntx) {
  if (auto err = parser->TakeError(); err || !result) {
    cmd_cntx->SendError(!result ? result.error() : err.MakeReply());
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
    } else if (parser->Check("TYPE")) {
      params.data_type = absl::AsciiStrToUpper(parser->Next<string_view>());
    } else if (parser->Check("EF_RUNTIME")) {
      params.hnsw_ef_runtime = parser->Next<uint32_t>();
    } else if (parser->Check("EPSILON")) {
      double epsilon = parser->Next<double>("Invalid EPSILON value");
      if (!params.use_hnsw) {
        parser->ReportCustom("EPSILON is supported only for HNSW vector indexes");
      } else if (!search::SchemaField::VectorParams::IsValidSchemaHnswEpsilon(epsilon)) {
        parser->ReportCustom("Invalid EPSILON value");
      } else {
        params.hnsw_epsilon =
            search::SchemaField::VectorParams::NormalizeSchemaHnswEpsilon(epsilon);
      }
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

ParseResult<std::string> ParseLanguageArg(CmdArgParser* parser) {
  std::string lang = absl::AsciiStrToLower(parser->Next<std::string_view>());
  if (!search::Stemmer::TryCreate(lang)) {
    return CreateSyntaxError(absl::StrCat("Unsupported language: "sv, lang));
  }
  return lang;
}

ParseResult<search::SchemaField::TextParams> ParseTextParams(CmdArgParser* parser) {
  search::SchemaField::TextParams params{};
  while (parser->HasNext()) {
    if (parser->Check("WITHSUFFIXTRIE")) {
      params.with_suffixtrie = true;
      continue;
    }
    if (parser->Check("NOSTEM")) {
      params.no_stem = true;
      continue;
    }
    break;
  }
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

  // Validate that the initial allocation (capacity * (dim+1) floats) cannot
  // overflow size_t or request an unreasonable amount of memory.  Without this
  // check FlatVectorIndex::FlatVectorIndex() would throw std::bad_alloc,
  // leaving a half-initialised index registered in ShardDocIndices.
  static constexpr size_t kMaxFlatBufEntries = size_t{1} << 30;  // ~4 GiB of floats
  if (vector_params.dim >= kMaxFlatBufEntries) {
    return CreateSyntaxError("Vector index initial allocation is too large"sv);
  }
  size_t dim_plus1 = vector_params.dim + 1;
  if (vector_params.capacity > kMaxFlatBufEntries / dim_plus1) {
    return CreateSyntaxError("Vector index initial allocation is too large"sv);
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
  size_t i = 0;
  for (; i < count && parser->HasNext(); i++) {
    index->prefixes.push_back(parser->Next<std::string>());
  }
  // If fewer prefixes were consumed than promised, trigger an out-of-bounds error.
  // This prevents unbounded loops for huge user-supplied counts while
  // preserving the existing syntax-error behavior for mismatched counts.
  if (i < count) {
    parser->Next();  // triggers OUT_OF_BOUNDS
  }
  return true;
}

ParseResult<bool> ParseIndexLanguage(CmdArgParser* parser, DocIndex* index) {
  auto lang = ParseLanguageArg(parser);
  if (!lang)
    return make_unexpected(lang.error());
  index->schema.default_language = std::move(lang).value();
  return true;
}

ParseResult<bool> ParseIndexLanguageField(CmdArgParser* parser, DocIndex* index) {
  index->schema.language_field = std::string{parser->Next<std::string_view>()};
  return true;
}

// NOOFFSETS (index-level): disable storing token positions in TEXT posting lists.
// Saves memory but breaks phrase queries.
ParseResult<bool> ParseNoOffsets(CmdArgParser* /*parser*/, DocIndex* index) {
  index->options.no_offsets = true;
  return true;
}

// STOPWORDS count [words...]
ParseResult<bool> ParseStopwords(CmdArgParser* parser, DocIndex* index) {
  index->options.stopwords.clear();
  index->options.custom_stopwords = true;
  size_t count = parser->Next<size_t>();
  size_t i = 0;
  for (; i < count && parser->HasNext(); i++) {
    index->options.stopwords.emplace(parser->Next());
  }
  if (i < count) {
    parser->Next();  // triggers OUT_OF_BOUNDS
  }
  return true;
}

constexpr std::array<const std::string_view, 3> kIgnoredOptions = {"UNF"sv, "INDEXMISSING"sv,
                                                                   "INDEXEMPTY"sv};
constexpr std::array<const std::string_view, 3> kIgnoredOptionsWithArg = {"WEIGHT"sv, "PHONETIC"sv};

// SCHEMA field [AS alias] type [flags...]
ParseResult<bool> ParseSchema(CmdArgParser* parser, DocIndex* index) {
  auto& schema = index->schema;

  if (!parser->HasNext()) {
    return CreateSyntaxError("Fields arguments are missing"sv);
  }

  while (parser->HasNext()) {
    string_view field_token = parser->Next();
    string field_path_storage;
    string_view field_path = field_token;
    string_view field_alias = field_token;

    // On JSON indexes, a bare identifier (no leading $) expands to $.identifier,
    // matching the Valkey-search relaxation of the JSONPath requirement.
    if (index->type == DocIndex::JSON && !field_token.empty() && field_token[0] != '$') {
      field_path_storage = absl::StrCat("$.", field_token);
      field_path = field_path_storage;
    }

    if (index->type == DocIndex::JSON && !IsValidJsonPath(field_path)) {
      return CreateSyntaxError(absl::StrCat("Bad json path: "sv, field_path));
    }

    // AS [alias]
    parser->Check("AS", &field_alias);

    if (schema.field_names.contains(field_alias)) {
      return CreateSyntaxError(absl::StrCat("Duplicate field in schema - "sv, field_alias));
    }
    // Same identifier may appear twice on HASH (e.g. one TAG spec + one TEXT spec with
    // different aliases). On JSON, a bare identifier expands to $.<id> and would silently
    // shadow an earlier $.<id> spec; reject that collision instead of last-write-wins.
    if (index->type == DocIndex::JSON && schema.fields.contains(field_path)) {
      return CreateSyntaxError(absl::StrCat("Duplicate field in schema - "sv, field_path));
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
        if (field_type == search::SchemaField::TEXT && option == "NOSTEM"sv) {
          std::get<search::SchemaField::TextParams>(params).no_stem = true;
          parser->Skip(1);
          continue;
        }
        if (rng::find(kIgnoredOptions, option) != kIgnoredOptions.end()) {
          LOG_IF(WARNING, option != "INDEXMISSING"sv && option != "INDEXEMPTY"sv)
              << "Ignoring unsupported field option in FT.CREATE: " << option;
          // Ignore these options
          parser->Skip(1);
          continue;
        }
        if (rng::find(kIgnoredOptionsWithArg, option) != kIgnoredOptionsWithArg.end()) {
          LOG(WARNING) << "Ignoring unsupported field option in FT.CREATE: " << option;
          // Ignore these options with argument
          parser->Skip(2);
          continue;
        }
        break;
      }

      flags |= *flag;
    }

    schema.fields[field_path] = {field_type, flags, string{field_alias}, params};
    schema.field_names[field_alias] = field_path;
  }

  return false;
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

ParseResult<DocIndex> CreateDocIndex(std::string_view name, CmdArgParser* parser) {
  DocIndex index{};
  index.name = name;

  while (parser->HasNext()) {
    auto option_parser = parser->TryMapNext(
        "ON"sv, &ParseOnOption, "PREFIX"sv, &ParsePrefix, "STOPWORDS"sv, &ParseStopwords,
        "LANGUAGE"sv, &ParseIndexLanguage, "LANGUAGE_FIELD"sv, &ParseIndexLanguageField,
        "NOOFFSETS"sv, &ParseNoOffsets, "SCHEMA"sv, &ParseSchema);

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
  // Count is the number of tokens that follow; a field-spec is 1 or 3 tokens.
  std::vector<FieldReference> fields;
  size_t tokens_left = parser->Next<size_t>();

  while (parser->HasNext() && tokens_left > 0) {
    string_view field = is_load ? ParseField(parser) : parser->Next();
    --tokens_left;

    string_view alias;
    if (tokens_left >= 2 && parser->Check("AS", &alias)) {
      tokens_left -= 2;
    }
    fields.emplace_back(field, alias);
  }

  if (parser->HasNext() && absl::EqualsIgnoreCase(parser->Peek(), "AS")) {
    parser->ReportCustom("Unexpected parameter `AS`");
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

std::optional<search::ScorerSpec> ParseScorer(CmdArgParser* parser) {
  auto kind = parser->TryMapNext(
      "BM25STD", search::ScorerKind::BM25STD, "BM25STD.NORM", search::ScorerKind::BM25STD_NORM,
      "BM25STD.TANH", search::ScorerKind::BM25STD_TANH, "TFIDF", search::ScorerKind::TFIDF,
      "TFIDF.DOCNORM", search::ScorerKind::TFIDF_DOCNORM);
  if (!kind)
    return nullopt;
  return search::ScorerSpec{*kind};
}

constexpr string_view kBM25StdTanhFactorErr = "BM25STD_TANH_FACTOR must be a positive integer";

// Parses a BM25STD_TANH_FACTOR value: an integer >= 1 (no upper bound). On a missing or invalid
// value the parser reports kBM25StdTanhFactorErr, surfaced by the caller's error handling.
uint64_t ParseBM25StdTanhFactor(CmdArgParser* parser) {
  using TanhFactorInt = facade::FInt<uint64_t{1}, std::numeric_limits<uint64_t>::max()>;
  return parser->Next<TanhFactorInt>(kBM25StdTanhFactorErr);
}

ParseResult<SearchParams> ParseSearchParams(CmdArgParser* parser) {
  SearchParams params;
  uint64_t tanh_factor = search::kDefaultBM25StdTanhFactor;

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
    } else if (parser->Check("WITHSORTKEYS")) {
      params.with_sortkeys = true;
    } else if (parser->Check("WITHSCORES")) {
      params.with_scores = true;
    } else if (parser->Check("SCORER")) {
      auto scorer = ParseScorer(parser);
      if (!scorer)
        return CreateSyntaxError(absl::StrCat("No such scorer: ", parser->Peek()));
      params.scorer = *scorer;
    } else if (parser->Check("BM25STD_TANH_FACTOR")) {
      tanh_factor = ParseBM25StdTanhFactor(parser);
    } else if (parser->Check("DIALECT")) {
      parser->Skip(1);  // Accepted and ignored — DF always behaves as dialect 2
    } else {
      // Unsupported parameters are ignored for now
      parser->Skip(1);
    }
  }

  params.limit_total = std::min(params.limit_total, max_results);
  if (params.scorer)
    params.scorer->bm25std_tanh_factor = tanh_factor;

  return params;
}

ParseResult<aggregate::SortParams> ParseAggregatorSortParams(CmdArgParser* parser) {
  size_t strings_num = parser->Next<size_t>();

  if (!parser->HasError() && !parser->HasAtLeast(strings_num)) {
    return CreateSyntaxError("bad arguments for SORTBY: specified invalid number of strings"sv);
  }

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
  if (!parser->HasAtLeast(num_fields))
    return CreateSyntaxError("bad arguments for LOAD_FROM: not enough arguments"sv);
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
  uint64_t tanh_factor = search::kDefaultBM25StdTanhFactor;
  tie(params.index, params.query) = parser->Next<string_view, string_view>();

  // Used for join params
  absl::flat_hash_set<std::string> current_known_indexes;
  current_known_indexes.insert(std::string{params.index});

  // LOAD/LOAD_FROM are rejected only after a projector or reducer step, not after
  // query-level keywords such as SCORER/ADDSCORES.
  bool has_pipeline_step = false;

  while (parser->HasNext()) {
    // LOAD count field [field ...]
    if (parser->Check("LOAD")) {
      if (has_pipeline_step)
        return CreateSyntaxError("LOAD cannot be applied after projectors or reducers"sv);
      auto fields = ParseLoadOrReturnFields(parser, true);
      if (!params.load_fields.has_value())
        params.load_fields = std::move(fields);
      else
        params.load_fields->insert(params.load_fields->end(), make_move_iterator(fields.begin()),
                                   make_move_iterator(fields.end()));
      continue;
    }

    if (parser->Check("LOAD_FROM")) {
      if (has_pipeline_step)
        return CreateSyntaxError("LOAD_FROM cannot be applied after projectors or reducers"sv);
      auto join_params = ParseAggregatorJoinParams(parser, &current_known_indexes);
      if (!join_params)
        return make_unexpected(join_params.error());
      params.joins.emplace_back(std::move(join_params).value());
      continue;
    }

    // GROUPBY nargs property [property ...]
    if (parser->Check("GROUPBY")) {
      has_pipeline_step = true;
      size_t num_fields = parser->Next<size_t>();

      std::vector<std::string> fields;
      if (parser->HasAtLeast(num_fields))
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
      has_pipeline_step = true;
      auto sort_params = ParseAggregatorSortParams(parser);
      if (!sort_params) {
        return make_unexpected(sort_params.error());  // Propagate the specific error
      }

      if (params.joins.empty() || params.join_agg_params.HasValue()) {
        params.steps.push_back(aggregate::MakeSortStep(std::move(sort_params).value()));
      } else {
        params.join_agg_params.sort = std::move(sort_params).value();
      }
      continue;
    }

    // LIMIT
    if (parser->Check("LIMIT")) {
      has_pipeline_step = true;
      auto [offset, num] = parser->Next<size_t, size_t>();
      if (params.joins.empty() || params.join_agg_params.HasLimit()) {
        params.steps.push_back(aggregate::MakeLimitStep(offset, num));
      } else {
        params.join_agg_params.limit_offset = offset;
        params.join_agg_params.limit_total = num;
      }
      continue;
    }

    // FILTER "expr"
    if (parser->Check("FILTER")) {
      has_pipeline_step = true;
      std::string filter_expr{parser->Next<std::string_view>()};
      auto step_or_err = aggregate::MakeFilterStep(filter_expr);
      if (std::holds_alternative<std::string>(step_or_err)) {
        return CreateSyntaxError(
            absl::StrCat("FILTER expression error: ", std::get<std::string>(step_or_err)));
      }
      params.steps.push_back(std::move(std::get<aggregate::AggregationStep>(step_or_err)));
      continue;
    }

    // PARAMS
    if (parser->Check("PARAMS")) {
      params.params = ParseQueryParams(parser);
      continue;
    }

    if (parser->Check("BM25STD_TANH_FACTOR")) {
      tanh_factor = ParseBM25StdTanhFactor(parser);
      continue;
    }

    // DIALECT (accepted and ignored — DF always behaves as dialect 2)
    if (parser->Check("DIALECT")) {
      parser->Skip(1);
      continue;
    }

    // APPLY "expr" AS alias
    if (parser->Check("APPLY")) {
      has_pipeline_step = true;
      string expr{parser->Next<string_view>()};
      parser->ExpectTag("AS");
      string alias = parser->Next<string>();
      auto step_or_err = aggregate::MakeApplyStep(expr, std::move(alias));
      if (std::holds_alternative<std::string>(step_or_err)) {
        return CreateSyntaxError(
            absl::StrCat("APPLY expression error: ", std::get<std::string>(step_or_err)));
      }
      params.steps.push_back(std::move(std::get<aggregate::AggregationStep>(step_or_err)));
      continue;
    }

    // SCORER, ADDSCORES, WITHSCORES can appear anywhere in the command
    if (parser->Check("SCORER")) {
      auto scorer = ParseScorer(parser);
      if (!scorer)
        return CreateSyntaxError(absl::StrCat("No such scorer: ", parser->Peek()));
      params.scorer = *scorer;
      continue;
    }

    if (parser->Check("ADDSCORES")) {
      params.add_scores = true;
      continue;
    }

    if (parser->Check("WITHSCORES")) {
      // Silently ignored for FT.AGGREGATE (use ADDSCORES instead)
      continue;
    }

    return CreateSyntaxError(absl::StrCat("Unknown clause: ", parser->Peek()));
  }

  if (params.scorer)
    params.scorer->bm25std_tanh_factor = tanh_factor;

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
        rng::sort(*joined_entries, std::move(comparator));
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

  rb->StartCollection(doc.values.size(), CollectionType::MAP);
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

// Global SORTBY merge across shards. ValueCmp (std::less/std::greater) selects the direction for
// present values; docs missing the field hold a monostate sort_score and always rank last,
// independent of direction, so that rule wraps ValueCmp (a plain variant comparison would sort
// monostate first in ASC). ranks_before(l, r) is true when l should be ordered before r.
void PartialSortBySortScore(absl::Span<SerializedSearchDoc*> docs, size_t limit, SortOrder order) {
  auto run = [&](auto value_cmp) {
    auto ranks_before = [value_cmp](SerializedSearchDoc* l, SerializedSearchDoc* r) {
      bool l_missing = std::holds_alternative<std::monostate>(l->sort_score);
      bool r_missing = std::holds_alternative<std::monostate>(r->sort_score);
      if (l_missing != r_missing)
        return !l_missing;  // present ranks before missing
      return value_cmp(l->sort_score, r->sort_score);
    };
    partial_sort(docs.begin(), docs.begin() + min(limit, docs.size()), docs.end(), ranks_before);
  };
  if (order == SortOrder::ASC)
    run(std::less<>{});
  else
    run(std::greater<>{});
}

bool IsBM25StdNorm(const std::optional<search::ScorerSpec>& scorer) {
  return scorer && scorer->kind == search::ScorerKind::BM25STD_NORM;
}

// Global max raw text score across per-shard results, used to normalize BM25STD.NORM by the
// max over the full matched set. Returns 0 when there are no scored docs.
float GlobalMaxTextScore(absl::Span<const SearchResult> results) {
  auto max_it = std::ranges::max_element(
      results, [](const auto& a, const auto& b) { return a.max_text_score < b.max_text_score; });
  return max_it == results.end() ? 0.0f : max_it->max_text_score;
}

void SearchReply(const SearchParams& params,
                 std::optional<search::KnnScoreSortOption> knn_sort_option,
                 std::string_view inject_score_alias, absl::Span<SearchResult> results,
                 SinkReplyBuilder* builder, bool is_css) {
  size_t total_hits = 0;
  absl::InlinedVector<SerializedSearchDoc*, 5> docs;
  docs.reserve(results.size());
  for (auto& shard_results : results) {
    total_hits += shard_results.total_hits;
    for (auto& doc : shard_results.docs) {
      docs.push_back(&doc);
    }
  }

  if (IsBM25StdNorm(params.scorer)) {
    if (float max_text_score = GlobalMaxTextScore(results); max_text_score > 0) {
      for (auto* doc : docs)
        doc->text_score /= max_text_score;
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
    if (!knn_sort_option->score_field_alias.empty() &&
        params.ShouldReturnField(knn_sort_option->score_field_alias))
      knn_score_ret_field = knn_sort_option->score_field_alias;
  } else if (!inject_score_alias.empty() && params.ShouldReturnField(inject_score_alias)) {
    // FLAT VECTOR_RANGE without distance-based SORTBY: expose the alias without
    // forcing a global reorder. Matches Redis Stack default ordering.
    knn_score_ret_field = string{inject_score_alias};
  }

  // Apply LIMIT
  size_t offset = 0;
  size_t limit = 0;
  if (is_css) {
    limit = std::min(docs.size(), params.limit_total + params.limit_offset);
  } else {
    offset = std::min(params.limit_offset, docs.size());
    limit = std::min(docs.size() - offset, params.limit_total);
  }
  const size_t end = limit + offset;

  // Re-sort union of per-shard tops by (score, key) so LIMIT picks global top-K.
  const bool scoring_active = params.scorer || params.with_scores;
  const bool needs_text_score_sort =
      scoring_active && !knn_sort_option && (!params.sort_option || ignore_sort);
  if (needs_text_score_sort) {
    auto by_score_then_key = [](SerializedSearchDoc* l, SerializedSearchDoc* r) {
      if (l->text_score != r->text_score)
        return l->text_score > r->text_score;
      return l->key < r->key;
    };
    partial_sort(docs.begin(), docs.begin() + std::min(end, docs.size()), docs.end(),
                 by_score_then_key);
  }

  // Apply SORTBY if its different from the KNN sort
  if (params.sort_option && !ignore_sort)
    PartialSortBySortScore(absl::MakeSpan(docs), end, params.sort_option->order);

  const bool reply_with_ids_only = params.IdsOnly();
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  const size_t items_per_field =
      (reply_with_ids_only ? 1 : 2) + params.with_sortkeys + params.with_scores;
  RedisReplyBuilder::ArrayScope scope{rb, limit * items_per_field + 1};

  Overloaded sortable_value_sender{
      [rb](monostate) { rb->SendNull(); },
      [rb](double d) { rb->SendBulkString(absl::StrCat("#", d)); },
      [rb](const string& s) { rb->SendBulkString("$" + s); },
  };

  rb->SendLong(total_hits);
  for (size_t i = offset; i < end; i++) {
    rb->SendBulkString(docs[i]->key);
    if (params.with_scores) {
      rb->SendBulkString(absl::StrCat(docs[i]->text_score));
    }
    if (params.with_sortkeys) {
      visit(sortable_value_sender, docs[i]->sort_score);
    }

    if (!reply_with_ids_only) {
      if (knn_score_ret_field)
        docs[i]->values[*knn_score_ret_field] = docs[i]->knn_score;

      SendSerializedDoc(*docs[i], builder);
    }
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

struct HnswLoadOptions {
  bool set_sort_score = false;
  bool remove_sort_field = false;
  std::string sort_score_field;
  std::optional<std::vector<FieldReference>> return_fields;
};

HnswLoadOptions PrepareHnswLoadOptions(const SearchParams& params,
                                       std::optional<search::KnnScoreSortOption> knn_score_option) {
  HnswLoadOptions options;
  options.return_fields = params.return_fields;

  options.set_sort_score =
      params.sort_option && (!knn_score_option || !params.sort_option->IsSame(*knn_score_option));
  if (!options.set_sort_score)
    return options;

  auto sort_field = params.sort_option->field.Name();
  options.sort_score_field = sort_field;

  if (!options.return_fields)
    return options;

  auto sort_return_field = rng::find_if(
      *options.return_fields,
      [sort_field](const FieldReference& field) { return field.Name() == sort_field; });

  if (sort_return_field == options.return_fields->end()) {
    options.return_fields->push_back(params.sort_option->field);
    options.remove_sort_field = true;
  } else {
    options.sort_score_field = sort_return_field->OutputName();
  }
  return options;
}

vector<SearchResult> LoadHnswSearchDocs(
    const std::string_view index_name,
    const std::optional<search::KnnScoreSortOption>& knn_score_option, const SearchParams& params,
    const CommandContext& cmd_cntx, std::vector<std::vector<SerializedSearchDoc>> shard_docs,
    bool finalize) {
  std::vector<SearchResult> results(1);
  const ShardId shard_size = shard_docs.size();

  std::vector<SerializedSearchDoc> knn_search_serialized_docs;
  size_t docs_count = 0;
  for (const auto& shard : shard_docs)
    docs_count += shard.size();
  knn_search_serialized_docs.reserve(docs_count);

  // Indicator if we serialized document on shard
  std::vector<std::vector<bool>> shard_docs_serialized_indicator(shard_size);

  // Fetch all docs from shards
  auto cb = [&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(index_name);

    // No index found or no docs on this shard
    if (!index || shard_docs[es->shard_id()].empty()) {
      return OpStatus::OK;
    }

    const auto& schema = index->base().schema;
    auto load_options = PrepareHnswLoadOptions(params, knn_score_option);
    const bool ids_only_without_sort_load = params.IdsOnly() && !load_options.set_sort_score;

    // Resize shard with default `true` value
    shard_docs_serialized_indicator[es->shard_id()].resize(shard_docs[es->shard_id()].size(), true);

    for (size_t i = 0; i < shard_docs[es->shard_id()].size(); i++) {
      auto& shard_doc = shard_docs[es->shard_id()][i];

      if (ids_only_without_sort_load) {
        const auto& keys = index->key_index();
        if (keys.IsValid(shard_doc.id)) {
          shard_doc.key = std::string{keys.Get(shard_doc.id)};
        } else {
          shard_docs_serialized_indicator[es->shard_id()][i] = false;
        }
        continue;
      }

      if (auto doc = index->SerializeDocWithKey(shard_doc.id, t->GetOpArgs(es), schema,
                                                load_options.return_fields);
          doc) {
        auto& [key, fields] = *doc;

        // Handle sort_score and remove field if we don't need it
        search::SortableValue sort_score = std::monostate{};
        if (load_options.set_sort_score) {
          if (auto it = fields.find(load_options.sort_score_field); it != fields.end())
            sort_score = it->second;
          if (load_options.remove_sort_field) {
            fields.erase(load_options.sort_score_field);
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
  };
  if (finalize)
    cmd_cntx.tx()->Execute(std::move(cb), true);
  else
    cmd_cntx.tx()->ScheduleSingleHop(std::move(cb));

  // Transform shard results back to a flat list of serialized docs. A shard whose index went
  // missing during the hop (e.g. dropped between hops) leaves its indicator empty, not resized.
  size_t shard_id = 0;
  std::for_each(shard_docs.begin(), shard_docs.end(),
                [&](const std::vector<SerializedSearchDoc>& shard) {
                  const auto& serialized = shard_docs_serialized_indicator[shard_id];
                  for (size_t doc_index = 0; doc_index < shard.size(); ++doc_index) {
                    if (doc_index < serialized.size() && serialized[doc_index])
                      knn_search_serialized_docs.push_back(shard[doc_index]);
                  }
                  shard_id++;
                });

  results[0].total_hits = knn_search_serialized_docs.size();
  for (const auto& doc : knn_search_serialized_docs)
    results[0].max_text_score = std::max(results[0].max_text_score, doc.text_score);
  results[0].docs = std::move(knn_search_serialized_docs);

  return results;
}

std::vector<std::pair<float, search::GlobalDocId>> SearchHnswWithPrefilter(
    const search::AstKnnNode* knn, const shared_ptr<search::HnswVectorIndex>& index,
    std::optional<std::vector<search::GlobalDocId>> prefilter_global_docs_ids) {
  if (!prefilter_global_docs_ids)
    return index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime);

  auto& ids = *prefilter_global_docs_ids;
  VLOG(1) << "Searching HNSW index with prefilter size: " << ids.size();

  if (ids.size() < absl::GetFlag(FLAGS_subset_knn_search_threshold))
    return index->SubsetKnn(knn->vec.first.get(), knn->limit, ids);

  // HnswVectorIndex::Knn(... allowed) uses binary_search for membership.
  if (!is_sorted(ids.begin(), ids.end()))
    sort(ids.begin(), ids.end());

  return index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime, ids);
}

vector<SearchResult> SearchGlobalHnswIndex(
    const search::AstKnnNode* knn, const shared_ptr<search::HnswVectorIndex>& index,
    const std::string_view index_name,
    const std::optional<search::KnnScoreSortOption>& knn_score_option, const SearchParams& params,
    const CommandContext& cmd_cntx, ShardId shard_size) {
  auto knn_results = SearchHnswWithPrefilter(knn, index, std::nullopt);

  std::vector<std::vector<SerializedSearchDoc>> shard_docs(shard_size);
  for (const auto& [score, global_doc_id] : knn_results) {
    auto [shard_id, local_doc_id] = search::DecomposeGlobalDocId(global_doc_id);
    SerializedSearchDoc doc;
    doc.id = local_doc_id;
    doc.knn_score = score;
    shard_docs[shard_id].emplace_back(std::move(doc));
  }

  return LoadHnswSearchDocs(index_name, knn_score_option, params, cmd_cntx, std::move(shard_docs),
                            false);
}

vector<SearchResult> SearchGlobalHnswIndex(
    const search::AstKnnNode* knn, const shared_ptr<search::HnswVectorIndex>& index,
    const std::string_view index_name,
    const std::optional<search::KnnScoreSortOption>& knn_score_option,
    const std::vector<SearchIdResult>& sharded_prefilter_docs, const SearchParams& params,
    const CommandContext& cmd_cntx) {
  std::vector<search::GlobalDocId> prefilter_global_docs_ids;
  absl::flat_hash_map<search::GlobalDocId, float> text_scores;
  const bool keep_text_scores = params.with_scores || params.scorer;

  for (size_t shard_id = 0; shard_id < sharded_prefilter_docs.size(); shard_id++) {
    const auto& shard = sharded_prefilter_docs[shard_id];
    prefilter_global_docs_ids.reserve(prefilter_global_docs_ids.size() + shard.ids.size());
    if (keep_text_scores)
      text_scores.reserve(text_scores.size() + shard.text_scores.size());

    for (search::DocId doc_id : shard.ids) {
      auto global_doc_id = search::CreateGlobalDocId(shard_id, doc_id);
      prefilter_global_docs_ids.emplace_back(global_doc_id);
      if (keep_text_scores) {
        if (auto it = shard.text_scores.find(doc_id); it != shard.text_scores.end())
          text_scores[global_doc_id] = it->second;
      }
    }
  }

  auto knn_results =
      SearchHnswWithPrefilter(knn, index, std::make_optional(std::move(prefilter_global_docs_ids)));

  std::vector<std::vector<SerializedSearchDoc>> shard_docs(sharded_prefilter_docs.size());
  for (const auto& [score, global_doc_id] : knn_results) {
    auto [shard_id, local_doc_id] = search::DecomposeGlobalDocId(global_doc_id);
    SerializedSearchDoc doc;
    doc.id = local_doc_id;
    doc.knn_score = score;
    if (keep_text_scores) {
      if (auto it = text_scores.find(global_doc_id); it != text_scores.end())
        doc.text_score = it->second;
    }
    shard_docs[shard_id].emplace_back(std::move(doc));
  }

  return LoadHnswSearchDocs(index_name, knn_score_option, params, cmd_cntx, std::move(shard_docs),
                            true);
}

// Search HNSW index for all documents within the given radius.
// Similar to SearchGlobalHnswIndex but uses RangeQuery instead of Knn.
vector<SearchResult> SearchGlobalHnswIndexRange(
    const search::AstVectorRangeNode* range, const shared_ptr<search::HnswVectorIndex>& index,
    string_view index_name, const std::optional<search::KnnScoreSortOption>& knn_score_option,
    const SearchParams& params, const CommandContext& cmd_cntx) {
  std::vector<SearchResult> results(1);
  const ShardId shard_size = shard_set->size();

  auto range_results =
      index->RangeQuery(range->vec.first.get(), static_cast<float>(range->radius), range->epsilon);

  std::vector<std::vector<SerializedSearchDoc>> shard_docs(shard_size);
  for (const auto& [score, global_doc_id] : range_results) {
    auto [shard_id, local_doc_id] = search::DecomposeGlobalDocId(global_doc_id);
    SerializedSearchDoc doc;
    doc.id = local_doc_id;
    doc.knn_score = score;
    shard_docs[shard_id].emplace_back(doc);
  }

  auto load_options = PrepareHnswLoadOptions(params, knn_score_option);

  cmd_cntx.tx()->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* idx = es->search_indices()->GetIndex(index_name);
    if (!idx || shard_docs[es->shard_id()].empty())
      return OpStatus::OK;
    const auto& schema = idx->base().schema;
    for (auto& shard_doc : shard_docs[es->shard_id()]) {
      if (auto doc = idx->SerializeDocWithKey(shard_doc.id, t->GetOpArgs(es), schema,
                                              load_options.return_fields);
          doc) {
        auto& [key, fields] = *doc;
        search::SortableValue sort_score = std::monostate{};
        if (load_options.set_sort_score) {
          if (auto it = fields.find(load_options.sort_score_field); it != fields.end())
            sort_score = it->second;
          if (load_options.remove_sort_field)
            fields.erase(load_options.sort_score_field);
        }
        shard_doc.key = std::string{key};
        shard_doc.values = std::move(fields);
        shard_doc.sort_score = sort_score;
      }
    }
    return OpStatus::OK;
  });

  std::vector<SerializedSearchDoc> serialized_docs;
  serialized_docs.reserve(range_results.size());
  for (const auto& shard : shard_docs) {
    for (const auto& doc : shard) {
      if (!doc.key.empty())
        serialized_docs.push_back(doc);
    }
  }

  results[0].total_hits = serialized_docs.size();
  results[0].docs = std::move(serialized_docs);
  return results;
}

// Runs RangeQuery and keeps results that also appear in `sharded_prefilter_docs` (the filter
// matches, already loaded), reusing the loaded docs without an extra transaction hop. Exact for
// a range query, unlike top-k KNN where dropping out-of-set docs after the scan loses results.
vector<SearchResult> SearchGlobalHnswIndexRangePrefiltered(
    const search::AstVectorRangeNode* range, const shared_ptr<search::HnswVectorIndex>& index,
    std::vector<SearchResult>& sharded_prefilter_docs) {
  // Sort each shard's prefilter docs by local id so a range hit can be matched with binary search.
  for (auto& shard : sharded_prefilter_docs)
    std::sort(
        shard.docs.begin(), shard.docs.end(),
        [](const SerializedSearchDoc& a, const SerializedSearchDoc& b) { return a.id < b.id; });

  auto range_results =
      index->RangeQuery(range->vec.first.get(), static_cast<float>(range->radius), range->epsilon);

  std::vector<SerializedSearchDoc> out;
  out.reserve(range_results.size());
  for (const auto& [dist, global_doc_id] : range_results) {
    auto [shard_id, local_id] = search::DecomposeGlobalDocId(global_doc_id);
    if (shard_id >= sharded_prefilter_docs.size())
      continue;
    auto& docs = sharded_prefilter_docs[shard_id].docs;
    auto it =
        std::lower_bound(docs.begin(), docs.end(), local_id,
                         [](const SerializedSearchDoc& d, search::DocId id) { return d.id < id; });
    if (it != docs.end() && it->id == local_id) {
      out.emplace_back(std::move(*it));
      out.back().knn_score = dist;
    }
  }

  std::vector<SearchResult> results(1);
  results[0].total_hits = out.size();
  results[0].docs = std::move(out);
  return results;
}

// Try creating global hnsw indices for given fields and return true on success
bool CreateHnswIndices(std::string_view idx_name, const DocIndex& index) {
  std::vector<std::string> created_vector_indices;
  for (const auto& [field_ident, field_info] : index.schema.fields) {
    if (!field_info.IsIndexableHnswField())
      continue;

    const auto& vparams = std::get<search::SchemaField::VectorParams>(field_info.special_params);

    bool success = GlobalHnswIndexRegistry::Instance().Create(idx_name, field_info.short_name,
                                                              vparams, index.type);
    if (!success) {
      // Clean created indices
      for (const auto& cfname : created_vector_indices)
        GlobalHnswIndexRegistry::Instance().Remove(idx_name, cfname);
      return false;
    }

    created_vector_indices.emplace_back(field_info.short_name);
  }
  return true;
}

// Validate HNSW VECTOR_RANGE parameters and return the index on success.
// On failure, sends an error reply via builder and returns nullptr.
std::shared_ptr<search::HnswVectorIndex> GetValidatedHnswRangeIndex(
    std::string_view index_name, const search::AstVectorRangeNode* hnsw_range,
    SinkReplyBuilder* builder) {
  auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(index_name, hnsw_range->field);
  if (!hnsw_index) {
    builder->SendError(string{index_name} + ": no such global hnsw index");
    return nullptr;
  }
  if (hnsw_range->vec.second == 0) {
    builder->SendError("Parse error of vector parameters");
    return nullptr;
  }
  if (!(hnsw_range->radius >= 0) || !std::isfinite(hnsw_range->radius)) {
    builder->SendError(
        absl::StrCat("VECTOR_RANGE radius must be non-negative, got: ", hnsw_range->radius));
    return nullptr;
  }
  if (hnsw_range->epsilon &&
      !search::SchemaField::VectorParams::IsValidRuntimeHnswEpsilon(*hnsw_range->epsilon)) {
    builder->SendError("VECTOR_RANGE EPSILON must be greater than zero");
    return nullptr;
  }
  if (hnsw_index->GetDim() != hnsw_range->vec.second) {
    builder->SendError(absl::StrCat("Wrong vector index dimensions, got: ", hnsw_range->vec.second,
                                    ", expected: ", hnsw_index->GetDim()));
    return nullptr;
  }
  return hnsw_index;
}

// Group global (score, GlobalDocId) pairs into per-shard vectors of (local DocId, score).
std::vector<std::vector<std::pair<search::DocId, float>>> GroupByShardId(
    const std::vector<std::pair<float, search::GlobalDocId>>& global_results, ShardId shard_count) {
  std::vector<std::vector<std::pair<search::DocId, float>>> shard_docs(shard_count);
  for (const auto& [score, global_doc_id] : global_results) {
    auto [shard_id, local_doc_id] = search::DecomposeGlobalDocId(global_doc_id);
    shard_docs[shard_id].emplace_back(local_doc_id, score);
  }
  return shard_docs;
}

// Collect GlobalDocIds from sharded prefilter search results.
std::vector<search::GlobalDocId> CollectPrefilterGlobalIds(
    absl::Span<const SearchResult> prefilter_docs) {
  std::vector<search::GlobalDocId> ids;
  for (size_t shard_id = 0; shard_id < prefilter_docs.size(); shard_id++) {
    for (const auto& doc : prefilter_docs[shard_id].docs) {
      ids.push_back(search::CreateGlobalDocId(shard_id, doc.id));
    }
  }
  return ids;
}

// Try to pop KNN node from search algorithm if HNSW index exists for the field.
// Returns {knn_node, knn_ptr} pair; both null if not a HNSW KNN query.
std::pair<std::unique_ptr<search::AstNode>, search::AstKnnNode*> TryPopHnswKnnNode(
    search::SearchAlgorithm& search_algo, std::string_view index_name) {
  if (search_algo.IsKnnQuery()) {
    if (GlobalHnswIndexRegistry::Instance().Exist(index_name, search_algo.GetKnnNode()->field)) {
      auto knn_node = search_algo.PopKnnNode();
      auto* knn = std::get_if<search::AstKnnNode>(knn_node.get());
      return {std::move(knn_node), knn};
    }
  }
  return {nullptr, nullptr};
}

struct HybridSearchParams {
  enum class CombineMethod { LINEAR, RRF };

  string text_query;
  string yield_text_score_as;
  std::optional<search::ScorerSpec> scorer;  // carries the BM25STD.TANH factor when applicable

  string vsim_field;
  string vsim_param;
  string yield_vsim_score_as;
  string vsim_filter;
  bool use_range = false;
  float range_radius = 0;
  std::optional<double> range_epsilon;

  CombineMethod combine_method = CombineMethod::RRF;
  float alpha = 0.5f;
  float beta = 0.5f;
  float rrf_constant = 60.0f;
  size_t rrf_window = 0;
  string yield_combined_score_as;

  size_t num_candidates = 0;
  std::optional<uint32_t> ef_runtime;

  size_t limit_offset = 0;
  size_t limit_total = 10;
  std::optional<std::vector<FieldReference>> return_fields;
  bool load_all_fields = false;

  search::QueryParams query_params;
};

struct HybridDocEntry {
  string key;
  SearchDocData values;
  float text_score = 0.0f;
  float knn_dist = 0.0f;
  bool has_text = false;
  bool has_knn = false;
  size_t text_rank = 0;
  size_t knn_rank = 0;
  float combined = 0.0f;
};

using HybridDocMap = absl::flat_hash_map<string, HybridDocEntry>;

// Validated doubles for FT.HYBRID RANGE, used via CmdArgParser::Next<T>(). validate() runs inside
// the parser so callers don't re-check the parsed value.
struct NonNegativeDouble : facade::VNum<double> {
  static bool validate(double v) {
    return v >= 0 && std::isfinite(v);
  }
};
struct HnswRangeEpsilon : facade::VNum<double> {
  static bool validate(double v) {
    return search::SchemaField::VectorParams::IsValidRuntimeHnswEpsilon(v);
  }
};

ParseResult<HybridSearchParams> ParseHybridParams(CmdArgParser* parser) {
  using facade::Map;
  using facade::Tag;

  HybridSearchParams p;
  uint64_t tanh_factor = search::kDefaultBM25StdTanhFactor;

  auto parse_scorer = [&](CmdArgParser* sub) {
    if (p.scorer) {
      sub->Next();
      return;
    }
    auto scorer_fn = ParseScorer(sub);
    if (!scorer_fn)
      sub->ReportCustom(absl::StrCat("No such scorer: ", sub->Peek()));
    else
      p.scorer = *scorer_fn;
  };
  auto parse_tanh_factor = [&](CmdArgParser* sub) { tanh_factor = ParseBM25StdTanhFactor(sub); };
  auto read_range_epsilon = [&](CmdArgParser* sub) {
    double epsilon = sub->Next<HnswRangeEpsilon>("Invalid EPSILON value");
    if (!sub->HasError())
      p.range_epsilon = epsilon;
  };
  auto parse_hybrid_range = [&](CmdArgParser* sub) {
    p.use_range = true;

    // Two accepted forms after RANGE: the counted form "<nargs> RADIUS <r> [EPSILON <e>]" (keywords
    // in any order) and the shorthand "<radius> [EPSILON <e>]". The counted form starts with an
    // integer count immediately followed by a RADIUS/EPSILON keyword; the shorthand starts with the
    // bare radius value.
    size_t nargs = 0;
    bool keyword_form =
        absl::SimpleAtoi(sub->Peek(0), &nargs) && (absl::EqualsIgnoreCase(sub->Peek(1), "RADIUS") ||
                                                   absl::EqualsIgnoreCase(sub->Peek(1), "EPSILON"));
    if (keyword_form) {
      sub->Skip(1);  // The counted-form argument count is not otherwise needed.
      bool has_radius = false;
      while (!sub->HasError() && (absl::EqualsIgnoreCase(sub->Peek(), "RADIUS") ||
                                  absl::EqualsIgnoreCase(sub->Peek(), "EPSILON"))) {
        if (sub->Check("RADIUS")) {
          if (has_radius) {
            sub->ReportCustom("Duplicate RADIUS argument");
            return;
          }
          p.range_radius = sub->Next<NonNegativeDouble>("Invalid RADIUS value");
          has_radius = !sub->HasError();
        } else if (sub->Check("EPSILON")) {
          if (p.range_epsilon) {
            sub->ReportCustom("Duplicate EPSILON argument");
            return;
          }
          read_range_epsilon(sub);
        }
      }
      if (!sub->HasError() && !has_radius)
        sub->ReportCustom("Missing required argument RADIUS");
      return;
    }

    p.range_radius = sub->Next<NonNegativeDouble>("Invalid RADIUS value");
    if (sub->Check("EPSILON"))
      read_range_epsilon(sub);
  };

  parser->ExpectTag("SEARCH", "expected SEARCH keyword");
  p.text_query = parser->Next<string>();
  parser->Apply(Tag("SCORER", parse_scorer), Tag("BM25STD_TANH_FACTOR", parse_tanh_factor),
                Tag("YIELD_SCORE_AS", &p.yield_text_score_as));
  if (p.scorer)
    p.scorer->bm25std_tanh_factor = tanh_factor;

  parser->ExpectTag("VSIM", "expected VSIM keyword");
  p.vsim_field = string{parser->ExpectStartsWith("@", "VSIM field must start with @")};
  p.vsim_param = string{parser->ExpectStartsWith("$", "VSIM parameter must start with $")};

  float shard_k_ratio = 1.0f;
  if (parser->Check("KNN", &p.num_candidates)) {
    parser->Apply(Tag("K", &p.num_candidates), Tag("EF_RUNTIME", &p.ef_runtime),
                  Tag("SHARD_K_RATIO", &shard_k_ratio));
    p.num_candidates =
        static_cast<size_t>(std::ceil(static_cast<float>(p.num_candidates) * shard_k_ratio));
  } else if (parser->Check("RANGE")) {
    parse_hybrid_range(parser);
  }

  if (parser->Check("FILTER", &p.vsim_filter) && p.use_range)
    parser->ReportCustom("VSIM RANGE cannot be combined with FILTER");
  parser->Apply(Tag("YIELD_SCORE_AS", &p.yield_vsim_score_as));

  if (parser->Check("COMBINE")) {
    using CM = HybridSearchParams::CombineMethod;
    if (auto method = parser->TryMapNext("LINEAR", CM::LINEAR, "RRF", CM::RRF); method) {
      p.combine_method = *method;
      parser->Next<size_t>();  // nargs hint, not validated -- key/value pairs drive parsing.
      if (p.combine_method == CM::LINEAR) {
        parser->Apply(Tag("ALPHA", &p.alpha), Tag("BETA", &p.beta),
                      Tag("YIELD_SCORE_AS", &p.yield_combined_score_as));
      } else {
        parser->Apply(Tag("CONSTANT", &p.rrf_constant), Tag("WINDOW", &p.rrf_window),
                      Tag("YIELD_SCORE_AS", &p.yield_combined_score_as));
      }
    } else {
      parser->ReportCustom(absl::StrCat("unsupported COMBINE method: ", parser->Peek()));
    }
  }

  auto parse_load = [&](CmdArgParser* sub) {
    if (sub->Check("*")) {
      p.load_all_fields = true;
      return;
    }
    const size_t n = sub->Next<size_t>();
    std::vector<FieldReference> fields;
    fields.reserve(n);
    for (size_t i = 0; i < n; i++) {
      string_view f = sub->Next();
      if (absl::StartsWith(f, "@"))
        f.remove_prefix(1);
      string_view alias;
      sub->Check("AS", &alias);
      fields.emplace_back(f, alias);
    }
    p.return_fields = std::move(fields);
  };

  auto parse_params = [&](CmdArgParser* sub) { p.query_params = ParseQueryParams(sub); };

  parser->ApplyOrSkip(Tag("SCORER", parse_scorer), Tag("LOAD", parse_load),
                      Tag("LIMIT", &p.limit_offset, &p.limit_total), Tag("PARAMS", parse_params));

  if (parser->HasError())
    return make_unexpected(parser->TakeError().MakeReply());

  const size_t max_results = absl::GetFlag(FLAGS_MAXSEARCHRESULTS);
  if (p.limit_total > max_results)
    return CreateSyntaxError(absl::StrFormat("LIMIT exceeds maximum of %d", max_results));

  if (p.num_candidates == 0) {
    p.num_candidates =
        (p.rrf_window > 0) ? p.rrf_window : std::max(p.limit_offset + p.limit_total, size_t{10});
  }

  return p;
}

void ComputeLinearCombined(float alpha, float beta, search::VectorSimilarity vsim_metric,
                           absl::flat_hash_map<string, HybridDocEntry>& docs) {
  for (auto& [_, e] : docs) {
    float text_score = e.has_text ? e.text_score : 0.f;
    float knn_score = e.has_knn ? search::DistanceToSimilarity(e.knn_dist, vsim_metric) : 0.f;
    e.combined = alpha * text_score + beta * knn_score;  // ALPHA=text, BETA=vector
  }
}

void ComputeRrfCombined(float rrf_constant, absl::flat_hash_map<string, HybridDocEntry>& docs) {
  const float k = rrf_constant;
  std::vector<HybridDocEntry*> text_ranked, knn_ranked;
  for (auto& [_, e] : docs) {
    if (e.has_text)
      text_ranked.push_back(&e);
    if (e.has_knn)
      knn_ranked.push_back(&e);
  }

  std::sort(
      text_ranked.begin(), text_ranked.end(), [](const HybridDocEntry* a, const HybridDocEntry* b) {
        return a->text_score != b->text_score ? a->text_score > b->text_score : a->key < b->key;
      });
  for (size_t r = 0; r < text_ranked.size(); ++r)
    text_ranked[r]->text_rank = r + 1;

  std::sort(knn_ranked.begin(), knn_ranked.end(),
            [](const HybridDocEntry* a, const HybridDocEntry* b) {
              return a->knn_dist != b->knn_dist ? a->knn_dist < b->knn_dist : a->key < b->key;
            });
  for (size_t r = 0; r < knn_ranked.size(); ++r)
    knn_ranked[r]->knn_rank = r + 1;

  for (auto& [_, e] : docs) {
    e.combined = (e.has_text ? 1.f / (k + static_cast<float>(e.text_rank)) : 0.f) +
                 (e.has_knn ? 1.f / (k + static_cast<float>(e.knn_rank)) : 0.f);
  }
}

void HybridReply(const HybridSearchParams& params, search::VectorSimilarity vsim_metric,
                 absl::Duration total_took, HybridDocMap& doc_map, SinkReplyBuilder* builder) {
  std::vector<HybridDocEntry*> docs;
  docs.reserve(doc_map.size());
  for (auto& [_, e] : doc_map)
    docs.push_back(&e);

  const size_t total_hits = docs.size();
  const size_t end = std::min(params.limit_offset + params.limit_total, docs.size());

  partial_sort(docs.begin(), docs.begin() + end, docs.end(),
               [](const HybridDocEntry* a, const HybridDocEntry* b) {
                 return a->combined != b->combined ? a->combined > b->combined : a->key < b->key;
               });

  const size_t offset = std::min(params.limit_offset, docs.size());
  const size_t limit = std::min(docs.size() - offset, params.limit_total);

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartCollection(4, CollectionType::MAP);

  rb->SendBulkString("total_results");
  rb->SendLong(static_cast<long>(total_hits));

  const bool yield_text_score = !params.yield_text_score_as.empty();
  const bool yield_vsim_score = !params.yield_vsim_score_as.empty();
  const bool yield_combined_score = !params.yield_combined_score_as.empty();
  const bool emit_fields = params.load_all_fields || params.return_fields.has_value();

  rb->SendBulkString("results");
  rb->StartArray(limit);
  for (size_t i = offset; i < offset + limit; i++) {
    const auto& doc = *docs[i];
    // Per-doc layout:
    //   without LOAD: [text_alias, X]? [__key, key] [__score | combined_alias, val] [vsim_alias,
    //   X]? with LOAD:    [text_alias, X]? [fields...]  [combined_alias, val]? [vsim_alias, X]?
    // Score aliases are skipped for docs missing from their pipeline to avoid an ambiguous 0.
    const bool emit_text = yield_text_score && doc.has_text;
    const bool emit_vsim = yield_vsim_score && doc.has_knn;
    const size_t n_pairs = (emit_text ? 1 : 0) + (emit_vsim ? 1 : 0) +
                           (emit_fields ? doc.values.size() + (yield_combined_score ? 1 : 0) : 2);
    rb->StartArray(n_pairs * 2);

    if (emit_text) {
      rb->SendBulkString(params.yield_text_score_as);
      rb->SendBulkString(absl::StrCat(doc.text_score));
    }
    if (emit_fields) {
      for (const auto& [k, v] : doc.values) {
        rb->SendBulkString(k);
        visit(SortableValueSender(rb), v);
      }
      if (yield_combined_score) {
        rb->SendBulkString(params.yield_combined_score_as);
        rb->SendBulkString(absl::StrCat(doc.combined));
      }
    } else {
      rb->SendBulkString("__key");
      rb->SendBulkString(doc.key);
      rb->SendBulkString(yield_combined_score ? string_view{params.yield_combined_score_as}
                                              : "__score"sv);
      rb->SendBulkString(absl::StrCat(doc.combined));
    }
    if (emit_vsim) {
      rb->SendBulkString(params.yield_vsim_score_as);
      rb->SendBulkString(absl::StrCat(search::DistanceToSimilarity(doc.knn_dist, vsim_metric)));
    }
  }

  rb->SendBulkString("warnings");
  rb->StartArray(0);

  rb->SendBulkString("execution_time");
  rb->SendBulkString(absl::StrFormat("%.6f", absl::ToDoubleMilliseconds(total_took)));
}

// Returns false if the index is not found on any shard; caller must Conclude() the tx.
bool CollectGlobalScoringStats(string_view index_name, search::SearchAlgorithm& text_algo,
                               Transaction* tx, size_t shard_count,
                               search::GlobalScoringStats& out_stats) {
  atomic<bool> not_found{false};
  vector<search::ShardScoringStats> shard_stats(shard_count);
  tx->Execute(
      [&](Transaction* t, EngineShard* es) {
        if (auto* idx = es->search_indices()->GetIndex(index_name); idx)
          shard_stats[es->shard_id()] = idx->CollectScoringStats(&text_algo);
        else
          not_found.store(true, memory_order_relaxed);
        return OpStatus::OK;
      },
      false);
  if (not_found.load(memory_order_relaxed))
    return false;
  for (auto& s : shard_stats)
    out_stats.Merge(s);
  return true;
}

std::optional<string> ValidateAndExtractHnswVector(const search::HnswVectorIndex& hnsw_index,
                                                   const HybridSearchParams& params,
                                                   search::OwnedFtVector* out_vec) {
  auto vec_bytes = params.query_params[params.vsim_param];
  if (vec_bytes.empty())
    return absl::StrCat("Vector parameter not found: $", params.vsim_param);

  auto vec = search::BytesToFtVectorSafe(vec_bytes);
  if (!vec)
    return absl::StrCat("Invalid vector bytes for parameter: $", params.vsim_param);

  if (vec->second != hnsw_index.GetDim())
    return absl::StrCat("Query vector blob size (", vec->second * sizeof(float),
                        ") does not match index's expected size (",
                        hnsw_index.GetDim() * sizeof(float), ")");

  *out_vec = std::move(*vec);
  return std::nullopt;
}

// Runs HNSW Knn/RangeQuery before the shard hop; per-shard key/value loading happens in the hop.
std::optional<string> RunHnswPreSearch(string_view index_name, const HybridSearchParams& params,
                                       vector<vector<SerializedSearchDoc>>& out_docs) {
  auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(index_name, params.vsim_field);
  if (!hnsw_index)
    return absl::StrCat("No HNSW index for field: ", params.vsim_field);

  search::OwnedFtVector vec;
  if (auto err = ValidateAndExtractHnswVector(*hnsw_index, params, &vec))
    return err;

  auto populate = [&](const vector<pair<float, search::GlobalDocId>>& results) {
    for (const auto& [dist, global_id] : results) {
      auto [shard_id, local_id] = search::DecomposeGlobalDocId(global_id);
      SerializedSearchDoc doc;
      doc.id = local_id;
      doc.knn_score = dist;
      out_docs[shard_id].push_back(std::move(doc));
    }
  };

  if (params.use_range)
    populate(hnsw_index->RangeQuery(vec.first.get(), params.range_radius, params.range_epsilon));
  else
    populate(hnsw_index->Knn(vec.first.get(), params.num_candidates, params.ef_runtime));
  return std::nullopt;
}

// Post-hop HNSW KNN restricted to the docs returned by the FILTER subquery.
std::optional<string> RunHnswFilteredSearch(string_view index_name,
                                            const HybridSearchParams& params,
                                            const vector<SearchResult>& filter_docs,
                                            vector<vector<SerializedSearchDoc>>& out_docs) {
  auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(index_name, params.vsim_field);
  if (!hnsw_index)
    return absl::StrCat("No HNSW index for field: ", params.vsim_field);

  search::OwnedFtVector vec;
  if (auto err = ValidateAndExtractHnswVector(*hnsw_index, params, &vec))
    return err;

  vector<search::GlobalDocId> prefilter_ids;
  absl::flat_hash_map<search::GlobalDocId, const SerializedSearchDoc*> prefilter_map;
  for (size_t shard_id = 0; shard_id < filter_docs.size(); shard_id++) {
    for (const auto& doc : filter_docs[shard_id].docs) {
      auto gid = search::CreateGlobalDocId(shard_id, doc.id);
      prefilter_ids.push_back(gid);
      prefilter_map[gid] = &doc;
    }
  }

  auto knn_results =
      prefilter_ids.size() < absl::GetFlag(FLAGS_subset_knn_search_threshold)
          ? hnsw_index->SubsetKnn(vec.first.get(), params.num_candidates, prefilter_ids)
          : hnsw_index->Knn(vec.first.get(), params.num_candidates, params.ef_runtime,
                            prefilter_ids);

  for (const auto& [dist, global_id] : knn_results) {
    auto it = prefilter_map.find(global_id);
    if (it == prefilter_map.end())
      continue;
    auto [shard_id, local_id] = search::DecomposeGlobalDocId(global_id);
    SerializedSearchDoc doc = *it->second;
    doc.knn_score = dist;
    out_docs[shard_id].push_back(std::move(doc));
  }
  return std::nullopt;
}

HybridDocMap MergeHybridResults(const vector<SearchResult>& text_docs,
                                const vector<vector<SerializedSearchDoc>>& hnsw_shard_docs,
                                const vector<SearchResult>& flat_knn_docs, bool use_hnsw) {
  HybridDocMap doc_map;

  for (const auto& shard_res : text_docs) {
    for (const auto& doc : shard_res.docs) {
      if (doc.key.empty())
        continue;
      auto& entry = doc_map[doc.key];
      entry.key = doc.key;
      entry.values = doc.values;
      entry.text_score = doc.text_score;
      entry.has_text = true;
    }
  }

  auto merge_knn = [&](const SerializedSearchDoc& doc) {
    if (doc.key.empty())
      return;
    auto [it, inserted] = doc_map.emplace(doc.key, HybridDocEntry{});
    auto& entry = it->second;
    if (inserted) {
      entry.key = doc.key;
      entry.values = doc.values;
    }
    entry.knn_dist = doc.knn_score;
    entry.has_knn = true;
  };

  if (use_hnsw) {
    for (const auto& shard : hnsw_shard_docs)
      for (const auto& doc : shard)
        merge_knn(doc);
  } else {
    for (const auto& shard_res : flat_knn_docs)
      for (const auto& doc : shard_res.docs)
        merge_knn(doc);
  }

  return doc_map;
}

void NormalizeHybridTextScores(absl::Span<SearchResult> text_docs) {
  float max_text_score = GlobalMaxTextScore(text_docs);
  if (max_text_score == 0)
    return;

  for (auto& shard : text_docs) {
    for (auto& doc : shard.docs)
      doc.text_score /= max_text_score;
  }
}

void NormalizeAggregateScores(absl::Span<aggregate::DocValues> values) {
  static constexpr std::string_view kScoreField = "__score";

  auto get_score = [](const aggregate::DocValues& row) -> double {
    if (auto it = row.find(kScoreField); it != row.end())
      if (const auto* score = std::get_if<double>(&it->second))
        return *score;
    return 0.0;
  };

  if (values.empty())
    return;

  double max_score = std::ranges::max(values | std::views::transform(get_score));
  if (max_score == 0)
    return;

  for (auto& row : values) {
    if (auto it = row.find(kScoreField); it != row.end())
      if (auto* score = std::get_if<double>(&it->second))
        *score /= max_score;
  }
}

struct HybridExecResult {
  HybridDocMap doc_map;
  search::VectorSimilarity vsim_metric = search::VectorSimilarity::L2;

  absl::Duration text_phase_took;
  absl::Duration knn_phase_took;
  absl::Duration combine_phase_took;
  absl::Duration total_took;

  // Populated only when enable_profile=true. Per-shard wall-clock and trees for FT.PROFILE.
  std::vector<SearchResult> text_shard_results;
  std::vector<absl::Duration> shard_total_durations;
};

// On error, sends the reply via cmd_cntx and returns false.
bool RunHybridSearch(string_view index_name, HybridSearchParams* params, CommandContext* cmd_cntx,
                     bool enable_profile, HybridExecResult* out) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  const size_t shard_count = shard_set->size();
  const bool needs_global_stats = (shard_count > 1);
  std::atomic<bool> index_not_found{false};
  const absl::Time t_start = absl::Now();

  search::SearchAlgorithm text_algo;
  if (!text_algo.Init(params->text_query, &params->query_params, nullptr)) {
    rb->SendError("Query syntax error in SEARCH clause");
    return false;
  }
  text_algo.SetScorer(params->scorer ? *params->scorer : search::ScorerSpec{});
  if (enable_profile)
    text_algo.EnableProfiling();

  const bool emit_fields = params->load_all_fields || params->return_fields.has_value();

  // When the reply doesn't need any field values (no LOAD), tell the pipeline to skip per-doc
  // hash serialization by passing an explicit empty return_fields vector.
  auto apply_return_fields = [&](SearchParams& sp) {
    if (emit_fields)
      sp.return_fields = params->return_fields;
    else
      sp.return_fields.emplace();
  };

  SearchParams text_sp;
  text_sp.limit_total = params->num_candidates;
  apply_return_fields(text_sp);

  const bool use_hnsw = GlobalHnswIndexRegistry::Instance().Exist(index_name, params->vsim_field);
  const bool have_filter = !params->vsim_filter.empty();

  search::SearchAlgorithm knn_algo;
  SearchParams knn_sp;
  vector<SearchResult> flat_knn_docs(shard_count);
  if (!use_hnsw) {
    if (params->use_range && params->range_epsilon) {
      rb->SendError("EPSILON is supported only for HNSW VECTOR_RANGE");
      return false;
    }
    if (params->query_params[params->vsim_param].empty()) {
      rb->SendError(absl::StrCat("Vector parameter not found: $", params->vsim_param));
      return false;
    }
    string knn_query;
    if (params->use_range) {
      // VECTOR_RANGE does not support an AS alias for FLAT indices.
      knn_query = absl::StrFormat("@%s:[VECTOR_RANGE %g $%s]", params->vsim_field,
                                  params->range_radius, params->vsim_param);
    } else {
      string_view prefix = have_filter ? string_view{params->vsim_filter} : "*"sv;
      string ef_suffix;
      if (params->ef_runtime)
        ef_suffix = absl::StrFormat(" EF_RUNTIME %u", *params->ef_runtime);
      knn_query = absl::StrFormat("%s=>[KNN %zu @%s $%s%s AS __knn_dist_hybrid]", prefix,
                                  params->num_candidates, params->vsim_field, params->vsim_param,
                                  ef_suffix);
    }
    if (!knn_algo.Init(knn_query, &params->query_params, nullptr)) {
      rb->SendError("KNN query error for VSIM field");
      return false;
    }
    knn_sp.limit_total = params->num_candidates;
    apply_return_fields(knn_sp);
  }

  search::SearchAlgorithm filter_algo;
  SearchParams filter_sp;
  vector<SearchResult> hnsw_filter_docs(shard_count);
  if (use_hnsw && have_filter) {
    if (!filter_algo.Init(params->vsim_filter, &params->query_params, nullptr)) {
      rb->SendError("Query syntax error in VSIM FILTER clause");
      return false;
    }
    filter_sp.limit_total = params->num_candidates;
    apply_return_fields(filter_sp);

    // Pre-flight dim check spares us a multi-shard text+filter hop on a bad query vector.
    auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(index_name, params->vsim_field);
    if (!hnsw_index) {
      rb->SendError(absl::StrCat("No HNSW index for field: ", params->vsim_field));
      return false;
    }
    search::OwnedFtVector vec_ignored;
    if (auto err = ValidateAndExtractHnswVector(*hnsw_index, *params, &vec_ignored)) {
      rb->SendError(*err);
      return false;
    }
  }

  search::GlobalScoringStats global_stats;
  if (needs_global_stats && !CollectGlobalScoringStats(index_name, text_algo, cmd_cntx->tx(),
                                                       shard_count, global_stats)) {
    cmd_cntx->tx()->Conclude();
    cmd_cntx->SendError(string{index_name} + ": no such index");
    return false;
  }
  const search::GlobalScoringStats* stats_ptr = needs_global_stats ? &global_stats : nullptr;

  absl::Duration hnsw_pre_search_took;
  vector<vector<SerializedSearchDoc>> hnsw_shard_docs(shard_count);
  if (use_hnsw && !have_filter) {
    const absl::Time t = absl::Now();
    if (auto err = RunHnswPreSearch(index_name, *params, hnsw_shard_docs)) {
      if (needs_global_stats)
        cmd_cntx->tx()->Conclude();
      rb->SendError(*err);
      return false;
    }
    hnsw_pre_search_took = absl::Now() - t;
  }

  // captured_metric/vsim_* are written under std::call_once and read on the caller fiber
  // after Execute()/ScheduleSingleHop() -- the hop boundary supplies the happens-before edge.
  std::once_flag schema_validated;
  bool vsim_not_vector = false;
  bool vsim_dim_mismatch = false;
  size_t vsim_query_dim = 0, vsim_index_dim = 0;
  search::VectorSimilarity captured_metric = search::VectorSimilarity::L2;

  vector<SearchResult> text_docs(shard_count);
  vector<absl::Duration> shard_text_durations(shard_count);
  vector<absl::Duration> shard_knn_durations(shard_count);

  auto shard_cb = [&](Transaction* t, EngineShard* es) {
    auto* idx = es->search_indices()->GetIndex(index_name);
    if (!idx) {
      index_not_found.store(true, std::memory_order_relaxed);
      return OpStatus::OK;
    }
    const auto& schema = idx->base().schema;
    std::call_once(schema_validated, [&] {
      auto name_it = schema.field_names.find(params->vsim_field);
      if (name_it == schema.field_names.end()) {
        vsim_not_vector = true;
        return;
      }
      auto field_it = schema.fields.find(name_it->second);
      if (field_it == schema.fields.end() || field_it->second.type != search::SchemaField::VECTOR) {
        vsim_not_vector = true;
        return;
      }
      const auto& vp = std::get<search::SchemaField::VectorParams>(field_it->second.special_params);
      captured_metric = vp.sim;
      if (!use_hnsw) {
        auto vec_bytes = params->query_params[params->vsim_param];
        if (!vec_bytes.empty() && vec_bytes.size() != vp.dim * sizeof(float)) {
          vsim_dim_mismatch = true;
          vsim_index_dim = vp.dim;
          vsim_query_dim = vec_bytes.size() / sizeof(float);
        }
      }
    });
    if (vsim_not_vector || vsim_dim_mismatch)
      return OpStatus::OK;

    const ShardId sid = es->shard_id();

    const absl::Time t_text = absl::Now();
    text_docs[sid] = idx->Search(t->GetOpArgs(es), text_sp, &text_algo, false, stats_ptr);
    shard_text_durations[sid] = absl::Now() - t_text;

    const absl::Time t_knn = absl::Now();
    if (use_hnsw) {
      if (have_filter) {
        hnsw_filter_docs[sid] =
            idx->Search(t->GetOpArgs(es), filter_sp, &filter_algo, false, nullptr);
      } else {
        // Empty optional vs unset: unset would serialize the full hash even when no LOAD is used.
        const std::optional<std::vector<FieldReference>> empty_fields{std::in_place};
        const auto& serialize_fields = emit_fields ? params->return_fields : empty_fields;
        for (auto& doc : hnsw_shard_docs[sid]) {
          if (auto s = idx->SerializeDocWithKey(doc.id, t->GetOpArgs(es), schema, serialize_fields);
              s) {
            doc.key = string{s->first};
            doc.values = std::move(s->second);
          }
        }
      }
    } else {
      flat_knn_docs[sid] = idx->Search(t->GetOpArgs(es), knn_sp, &knn_algo, false, nullptr);
    }
    shard_knn_durations[sid] = absl::Now() - t_knn;
    return OpStatus::OK;
  };

  if (needs_global_stats)
    cmd_cntx->tx()->Execute(std::move(shard_cb), true);
  else
    cmd_cntx->tx()->ScheduleSingleHop(std::move(shard_cb));

  if (index_not_found.load(std::memory_order_relaxed)) {
    cmd_cntx->SendError(string{index_name} + ": no such index");
    return false;
  }
  if (vsim_not_vector) {
    rb->SendError(absl::StrCat("@", params->vsim_field, " is not a vector field"));
    return false;
  }
  if (vsim_dim_mismatch) {
    rb->SendError(absl::StrCat("Query vector blob size (", vsim_query_dim * sizeof(float),
                               ") does not match index's expected size (",
                               vsim_index_dim * sizeof(float), ")"));
    return false;
  }

  absl::Duration hnsw_post_search_took;
  if (use_hnsw && have_filter) {
    const absl::Time t = absl::Now();
    if (auto err = RunHnswFilteredSearch(index_name, *params, hnsw_filter_docs, hnsw_shard_docs)) {
      rb->SendError(*err);
      return false;
    }
    hnsw_post_search_took = absl::Now() - t;
  }

  // Wall-clock = max because shards run in parallel.
  absl::Duration text_max = absl::ZeroDuration();
  absl::Duration knn_max = absl::ZeroDuration();
  for (size_t i = 0; i < shard_count; i++) {
    text_max = std::max(text_max, shard_text_durations[i]);
    knn_max = std::max(knn_max, shard_knn_durations[i]);
  }

  const absl::Time t_combine = absl::Now();
  if (IsBM25StdNorm(params->scorer))
    NormalizeHybridTextScores(absl::MakeSpan(text_docs));
  auto doc_map = MergeHybridResults(text_docs, hnsw_shard_docs, flat_knn_docs, use_hnsw);
  if (params->combine_method == HybridSearchParams::CombineMethod::LINEAR)
    ComputeLinearCombined(params->alpha, params->beta, captured_metric, doc_map);
  else
    ComputeRrfCombined(params->rrf_constant, doc_map);
  const absl::Duration combine_took = absl::Now() - t_combine;

  out->doc_map = std::move(doc_map);
  out->vsim_metric = captured_metric;
  out->text_phase_took = text_max;
  out->knn_phase_took = hnsw_pre_search_took + knn_max + hnsw_post_search_took;
  out->combine_phase_took = combine_took;
  out->total_took = absl::Now() - t_start;
  if (enable_profile) {
    out->text_shard_results = std::move(text_docs);
    out->shard_total_durations.reserve(shard_count);
    for (size_t i = 0; i < shard_count; i++)
      out->shard_total_durations.push_back(shard_text_durations[i] + shard_knn_durations[i]);
  }
  return true;
}

// Recursively emits events[start] as a MAP and returns the index of the next sibling event.
// When `limited`, `children` is collapsed to an integer count instead of the nested array.
size_t RenderProfileEvent(RedisReplyBuilder* rb,
                          const vector<search::AlgorithmProfile::ProfileEvent>& events,
                          size_t start, bool limited) {
  const auto& event = events[start];

  vector<size_t> child_indices;
  size_t children_micros = 0;
  for (size_t j = start + 1; j < events.size(); j++) {
    if (events[j].depth <= event.depth)
      break;
    if (events[j].depth == event.depth + 1) {
      child_indices.push_back(j);
      children_micros += events[j].micros;
    }
  }

  const bool has_children = !child_indices.empty();
  rb->StartCollection(4 + (has_children ? 1 : 0), CollectionType::MAP);
  rb->SendSimpleString("total_time");
  rb->SendLong(event.micros);
  rb->SendSimpleString("operation");
  rb->SendSimpleString(event.descr);
  rb->SendSimpleString("self_time");
  rb->SendLong(event.micros - children_micros);
  rb->SendSimpleString("processed");
  rb->SendLong(event.num_processed);

  if (has_children) {
    rb->SendSimpleString("children");
    if (limited) {
      rb->SendLong(static_cast<long>(child_indices.size()));
    } else {
      rb->StartArray(child_indices.size());
      for (size_t child : child_indices)
        RenderProfileEvent(rb, events, child, limited);
    }
  }

  size_t j = start + 1;
  while (j < events.size() && events[j].depth > event.depth)
    j++;
  return j;
}

void RenderShardProfileTree(RedisReplyBuilder* rb, const SearchResult& shard_result, bool limited) {
  if (shard_result.error || !shard_result.profile || shard_result.profile->events.empty()) {
    rb->SendEmptyArray();
    return;
  }
  RenderProfileEvent(rb, shard_result.profile->events, 0, limited);
}

}  // namespace

void CmdFtCreate(CmdArgParser parser, CommandContext* cmd_cntx) {
  WarmupQueryParser();

  auto* builder = cmd_cntx->rb();
  if (cmd_cntx->server_conn_cntx()->conn_state.db_index != 0) {
    return builder->SendError("Cannot create index on db != 0"sv);
  }

  string_view idx_name = parser.Next();

  // Args after the index name, captured before further parsing for the CSS cluster dispatch below.
  ParsedArgs create_args = parser.UnparsedArgs();

  // Parse optional NX (Only create if not exists) parameter for internal usage
  bool is_NX = parser.Check("NX");

  bool is_cross_shard = parser.Check("CSS");

  auto parsed_index = CreateDocIndex(idx_name, &parser);
  if (SendErrorIfOccurred(parsed_index, &parser, cmd_cntx)) {
    return;
  }

  // Check if index already exists
  atomic_uint exists_cnt = 0;
  cmd_cntx->tx()->Execute(
      [idx_name, &exists_cnt](auto* tx, auto* es) {
        if (es->search_indices()->GetIndex(idx_name) != nullptr)
          exists_cnt.fetch_add(1, std::memory_order_relaxed);
        return OpStatus::OK;
      },
      false);

  DCHECK(exists_cnt == 0u || exists_cnt == shard_set->size());

  if (exists_cnt.load(memory_order_relaxed) > 0) {
    cmd_cntx->tx()->Conclude();
    return is_NX ? builder->SendOk() : builder->SendError("Index already exists");
  }

  if (absl::GetFlag(FLAGS_cluster_search) && !is_cross_shard && IsClusterEnabled()) {
    std::string args_str = absl::StrJoin(create_args, " ");
    std::string cmd = absl::StrCat("FT.CREATE ", idx_name, " CSS ", args_str);

    // TODO add processing of the reply to make sure index was created successfully on all shards,
    // and prevent simultaneous creation of the same index.
    auto req_future = cluster::Coordinator::Current().DispatchAll(cmd, [](const RESPObj&) {});
    // TODO add error handling
    CHECK(!req_future.Get());
  }

  if (!CreateHnswIndices(idx_name, *parsed_index)) {
    cmd_cntx->tx()->Conclude();
    return builder->SendError("Index already exists");
  }

  auto idx_ptr = make_shared<DocIndex>(std::move(parsed_index).value());
  const bool is_journal = cmd_cntx->server_conn_cntx()->journal_emulated;
  cmd_cntx->tx()->Execute(
      [idx_name, idx_ptr, is_journal](auto* tx, auto* es) {
        es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, idx_ptr, is_journal);
        return OpStatus::OK;
      },
      true);

  builder->SendOk();
}

void CmdFtAlter(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view idx_name = parser.Next();
  parser.ExpectTag("SCHEMA");
  parser.ExpectTag("ADD");
  auto* builder = cmd_cntx->rb();
  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  // First, extract existing index info
  shared_ptr<DocIndex> index_info;
  auto idx_cb = [idx_name, &index_info](auto* tx, EngineShard* es) {
    if (es->shard_id() > 0)  // all shards have the same data, fetch from first
      return OpStatus::OK;

    if (auto* idx = es->search_indices()->GetIndex(idx_name); idx != nullptr)
      index_info = make_shared<DocIndex>(idx->base());
    return OpStatus::OK;
  };
  cmd_cntx->tx()->Execute(idx_cb, false);

  if (!index_info) {
    cmd_cntx->tx()->Conclude();
    return cmd_cntx->SendError("Index not found");
  }

  // Parse additional schema
  DocIndex new_index{};
  new_index.type = index_info->type;
  auto parse_result = ParseSchema(&parser, &new_index);
  if (SendErrorIfOccurred(parse_result, &parser, cmd_cntx)) {
    cmd_cntx->tx()->Conclude();
    return;
  }

  auto& new_fields = new_index.schema;

  // For logging we copy the whole schema
  // TODO: Use a more efficient way for logging
  LOG(INFO) << "Adding "
            << DocIndexInfo{.base_index = new_index, .hnsw_metadata = {}}.BuildRestoreCommand();

  // Merge schemas
  search::Schema& schema = index_info->schema;
  schema.fields.insert(new_fields.fields.begin(), new_fields.fields.end());
  schema.field_names.insert(new_fields.field_names.begin(), new_fields.field_names.end());

  // Rebuild index
  // TODO: Introduce partial rebuild
  const bool is_journal = cmd_cntx->server_conn_cntx()->journal_emulated;
  auto upd_cb = [idx_name, index_info, is_journal](Transaction* tx, EngineShard* es) {
    (void)es->search_indices()->DropIndex(idx_name);
    es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, index_info, is_journal);
    return OpStatus::OK;
  };
  cmd_cntx->tx()->Execute(upd_cb, true);

  builder->SendOk();
}

void CmdFtDropIndex(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view idx_name = parser.Next();

  // Parse optional DD (Delete Documents) parameter
  bool delete_docs = parser.Check("DD");

  shared_ptr<DocIndex> index_info;
  atomic_uint num_deleted{0};

  // Collect dropped indices per shard. We must NOT destroy ShardDocIndex inside the transaction
  // callback because it runs on the shard's FiberQueue, and ~ShardDocIndex -> CancelBuilder ->
  // IndexBuilder::Cancel joins the builder fiber. If the builder's VectorLoop dispatched work
  // to the same FiberQueue (via shard_set->Await), joining from within the FiberQueue deadlocks.
  vector<unique_ptr<ShardDocIndex>> dropped(shard_set->size());

  auto cb = [&](Transaction* t, EngineShard* es) {
    // Get index info from first shard for global cleanup
    if (es->shard_id() == 0) {
      if (auto* idx = es->search_indices()->GetIndex(idx_name); idx != nullptr) {
        index_info = make_shared<DocIndex>(idx->base());
      }
    }
    // Drop the index and get its pointer
    auto index = es->search_indices()->DropIndex(idx_name);
    if (!index)
      return OpStatus::OK;

    num_deleted.fetch_add(1);

    // If DD is set, delete all documents that were in the index
    if (delete_docs) {
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

    // Defer destruction — will be destroyed on the shard thread after the transaction.
    dropped[es->shard_id()] = std::move(index);
    return OpStatus::OK;
  };

  cmd_cntx->tx()->Execute(cb, true);

  // Destroy indices on their shard threads outside the FiberQueue.
  // ~ShardDocIndex calls CancelBuilder which joins the builder fiber. We must not run this
  // on the FiberQueue because the builder's VectorLoop may have work queued on the same
  // FiberQueue — joining from within the FiberQueue consumer would deadlock.
  shard_set->RunBlockingInParallel(
      [&dropped](EngineShard* es) { dropped[es->shard_id()].reset(); });

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
    return cmd_cntx->SendError(IndexNotFoundMsg(idx_name));
  return cmd_cntx->rb()->SendOk();
}

void CmdFtInfo(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view idx_name = parser.Next();

  vector<DocIndexInfo> infos(shard_set->size());

  cmd_cntx->tx()->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(idx_name);
    if (index != nullptr)
      infos[es->shard_id()] = index->GetInfo();
    return OpStatus::OK;
  });

  // Count how many shards didn't find the index by checking empty entries.
  size_t num_notfound = std::count_if(infos.begin(), infos.end(), [](const DocIndexInfo& info) {
    return info.base_index.schema.fields.empty();
  });

  DCHECK(num_notfound == 0u || num_notfound == shard_set->size());
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (num_notfound > 0u)
    return rb->SendError(IndexNotFoundMsg(idx_name));

  DCHECK(infos.front().base_index.schema.fields.size() ==
         infos.back().base_index.schema.fields.size());

  bool indexing = false;
  float percent_indexed = 1.0;
  size_t total_num_docs = 0;
  for (const auto& info : infos) {
    total_num_docs += info.num_docs;
    indexing |= info.indexing;
    percent_indexed = std::min(percent_indexed, info.percent_indexed);
  }

  const auto& info = infos.front();
  const auto& schema = info.base_index.schema;

  bool has_custom_stopwords = info.base_index.options.custom_stopwords;
  // Reply with a flat array under RESP3 too, not a map.
  rb->StartArray((has_custom_stopwords ? 8 : 7) * 2);

  rb->SendSimpleString("index_name");
  rb->SendSimpleString(idx_name);

  rb->SendSimpleString("index_definition");
  {
    const bool has_lang_field = !schema.language_field.empty();
    rb->StartArray((has_lang_field ? 5 : 4) * 2);
    rb->SendSimpleString("key_type");
    rb->SendSimpleString(info.base_index.type == DocIndex::JSON ? "JSON" : "HASH");
    rb->SendSimpleString("prefixes");
    rb->StartArray(info.base_index.prefixes.size());
    for (const auto& prefix : info.base_index.prefixes) {
      rb->SendBulkString(prefix);
    }
    rb->SendSimpleString("default_language");
    rb->SendSimpleString(schema.default_language);
    if (has_lang_field) {
      rb->SendSimpleString("language_field");
      rb->SendSimpleString(schema.language_field);
    }
    rb->SendSimpleString("default_score");
    rb->SendLong(1);
  }

  rb->SendSimpleString("index_options");
  {
    std::vector<std::string_view> opts;
    if (info.base_index.options.no_offsets)
      opts.emplace_back("NOOFFSETS");
    rb->StartArray(opts.size());
    for (auto opt : opts)
      rb->SendSimpleString(opt);
  }

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

    if (field_info.type == search::SchemaField::VECTOR) {
      auto& vparams = std::get<search::SchemaField::VectorParams>(field_info.special_params);
      info.emplace_back("algorithm");
      info.emplace_back(vparams.use_hnsw ? "HNSW" : "FLAT");
      info.emplace_back("data_type");
      info.emplace_back(vparams.data_type);
      info.emplace_back("dim");
      info.emplace_back(std::to_string(vparams.dim));
      info.emplace_back("distance_metric");
      info.emplace_back(search::VectorSimilarityToString(vparams.sim));
      info.emplace_back("initial_cap");
      info.emplace_back(std::to_string(vparams.capacity));
      if (vparams.use_hnsw) {
        info.emplace_back("M");
        info.emplace_back(std::to_string(vparams.hnsw_m));
        info.emplace_back("ef_construction");
        info.emplace_back(std::to_string(vparams.hnsw_ef_construction));
        info.emplace_back("ef_runtime");
        info.emplace_back(std::to_string(vparams.hnsw_ef_runtime));
        info.emplace_back("epsilon");
        info.emplace_back(std::to_string(vparams.hnsw_epsilon));
      }
    } else if (field_info.type == search::SchemaField::TAG) {
      auto& tparams = std::get<search::SchemaField::TagParams>(field_info.special_params);
      info.emplace_back("SEPARATOR");
      info.emplace_back(std::string(1, tparams.separator));
      if (tparams.case_sensitive)
        info.emplace_back("CASESENSITIVE");
      if (tparams.with_suffixtrie)
        info.emplace_back("WITHSUFFIXTRIE");
    } else if (field_info.type == search::SchemaField::TEXT) {
      auto& tparams = std::get<search::SchemaField::TextParams>(field_info.special_params);
      if (tparams.with_suffixtrie)
        info.emplace_back("WITHSUFFIXTRIE");
      if (tparams.no_stem)
        info.emplace_back("NOSTEM");
    } else if (field_info.type == search::SchemaField::NUMERIC) {
      auto& numeric_params =
          std::get<search::SchemaField::NumericParams>(field_info.special_params);
      info.emplace_back("blocksize"sv);
      info.emplace_back(std::to_string(numeric_params.block_size));
    }

    rb->SendSimpleStrArr(info);
  }

  rb->SendSimpleString("num_docs");
  rb->SendLong(total_num_docs);

  if (has_custom_stopwords) {
    const auto& stopwords = info.base_index.options.stopwords;
    rb->SendSimpleString("stopwords_list");
    rb->StartArray(stopwords.size());
    for (const auto& sw : stopwords) {
      rb->SendBulkString(sw);
    }
  }

  rb->SendSimpleString("indexing");
  rb->SendLong(indexing ? 1 : 0);

  rb->SendSimpleString("percent_indexed");
  rb->SendDouble(percent_indexed);
}

void CmdFtList(CmdArgParser /*parser*/, CommandContext* cmd_cntx) {
  atomic_int first{0};
  vector<string> names;

  cmd_cntx->tx()->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    // Using `first` to assign `names` only once without a race
    if (first.fetch_add(1) == 0)
      names = es->search_indices()->GetIndexNames();
    return OpStatus::OK;
  });
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  rb->SendBulkStrArr(names);
}

static vector<SearchResult> FtSearchCSS(std::string_view idx, std::string_view query,
                                        std::string_view args_str, const SearchParams& params) {
  vector<SearchResult> results;
  const bool sorted = params.sort_option.has_value();
  const std::string_view with_sortkeys = sorted && !params.with_sortkeys ? " WITHSORTKEYS"sv : ""sv;
  std::string cmd = absl::StrCat("FT.SEARCH ", idx, " ", query, " CSS ", args_str, with_sortkeys);

  util::fb2::Mutex mu_;
  auto req_future = cluster::Coordinator::Current().DispatchAll(cmd, [&](const RESPObj& resp_obj) {
    RESPIterator it{resp_obj};
    const auto size = it.Next<uint64_t>();

    std::lock_guard lock{mu_};
    auto& res = results.emplace_back();
    results.back().total_hits = size;

    while (it.HasNext()) {
      auto& search_doc = res.docs.emplace_back();
      search_doc.key = it.Next<std::string>();
      if (sorted) {
        auto sort_score = it.Next<std::string_view>();
        if (sort_score.empty() || (sort_score[0] != '#' && sort_score[0] != '$')) {
          it.SetError();
          break;
        }
        if (sort_score[0] == '#') {  // It's a double
          double sort_res = 0;
          if (ParseDouble(sort_score.substr(1), &sort_res)) {
            search_doc.sort_score = sort_res;
          } else {
            it.SetError();
            break;
          }
        } else {  // It's a string
          search_doc.sort_score = std::string(sort_score.substr(1));
        }
      }

      for (auto arr_fields = it.Next<RESPIterator>(); arr_fields.HasNext();) {
        auto [key, value] = arr_fields.Next<std::string, std::string>();
        search_doc.values.emplace(std::move(key), std::move(value));
      }
    }
    if (it.HasError()) {
      LOG(ERROR) << "FT.SEARCH CSS reply parsing error: " << resp_obj;
    }
  });
  // TODO add error handling
  CHECK(!req_future.Get());
  return results;
}

void CmdFtSearch(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view index_name = parser.Next();
  string_view query_str = parser.Next();

  // Args after index name and query, captured before further parsing for the CSS dispatch below.
  ParsedArgs search_args = parser.UnparsedArgs();

  bool is_cross_shard = parser.Check("CSS");

  auto* builder = cmd_cntx->rb();
  auto params = ParseSearchParams(&parser);
  if (SendErrorIfOccurred(params, &parser, cmd_cntx))
    return;

  // Check query string length limit
  size_t max_query_bytes = absl::GetFlag(FLAGS_search_query_string_bytes);
  if (query_str.size() > max_query_bytes) {
    return builder->SendError(
        absl::StrCat("Query string is too long, max length is ", max_query_bytes, " bytes"));
  }

  vector<SearchResult> css_docs;
  if (absl::GetFlag(FLAGS_cluster_search) && !is_cross_shard && IsClusterEnabled()) {
    if (params->with_scores || params->scorer) {
      return builder->SendError("WITHSCORES/SCORER is not yet supported in cluster search mode");
    }
    std::string args_str = absl::StrJoin(search_args, " ");

    css_docs = FtSearchCSS(index_name, query_str, args_str, *params);
  }

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(query_str, &params->query_params, &params->optional_filters))
    return builder->SendError("Query syntax error");

  // Enable scorer: explicit SCORER param, or default BM25STD when WITHSCORES is set
  if (params->scorer)
    search_algo.SetScorer(*params->scorer);
  else if (params->with_scores)
    search_algo.SetScorer(search::ScorerSpec{});

  auto [knn_node, knn] = TryPopHnswKnnNode(search_algo, index_name);

  // HNSW range (mutually exclusive with KNN): bare -> RangeQuery directly; AND-ed with a filter
  // -> pre-filter then intersect; OR/NOT -> no execution path.
  const search::AstVectorRangeNode* hnsw_range = nullptr;
  std::unique_ptr<search::AstNode> hnsw_range_holder;
  const search::AstVectorRangeNode* hnsw_range_prefilter = nullptr;
  if (!knn) {
    auto ranges = search_algo.CollectVectorRangeNodes();
    if (ranges.size() > 1 &&
        std::any_of(ranges.begin(), ranges.end(), [&](const search::AstVectorRangeNode* r) {
          return GlobalHnswIndexRegistry::Instance().Exist(index_name, r->field);
        })) {
      return builder->SendError(
          "Combining multiple VECTOR_RANGE clauses with an HNSW index is not supported");
    }
    if (auto* vr = search_algo.GetVectorRangeNode(); vr != nullptr) {
      if (GlobalHnswIndexRegistry::Instance().Exist(index_name, vr->field)) {
        if (search_algo.IsBareVectorRange()) {
          hnsw_range = vr;
        } else if (search_algo.IsAndedVectorRange()) {
          hnsw_range_holder = search_algo.ExtractVectorRangeAsPrefilter();
          hnsw_range_prefilter = &std::get<search::AstVectorRangeNode>(*hnsw_range_holder);
        } else {
          return builder->SendError(
              "Combining VECTOR_RANGE with OR/NOT is not supported on HNSW indexes");
        }
      }
    }
  }
  const bool hnsw_range_has_prefilter = hnsw_range_prefilter != nullptr;

  // The KNN-prefilter flow leaves the transaction open between hops; conclude it if an exception
  // unwinds out of the command, otherwise the scheduled global tx leaks and stalls the shards.
  const int uncaught_on_entry = std::uncaught_exceptions();
  absl::Cleanup conclude_on_unwind = [&] {
    if (std::uncaught_exceptions() > uncaught_on_entry)
      cmd_cntx->tx()->Conclude();
  };

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  atomic<bool> index_not_found{false};
  vector<SearchResult> docs(shard_set->size());
  vector<SearchIdResult> knn_prefilter_docs(shard_set->size());

  const bool knn_has_prefilter = knn && knn->HasPreFilter();
  bool empty_prefilter_result = true;

  // Phase 1 collects per-shard counts; phase 2 scores with the global aggregate.
  // Skipped for bare KNN/HNSW range and single-shard; the prefilter search still scores its filter.
  const bool scoring_active =
      (params->scorer || params->with_scores) && (!knn || knn_has_prefilter) && !hnsw_range;
  const bool needs_global_stats = scoring_active && shard_set->size() > 1;
  search::GlobalScoringStats global_scoring_stats;
  const search::GlobalScoringStats* global_stats_ptr = nullptr;

  if (needs_global_stats) {
    std::vector<search::ShardScoringStats> shard_stats(shard_set->size());
    cmd_cntx->tx()->Execute(
        [&](Transaction* t, EngineShard* es) {
          if (auto* index = es->search_indices()->GetIndex(index_name); index)
            shard_stats[es->shard_id()] = index->CollectScoringStats(&search_algo);
          else
            index_not_found.store(true, memory_order_relaxed);
          return OpStatus::OK;
        },
        false);

    if (index_not_found.load(memory_order_relaxed)) {
      cmd_cntx->tx()->Conclude();  // phase 1 ran with conclude=false
      return cmd_cntx->SendError(string{index_name} + ": no such index");
    }

    for (auto& s : shard_stats)
      global_scoring_stats.Merge(s);
    global_stats_ptr = &global_scoring_stats;
  }

  // If the query does not contain knn component, or it is a hybrid query.
  // HNSW vector range has no prefilter, so skip per-shard search entirely.
  if ((!knn || knn_has_prefilter) && !hnsw_range) {
    auto search_cb = [&](Transaction* t, EngineShard* es) {
      if (auto* index = es->search_indices()->GetIndex(index_name); index) {
        if (knn_has_prefilter) {
          knn_prefilter_docs[es->shard_id()] =
              index->SearchIds(t->GetOpArgs(es), *params, &search_algo, global_stats_ptr);
        } else {
          docs[es->shard_id()] = index->Search(t->GetOpArgs(es), *params, &search_algo,
                                               hnsw_range_has_prefilter, global_stats_ptr);
        }
      } else {
        index_not_found.store(true, memory_order_relaxed);
      }
      return OpStatus::OK;
    };
    if (knn_has_prefilter) {
      cmd_cntx->tx()->Execute(std::move(search_cb), false);
    } else if (needs_global_stats) {
      cmd_cntx->tx()->Execute(std::move(search_cb), true);
    } else {
      cmd_cntx->tx()->ScheduleSingleHop(std::move(search_cb));
    }

    if (index_not_found.load(memory_order_relaxed)) {
      if (knn_has_prefilter)
        cmd_cntx->tx()->Conclude();
      return cmd_cntx->SendError(string{index_name} + ": no such index");
    }

    if (knn_has_prefilter) {
      for (const auto& res : knn_prefilter_docs) {
        empty_prefilter_result &= res.ids.empty();
        if (res.error) {
          cmd_cntx->tx()->Conclude();
          return cmd_cntx->SendError(*res.error);
        }
      }
      if (empty_prefilter_result)
        cmd_cntx->tx()->Conclude();
    } else {
      for (const auto& res : docs) {
        empty_prefilter_result &= res.docs.empty();
        if (res.error)
          return cmd_cntx->SendError(*res.error);
      }
    }
  }

  if (knn_node && (!knn_has_prefilter || !empty_prefilter_result)) {
    auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(index_name, knn->field);
    if (!hnsw_index) {
      if (knn_has_prefilter)
        cmd_cntx->tx()->Conclude();
      return builder->SendError(string{index_name} + ": no such global hnsw index");
    }
    if (knn_has_prefilter) {
      docs = SearchGlobalHnswIndex(knn, hnsw_index, index_name, search_algo.GetKnnScoreSortOption(),
                                   knn_prefilter_docs, *params, *cmd_cntx);
    } else {
      docs = SearchGlobalHnswIndex(knn, hnsw_index, index_name, search_algo.GetKnnScoreSortOption(),
                                   *params, *cmd_cntx, shard_set->size());
    }
  }

  auto knn_sort_option = search_algo.GetKnnScoreSortOption();

  if (hnsw_range) {
    auto hnsw_index = GetValidatedHnswRangeIndex(index_name, hnsw_range, builder);
    if (!hnsw_index)
      return;
    if (!hnsw_range->score_alias.empty())
      knn_sort_option =
          search::KnnScoreSortOption{hnsw_range->score_alias, std::numeric_limits<size_t>::max()};
    docs = SearchGlobalHnswIndexRange(hnsw_range, hnsw_index, index_name, knn_sort_option, *params,
                                      *cmd_cntx);
  }

  // `docs` holds the pre-filter matches from the per-shard search above; intersect RangeQuery
  // with them. Empty pre-filter -> empty result, so `docs` is left as-is.
  if (hnsw_range_prefilter && !empty_prefilter_result) {
    auto hnsw_index = GetValidatedHnswRangeIndex(index_name, hnsw_range_prefilter, builder);
    if (!hnsw_index)
      return;
    if (!hnsw_range_prefilter->score_alias.empty())
      knn_sort_option = search::KnnScoreSortOption{hnsw_range_prefilter->score_alias,
                                                   std::numeric_limits<size_t>::max()};
    docs = SearchGlobalHnswIndexRangePrefiltered(hnsw_range_prefilter, hnsw_index, docs);
  }

  // FLAT VECTOR_RANGE alias: HNSW is handled above. For FLAT, the per-shard Search()
  // populates SerializedSearchDoc::knn_score but doesn't expose the alias name.
  // If the user is sorting by the alias, route through knn_sort_option to get distance
  // ordering; otherwise expose the alias without forcing a global reorder.
  std::string_view inject_score_alias;
  if (!knn && !hnsw_range && !knn_sort_option) {
    if (auto* vr = search_algo.GetVectorRangeNode(); vr && !vr->score_alias.empty()) {
      search::KnnScoreSortOption opt{vr->score_alias, std::numeric_limits<size_t>::max()};
      if (params->sort_option && params->sort_option->IsSame(opt)) {
        knn_sort_option = opt;
      } else {
        inject_score_alias = vr->score_alias;
      }
    }
  }

  // TODO add merging of CSS results with local results (SORT, LIMIT, etc)
  docs.insert(docs.end(), std::make_move_iterator(css_docs.begin()),
              std::make_move_iterator(css_docs.end()));

  SearchReply(*params, knn_sort_option, inject_score_alias, absl::MakeSpan(docs), builder,
              is_cross_shard);
}

void CmdFtProfileHybrid(string_view index_name, CmdArgParser* parser, CommandContext* cmd_cntx,
                        bool limited) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  auto params = ParseHybridParams(parser);
  if (SendErrorIfOccurred(params, parser, cmd_cntx))
    return;

  HybridExecResult exec;
  if (!RunHybridSearch(index_name, &*params, cmd_cntx, /*enable_profile=*/true, &exec))
    return;

  rb->StartArray(2);
  HybridReply(*params, exec.vsim_metric, exec.total_took, exec.doc_map, rb);

  const size_t shard_count = exec.text_shard_results.size();
  rb->StartArray(shard_count + 1);

  rb->StartCollection(4, CollectionType::MAP);
  rb->SendBulkString("took");
  rb->SendLong(absl::ToInt64Microseconds(exec.total_took));
  rb->SendBulkString("hits");
  rb->SendLong(static_cast<long>(exec.doc_map.size()));
  rb->SendBulkString("phases");
  rb->StartCollection(3, CollectionType::MAP);
  rb->SendBulkString("text_search");
  rb->SendLong(absl::ToInt64Microseconds(exec.text_phase_took));
  rb->SendBulkString("knn_search");
  rb->SendLong(absl::ToInt64Microseconds(exec.knn_phase_took));
  rb->SendBulkString("combine");
  rb->SendLong(absl::ToInt64Microseconds(exec.combine_phase_took));
  rb->SendBulkString("serialized");
  rb->SendLong(static_cast<long>(exec.doc_map.size()));

  // Per-shard `took` is total wall time (text + knn) spent in shard callback.
  for (size_t shard_id = 0; shard_id < shard_count; shard_id++) {
    rb->StartCollection(2, CollectionType::MAP);
    rb->SendBulkString("took");
    rb->SendLong(absl::ToInt64Microseconds(exec.shard_total_durations[shard_id]));
    rb->SendBulkString("tree");
    RenderShardProfileTree(rb, exec.text_shard_results[shard_id], limited);
  }
}

void SendFtProfileSearchResponse(const SearchParams& params,
                                 std::optional<search::KnnScoreSortOption> knn_sort_option,
                                 std::string_view inject_score_alias,
                                 absl::Span<SearchResult> search_results,
                                 absl::Span<SearchResult> profile_results,
                                 absl::Span<const absl::Duration> shard_durations,
                                 absl::Duration took, RedisReplyBuilder* rb, bool limited) {
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
    SearchReply(params, knn_sort_option, inject_score_alias, search_results, rb, false);
  } else {
    rb->StartArray(1);
    rb->SendLong(0);
  }

  // Profile information
  rb->StartArray(profile_results.size() + 1);

  // General stats
  rb->StartCollection(3, CollectionType::MAP);
  rb->SendBulkString("took");
  rb->SendLong(absl::ToInt64Microseconds(took));
  rb->SendBulkString("hits");
  rb->SendLong(static_cast<long>(total_docs));
  rb->SendBulkString("serialized");
  rb->SendLong(static_cast<long>(total_serialized));

  // Per-shard stats
  for (size_t shard_id = 0; shard_id < profile_results.size(); shard_id++) {
    rb->StartCollection(2, CollectionType::MAP);
    rb->SendBulkString("took");
    rb->SendLong(absl::ToInt64Microseconds(shard_durations[shard_id]));
    rb->SendBulkString("tree");
    RenderShardProfileTree(rb, profile_results[shard_id], limited);
  }
}

void CmdFtProfile(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view index_name = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  const bool is_hybrid = parser.Check("HYBRID");
  if (!is_hybrid && !parser.Check("SEARCH") && !parser.Check("AGGREGATE")) {
    return rb->SendError("no `SEARCH`, `AGGREGATE` or `HYBRID` provided");
  }

  // LIMITED collapses each event's `children` array into the count of immediate children.
  const bool limited = parser.Check("LIMITED");

  if (is_hybrid) {
    parser.ExpectTag("QUERY");
    CmdFtProfileHybrid(index_name, &parser, cmd_cntx, limited);
    return;
  }

  parser.ExpectTag("QUERY");

  string_view query_str = parser.Next();

  auto params = ParseSearchParams(&parser);
  if (SendErrorIfOccurred(params, &parser, cmd_cntx))
    return;

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(query_str, &params->query_params, &params->optional_filters))
    return cmd_cntx->SendError("query syntax error");

  // Enable scorer: explicit SCORER param, or default BM25STD when WITHSCORES is set
  if (params->scorer)
    search_algo.SetScorer(*params->scorer);
  else if (params->with_scores)
    search_algo.SetScorer(search::ScorerSpec{});

  search_algo.EnableProfiling();

  absl::Time start = absl::Now();
  const size_t shards_count = shard_set->size();

  // Prefilter/range profile flows leave the transaction open between hops; conclude it if an
  // exception unwinds out of the command so the scheduled global tx cannot leak.
  const int uncaught_on_entry = std::uncaught_exceptions();
  absl::Cleanup conclude_on_unwind = [&] {
    if (std::uncaught_exceptions() > uncaught_on_entry)
      cmd_cntx->tx()->Conclude();
  };

  auto [profile_knn_node, profile_knn] = TryPopHnswKnnNode(search_algo, index_name);
  if (profile_knn) {
    auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(index_name, profile_knn->field);
    if (!hnsw_index)
      return rb->SendError(std::string{index_name} + ": no such global hnsw index");

    std::vector<SearchResult> search_results(shards_count);
    std::vector<SearchResult> profile_search_results(shards_count);
    std::vector<absl::Duration> profile_results(shards_count);

    const bool knn_has_prefilter = profile_knn->HasPreFilter();
    const bool needs_global_stats =
        knn_has_prefilter && (params->scorer || params->with_scores) && shards_count > 1;
    search::GlobalScoringStats global_scoring_stats;
    const search::GlobalScoringStats* global_stats_ptr = nullptr;

    if (needs_global_stats) {
      if (!CollectGlobalScoringStats(index_name, search_algo, cmd_cntx->tx(), shards_count,
                                     global_scoring_stats)) {
        cmd_cntx->tx()->Conclude();
        return rb->SendError(std::string{index_name} + ": no such index");
      }
      global_stats_ptr = &global_scoring_stats;
    }

    if (knn_has_prefilter) {
      std::atomic<bool> index_not_found{false};
      std::vector<SearchIdResult> knn_prefilter_docs(shards_count);

      cmd_cntx->tx()->Execute(
          [&](Transaction* t, EngineShard* es) {
            auto* index = es->search_indices()->GetIndex(index_name);
            if (!index) {
              index_not_found.store(true, memory_order_relaxed);
              return OpStatus::OK;
            }

            const ShardId shard_id = es->shard_id();
            auto shard_start = absl::Now();
            knn_prefilter_docs[shard_id] =
                index->SearchIds(t->GetOpArgs(es), *params, &search_algo, global_stats_ptr);
            profile_results[shard_id] = absl::Now() - shard_start;
            return OpStatus::OK;
          },
          false);

      if (index_not_found.load(memory_order_relaxed)) {
        cmd_cntx->tx()->Conclude();
        return rb->SendError(std::string{index_name} + ": no such index");
      }

      bool empty_prefilter_result = true;
      for (size_t shard_id = 0; shard_id < knn_prefilter_docs.size(); shard_id++) {
        auto& res = knn_prefilter_docs[shard_id];
        empty_prefilter_result &= res.ids.empty();
        if (res.error) {
          cmd_cntx->tx()->Conclude();
          return cmd_cntx->SendError(*res.error);
        }
        profile_search_results[shard_id].total_hits = res.total_hits;
        profile_search_results[shard_id].profile = std::move(res.profile);
      }

      if (empty_prefilter_result) {
        cmd_cntx->tx()->Conclude();
      } else {
        search_results = SearchGlobalHnswIndex(profile_knn, hnsw_index, index_name,
                                               search_algo.GetKnnScoreSortOption(),
                                               knn_prefilter_docs, *params, *cmd_cntx);
      }
    } else {
      search_results = SearchGlobalHnswIndex(profile_knn, hnsw_index, index_name,
                                             search_algo.GetKnnScoreSortOption(), *params,
                                             *cmd_cntx, shards_count);
      profile_search_results.clear();
      profile_results.clear();
    }

    auto took = absl::Now() - start;
    SendFtProfileSearchResponse(
        *params, search_algo.GetKnnScoreSortOption(), {}, absl::MakeSpan(search_results),
        absl::MakeSpan(profile_search_results), absl::MakeSpan(profile_results), took, rb, limited);
    return;
  }

  const search::AstVectorRangeNode* hnsw_range = nullptr;
  std::unique_ptr<search::AstNode> hnsw_range_holder;
  const search::AstVectorRangeNode* hnsw_range_prefilter = nullptr;
  auto ranges = search_algo.CollectVectorRangeNodes();
  if (ranges.size() > 1 &&
      std::any_of(ranges.begin(), ranges.end(), [&](const search::AstVectorRangeNode* r) {
        return GlobalHnswIndexRegistry::Instance().Exist(index_name, r->field);
      })) {
    return rb->SendError(
        "Combining multiple VECTOR_RANGE clauses with an HNSW index is not "
        "supported");
  }
  if (auto* vr = search_algo.GetVectorRangeNode(); vr != nullptr) {
    if (GlobalHnswIndexRegistry::Instance().Exist(index_name, vr->field)) {
      if (search_algo.IsBareVectorRange()) {
        hnsw_range = vr;
      } else if (search_algo.IsAndedVectorRange()) {
        hnsw_range_holder = search_algo.ExtractVectorRangeAsPrefilter();
        hnsw_range_prefilter = &std::get<search::AstVectorRangeNode>(*hnsw_range_holder);
      } else {
        return rb->SendError("Combining VECTOR_RANGE with OR/NOT is not supported on HNSW indexes");
      }
    }
  }

  std::optional<search::KnnScoreSortOption> hnsw_range_sort_option;
  if (hnsw_range || hnsw_range_prefilter) {
    const auto* range_node = hnsw_range ? hnsw_range : hnsw_range_prefilter;
    if (!range_node->score_alias.empty())
      hnsw_range_sort_option =
          search::KnnScoreSortOption{range_node->score_alias, std::numeric_limits<size_t>::max()};
  }

  if (hnsw_range) {
    auto hnsw_index = GetValidatedHnswRangeIndex(index_name, hnsw_range, rb);
    if (!hnsw_index)
      return;

    auto search_results = SearchGlobalHnswIndexRange(hnsw_range, hnsw_index, index_name,
                                                     hnsw_range_sort_option, *params, *cmd_cntx);
    std::vector<SearchResult> profile_search_results;
    std::vector<absl::Duration> profile_results;
    auto took = absl::Now() - start;
    SendFtProfileSearchResponse(*params, hnsw_range_sort_option, {}, absl::MakeSpan(search_results),
                                absl::MakeSpan(profile_search_results),
                                absl::MakeSpan(profile_results), took, rb, limited);
    return;
  }

  if (hnsw_range_prefilter) {
    std::atomic<bool> index_not_found{false};
    std::vector<SearchResult> prefilter_results(shards_count);
    std::vector<absl::Duration> profile_results(shards_count);

    const bool needs_global_stats = (params->scorer || params->with_scores) && shards_count > 1;
    search::GlobalScoringStats global_scoring_stats;
    const search::GlobalScoringStats* global_stats_ptr = nullptr;
    if (needs_global_stats) {
      if (!CollectGlobalScoringStats(index_name, search_algo, cmd_cntx->tx(), shards_count,
                                     global_scoring_stats)) {
        cmd_cntx->tx()->Conclude();
        return rb->SendError(std::string{index_name} + ": no such index");
      }
      global_stats_ptr = &global_scoring_stats;
    }

    cmd_cntx->tx()->Execute(
        [&](Transaction* t, EngineShard* es) {
          auto* index = es->search_indices()->GetIndex(index_name);
          if (!index) {
            index_not_found.store(true, memory_order_relaxed);
            return OpStatus::OK;
          }
          const ShardId shard_id = es->shard_id();
          auto shard_start = absl::Now();
          prefilter_results[shard_id] = index->Search(t->GetOpArgs(es), *params, &search_algo,
                                                      /*is_knn_prefilter=*/true, global_stats_ptr);
          profile_results[shard_id] = absl::Now() - shard_start;
          return OpStatus::OK;
        },
        true);

    if (index_not_found.load(memory_order_relaxed))
      return rb->SendError(std::string{index_name} + ": no such index");

    bool result_is_empty = true;
    for (const auto& res : prefilter_results) {
      result_is_empty &= res.docs.empty();
      if (res.error)
        return cmd_cntx->SendError(*res.error);
    }

    std::vector<SearchResult> search_results;
    if (result_is_empty) {
      search_results = prefilter_results;
    } else {
      auto hnsw_index = GetValidatedHnswRangeIndex(index_name, hnsw_range_prefilter, rb);
      if (!hnsw_index)
        return;
      search_results = SearchGlobalHnswIndexRangePrefiltered(hnsw_range_prefilter, hnsw_index,
                                                             prefilter_results);
    }

    auto took = absl::Now() - start;
    SendFtProfileSearchResponse(*params, hnsw_range_sort_option, {}, absl::MakeSpan(search_results),
                                absl::MakeSpan(prefilter_results), absl::MakeSpan(profile_results),
                                took, rb, limited);
    return;
  }

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  std::atomic<bool> index_not_found{false};
  std::vector<SearchResult> search_results(shards_count);
  std::vector<absl::Duration> profile_results(shards_count);

  const bool needs_global_stats = (params->scorer || params->with_scores) && shards_count > 1;
  search::GlobalScoringStats global_scoring_stats;
  const search::GlobalScoringStats* global_stats_ptr = nullptr;
  if (needs_global_stats) {
    if (!CollectGlobalScoringStats(index_name, search_algo, cmd_cntx->tx(), shards_count,
                                   global_scoring_stats)) {
      cmd_cntx->tx()->Conclude();
      return rb->SendError(std::string{index_name} + ": no such index");
    }
    global_stats_ptr = &global_scoring_stats;
  }

  auto search_cb = [&](Transaction* t, EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(index_name);
    if (!index) {
      index_not_found.store(true, memory_order_relaxed);
      return OpStatus::OK;
    }

    const ShardId shard_id = es->shard_id();

    auto shard_start = absl::Now();
    search_results[shard_id] = index->Search(t->GetOpArgs(es), *params, &search_algo,
                                             /*is_knn_prefilter=*/false, global_stats_ptr);
    profile_results[shard_id] = {absl::Now() - shard_start};

    return OpStatus::OK;
  };
  if (needs_global_stats)
    cmd_cntx->tx()->Execute(std::move(search_cb), true);
  else
    cmd_cntx->tx()->ScheduleSingleHop(std::move(search_cb));

  if (index_not_found.load())
    return rb->SendError(std::string{index_name} + ": no such index");

  auto took = absl::Now() - start;

  auto knn_sort_option = search_algo.GetKnnScoreSortOption();
  std::string_view inject_score_alias;
  if (!knn_sort_option) {
    if (auto* vr = search_algo.GetVectorRangeNode(); vr && !vr->score_alias.empty()) {
      search::KnnScoreSortOption opt{vr->score_alias, std::numeric_limits<size_t>::max()};
      if (params->sort_option && params->sort_option->IsSame(opt)) {
        knn_sort_option = opt;
      } else {
        inject_score_alias = vr->score_alias;
      }
    }
  }

  SendFtProfileSearchResponse(*params, knn_sort_option, inject_score_alias,
                              absl::MakeSpan(search_results), absl::MakeSpan(search_results),
                              absl::MakeSpan(profile_results), took, rb, limited);
}

void CmdFtTagVals(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view index_name = parser.Next();
  string_view field_name = parser.Next();
  VLOG(1) << "FtTagVals: " << index_name << " " << field_name;

  vector<io::Result<StringVec, ErrorReply>> shard_results(shard_set->size(), StringVec{});

  cmd_cntx->tx()->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(index_name); index)
      shard_results[es->shard_id()] = index->GetTagVals(field_name);
    else
      shard_results[es->shard_id()] =
          nonstd::make_unexpected(ErrorReply(IndexNotFoundMsg(index_name)));

    return OpStatus::OK;
  });

  absl::flat_hash_set<string> result_set;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  // Check first if either shard had errors. Also merge the results into a single set.
  for (auto& res : shard_results) {
    if (res) {
      result_set.insert(make_move_iterator(res->begin()), make_move_iterator(res->end()));
    } else {
      res.error().kind = facade::kSearchErrType;
      return cmd_cntx->SendError(res.error());
    }
  }

  shard_results.clear();
  vector<string> vec(result_set.begin(), result_set.end());

  rb->SendBulkStrArr(vec, CollectionType::SET);
}

// Runs the ids-only prefilter search (a non-finalizing tx hop) and returns the matched global ids.
// When ADDSCORES is on, also records per-shard text scores for later __score injection.
static std::vector<search::GlobalDocId> RunAggregatePrefilter(
    CommandContext* cmd_cntx, const AggregateParams& params, search::SearchAlgorithm& search_algo,
    std::vector<absl::flat_hash_map<search::DocId, float>>& text_scores) {
  std::vector<SearchResult> prefilter_docs(shard_set->size());
  cmd_cntx->tx()->Execute(
      [&](Transaction* t, EngineShard* es) {
        if (auto* index = es->search_indices()->GetIndex(params.index); index) {
          SearchParams sp;
          sp.limit_total = std::numeric_limits<size_t>::max();
          sp.return_fields.emplace();  // ids-only, skip field serialization
          prefilter_docs[es->shard_id()] = index->Search(t->GetOpArgs(es), sp, &search_algo,
                                                         /*is_knn_prefilter=*/true, nullptr);
        }
        return OpStatus::OK;
      },
      false);

  if (params.add_scores) {
    for (size_t shard_id = 0; shard_id < prefilter_docs.size(); shard_id++)
      for (const auto& doc : prefilter_docs[shard_id].docs)
        text_scores[shard_id][doc.id] = doc.text_score;
  }
  return CollectPrefilterGlobalIds(prefilter_docs);
}

// Loads HNSW result docs (KNN or VECTOR_RANGE) into query_results. Finalizes the multi-hop tx when
// `finalize` is set (a prefilter hop already ran), otherwise schedules a single hop.
static void RunHnswAggregateLoad(
    CommandContext* cmd_cntx, const AggregateParams& params,
    std::vector<std::vector<SearchDocData>>& query_results,
    const std::vector<std::vector<std::pair<search::DocId, float>>>& shard_docs,
    std::string_view score_alias,
    const std::vector<absl::flat_hash_map<search::DocId, float>>& text_scores, bool finalize) {
  auto cb = [&query_results, &params, &shard_docs, score_alias, &text_scores](Transaction* t,
                                                                              EngineShard* es) {
    auto* index = es->search_indices()->GetIndex(params.index);
    if (!index || shard_docs[es->shard_id()].empty())
      return OpStatus::OK;
    DCHECK_LT(es->shard_id(), text_scores.size());
    query_results[es->shard_id()] =
        index->LoadHnswRangeDocsForAggregator(t->GetOpArgs(es), params, shard_docs[es->shard_id()],
                                              score_alias, text_scores[es->shard_id()]);
    return OpStatus::OK;
  };
  if (finalize)
    cmd_cntx->tx()->Execute(std::move(cb), true);
  else
    cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
}

// FT.AGGREGATE over a global HNSW KNN query. Returns false if an error was already sent.
static bool AggregateHnswKnn(CommandContext* cmd_cntx, AggregateParams& params,
                             search::SearchAlgorithm& search_algo, const search::AstKnnNode* knn,
                             std::vector<std::vector<SearchDocData>>& query_results) {
  auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(params.index, knn->field);
  if (!hnsw_index) {
    cmd_cntx->rb()->SendError(string{params.index} + ": no such global hnsw index");
    return false;
  }

  const bool has_prefilter = knn->HasPreFilter();
  std::vector<absl::flat_hash_map<search::DocId, float>> text_scores(shard_set->size());
  std::optional<std::vector<search::GlobalDocId>> prefilter_ids;
  if (has_prefilter)
    prefilter_ids = RunAggregatePrefilter(cmd_cntx, params, search_algo, text_scores);

  auto knn_results =
      prefilter_ids
          ? hnsw_index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime, *prefilter_ids)
          : hnsw_index->Knn(knn->vec.first.get(), knn->limit, knn->ef_runtime);
  auto shard_docs = GroupByShardId(knn_results, shard_set->size());
  RunHnswAggregateLoad(cmd_cntx, params, query_results, shard_docs, knn->score_alias, text_scores,
                       has_prefilter);
  return true;
}

// FT.AGGREGATE over a global HNSW VECTOR_RANGE query. Bare range -> RangeQuery directly; AND-ed
// with a filter -> pre-filter then intersect; OR/NOT -> unsupported. Returns false on error.
static bool AggregateHnswRange(CommandContext* cmd_cntx, AggregateParams& params,
                               search::SearchAlgorithm& search_algo,
                               const search::AstVectorRangeNode* vr,
                               std::vector<std::vector<SearchDocData>>& query_results) {
  auto* builder = cmd_cntx->rb();
  std::unique_ptr<search::AstNode> range_holder;
  const search::AstVectorRangeNode* hnsw_range = vr;
  bool has_prefilter = false;
  if (!search_algo.IsBareVectorRange()) {
    if (!search_algo.IsAndedVectorRange()) {
      builder->SendError("Combining VECTOR_RANGE with OR/NOT is not supported on HNSW indexes");
      return false;
    }
    range_holder = search_algo.ExtractVectorRangeAsPrefilter();
    hnsw_range = &std::get<search::AstVectorRangeNode>(*range_holder);
    has_prefilter = true;
  }

  auto hnsw_index = GetValidatedHnswRangeIndex(params.index, hnsw_range, builder);
  if (!hnsw_index)
    return false;

  std::vector<absl::flat_hash_map<search::DocId, float>> text_scores(shard_set->size());
  std::optional<std::vector<search::GlobalDocId>> prefilter_ids;
  if (has_prefilter) {
    auto ids = RunAggregatePrefilter(cmd_cntx, params, search_algo, text_scores);
    std::sort(ids.begin(), ids.end());  // sorted for the binary_search intersection below
    prefilter_ids = std::move(ids);
  }

  // Intersect the range hits with the filter matches: keep only range results whose global id is
  // in the sorted prefilter id set. Memory is O(filter matches) of ids plus O(range hits).
  auto range_results = hnsw_index->RangeQuery(
      hnsw_range->vec.first.get(), static_cast<float>(hnsw_range->radius), hnsw_range->epsilon);
  if (prefilter_ids) {
    erase_if(range_results, [&](const auto& r) {
      return !std::binary_search(prefilter_ids->begin(), prefilter_ids->end(), r.second);
    });
  }
  auto shard_docs = GroupByShardId(range_results, shard_set->size());
  RunHnswAggregateLoad(cmd_cntx, params, query_results, shard_docs, hnsw_range->score_alias,
                       text_scores, has_prefilter);
  return true;
}

// FT.AGGREGATE over a non-vector query: optional global-stats phase, then per-shard search.
static void AggregateGeneric(CommandContext* cmd_cntx, AggregateParams& params,
                             search::SearchAlgorithm& search_algo,
                             std::vector<std::vector<SearchDocData>>& query_results) {
  // Same global-stats phase as FT.SEARCH so SORTBY @__score is stable across shard counts.
  const bool scoring_active = params.scorer || params.add_scores;
  const bool needs_global_stats = scoring_active && shard_set->size() > 1;
  search::GlobalScoringStats global_stats;

  if (needs_global_stats) {
    std::vector<search::ShardScoringStats> shard_stats(shard_set->size());
    cmd_cntx->tx()->Execute(
        [&](Transaction* t, EngineShard* es) {
          if (auto* index = es->search_indices()->GetIndex(params.index); index)
            shard_stats[es->shard_id()] = index->CollectScoringStats(&search_algo);
          return OpStatus::OK;
        },
        false);
    for (auto& s : shard_stats)
      global_stats.Merge(s);
    params.global_scoring_stats = &global_stats;
  }

  auto search_cb = [&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(params.index); index)
      query_results[es->shard_id()] =
          index->SearchForAggregator(t->GetOpArgs(es), params, &search_algo);
    return OpStatus::OK;
  };
  if (needs_global_stats)
    cmd_cntx->tx()->Execute(std::move(search_cb), true);
  else
    cmd_cntx->tx()->ScheduleSingleHop(std::move(search_cb));
}

void CmdFtAggregate(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* builder = cmd_cntx->rb();

  auto params = ParseAggregatorParams(&parser);
  if (SendErrorIfOccurred(params, &parser, cmd_cntx))
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

    // Enable scorer: explicit SCORER param, or default BM25STD when ADDSCORES is set
    if (params->scorer)
      search_algo.SetScorer(*params->scorer);
    else if (params->add_scores)
      search_algo.SetScorer(search::ScorerSpec{});

    std::vector<std::vector<SearchDocData>> query_results(shard_set->size());

    auto [knn_node, knn] = TryPopHnswKnnNode(search_algo, params->index);

    if (!knn) {
      auto ranges = search_algo.CollectVectorRangeNodes();
      if (ranges.size() > 1 &&
          std::any_of(ranges.begin(), ranges.end(), [&](const search::AstVectorRangeNode* r) {
            return GlobalHnswIndexRegistry::Instance().Exist(params->index, r->field);
          })) {
        return builder->SendError(
            "Combining multiple VECTOR_RANGE clauses with an HNSW index is not supported");
      }
    }

    if (knn) {
      if (!AggregateHnswKnn(cmd_cntx, params.value(), search_algo, knn, query_results))
        return;
    } else if (auto* vr = search_algo.GetVectorRangeNode(); vr) {
      if (GlobalHnswIndexRegistry::Instance().Exist(params->index, vr->field)) {
        if (!AggregateHnswRange(cmd_cntx, params.value(), search_algo, vr, query_results))
          return;
      } else {
        if (vr->epsilon)
          return builder->SendError("EPSILON is supported only for HNSW VECTOR_RANGE");
        AggregateGeneric(cmd_cntx, params.value(), search_algo, query_results);
      }
    } else {
      AggregateGeneric(cmd_cntx, params.value(), search_algo, query_results);
    }

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
        return cmd_cntx->SendError(absl::StrCat("Join query string is too long, max length is ",
                                                max_query_bytes, " bytes"));
      }

      search::QueryParams empty_params;
      if (!search_algos[i + 1].Init(params->joins[i].query, &empty_params)) {
        return cmd_cntx->SendError("Query syntax error in JOIN");
      }
    }

    auto data_for_join = PreprocessDataForJoin(params->index, *params);
    if (!data_for_join) {
      return cmd_cntx->SendError(data_for_join.error());
    }

    // preaggregated_shard_data is preaggregation results per index per shard
    // preaggregated_shard_data[shard_id][i] is the results of index i on shard shard_id
    using JoinDataVector = join::Vector<join::OwnedEntry>;
    std::vector<std::vector<JoinDataVector>> preaggregated_shard_data(
        shard_set->size(), std::vector<JoinDataVector>(indexes_count));
    cmd_cntx->tx()->Execute(
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
    cmd_cntx->tx()->Execute(
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

  if (params->add_scores && IsBM25StdNorm(params->scorer))
    NormalizeAggregateScores(absl::MakeSpan(values));

  std::vector<std::string_view> load_fields;
  if (params->load_fields) {
    load_fields.reserve(params->load_fields->size());
    for (const auto& field : params->load_fields.value()) {
      load_fields.push_back(field.OutputName());
    }
  }

  // Auto-add __score to visible fields when ADDSCORES is set
  static constexpr std::string_view kScoreField = "__score";
  if (params->add_scores &&
      std::find(load_fields.begin(), load_fields.end(), kScoreField) == load_fields.end()) {
    load_fields.push_back(kScoreField);
  }

  auto agg_results = aggregate::Process(std::move(values), load_fields, params->steps);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
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

void CmdFtSynDump(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view index_name = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  atomic_bool index_not_found{true};
  // Store per-shard synonym data
  vector<absl::flat_hash_map<std::string, absl::flat_hash_set<std::string>>> shard_term_groups(
      shard_set->size());

  // Collect synonym data from all shards
  cmd_cntx->tx()->Execute(
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
    rng::sort(sorted_ids);

    for (const auto& id : sorted_ids) {
      rb->SendBulkString(id);
    }
  }
}

void FtConfigHelp(CmdArgParser* parser, CommandContext* cmd_cntx) {
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

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
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

void FtConfigGet(CmdArgParser* parser, CommandContext* cmd_cntx) {
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
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  return rb->SendBulkStrArr(res, CollectionType::MAP);
}

void FtConfigSet(CmdArgParser* parser, CommandContext* cmd_cntx) {
  auto [param, value] = parser->Next<string_view, string_view>();

  if (!parser->Finalize()) {
    cmd_cntx->SendError(parser->TakeError().MakeReply());
    return;
  }

  vector<string> names = config_registry.List(param);
  if (names.size() != 1 ||
      config_registry.GetFlag(names[0])->Filename().find(kCurrentFile) == std::string::npos) {
    return cmd_cntx->SendError("Invalid option name");
  }

  ConfigRegistry::SetResult result = config_registry.Set(param, value);

  const char kErrPrefix[] = "FT.CONFIG SET failed (possibly related to argument '";
  switch (result) {
    case ConfigRegistry::SetResult::OK:
      return cmd_cntx->SendOk();
    case ConfigRegistry::SetResult::UNKNOWN:
      return cmd_cntx->SendError(
          absl::StrCat("Unknown option or number of arguments for CONFIG SET - '", param, "'"),
          kConfigErrType);

    case ConfigRegistry::SetResult::READONLY:
      return cmd_cntx->SendError(absl::StrCat(kErrPrefix, param, "') - can't set immutable config"),
                                 kConfigErrType);

    case ConfigRegistry::SetResult::INVALID:
      return cmd_cntx->SendError(absl::StrCat(kErrPrefix, param, "') - argument can not be set"),
                                 kConfigErrType);
  }
  ABSL_UNREACHABLE();
}

void CmdFtConfig(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto func = parser.MapNext("GET", &FtConfigGet, "SET", &FtConfigSet, "HELP", &FtConfigHelp);

  if (auto err = parser.TakeError(); err) {
    cmd_cntx->SendError("Unknown subcommand");
    return;
  }
  func(&parser, cmd_cntx);
}

void CmdFtSynUpdate(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto [index_name, group_id] = parser.Next<string_view, string>();

  // Redis ignores this parameter. Checked on redis_version:6.2.13
  [[maybe_unused]] bool skip_initial_scan = parser.Check("SKIPINITIALSCAN");

  // Collect terms
  std::vector<std::string_view> terms;
  while (parser.HasNext()) {
    terms.emplace_back(parser.Next());
  }

  if (terms.empty()) {
    return cmd_cntx->SendError("No terms specified");
  }

  if (!parser.Finalize()) {
    return cmd_cntx->SendError(parser.TakeError().MakeReply());
  }

  std::atomic_bool index_not_found{true};

  // Update synonym groups in all shards
  cmd_cntx->tx()->Execute(
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
    return cmd_cntx->SendError(string{index_name} + ": no such index");

  cmd_cntx->rb()->SendOk();
}

void CmdFtDebug(CmdArgParser parser, CommandContext* cmd_cntx) {
  // FT._DEBUG command stub for test compatibility
  // This command is used by integration tests to control internal behavior
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (parser.Check("HELP")) {
    rb->SendSimpleString("FT._DEBUG - Debug command stub (not fully implemented)");
    return;
  }

  // Handle CONTROLLED_VARIABLE subcommand used by tests
  if (parser.Check("CONTROLLED_VARIABLE")) {
    if (parser.Check("SET")) {
      // Consume variable name and value - these are required by the command
      parser.Next();  // variable name
      parser.Next();  // variable value

      RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

      // Just acknowledge the command
      rb->SendOk();
      return;
    }
  }

  // For any other subcommand, just return OK
  rb->SendOk();
}

void CmdFtHybrid(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view index_name = parser.Next();

  auto params = ParseHybridParams(&parser);
  if (SendErrorIfOccurred(params, &parser, cmd_cntx))
    return;

  HybridExecResult exec;
  if (!RunHybridSearch(index_name, &*params, cmd_cntx, /*enable_profile=*/false, &exec))
    return;

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  HybridReply(*params, exec.vsim_metric, exec.total_took, exec.doc_map, rb);
}

#define HFUNC(x) SetHandler(&Cmd##x)

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
      << CI{"FT.INFO", CO::NO_KEY_TRANSACTIONAL | CO::NO_KEY_TX_SPAN_ALL | CO::NO_AUTOJOURNAL,
            -2,        0,
            0,         acl::FT_SEARCH}
             .HFUNC(FtInfo)
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
      << CI{"FT._DEBUG", kReadOnlyMask, -1, 0, 0, acl::FT_SEARCH}.HFUNC(FtDebug)
      << CI{"FT.HYBRID", kReadOnlyMask, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtHybrid);
}

void SearchFamily::Shutdown() {
  shard_set->RunBlockingInParallel([](EngineShard* es) { es->search_indices()->DropAllIndices(); });
}

}  // namespace dfly
