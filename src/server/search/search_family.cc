// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/search_family.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_format.h>

#include <atomic>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/search/search.h"
#include "core/search/vector_utils.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/search/aggregator.h"
#include "server/search/doc_index.h"
#include "server/transaction.h"
#include "src/core/overloaded.h"

namespace dfly {

using namespace std;
using namespace facade;

namespace {

using nonstd::make_unexpected;

template <typename T> using ParseResult = io::Result<T, ErrorReply>;

nonstd::unexpected_type<ErrorReply> CreateSyntaxError(std::string message) {
  return make_unexpected(ErrorReply{std::move(message), kSyntaxErrType});
}

nonstd::unexpected_type<ErrorReply> CreateSyntaxError(std::string_view message) {
  return make_unexpected(ErrorReply{message, kSyntaxErrType});
}

// Send error from parser or result
// Returns false if no errors occured
template <typename T>
bool SendErrorIfOccurred(const ParseResult<T>& result, CmdArgParser* parser,
                         SinkReplyBuilder* builder) {
  if (auto err = parser->Error(); err || !result) {
    builder->SendError(!result ? result.error() : err->MakeReply());
    return true;
  }

  return false;
}

static const set<string_view> kIgnoredOptions = {"WEIGHT", "SEPARATOR"};

bool IsValidJsonPath(string_view path) {
  error_code ec;
  MakeJsonPathExpr(path, ec);
  return !ec;
}

search::SchemaField::VectorParams ParseVectorParams(CmdArgParser* parser) {
  search::SchemaField::VectorParams params{};

  params.use_hnsw = parser->MapNext("HNSW", true, "FLAT", false);
  const size_t num_args = parser->Next<size_t>();

  for (size_t i = 0; i * 2 < num_args; i++) {
    if (parser->Check("DIM", &params.dim)) {
    } else if (parser->Check("DISTANCE_METRIC")) {
      params.sim = parser->MapNext("L2", search::VectorSimilarity::L2, "COSINE",
                                   search::VectorSimilarity::COSINE);
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

    break;
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
  return std::make_pair(search::SchemaField::TEXT, std::monostate{});
}

ParsedSchemaField ParseNumeric(CmdArgParser* parser) {
  return std::make_pair(search::SchemaField::NUMERIC, std::monostate{});
}

// Vector fields include: {algorithm} num_args args...
ParsedSchemaField ParseVector(CmdArgParser* parser) {
  auto vector_params = ParseVectorParams(parser);

  if (parser->HasError()) {
    auto err = *parser->Error();
    VLOG(1) << "Could not parse vector param " << err.index;
    return CreateSyntaxError("Parse error of vector parameters"sv);
  }

  if (vector_params.dim == 0) {
    return CreateSyntaxError("Knn vector dimension cannot be zero"sv);
  }
  return std::make_pair(search::SchemaField::VECTOR, vector_params);
}

// ON HASH | JSON
ParseResult<bool> ParseOnOption(CmdArgParser* parser, DocIndex* index) {
  index->type = parser->MapNext("HASH"sv, DocIndex::HASH, "JSON"sv, DocIndex::JSON);
  return true;
}

// PREFIX count prefix [prefix ...]
ParseResult<bool> ParsePrefix(CmdArgParser* parser, DocIndex* index) {
  if (!parser->Check("1")) {
    return CreateSyntaxError("Multiple prefixes are not supported"sv);
  }
  index->prefix = parser->Next<std::string>();
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

// SCHEMA field [AS alias] type [flags...]
ParseResult<bool> ParseSchema(CmdArgParser* parser, DocIndex* index) {
  auto& schema = index->schema;

  while (parser->HasNext()) {
    string_view field = parser->Next();
    string_view field_alias = field;

    // Verify json path is correct
    if (index->type == DocIndex::JSON && !IsValidJsonPath(field)) {
      return CreateSyntaxError(absl::StrCat("Bad json path: "sv, field));
    }

    // AS [alias]
    parser->Check("AS", &field_alias);

    // Determine type
    using search::SchemaField;
    auto parsed_params = parser->MapNext("TAG"sv, &ParseTag, "TEXT"sv, &ParseText, "NUMERIC"sv,
                                         &ParseNumeric, "VECTOR"sv, &ParseVector)(parser);
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
        break;
      }

      flags |= *flag;
    }

    // Skip all trailing ignored parameters
    while (kIgnoredOptions.count(parser->Peek()) > 0)
      parser->Skip(2);

    schema.fields[field] = {field_type, flags, string{field_alias}, params};
  }

  // Build field name mapping table
  for (const auto& [field_ident, field_info] : schema.fields)
    schema.field_names[field_info.short_name] = field_ident;

  return false;
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

ParseResult<DocIndex> ParseCreateParams(CmdArgParser* parser) {
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

std::string_view ParseFieldWithAtSign(CmdArgParser* parser) {
  std::string_view field = parser->Next();
  if (absl::StartsWith(field, "@"sv)) {
    field.remove_prefix(1);  // remove leading @
  } else {
    // Temporary warning until we can throw an error. Log every 30 seconds
    LOG_EVERY_T(WARNING, 30) << "bad arguments: Field name '" << field
                             << "' should start with '@'. '@" << field << "' is expected";
  }
  return field;
}

void ParseLoadFields(CmdArgParser* parser, std::optional<SearchFieldsList>* load_fields) {
  // TODO: Change to num_strings. In Redis strings number is expected. For example: LOAD 3 $.a AS a
  size_t num_fields = parser->Next<size_t>();
  if (!load_fields->has_value()) {
    load_fields->emplace();
  }

  while (parser->HasNext() && num_fields--) {
    string_view str = parser->Next();

    if (absl::StartsWith(str, "@"sv)) {
      str.remove_prefix(1);  // remove leading @
    }

    StringOrView name = StringOrView::FromString(std::string{str});
    if (parser->Check("AS")) {
      load_fields->value().emplace_back(name, true,
                                        StringOrView::FromString(parser->Next<std::string>()));
    } else {
      load_fields->value().emplace_back(name, true);
    }
  }
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

  while (parser->HasNext()) {
    // [LIMIT offset total]
    if (parser->Check("LIMIT")) {
      params.limit_offset = parser->Next<size_t>();
      params.limit_total = parser->Next<size_t>();
    } else if (parser->Check("LOAD")) {
      if (params.return_fields) {
        return CreateSyntaxError("LOAD cannot be applied after RETURN"sv);
      }

      ParseLoadFields(parser, &params.load_fields);
    } else if (parser->Check("RETURN")) {
      if (params.load_fields) {
        return CreateSyntaxError("RETURN cannot be applied after LOAD"sv);
      }

      // RETURN {num} [{ident} AS {name}...]
      /* TODO: Change to num_strings. In Redis strings number is expected. For example: RETURN 3 $.a
       * AS a */
      size_t num_fields = parser->Next<size_t>();
      params.return_fields.emplace();
      while (parser->HasNext() && params.return_fields->size() < num_fields) {
        StringOrView name = StringOrView::FromString(parser->Next<std::string>());

        if (parser->Check("AS")) {
          params.return_fields->emplace_back(std::move(name), true,
                                             StringOrView::FromString(parser->Next<std::string>()));
        } else {
          params.return_fields->emplace_back(std::move(name), true);
        }
      }
    } else if (parser->Check("NOCONTENT")) {  // NOCONTENT
      params.no_content = true;
    } else if (parser->Check("PARAMS")) {  // [PARAMS num(ignored) name(ignored) knn_vector]
      params.query_params = ParseQueryParams(parser);
    } else if (parser->Check("SORTBY")) {
      params.sort_option =
          search::SortOption{parser->Next<std::string>(), bool(parser->Check("DESC"))};
    } else {
      // Unsupported parameters are ignored for now
      parser->Skip(1);
    }
  }

  return params;
}

std::optional<aggregate::SortParams> ParseAggregatorSortParams(CmdArgParser* parser) {
  using SortOrder = aggregate::SortParams::SortOrder;

  size_t strings_num = parser->Next<size_t>();

  aggregate::SortParams sort_params;
  sort_params.fields.reserve(strings_num / 2);

  while (parser->HasNext() && strings_num > 0) {
    // TODO: Throw an error if the field has no '@' sign at the beginning
    std::string_view parsed_field = ParseFieldWithAtSign(parser);
    strings_num--;

    SortOrder sord_order = SortOrder::ASC;
    if (strings_num > 0) {
      auto order = parser->TryMapNext("ASC", SortOrder::ASC, "DESC", SortOrder::DESC);
      if (order) {
        sord_order = order.value();
        strings_num--;
      }
    }

    sort_params.fields.emplace_back(parsed_field, sord_order);
  }

  if (strings_num) {
    return std::nullopt;
  }

  if (parser->Check("MAX")) {
    sort_params.max = parser->Next<size_t>();
  }

  return sort_params;
}

ParseResult<AggregateParams> ParseAggregatorParams(CmdArgParser* parser) {
  AggregateParams params;
  tie(params.index, params.query) = parser->Next<string_view, string_view>();

  // Parse LOAD count field [field ...]
  // LOAD options are at the beginning of the query, so we need to parse them first
  while (parser->HasNext() && parser->Check("LOAD")) {
    ParseLoadFields(parser, &params.load_fields);
  }

  while (parser->HasNext()) {
    // GROUPBY nargs property [property ...]
    if (parser->Check("GROUPBY")) {
      size_t num_fields = parser->Next<size_t>();

      std::vector<std::string> fields;
      fields.reserve(num_fields);
      while (parser->HasNext() && num_fields > 0) {
        auto parsed_field = ParseFieldWithAtSign(parser);

        /*
        TODO: Throw an error if the field has no '@' sign at the beginning
        if (!parsed_field) {
          builder->SendError(absl::StrCat("bad arguments for GROUPBY: Unknown property '", field,
                                       "'. Did you mean '@", field, "`?"));
          return nullopt;
        } */

        fields.emplace_back(parsed_field);
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
        return CreateSyntaxError("bad arguments for SORTBY: specified invalid number of strings"sv);
      }

      params.steps.push_back(aggregate::MakeSortStep(std::move(sort_params).value()));
      continue;
    }

    // LIMIT
    if (parser->Check("LIMIT")) {
      auto [offset, num] = parser->Next<size_t, size_t>();
      params.steps.push_back(aggregate::MakeLimitStep(offset, num));
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

    return CreateSyntaxError(absl::StrCat("Unknown clause: ", parser->Peek()));
  }

  return params;
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

void SearchReply(const SearchParams& params, std::optional<search::AggregationInfo> agg_info,
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

  size_t size = docs.size();
  bool should_add_score_field = false;

  if (agg_info) {
    size = std::min(size, agg_info->limit);
    total_hits = std::min(total_hits, agg_info->limit);
    should_add_score_field = params.ShouldReturnField(agg_info->alias);

    using Comparator = bool (*)(const SerializedSearchDoc*, const SerializedSearchDoc*);
    auto comparator =
        !agg_info->descending
            ? static_cast<Comparator>([](const SerializedSearchDoc* l,
                                         const SerializedSearchDoc* r) { return *l < *r; })
            : [](const SerializedSearchDoc* l, const SerializedSearchDoc* r) { return *r < *l; };

    const size_t prefix_size_to_sort = std::min(params.limit_offset + params.limit_total, size);
    if (prefix_size_to_sort == docs.size()) {
      std::sort(docs.begin(), docs.end(), std::move(comparator));
    } else {
      std::partial_sort(docs.begin(), docs.begin() + prefix_size_to_sort, docs.end(),
                        std::move(comparator));
    }
  }

  const size_t offset = std::min(params.limit_offset, size);
  const size_t limit = std::min(size - offset, params.limit_total);

  const bool reply_with_ids_only = params.IdsOnly();
  const size_t reply_size = reply_with_ids_only ? (limit + 1) : (limit * 2 + 1);

  facade::SinkReplyBuilder::ReplyAggregator agg{builder};

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(reply_size);
  rb->SendLong(total_hits);

  const size_t end = offset + limit;
  for (size_t i = offset; i < end; i++) {
    if (reply_with_ids_only) {
      rb->SendBulkString(docs[i]->key);
      continue;
    }

    if (should_add_score_field && holds_alternative<float>(docs[i]->score))
      docs[i]->values[agg_info->alias] = absl::StrCat(get<float>(docs[i]->score));

    SendSerializedDoc(*docs[i], builder);
  }
}

}  // namespace

void SearchFamily::FtCreate(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* builder = cmd_cntx.rb;
  if (cmd_cntx.conn_cntx->conn_state.db_index != 0) {
    return builder->SendError("Cannot create index on db != 0"sv);
  }

  CmdArgParser parser{args};
  string_view idx_name = parser.Next();

  auto parsed_index = ParseCreateParams(&parser);
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

  auto idx_ptr = make_shared<DocIndex>(std::move(parsed_index).value());
  cmd_cntx.tx->Execute(
      [idx_name, idx_ptr](auto* tx, auto* es) {
        es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, idx_ptr);
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
  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

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
    es->search_indices()->DropIndex(idx_name);
    es->search_indices()->InitIndex(tx->GetOpArgs(es), idx_name, index_info);
    return OpStatus::OK;
  };
  cmd_cntx.tx->Execute(upd_cb, true);

  builder->SendOk();
}

void SearchFamily::FtDropIndex(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view idx_name = ArgS(args, 0);
  // TODO: Handle optional DD param

  atomic_uint num_deleted{0};
  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (es->search_indices()->DropIndex(idx_name))
      num_deleted.fetch_add(1);
    return OpStatus::OK;
  });

  DCHECK(num_deleted == 0u || num_deleted == shard_set->size());
  if (num_deleted == 0u)
    return cmd_cntx.rb->SendError("-Unknown Index name");
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
    return rb->SendError("Unknown Index name");

  DCHECK(infos.front().base_index.schema.fields.size() ==
         infos.back().base_index.schema.fields.size());

  size_t total_num_docs = 0;
  for (const auto& info : infos)
    total_num_docs += info.num_docs;

  const auto& info = infos.front();
  const auto& schema = info.base_index.schema;

  rb->StartCollection(4, RedisReplyBuilder::MAP);

  rb->SendSimpleString("index_name");
  rb->SendSimpleString(idx_name);

  rb->SendSimpleString("index_definition");
  {
    rb->StartCollection(2, RedisReplyBuilder::MAP);
    rb->SendSimpleString("key_type");
    rb->SendSimpleString(info.base_index.type == DocIndex::JSON ? "JSON" : "HASH");
    rb->SendSimpleString("prefix");
    rb->SendSimpleString(info.base_index.prefix);
  }

  rb->SendSimpleString("attributes");
  rb->StartArray(schema.fields.size());
  for (const auto& [field_ident, field_info] : schema.fields) {
    vector<string> info;

    string_view base[] = {"identifier"sv, string_view{field_ident},
                          "attribute",    field_info.short_name,
                          "type"sv,       SearchFieldTypeToString(field_info.type)};
    info.insert(info.end(), base, base + ABSL_ARRAYSIZE(base));

    if (field_info.flags & search::SchemaField::NOINDEX)
      info.push_back("NOINDEX");

    if (field_info.flags & search::SchemaField::SORTABLE)
      info.push_back("SORTABLE");

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

void SearchFamily::FtSearch(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  string_view index_name = parser.Next();
  string_view query_str = parser.Next();

  auto* builder = cmd_cntx.rb;
  auto params = ParseSearchParams(&parser);
  if (SendErrorIfOccurred(params, &parser, builder))
    return;

  search::SearchAlgorithm search_algo;
  search::SortOption* sort_opt = params->sort_option.has_value() ? &*params->sort_option : nullptr;
  if (!search_algo.Init(query_str, &params->query_params, sort_opt))
    return builder->SendError("Query syntax error");

  // Because our coordinator thread may not have a shard, we can't check ahead if the index exists.
  atomic<bool> index_not_found{false};
  vector<SearchResult> docs(shard_set->size());

  cmd_cntx.tx->ScheduleSingleHop([&](Transaction* t, EngineShard* es) {
    if (auto* index = es->search_indices()->GetIndex(index_name); index)
      docs[es->shard_id()] = index->Search(t->GetOpArgs(es), *params, &search_algo);
    else
      index_not_found.store(true, memory_order_relaxed);
    return OpStatus::OK;
  });

  if (index_not_found.load())
    return builder->SendError(string{index_name} + ": no such index");

  for (const auto& res : docs) {
    if (res.error)
      return builder->SendError(*res.error);
  }

  SearchReply(*params, search_algo.GetAggregationInfo(), absl::MakeSpan(docs), builder);
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
  search::SortOption* sort_opt = params->sort_option.has_value() ? &*params->sort_option : nullptr;
  if (!search_algo.Init(query_str, &params->query_params, sort_opt))
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
    SearchReply(*params, search_algo.GetAggregationInfo(), absl::MakeSpan(search_results), rb);
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
      for (size_t j = i + 1; j < events.size(); j++) {
        if (events[j].depth == event.depth)
          break;
        if (events[j].depth == event.depth + 1)
          children++;
      }

      if (children > 0)
        rb->StartArray(2);

      rb->SendSimpleString(
          absl::StrFormat("t=%-10u n=%-10u %s", event.micros, event.num_processed, event.descr));

      if (children > 0)
        rb->StartArray(children);
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
      shard_results[es->shard_id()] = nonstd::make_unexpected(ErrorReply("-Unknown Index name"));

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

  search::SearchAlgorithm search_algo;
  if (!search_algo.Init(params->query, &params->params, nullptr))
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
  std::vector<aggregate::DocValues> values;
  for (auto& sub_results : query_results) {
    for (auto& docs : sub_results) {
      aggregate::DocValues doc_value;
      for (auto& doc : docs) {
        doc_value[doc.first] = std::move(doc.second);
      }
      values.push_back(std::move(doc_value));
    }
  }

  std::vector<std::string_view> load_fields;
  if (params->load_fields) {
    load_fields.reserve(params->load_fields->size());
    for (const auto& field : params->load_fields.value()) {
      load_fields.push_back(field.GetShortName());
    }
  }

  auto agg_results = aggregate::Process(std::move(values), load_fields, params->steps);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  auto sortable_value_sender = SortableValueSender(rb);

  const size_t result_size = agg_results.values.size();
  rb->StartArray(result_size + 1);
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

#define HFUNC(x) SetHandler(&SearchFamily::x)

// Redis search is a module. Therefore we introduce dragonfly extension search
// to set as the default for the search family of commands. More sensible defaults,
// should also be considered in the future

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  // Disable journaling, because no-key-transactional enables it by default
  const uint32_t kReadOnlyMask =
      CO::NO_KEY_TRANSACTIONAL | CO::NO_KEY_TX_SPAN_ALL | CO::NO_AUTOJOURNAL;

  registry->StartFamily();
  *registry << CI{"FT.CREATE", CO::WRITE | CO::GLOBAL_TRANS, -2, 0, 0, acl::FT_SEARCH}.HFUNC(
                   FtCreate)
            << CI{"FT.ALTER", CO::WRITE | CO::GLOBAL_TRANS, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtAlter)
            << CI{"FT.DROPINDEX", CO::WRITE | CO::GLOBAL_TRANS, -2, 0, 0, acl::FT_SEARCH}.HFUNC(
                   FtDropIndex)
            << CI{"FT.INFO", kReadOnlyMask, 2, 0, 0, acl::FT_SEARCH}.HFUNC(FtInfo)
            // Underscore same as in RediSearch because it's "temporary" (long time already)
            << CI{"FT._LIST", kReadOnlyMask, 1, 0, 0, acl::FT_SEARCH}.HFUNC(FtList)
            << CI{"FT.SEARCH", kReadOnlyMask, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtSearch)
            << CI{"FT.AGGREGATE", kReadOnlyMask, -3, 0, 0, acl::FT_SEARCH}.HFUNC(FtAggregate)
            << CI{"FT.PROFILE", kReadOnlyMask, -4, 0, 0, acl::FT_SEARCH}.HFUNC(FtProfile)
            << CI{"FT.TAGVALS", kReadOnlyMask, 3, 0, 0, acl::FT_SEARCH}.HFUNC(FtTagVals);
}

}  // namespace dfly
