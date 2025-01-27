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

std::optional<search::SchemaField::TagParams> ParseTagParams(CmdArgParser* parser,
                                                             SinkReplyBuilder* builder) {
  search::SchemaField::TagParams params{};
  while (parser->HasNext()) {
    if (parser->Check("SEPARATOR")) {
      std::string_view separator = parser->NextOrDefault();
      if (separator.size() != 1) {
        builder->SendError(
            absl::StrCat("Tag separator must be a single character. Got `", separator, "`"),
            kSyntaxErrType);
        return std::nullopt;
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

optional<search::Schema> ParseSchemaOrReply(DocIndex::DataType type, CmdArgParser parser,
                                            SinkReplyBuilder* builder) {
  search::Schema schema;

  while (parser.HasNext()) {
    string_view field = parser.Next();
    string_view field_alias = field;

    // Verify json path is correct
    if (type == DocIndex::JSON && !IsValidJsonPath(field)) {
      builder->SendError("Bad json path: " + string{field});
      return nullopt;
    }

    // AS [alias]
    parser.Check("AS", &field_alias);

    // Determine type
    using search::SchemaField;
    auto type = parser.MapNext("TAG", SchemaField::TAG, "TEXT", SchemaField::TEXT, "NUMERIC",
                               SchemaField::NUMERIC, "VECTOR", SchemaField::VECTOR);
    if (auto err = parser.Error(); err) {
      builder->SendError(err->MakeReply());
      return nullopt;
    }

    // Tag fields include: [separator char] [casesensitive]
    // Vector fields include: {algorithm} num_args args...
    search::SchemaField::ParamsVariant params(monostate{});
    if (type == search::SchemaField::TAG) {
      auto tag_params = ParseTagParams(&parser, builder);
      if (!tag_params) {
        return std::nullopt;
      }
      params = tag_params.value();
    } else if (type == search::SchemaField::VECTOR) {
      auto vector_params = ParseVectorParams(&parser);
      if (parser.HasError()) {
        auto err = *parser.Error();
        VLOG(1) << "Could not parse vector param " << err.index;
        builder->SendError("Parse error of vector parameters", kSyntaxErrType);
        return nullopt;
      }

      if (vector_params.dim == 0) {
        builder->SendError("Knn vector dimension cannot be zero", kSyntaxErrType);
        return nullopt;
      }
      params = vector_params;
    }

    // Flags: check for SORTABLE and NOINDEX
    uint8_t flags = 0;
    while (parser.HasNext()) {
      if (parser.Check("NOINDEX")) {
        flags |= search::SchemaField::NOINDEX;
        continue;
      }

      if (parser.Check("SORTABLE")) {
        flags |= search::SchemaField::SORTABLE;
        continue;
      }

      break;
    }

    // Skip all trailing ignored parameters
    while (kIgnoredOptions.count(parser.Peek()) > 0)
      parser.Skip(2);

    schema.fields[field] = {type, flags, string{field_alias}, std::move(params)};
  }

  // Build field name mapping table
  for (const auto& [field_ident, field_info] : schema.fields)
    schema.field_names[field_info.short_name] = field_ident;

  if (auto err = parser.Error(); err) {
    builder->SendError(err->MakeReply());
    return nullopt;
  }

  return schema;
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

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

  while (num_fields--) {
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

optional<SearchParams> ParseSearchParamsOrReply(CmdArgParser* parser, SinkReplyBuilder* builder) {
  SearchParams params;

  while (parser->HasNext()) {
    // [LIMIT offset total]
    if (parser->Check("LIMIT")) {
      params.limit_offset = parser->Next<size_t>();
      params.limit_total = parser->Next<size_t>();
    } else if (parser->Check("LOAD")) {
      if (params.return_fields) {
        builder->SendError("LOAD cannot be applied after RETURN");
        return std::nullopt;
      }

      ParseLoadFields(parser, &params.load_fields);
    } else if (parser->Check("RETURN")) {
      if (params.load_fields) {
        builder->SendError("RETURN cannot be applied after LOAD");
        return std::nullopt;
      }

      // RETURN {num} [{ident} AS {name}...]
      /* TODO: Change to num_strings. In Redis strings number is expected. For example: RETURN 3 $.a
       * AS a */
      size_t num_fields = parser->Next<size_t>();
      params.return_fields.emplace();
      while (params.return_fields->size() < num_fields) {
        StringOrView name = StringOrView::FromString(parser->Next<std::string>());

        if (parser->Check("AS")) {
          params.return_fields->emplace_back(std::move(name), true,
                                             StringOrView::FromString(parser->Next<std::string>()));
        } else {
          params.return_fields->emplace_back(std::move(name), true);
        }
      }
    } else if (parser->Check("NOCONTENT")) {  // NOCONTENT
      params.load_fields.emplace();
      params.return_fields.emplace();
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

  if (auto err = parser->Error(); err) {
    builder->SendError(err->MakeReply());
    return nullopt;
  }

  return params;
}

std::optional<aggregate::SortParams> ParseAggregatorSortParams(CmdArgParser* parser) {
  using SordOrder = aggregate::SortParams::SortOrder;

  size_t strings_num = parser->Next<size_t>();

  aggregate::SortParams sort_params;
  sort_params.fields.reserve(strings_num / 2);

  while (parser->HasNext() && strings_num > 0) {
    // TODO: Throw an error if the field has no '@' sign at the beginning
    std::string_view parsed_field = ParseFieldWithAtSign(parser);
    strings_num--;

    SordOrder sord_order = SordOrder::ASC;
    if (strings_num > 0) {
      auto order = parser->TryMapNext("ASC", SordOrder::ASC, "DESC", SordOrder::DESC);
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

optional<AggregateParams> ParseAggregatorParamsOrReply(CmdArgParser parser,
                                                       SinkReplyBuilder* builder) {
  AggregateParams params;
  tie(params.index, params.query) = parser.Next<string_view, string_view>();

  // Parse LOAD count field [field ...]
  // LOAD options are at the beginning of the query, so we need to parse them first
  while (parser.HasNext() && parser.Check("LOAD")) {
    ParseLoadFields(&parser, &params.load_fields);
  }

  while (parser.HasNext()) {
    // GROUPBY nargs property [property ...]
    if (parser.Check("GROUPBY")) {
      size_t num_fields = parser.Next<size_t>();

      std::vector<std::string> fields;
      fields.reserve(num_fields);
      while (num_fields > 0 && parser.HasNext()) {
        auto parsed_field = ParseFieldWithAtSign(&parser);

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
      while (parser.Check("REDUCE")) {
        using RF = aggregate::ReducerFunc;
        auto func_name =
            parser.TryMapNext("COUNT", RF::COUNT, "COUNT_DISTINCT", RF::COUNT_DISTINCT, "SUM",
                              RF::SUM, "AVG", RF::AVG, "MAX", RF::MAX, "MIN", RF::MIN);

        if (!func_name) {
          builder->SendError(absl::StrCat("reducer function ", parser.Next(), " not found"));
          return nullopt;
        }

        auto func = aggregate::FindReducerFunc(*func_name);
        auto nargs = parser.Next<size_t>();

        string source_field;
        if (nargs > 0) {
          source_field = ParseField(&parser);
        }

        parser.ExpectTag("AS");
        string result_field = parser.Next<string>();

        reducers.push_back(
            aggregate::Reducer{std::move(source_field), std::move(result_field), std::move(func)});
      }

      params.steps.push_back(aggregate::MakeGroupStep(std::move(fields), std::move(reducers)));
      continue;
    }

    // SORTBY nargs
    if (parser.Check("SORTBY")) {
      auto sort_params = ParseAggregatorSortParams(&parser);
      if (!sort_params) {
        builder->SendError("bad arguments for SORTBY: specified invalid number of strings");
        return nullopt;
      }

      params.steps.push_back(aggregate::MakeSortStep(std::move(sort_params).value()));
      continue;
    }

    // LIMIT
    if (parser.Check("LIMIT")) {
      auto [offset, num] = parser.Next<size_t, size_t>();
      params.steps.push_back(aggregate::MakeLimitStep(offset, num));
      continue;
    }

    // PARAMS
    if (parser.Check("PARAMS")) {
      params.params = ParseQueryParams(&parser);
      continue;
    }

    if (parser.Check("LOAD")) {
      builder->SendError("LOAD cannot be applied after projectors or reducers");
      return nullopt;
    }

    builder->SendError(absl::StrCat("Unknown clause: ", parser.Peek()));
    return nullopt;
  }

  if (auto err = parser.Error(); err) {
    builder->SendError(err->MakeReply());
    return nullopt;
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

void ReplyWithResults(const SearchParams& params, absl::Span<SearchResult> results,
                      SinkReplyBuilder* builder) {
  size_t total_count = 0;
  for (const auto& shard_docs : results)
    total_count += shard_docs.total_hits;

  size_t result_count =
      min(total_count - min(total_count, params.limit_offset), params.limit_total);

  facade::SinkReplyBuilder::ReplyAggregator agg{builder};

  bool ids_only = params.IdsOnly();
  size_t reply_size = ids_only ? (result_count + 1) : (result_count * 2 + 1);

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(reply_size);
  rb->SendLong(total_count);

  size_t sent = 0;
  size_t to_skip = params.limit_offset;
  for (const auto& shard_docs : results) {
    for (const auto& serialized_doc : shard_docs.docs) {
      // Scoring is not implemented yet, so we just cut them in the order they were retrieved
      if (to_skip > 0) {
        to_skip--;
        continue;
      }

      if (sent++ >= result_count)
        return;

      if (ids_only)
        rb->SendBulkString(serialized_doc.key);
      else
        SendSerializedDoc(serialized_doc, builder);
    }
  }
}

void ReplySorted(search::AggregationInfo agg, const SearchParams& params,
                 absl::Span<SearchResult> results, SinkReplyBuilder* builder) {
  size_t total = 0;
  vector<SerializedSearchDoc*> docs;
  for (auto& shard_results : results) {
    total += shard_results.total_hits;
    for (auto& doc : shard_results.docs) {
      docs.push_back(&doc);
    }
  }

  size_t agg_limit = agg.limit.value_or(total);
  size_t prefix = min(params.limit_offset + params.limit_total, agg_limit);

  partial_sort(docs.begin(), docs.begin() + min(docs.size(), prefix), docs.end(),
               [desc = agg.descending](const auto* l, const auto* r) {
                 return desc ? (*l >= *r) : (*l < *r);
               });

  docs.resize(min(docs.size(), agg_limit));

  size_t start_idx = min(params.limit_offset, docs.size());
  size_t result_count = min(docs.size() - start_idx, params.limit_total);
  bool ids_only = params.IdsOnly();
  size_t reply_size = ids_only ? (result_count + 1) : (result_count * 2 + 1);

  // Clear score alias if it's excluded from return values
  if (!params.ShouldReturnField(agg.alias))
    agg.alias = "";

  facade::SinkReplyBuilder::ReplyAggregator agg_reply{builder};
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(reply_size);
  rb->SendLong(min(total, agg_limit));
  for (auto* doc : absl::MakeSpan(docs).subspan(start_idx, result_count)) {
    if (ids_only) {
      rb->SendBulkString(doc->key);
      continue;
    }

    if (!agg.alias.empty() && holds_alternative<float>(doc->score))
      doc->values[agg.alias] = absl::StrCat(get<float>(doc->score));

    SendSerializedDoc(*doc, builder);
  }
}

}  // namespace

void SearchFamily::FtCreate(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* builder = cmd_cntx.rb;
  if (cmd_cntx.conn_cntx->conn_state.db_index != 0) {
    return builder->SendError("Cannot create index on db != 0"sv);
  }

  DocIndex index{};

  CmdArgParser parser{args};
  string_view idx_name = parser.Next();

  while (parser.HasNext()) {
    // ON HASH | JSON
    if (parser.Check("ON")) {
      index.type = parser.MapNext("HASH"sv, DocIndex::HASH, "JSON"sv, DocIndex::JSON);
      continue;
    }

    // PREFIX count prefix [prefix ...]
    if (parser.Check("PREFIX")) {
      if (size_t num = parser.Next<size_t>(); num != 1)
        return builder->SendError("Multiple prefixes are not supported");
      index.prefix = string(parser.Next());
      continue;
    }

    // STOWORDS count [words...]
    if (parser.Check("STOPWORDS")) {
      index.options.stopwords.clear();
      for (size_t num = parser.Next<size_t>(); num > 0; num--)
        index.options.stopwords.emplace(parser.Next());
      continue;
    }

    // SCHEMA
    if (parser.Check("SCHEMA")) {
      auto schema = ParseSchemaOrReply(index.type, parser.Tail(), builder);
      if (!schema)
        return;
      index.schema = std::move(*schema);
      break;  // SCHEMA always comes last
    }

    // Unsupported parameters are ignored for now
    parser.Skip(1);
  }

  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

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

  auto idx_ptr = make_shared<DocIndex>(std::move(index));
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
  optional<search::Schema> new_fields = ParseSchemaOrReply(index_info->type, parser, builder);
  if (!new_fields) {
    cmd_cntx.tx->Conclude();
    return;
  }

  LOG(INFO) << "Adding "
            << DocIndexInfo{.base_index = DocIndex{.schema = *new_fields}}.BuildRestoreCommand();

  // Merge schemas
  search::Schema& schema = index_info->schema;
  schema.fields.insert(new_fields->fields.begin(), new_fields->fields.end());
  schema.field_names.insert(new_fields->field_names.begin(), new_fields->field_names.end());

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
  auto params = ParseSearchParamsOrReply(&parser, builder);
  if (!params.has_value())
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

  if (auto agg = search_algo.HasAggregation(); agg)
    ReplySorted(*agg, *params, absl::MakeSpan(docs), builder);
  else
    ReplyWithResults(*params, absl::MakeSpan(docs), builder);
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

  optional<SearchParams> params = ParseSearchParamsOrReply(&parser, rb);
  if (!params.has_value())
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
    auto agg = search_algo.HasAggregation();
    if (agg) {
      ReplySorted(*agg, *params, absl::MakeSpan(search_results), rb);
    } else {
      ReplyWithResults(*params, absl::MakeSpan(search_results), rb);
    }
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
  auto* builder = cmd_cntx.rb;
  const auto params = ParseAggregatorParamsOrReply(args, builder);
  if (!params)
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
