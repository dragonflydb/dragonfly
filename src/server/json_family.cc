// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/mergepatch/mergepatch.hpp>

#include "base/flags.h"
#include "base/logging.h"
#include "core/flatbuffers.h"
#include "core/json/json_object.h"
#include "core/json/path.h"
#include "facade/cmd_arg_parser.h"
#include "facade/op_status.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/detail/wrapped_json_path.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/search/doc_index.h"
#include "server/string_family.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"

ABSL_FLAG(bool, jsonpathv2, true,
          "If true uses Dragonfly jsonpath implementation, "
          "otherwise uses legacy jsoncons implementation.");
ABSL_FLAG(bool, experimental_flat_json, false, "If true uses flat json implementation.");

namespace dfly {

using namespace std;
using namespace jsoncons;
using facade::CmdArgParser;
using facade::kSyntaxErrType;

using JsonExpression = jsonpath::jsonpath_expression<JsonType>;
using JsonReplaceVerify = std::function<void(JsonType&)>;
using CI = CommandId;

namespace {

template <typename T> using ParseResult = io::Result<T, std::string>;

ParseResult<JsonExpression> ParseJsonPathAsExpression(std::string_view path) {
  std::error_code ec;
  JsonExpression res = MakeJsonPathExpr(path, ec);
  if (ec)
    return nonstd::make_unexpected(kSyntaxErr);
  return res;
}

ParseResult<WrappedJsonPath> ParseJsonPath(StringOrView path, bool is_legacy_mode_path) {
  if (absl::GetFlag(FLAGS_jsonpathv2)) {
    auto path_result = json::ParsePath(path.view());
    if (!path_result) {
      VLOG(1) << "Invalid Json path: " << path << ' ' << path_result.error() << std::endl;
      return nonstd::make_unexpected(kSyntaxErr);
    }
    return WrappedJsonPath{std::move(path_result).value(), std::move(path), is_legacy_mode_path};
  }

  auto expr_result = ParseJsonPathAsExpression(path.view());
  if (!expr_result) {
    VLOG(1) << "Invalid Json path: " << path << ' ' << expr_result.error() << std::endl;
    return nonstd::make_unexpected(kSyntaxErr);
  }
  return WrappedJsonPath{std::move(expr_result).value(), std::move(path), is_legacy_mode_path};
}

ParseResult<WrappedJsonPath> ParseJsonPathV1(std::string_view path) {
  if (path.empty() || path == WrappedJsonPath::kV1PathRootElement) {
    return ParseJsonPath(StringOrView::FromView(WrappedJsonPath::kV2PathRootElement), true);
  }

  std::string v2_path = absl::StrCat(
      WrappedJsonPath::kV2PathRootElement, path.front() != '.' && path.front() != '[' ? "." : "",
      path);  // Convert to V2 path; TODO(path.front() != all kinds of symbols)
  return ParseJsonPath(StringOrView::FromString(std::move(v2_path)), true);
}

ParseResult<WrappedJsonPath> ParseJsonPathV2(std::string_view path) {
  return ParseJsonPath(StringOrView::FromView(path), false);
}

bool IsJsonPathV2(std::string_view path) {
  return !path.empty() && path.front() == '$';
}

ParseResult<WrappedJsonPath> ParseJsonPath(std::string_view path) {
  return IsJsonPathV2(path) ? ParseJsonPathV2(path) : ParseJsonPathV1(path);
}

namespace reply_generic {

template <typename I> void Send(I begin, I end, RedisReplyBuilder* rb);

void Send(bool value, RedisReplyBuilder* rb) {
  rb->SendBulkString(value ? "true"sv : "false"sv);
}

void Send(long value, RedisReplyBuilder* rb) {
  rb->SendLong(value);
}

void Send(size_t value, RedisReplyBuilder* rb) {
  rb->SendLong(value);
}

void Send(double value, RedisReplyBuilder* rb) {
  rb->SendDouble(value);
}

void Send(const std::string& value, RedisReplyBuilder* rb) {
  rb->SendBulkString(value);
}

void Send(const std::vector<std::string>& vec, RedisReplyBuilder* rb) {
  Send(vec.begin(), vec.end(), rb);
}

void Send(const JsonType& value, RedisReplyBuilder* rb) {
  if (value.is_double()) {
    Send(value.as_double(), rb);
  } else if (value.is_number()) {
    Send(value.as_integer<long>(), rb);
  } else if (value.is_bool()) {
    rb->SendSimpleString(value.as_bool() ? "true" : "false");
  } else if (value.is_null()) {
    rb->SendNull();
  } else if (value.is_string()) {
    rb->SendBulkString(value.as_string_view());
  } else if (value.is_object()) {
    rb->StartArray(value.size() + 1);
    rb->SendSimpleString("{");
    for (const auto& item : value.object_range()) {
      rb->StartArray(2);
      rb->SendBulkString(item.key());
      Send(item.value(), rb);
    }
  } else if (value.is_array()) {
    rb->StartArray(value.size() + 1);
    rb->SendSimpleString("[");
    for (const auto& item : value.array_range()) {
      Send(item, rb);
    }
  }
}

template <typename T> void Send(const std::optional<T>& opt, RedisReplyBuilder* rb) {
  if (opt.has_value()) {
    Send(opt.value(), rb);
  } else {
    rb->SendNull();
  }
}

template <typename I> void Send(I begin, I end, RedisReplyBuilder* rb) {
  if (begin == end) {
    rb->SendEmptyArray();
  } else {
    if constexpr (is_same_v<decltype(*begin), const string>) {
      rb->SendStringArr(facade::OwnedArgSlice{begin, end});
    } else {
      rb->StartArray(end - begin);
      for (auto i = begin; i != end; ++i) {
        Send(*i, rb);
      }
    }
  }
}

template <typename T> void Send(const JsonCallbackResult<T>& result, RedisReplyBuilder* rb) {
  if (result.ShouldSendNil())
    return rb->SendNull();

  if (result.ShouldSendWrongType())
    return rb->SendError(OpStatus::WRONG_JSON_TYPE);

  if (result.IsV1()) {
    /* The specified path was restricted (JSON legacy mode), then the result consists only of a
     * single value */
    Send(result.AsV1(), rb);
  } else {
    /* The specified path was enhanced (starts with '$'), then the result is an array of multiple
     * values */
    const auto& arr = result.AsV2();
    Send(arr.begin(), arr.end(), rb);
  }
}

template <typename T> void Send(const OpResult<T>& result, RedisReplyBuilder* rb) {
  if (result) {
    Send(result.value(), rb);
  } else {
    rb->SendError(result.status());
  }
}

}  // namespace reply_generic

using OptSize = optional<size_t>;

facade::OpStatus SetJson(const OpArgs& op_args, string_view key, JsonType&& value) {
  auto& db_slice = op_args.GetDbSlice();

  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);

  auto& res = *op_res;

  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, res.it->second);

  if (absl::GetFlag(FLAGS_experimental_flat_json)) {
    flexbuffers::Builder fbb;
    json::FromJsonType(value, &fbb);
    fbb.Finish();
    const auto& buf = fbb.GetBuffer();
    res.it->second.SetJson(buf.data(), buf.size());
  } else {
    res.it->second.SetJson(std::move(value));
  }
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, res.it->second);
  return OpStatus::OK;
}

string JsonTypeToName(const JsonType& val) {
  using namespace std::string_literals;

  if (val.is_null()) {
    return "null"s;
  } else if (val.is_bool()) {
    return "boolean"s;
  } else if (val.is_string()) {
    return "string"s;
  } else if (val.is_int64() || val.is_uint64()) {
    return "integer"s;
  } else if (val.is_number()) {
    return "number"s;
  } else if (val.is_object()) {
    return "object"s;
  } else if (val.is_array()) {
    return "array"s;
  }

  return std::string{};
}

// Use this method on the coordinator thread
std::optional<JsonType> JsonFromString(std::string_view input) {
  return dfly::JsonFromString(input, PMR_NS::get_default_resource());
}

// Use this method on the shard thread
std::optional<JsonType> ShardJsonFromString(std::string_view input) {
  return dfly::JsonFromString(input, CompactObj::memory_resource());
}

OpResult<JsonType*> GetJson(const OpArgs& op_args, string_view key) {
  auto it_res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_JSON);
  if (!it_res.ok())
    return it_res.status();

  JsonType* json_val = it_res.value()->second.GetJson();
  DCHECK(json_val) << "should have a valid JSON object for key " << key;

  return json_val;
}

// Returns the index of the next right bracket
OptSize GetNextIndex(string_view str) {
  size_t current_idx = 0;
  while (current_idx + 1 < str.size()) {
    // ignore escaped character after the backslash (e.g. \').
    if (str[current_idx] == '\\') {
      current_idx += 2;
    } else if (str[current_idx] == '\'' && str[current_idx + 1] == ']') {
      return current_idx;
    } else {
      current_idx++;
    }
  }

  return nullopt;
}

// Encodes special characters when appending token to JSONPointer
struct JsonPointerFormatter {
  void operator()(std::string* out, string_view token) const {
    for (size_t i = 0; i < token.size(); i++) {
      char ch = token[i];
      if (ch == '~') {
        out->append("~0");
      } else if (ch == '/') {
        out->append("~1");
      } else if (ch == '\\') {
        // backslash for encoded another character should remove.
        if (i + 1 < token.size() && token[i + 1] == '\\') {
          out->append(1, '\\');
          i++;
        }
      } else {
        out->append(1, ch);
      }
    }
  }
};

// Returns the JsonPointer of a JsonPath
// e.g. $[a][b][0] -> /a/b/0
string ConvertToJsonPointer(string_view json_path) {
  if (json_path.empty() || json_path[0] != '$') {
    LOG(FATAL) << "Unexpected JSONPath syntax: " << json_path;
  }

  // remove prefix
  json_path.remove_prefix(1);

  // except the supplied string is compatible with JSONPath syntax.
  // Each item in the string is a left bracket followed by
  // numeric or '<key>' and then a right bracket.
  vector<string_view> parts;
  bool invalid_syntax = false;
  while (json_path.size() > 0) {
    bool is_array = false;
    bool is_object = false;

    // check string size is sufficient enough for at least one item.
    if (2 >= json_path.size()) {
      invalid_syntax = true;
      break;
    }

    if (json_path[0] == '[') {
      if (json_path[1] == '\'') {
        is_object = true;
        json_path.remove_prefix(2);
      } else if (isdigit(json_path[1])) {
        is_array = true;
        json_path.remove_prefix(1);
      } else {
        invalid_syntax = true;
        break;
      }
    } else {
      invalid_syntax = true;
      break;
    }

    if (is_array) {
      size_t end_val_idx = json_path.find(']');
      if (end_val_idx == string::npos) {
        invalid_syntax = true;
        break;
      }

      parts.emplace_back(json_path.substr(0, end_val_idx));
      json_path.remove_prefix(end_val_idx + 1);
    } else if (is_object) {
      OptSize end_val_idx = GetNextIndex(json_path);
      if (!end_val_idx) {
        invalid_syntax = true;
        break;
      }

      parts.emplace_back(json_path.substr(0, *end_val_idx));
      json_path.remove_prefix(*end_val_idx + 2);
    } else {
      invalid_syntax = true;
      break;
    }
  }

  if (invalid_syntax) {
    LOG(FATAL) << "Unexpected JSONPath syntax: " << json_path;
  }

  string result{"/"};  // initialize with a leading slash
  result += absl::StrJoin(parts, "/", JsonPointerFormatter());
  return result;
}

string ConvertExpressionToJsonPointer(string_view json_path) {
  if (json_path.empty() || !absl::StartsWith(json_path, "$.")) {
    VLOG(1) << "retrieved malformed JSON path expression: " << json_path;
    return {};
  }

  // remove prefix
  json_path.remove_prefix(2);

  std::string pointer;
  vector<string> splitted = absl::StrSplit(json_path, '.');
  for (auto& it : splitted) {
    if (it.front() == '[' && it.back() == ']') {
      std::string index = it.substr(1, it.size() - 2);
      if (index.empty()) {
        return {};
      }

      for (char ch : index) {
        if (!std::isdigit(ch)) {
          return {};
        }
      }

      pointer += '/' + index;
    } else {
      pointer += '/' + it;
    }
  }

  return pointer;
}

size_t CountJsonFields(const JsonType& j) {
  size_t res = 0;
  json_type type = j.type();
  if (type == json_type::array_value) {
    res += j.size();
    for (const auto& item : j.array_range()) {
      if (item.type() == json_type::array_value || item.type() == json_type::object_value) {
        res += CountJsonFields(item);
      }
    }

  } else if (type == json_type::object_value) {
    res += j.size();
    for (const auto& item : j.object_range()) {
      if (item.value().type() == json_type::array_value ||
          item.value().type() == json_type::object_value) {
        res += CountJsonFields(item.value());
      }
    }

  } else {
    res += 1;
  }

  return res;
}

struct EvaluateOperationOptions {
  bool save_first_result = false;
  bool return_nil_if_key_not_found = false;
};

template <typename T>
OpResult<JsonCallbackResult<T>> JsonEvaluateOperation(const OpArgs& op_args, std::string_view key,
                                                      const WrappedJsonPath& json_path,
                                                      JsonPathEvaluateCallback<T> cb,
                                                      EvaluateOperationOptions options = {}) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (options.return_nil_if_key_not_found && result == OpStatus::KEY_NOTFOUND) {
// GCC 13.1 throws spurious warnings around this code.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    return JsonCallbackResult<T>{true, false, true};  // set legacy mode to return nil
#pragma GCC diagnostic pop
  }

  RETURN_ON_BAD_STATUS(result);
  return json_path.Evaluate<T>(*result, cb, options.save_first_result);
}

template <typename T>
OpResult<JsonCallbackResult<optional<T>>> UpdateEntry(const OpArgs& op_args, std::string_view key,
                                                      const WrappedJsonPath& json_path,
                                                      JsonPathMutateCallback<T> cb,
                                                      JsonReplaceVerify verify_op = {}) {
  auto it_res = op_args.GetDbSlice().FindMutable(op_args.db_cntx, key, OBJ_JSON);
  RETURN_ON_BAD_STATUS(it_res);

  PrimeValue& pv = it_res->it->second;

  JsonType* json_val = pv.GetJson();
  DCHECK(json_val) << "should have a valid JSON object for key '" << key << "' the type for it is '"
                   << pv.ObjType() << "'";

  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv);

  auto mutate_res = json_path.Mutate(json_val, cb);

  // Make sure that we don't have other internal issue with the operation
  if (mutate_res && verify_op) {
    verify_op(*json_val);
  }

  it_res->post_updater.Run();
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);

  RETURN_ON_BAD_STATUS(mutate_res);
  return mutate_res;
}

bool LegacyModeIsEnabled(const std::vector<std::pair<std::string_view, WrappedJsonPath>>& paths) {
  return std::all_of(paths.begin(), paths.end(),
                     [](auto& parsed_path) { return parsed_path.second.IsLegacyModePath(); });
}

OpResult<std::string> OpJsonGet(const OpArgs& op_args, string_view key,
                                const vector<pair<string_view, WrappedJsonPath>>& paths,
                                const std::optional<std::string>& indent,
                                const std::optional<std::string>& new_line,
                                const std::optional<std::string>& space) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  const JsonType& json_entry = *(result.value());
  if (paths.empty()) {
    // this implicitly means that we're using . which
    // means we just brings all values
    return json_entry.to_string();
  }

  json_options options;
  options.spaces_around_comma(spaces_option::no_spaces)
      .spaces_around_colon(spaces_option::no_spaces)
      .object_array_line_splits(line_split_kind::multi_line)
      .indent_size(0)
      .new_line_chars("");

  if (indent) {
    options.indent_size(1);
    options.indent_chars(*indent);
  }

  if (new_line) {
    options.new_line_chars(*new_line);
  }

  if (space) {
    options.after_key_chars(*space);
  }

  const bool legacy_mode_is_enabled = LegacyModeIsEnabled(paths);

  auto cb = [](std::string_view, const JsonType& val) { return val; };

  auto eval_wrapped = [&json_entry, &cb, legacy_mode_is_enabled](
                          const WrappedJsonPath& json_path) -> std::optional<JsonType> {
    auto eval_result = json_path.Evaluate<JsonType>(&json_entry, cb, false, legacy_mode_is_enabled);

    DCHECK(legacy_mode_is_enabled == eval_result.IsV1());

    if (eval_result.IsV1()) {
      if (eval_result.Empty())
        return nullopt;
      return eval_result.AsV1();
    }

    return JsonType{eval_result.AsV2()};
  };

  JsonType out{
      jsoncons::json_object_arg};  // see https://github.com/danielaparker/jsoncons/issues/482
  if (paths.size() == 1) {
    auto eval_result = eval_wrapped(paths[0].second);
    if (!eval_result) {
      return OpStatus::INVALID_JSON_PATH;
    }
    out = std::move(eval_result).value();  // TODO(Print not existing path to the user)
  } else {
    for (const auto& [path_str, path] : paths) {
      auto eval_result = eval_wrapped(path);
      if (legacy_mode_is_enabled && !eval_result) {
        return OpStatus::INVALID_JSON_PATH;
      }
      out[path_str] = std::move(eval_result).value();  // TODO(Print not existing path to the user)
    }
  }

  jsoncons::json_printable jp(out, options, jsoncons::indenting::indent);
  std::stringstream ss;
  jp.dump(ss);
  return ss.str();
}

auto OpType(const OpArgs& op_args, string_view key, const WrappedJsonPath& json_path) {
  auto cb = [](const string_view&, const JsonType& val) -> std::string {
    return JsonTypeToName(val);
  };
  return JsonEvaluateOperation<std::string>(op_args, key, json_path, std::move(cb), {false, true});
}

OpResult<JsonCallbackResult<OptSize>> OpStrLen(const OpArgs& op_args, string_view key,
                                               const WrappedJsonPath& json_path) {
  auto cb = [](const string_view&, const JsonType& val) -> OptSize {
    if (val.is_string()) {
      return val.as_string_view().size();
    } else {
      return nullopt;
    }
  };

  return JsonEvaluateOperation<OptSize>(op_args, key, json_path, std::move(cb), {true, true});
}

OpResult<JsonCallbackResult<OptSize>> OpObjLen(const OpArgs& op_args, string_view key,
                                               const WrappedJsonPath& json_path) {
  auto cb = [](const string_view&, const JsonType& val) -> optional<size_t> {
    if (val.is_object()) {
      return val.size();
    } else {
      return nullopt;
    }
  };

  return JsonEvaluateOperation<OptSize>(op_args, key, json_path, std::move(cb),
                                        {true, json_path.IsLegacyModePath()});
}

OpResult<JsonCallbackResult<OptSize>> OpArrLen(const OpArgs& op_args, string_view key,
                                               const WrappedJsonPath& json_path) {
  auto cb = [](const string_view&, const JsonType& val) -> OptSize {
    if (val.is_array()) {
      return val.size();
    } else {
      return std::nullopt;
    }
  };
  return JsonEvaluateOperation<OptSize>(op_args, key, json_path, std::move(cb), {true, true});
}

template <typename T>
auto OpToggle(const OpArgs& op_args, string_view key,
              const WrappedJsonPath& json_path) {  // TODO(change the output type for enhanced path)
  auto cb = [](std::optional<std::string_view>,
               JsonType* val) -> MutateCallbackResult<std::optional<T>> {
    if (val->is_bool()) {
      bool next_val = val->as_bool() ^ true;
      *val = next_val;
      return {false, next_val};
    }
    return {};
  };
  return UpdateEntry<std::optional<T>>(op_args, key, json_path, std::move(cb));
}

template <typename T>
auto ExecuteToggle(string_view key, const WrappedJsonPath& json_path, ConnectionContext* cntx) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpToggle<T>(t->GetOpArgs(shard), key, json_path);
  };

  auto result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

enum ArithmeticOpType { OP_ADD, OP_MULTIPLY };

void BinOpApply(double num, bool num_is_double, ArithmeticOpType op, JsonType* val,
                bool* overflow) {
  double result = 0;
  switch (op) {
    case OP_ADD:
      result = val->as<double>() + num;
      break;
    case OP_MULTIPLY:
      result = val->as<double>() * num;
      break;
  }

  if (isinf(result)) {
    *overflow = true;
    return;
  }

  if (val->is_double() || num_is_double) {
    *val = result;
  } else {
    *val = (uint64_t)result;
  }
  *overflow = false;
}

OpResult<string> OpDoubleArithmetic(const OpArgs& op_args, string_view key,
                                    const WrappedJsonPath& json_path, double num,
                                    ArithmeticOpType op_type) {
  bool is_result_overflow = false;
  double int_part;
  bool has_fractional_part = (modf(num, &int_part) != 0);

  // Tmp solution with struct CallbackResult, because MutateCallbackResult<std::optional<JsonType>>
  // does not compile
  struct CallbackResult {
    explicit CallbackResult(bool legacy_mode_is_enabled_)
        : legacy_mode_is_enabled(legacy_mode_is_enabled_) {
      if (!legacy_mode_is_enabled) {
        json_value.emplace(jsoncons::json_array_arg);
      }
    }

    void AddValue(JsonType val) {
      if (legacy_mode_is_enabled) {
        json_value = std::move(val);
      } else {
        json_value->emplace_back(std::move(val));
      }
    }

    void AddEmptyValue() {
      if (!legacy_mode_is_enabled) {
        json_value->emplace_back(JsonType::null());
      }
    }

    std::optional<JsonType> json_value;
    bool legacy_mode_is_enabled;
  };

  CallbackResult result{json_path.IsLegacyModePath()};
  auto cb = [&](std::optional<std::string_view>, JsonType* val) -> MutateCallbackResult<> {
    if (val->is_number()) {
      bool res = false;
      BinOpApply(num, has_fractional_part, op_type, val, &res);
      if (res) {
        is_result_overflow = true;
      } else {
        result.AddValue(*val);
        return {};
      }
    }
    result.AddEmptyValue();
    return {};
  };

  auto res = UpdateEntry<Nothing>(op_args, key, json_path, std::move(cb));

  if (is_result_overflow)
    return OpStatus::INVALID_NUMERIC_RESULT;

  RETURN_ON_BAD_STATUS(res);

  if (!result.json_value) {
    return OpStatus::WRONG_JSON_TYPE;
  }
  return result.json_value->as_string();
}

// Deletes items specified by the expression/path.
OpResult<long> OpDel(const OpArgs& op_args, string_view key, string_view path,
                     const WrappedJsonPath& json_path) {
  if (json_path.RefersToRootElement()) {
    auto& db_slice = op_args.GetDbSlice();
    auto it = db_slice.FindMutable(op_args.db_cntx, key).it;  // post_updater will run immediately
    return static_cast<long>(db_slice.Del(op_args.db_cntx, it));
  }
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return 0;
  }

  if (json_path.HoldsJsonPath()) {
    const json::Path& path = json_path.AsJsonPath();
    long deletions = json::MutatePath(
        path, [](optional<string_view>, JsonType* val) { return true; }, *result);
    return deletions;
  }

  vector<string> deletion_items;
  auto cb = [&](std::optional<std::string_view> path, JsonType* val) -> MutateCallbackResult<> {
    deletion_items.emplace_back(*path);
    return {};
  };

  auto res = json_path.Mutate<Nothing>(result.value(), std::move(cb));
  RETURN_ON_BAD_STATUS(res);

  if (deletion_items.empty()) {
    return 0;
  }

  long total_deletions = 0;
  JsonType patch(jsoncons::json_array_arg, {});
  reverse(deletion_items.begin(), deletion_items.end());  // deletion should finish at root keys.
  for (const auto& item : deletion_items) {
    string pointer = ConvertToJsonPointer(item);
    total_deletions++;
    JsonType patch_item(jsoncons::json_object_arg, {{"op", "remove"}, {"path", pointer}});
    patch.emplace_back(patch_item);
  }

  std::error_code ec;
  jsoncons::jsonpatch::apply_patch(*result.value(), patch, ec);
  if (ec) {
    VLOG(1) << "Failed to apply patch on json with error: " << ec.message();
    return 0;
  }

  // SetString(op_args, key, j.as_string());
  return total_deletions;
}

// Returns a vector of string vectors,
// keys within the same object are stored in the same string vector.
auto OpObjKeys(const OpArgs& op_args, string_view key, const WrappedJsonPath& json_path) {
  auto cb = [](const string_view& path, const JsonType& val) {
    // Aligned with ElastiCache flavor.
    DVLOG(2) << "path: " << path << " val: " << val.to_string();

    StringVec vec;
    if (val.is_object()) {
      for (const auto& member : val.object_range()) {
        vec.emplace_back(member.key());
      }
    }
    return vec;
  };

  return JsonEvaluateOperation<StringVec>(op_args, key, json_path, std::move(cb),
                                          {true, json_path.IsLegacyModePath()});
}

OpResult<JsonCallbackResult<OptSize>> OpStrAppend(const OpArgs& op_args, string_view key,
                                                  const WrappedJsonPath& path, string_view value) {
  auto cb = [&](optional<string_view>, JsonType* val) -> MutateCallbackResult<size_t> {
    if (!val->is_string())
      return {};

    string new_val = absl::StrCat(val->as_string_view(), value);
    size_t len = new_val.size();
    *val = std::move(new_val);
    return {false, len};  // do not delete, new value len
  };

  return UpdateEntry<size_t>(op_args, key, path, std::move(cb));
}

// Returns the numbers of values cleared.
// Clears containers(arrays or objects) and zeroing numbers.
OpResult<long> OpClear(const OpArgs& op_args, string_view key, const WrappedJsonPath& path) {
  long clear_items = 0;

  auto cb = [&clear_items](std::optional<std::string_view>,
                           JsonType* val) -> MutateCallbackResult<> {
    if (!(val->is_object() || val->is_array() || val->is_number())) {
      return {};
    }

    if (val->is_object()) {
      val->erase(val->object_range().begin(), val->object_range().end());
    } else if (val->is_array()) {
      val->erase(val->array_range().begin(), val->array_range().end());
    } else if (val->is_number()) {
      *val = 0;
    }

    clear_items += 1;
    return {};
  };

  auto res = UpdateEntry<Nothing>(op_args, key, path, std::move(cb));
  RETURN_ON_BAD_STATUS(res);
  return clear_items;
}

// Returns string vector that represents the pop out values.
auto OpArrPop(const OpArgs& op_args, string_view key, WrappedJsonPath& path, int index) {
  auto cb = [index](std::optional<std::string_view>,
                    JsonType* val) -> MutateCallbackResult<std::string> {
    if (!val->is_array() || val->empty()) {
      return {};
    }

    size_t removal_index;
    if (index < 0) {
      int temp_index = index + val->size();
      removal_index = abs(temp_index);
    } else {
      removal_index = index;
    }

    if (removal_index >= val->size()) {
      removal_index %= val->size();  // rounded to the array boundaries.
    }

    auto it = val->array_range().begin() + removal_index;
    string str;
    error_code ec;
    it->dump(str, {}, ec);
    if (ec) {
      LOG(ERROR) << "Failed to dump JSON to string with the error: " << ec.message();
      return {};
    }

    val->erase(it);
    return {false, std::move(str)};
  };
  return UpdateEntry<std::string>(op_args, key, path, std::move(cb));
}

// Returns numeric vector that represents the new length of the array at each path.
auto OpArrTrim(const OpArgs& op_args, string_view key, const WrappedJsonPath& path, int start_index,
               int stop_index) {
  auto cb = [&](optional<string_view>, JsonType* val) -> MutateCallbackResult<size_t> {
    if (!val->is_array()) {
      return {};
    }

    if (val->empty()) {
      return {false, 0};
    }
    size_t trim_start_index;
    if (start_index < 0) {
      trim_start_index = 0;
    } else {
      trim_start_index = start_index;
    }

    size_t trim_end_index;
    if ((size_t)stop_index >= val->size()) {
      trim_end_index = val->size();
    } else {
      trim_end_index = stop_index;
    }

    if (trim_start_index >= val->size() || trim_start_index > trim_end_index) {
      val->erase(val->array_range().begin(), val->array_range().end());
      return {false, 0};
    }

    auto trim_start_it = std::next(val->array_range().begin(), trim_start_index);
    auto trim_end_it = val->array_range().end();
    if (trim_end_index < val->size()) {
      trim_end_it = std::next(val->array_range().begin(), trim_end_index + 1);
    }

    *val = jsoncons::json_array<JsonType>(trim_start_it, trim_end_it);
    return {false, val->size()};
  };
  return UpdateEntry<size_t>(op_args, key, path, std::move(cb));
}

// Returns numeric vector that represents the new length of the array at each path.
OpResult<JsonCallbackResult<OptSize>> OpArrInsert(const OpArgs& op_args, string_view key,
                                                  const WrappedJsonPath& json_path, int index,
                                                  const vector<JsonType>& new_values) {
  bool out_of_boundaries_encountered = false;

  // Insert user-supplied value into the supplied index that should be valid.
  // If at least one index isn't valid within an array in the json doc, the operation is discarded.
  // Negative indexes start from the end of the array.
  auto cb = [&](std::optional<std::string_view>, JsonType* val) -> MutateCallbackResult<size_t> {
    if (out_of_boundaries_encountered) {
      return {};
    }

    if (!val->is_array()) {
      return {};
    }

    size_t removal_index;
    if (index < 0) {
      if (val->empty()) {
        out_of_boundaries_encountered = true;
        return {};
      }

      int temp_index = index + val->size();
      if (temp_index < 0) {
        out_of_boundaries_encountered = true;
        return {};
      }

      removal_index = temp_index;
    } else {
      if ((size_t)index > val->size()) {
        out_of_boundaries_encountered = true;
        return {};
      }

      removal_index = index;
    }

    auto it = next(val->array_range().begin(), removal_index);
    for (auto& new_val : new_values) {
      it = val->insert(it, new_val);
      it++;
    }
    return {false, val->size()};
  };

  auto res = UpdateEntry<size_t>(op_args, key, json_path, std::move(cb));
  if (out_of_boundaries_encountered) {
    return OpStatus::OUT_OF_RANGE;
  }
  return res;
}

auto OpArrAppend(const OpArgs& op_args, string_view key, const WrappedJsonPath& path,
                 const vector<JsonType>& append_values) {
  auto cb = [&](std::optional<std::string_view>,
                JsonType* val) -> MutateCallbackResult<std::optional<std::size_t>> {
    if (!val->is_array()) {
      return {};
    }
    for (auto& new_val : append_values) {
      val->emplace_back(new_val);
    }
    return {false, val->size()};
  };
  return UpdateEntry<std::optional<std::size_t>>(op_args, key, path, std::move(cb));
}

// Returns a numeric vector representing each JSON value first index of the JSON scalar.
// An index value of -1 represents unfound in the array.
// JSON scalar has types of string, boolean, null, and number.
auto OpArrIndex(const OpArgs& op_args, string_view key, const WrappedJsonPath& json_path,
                const JsonType& search_val, int start_index, int end_index) {
  auto cb = [&](const string_view&, const JsonType& val) -> std::optional<long> {
    if (!val.is_array()) {
      return std::nullopt;
    }

    if (val.empty()) {
      return -1;
    }

    // Negative value or out-of-range index is handled by rounding the index to the array's start
    // and end. example: for array size 9 and index -11 the index mapped to index 7.
    if (start_index < 0) {
      start_index %= val.size();
      start_index += val.size();
    }

    // See the comment above.
    // A value index of 0 means searching until the end of the array.
    if (end_index == 0) {
      end_index = val.size();
    } else if (end_index < 0) {
      end_index %= val.size();
      end_index += val.size();
    }

    if (start_index > end_index) {
      return -1;
    }

    size_t pos = -1;
    auto it = next(val.array_range().begin(), start_index);
    while (it != val.array_range().end()) {
      if (search_val == *it) {
        pos = start_index;
        break;
      }

      ++it;
      if (++start_index == end_index) {
        break;
      }
    }

    return pos;
  };

  return JsonEvaluateOperation<std::optional<long>>(op_args, key, json_path, std::move(cb));
}

// Returns string vector that represents the query result of each supplied key.
std::vector<std::optional<std::string>> OpJsonMGet(const WrappedJsonPath& json_path,
                                                   const Transaction* t, EngineShard* shard) {
  ShardArgs args = t->GetShardArgs(shard->shard_id());
  DCHECK(!args.Empty());
  std::vector<std::optional<std::string>> response(args.Size());

  auto& db_slice = t->GetDbSlice(shard->shard_id());
  unsigned index = 0;
  for (string_view key : args) {
    auto it_res = db_slice.FindReadOnly(t->GetDbContext(), key, OBJ_JSON);
    auto& dest = response[index++];
    if (!it_res.ok())
      continue;

    JsonType* json_val = it_res.value()->second.GetJson();
    DCHECK(json_val) << "should have a valid JSON object for key " << key;

    auto cb = [](std::string_view, const JsonType& val) { return val; };

    auto eval_wrapped = [&json_val,
                         &cb](const WrappedJsonPath& json_path) -> std::optional<JsonType> {
      auto eval_result = json_path.Evaluate<JsonType>(json_val, std::move(cb), false);

      if (eval_result.IsV1()) {
        return eval_result.AsV1();
      }

      return JsonType{eval_result.AsV2()};
    };

    auto eval_result = eval_wrapped(json_path);

    if (!eval_result) {
      continue;
    }

    std::string str;
    std::error_code ec;
    eval_result->dump(str, {}, ec);
    if (ec) {
      VLOG(1) << "Failed to dump JSON array to string with the error: " << ec.message();
    }

    dest = std::move(str);
  }

  return response;
}

// Returns numeric vector that represents the number of fields of JSON value at each path.
auto OpFields(const OpArgs& op_args, string_view key, const WrappedJsonPath& json_path) {
  auto cb = [](const string_view&, const JsonType& val) -> std::optional<std::size_t> {
    return CountJsonFields(val);
  };

  return JsonEvaluateOperation<std::optional<std::size_t>>(op_args, key, json_path, std::move(cb));
}

// Returns json vector that represents the result of the json query.
auto OpResp(const OpArgs& op_args, string_view key, const WrappedJsonPath& json_path) {
  auto cb = [](const string_view&, const JsonType& val) { return val; };
  return JsonEvaluateOperation<JsonType>(op_args, key, json_path, std::move(cb));
}

// Returns boolean that represents the result of the operation.
OpResult<bool> OpSet(const OpArgs& op_args, string_view key, string_view path,
                     const WrappedJsonPath& json_path, std::string_view json_str,
                     bool is_nx_condition, bool is_xx_condition) {
  std::optional<JsonType> parsed_json = ShardJsonFromString(json_str);
  if (!parsed_json) {
    VLOG(1) << "got invalid JSON string '" << json_str << "' cannot be saved";
    return OpStatus::SYNTAX_ERR;
  }

  // The whole key should be replaced.
  // NOTE: unlike in Redis, we are overriding the value when the path is "$"
  // this is regardless of the current key type. In redis if the key exists
  // and its not JSON, it would return an error.
  if (json_path.RefersToRootElement()) {
    if (is_nx_condition || is_xx_condition) {
      auto it_res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_JSON);
      bool key_exists = (it_res.status() != OpStatus::KEY_NOTFOUND);
      if (is_nx_condition && key_exists) {
        return false;
      }

      if (is_xx_condition && !key_exists) {
        return false;
      }
    }

    OpStatus st = SetJson(op_args, key, std::move(parsed_json.value()));
    if (st != OpStatus::OK) {
      return st;
    }
    return true;
  }

  // Note that this operation would use copy and not move!
  // The reason being, that we are applying this multiple times
  // For each match we found. So for example if we have
  // an array that this expression will match each entry in it
  // then the assign here is called N times, where N == array.size().
  bool path_exists = false;
  bool operation_result = false;
  const JsonType& new_json = parsed_json.value();
  auto cb = [&](std::optional<std::string_view>, JsonType* val) -> MutateCallbackResult<> {
    path_exists = true;
    if (!is_nx_condition) {
      operation_result = true;
      *val = new_json;
    }
    return {};
  };

  auto inserter = [&](JsonType& json) {
    // Set a new value if the path doesn't exist and the nx condition is not set.
    if (!path_exists && !is_xx_condition) {
      string pointer = ConvertExpressionToJsonPointer(path);
      if (pointer.empty()) {
        VLOG(1) << "Failed to convert the following expression path to a valid JSON pointer: "
                << path;
        return OpStatus::SYNTAX_ERR;
      }

      error_code ec;
      jsoncons::jsonpointer::add(json, pointer, new_json, ec);
      if (ec) {
        VLOG(1) << "Failed to add a JSON value to the following path: " << path
                << " with the error: " << ec.message();
        return OpStatus::SYNTAX_ERR;
      }

      operation_result = true;
    }

    return OpStatus::OK;
  };

  auto res = UpdateEntry<Nothing>(op_args, key, json_path, std::move(cb), inserter);
  RETURN_ON_BAD_STATUS(res);
  return operation_result;
}

OpResult<bool> OpSet(const OpArgs& op_args, string_view key, string_view path,
                     std::string_view json_str, bool is_nx_condition, bool is_xx_condition) {
  auto res_json_path = ParseJsonPath(path);
  if (!res_json_path) {
    return OpStatus::SYNTAX_ERR;  // TODO(Return initial error)
  }
  return OpSet(op_args, key, path, res_json_path.value(), json_str, is_nx_condition,
               is_xx_condition);
}

OpStatus OpMSet(const OpArgs& op_args, const ShardArgs& args) {
  DCHECK_EQ(args.Size() % 3, 0u);

  OpStatus result = OpStatus::OK;
  size_t stored = 0;
  for (auto it = args.begin(); it != args.end();) {
    string_view key = *(it++);
    string_view path = *(it++);
    string_view value = *(it++);
    if (auto res = OpSet(op_args, key, path, value, false, false); !res.ok()) {
      result = res.status();
      break;
    }

    stored++;
  }

  // Replicate custom journal, see OpMSet
  if (auto journal = op_args.shard->journal(); journal) {
    if (stored * 3 == args.Size()) {
      RecordJournal(op_args, "JSON.MSET", args, op_args.tx->GetUniqueShardCnt());
      DCHECK_EQ(result, OpStatus::OK);
      return result;
    }

    string_view cmd = stored == 0 ? "PING" : "JSON.MSET";
    vector<string_view> store_args(args.begin(), args.end());
    store_args.resize(stored * 3);
    RecordJournal(op_args, cmd, store_args, op_args.tx->GetUniqueShardCnt());
  }

  return result;
}

// Note that currently OpMerge works only with jsoncons and json::Path support has not been
// implemented yet.
OpStatus OpMerge(const OpArgs& op_args, string_view key, string_view path,
                 const WrappedJsonPath& json_path, std::string_view json_str) {
  // DCHECK(!json_path.HoldsJsonPath());

  std::optional<JsonType> parsed_json = ShardJsonFromString(json_str);
  if (!parsed_json) {
    VLOG(1) << "got invalid JSON string '" << json_str << "' cannot be saved";
    return OpStatus::SYNTAX_ERR;
  }

  auto cb = [&](std::optional<std::string_view> cur_path, JsonType* val) -> MutateCallbackResult<> {
    string_view strpath = cur_path ? *cur_path : string_view{};

    DVLOG(2) << "Handling " << strpath << " " << val->to_string();
    // https://datatracker.ietf.org/doc/html/rfc7386#section-2
    try {
      mergepatch::apply_merge_patch(*val, *parsed_json);
    } catch (const std::exception& e) {
      LOG_EVERY_T(ERROR, 1) << "Exception in OpMerge: " << e.what() << " with obj: " << *val
                            << " and patch: " << *parsed_json << ", path: " << strpath;
    }

    return {};
  };

  auto res = UpdateEntry<Nothing>(op_args, key, json_path, std::move(cb));

  if (res.status() != OpStatus::KEY_NOTFOUND)
    return res.status();

  if (json_path.RefersToRootElement()) {
    return OpSet(op_args, key, path, json_path, json_str, false, false).status();
  }
  return OpStatus::SYNTAX_ERR;
}

}  // namespace

void JsonFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.Next();
  string_view json_str = parser.Next();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  int res = parser.HasNext() ? parser.Switch("NX", 1, "XX", 2) : 0;
  bool is_xx_condition = (res == 2), is_nx_condition = (res == 1);

  if (parser.Error() || parser.HasNext())  // also clear the parser error dcheck
    return cntx->SendError(kSyntaxErr);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, path, json_path, json_str, is_nx_condition,
                 is_xx_condition);
  };

  Transaction* trans = cntx->transaction;

  OpResult<bool> result = trans->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result) {
    if (*result) {
      rb->SendOk();
    } else {
      rb->SendNull();
    }
  } else {
    cntx->SendError(result.status());
  }
}

// JSON.MSET key path value [key path value ...]
void JsonFamily::MSet(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 3u);
  if (args.size() % 3 != 0) {
    return cntx->SendError(facade::WrongNumArgsError("json.mset"));
  }

  AggregateStatus status;
  auto cb = [&status](Transaction* t, EngineShard* shard) {
    auto op_args = t->GetOpArgs(shard);
    ShardArgs args = t->GetShardArgs(shard->shard_id());
    if (auto result = OpMSet(op_args, args); result != OpStatus::OK)
      status = result;
    return OpStatus::OK;
  };

  cntx->transaction->ScheduleSingleHop(cb);

  if (*status != OpStatus::OK)
    return cntx->SendError(*status);
  cntx->SendOk();
}

// JSON.MERGE key path value
// Based on https://datatracker.ietf.org/doc/html/rfc7386 spec
void JsonFamily::Merge(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.Next();
  string_view value = parser.Next();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpMerge(t->GetOpArgs(shard), key, path, json_path, value);
  };

  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));
  if (status == OpStatus::OK)
    return cntx->SendOk();
  cntx->SendError(status);
}

void JsonFamily::Resp(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpResp(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::Debug(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view command = parser.Next();

  // The 'MEMORY' sub-command is not supported yet, calling to operation function should be added
  // here.
  if (absl::EqualsIgnoreCase(command, "help")) {
    auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
    rb->StartArray(2);
    rb->SendBulkString(
        "JSON.DEBUG FIELDS <key> <path> - report number of fields in the JSON element.");
    rb->SendBulkString("JSON.DEBUG HELP - print help message.");
    return;
  } else if (!absl::EqualsIgnoreCase(command, "fields")) {
    cntx->SendError(facade::UnknownSubCmd(command, "JSON.DEBUG"), facade::kSyntaxErrType);
    return;
  }

  // JSON.DEBUG FIELDS

  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpFields(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::MGet(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1U);

  string_view path = ArgS(args, args.size() - 1);
  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  Transaction* transaction = cntx->transaction;
  unsigned shard_count = shard_set->size();
  std::vector<std::vector<std::optional<std::string>>> mget_resp(shard_count);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    mget_resp[sid] = OpJsonMGet(json_path, t, shard);
    return OpStatus::OK;
  };

  OpStatus result = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, result);

  std::vector<std::optional<std::string>> results(args.size() - 1);
  for (ShardId sid = 0; sid < shard_count; ++sid) {
    if (!transaction->IsActive(sid))
      continue;

    std::vector<std::optional<std::string>>& res = mget_resp[sid];
    ShardArgs shard_args = transaction->GetShardArgs(sid);
    unsigned src_index = 0;
    for (auto it = shard_args.begin(); it != shard_args.end(); ++it, ++src_index) {
      if (!res[src_index])
        continue;

      uint32_t dst_indx = it.index();
      results[dst_indx] = std::move(res[src_index]);
    }
  }

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(results.begin(), results.end(), rb);
}

void JsonFamily::ArrIndex(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  optional<JsonType> search_value = JsonFromString(ArgS(args, 2));
  if (!search_value) {
    cntx->SendError(kSyntaxErr);
    return;
  }

  if (search_value->is_object() || search_value->is_array()) {
    cntx->SendError(kWrongTypeErr);
    return;
  }

  int start_index = 0;
  if (args.size() >= 4) {
    if (!absl::SimpleAtoi(ArgS(args, 3), &start_index)) {
      VLOG(1) << "Failed to convert the start index to numeric" << ArgS(args, 3);
      cntx->SendError(kInvalidIntErr);
      return;
    }
  }

  int end_index = 0;
  if (args.size() >= 5) {
    if (!absl::SimpleAtoi(ArgS(args, 4), &end_index)) {
      VLOG(1) << "Failed to convert the stop index to numeric" << ArgS(args, 4);
      cntx->SendError(kInvalidIntErr);
      return;
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrIndex(t->GetOpArgs(shard), key, json_path, *search_value, start_index, end_index);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::ArrInsert(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  int index = -1;

  if (!absl::SimpleAtoi(ArgS(args, 2), &index)) {
    VLOG(1) << "Failed to convert the following value to numeric: " << ArgS(args, 2);
    cntx->SendError(kInvalidIntErr);
    return;
  }

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  vector<JsonType> new_values;
  for (size_t i = 3; i < args.size(); i++) {
    optional<JsonType> val = JsonFromString(ArgS(args, i));
    if (!val) {
      cntx->SendError(kSyntaxErr);
      return;
    }

    new_values.emplace_back(std::move(*val));
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrInsert(t->GetOpArgs(shard), key, json_path, index, new_values);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::ArrAppend(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  vector<JsonType> append_values;

  // TODO: there is a bug here, because we parse json using the allocator from
  // the coordinator thread, and we pass it to the shard thread, which is not safe.
  for (size_t i = 2; i < args.size(); ++i) {
    optional<JsonType> converted_val = JsonFromString(ArgS(args, i));
    if (!converted_val) {
      cntx->SendError(kSyntaxErr);
      return;
    }
    append_values.emplace_back(converted_val);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrAppend(t->GetOpArgs(shard), key, json_path, append_values);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::ArrTrim(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  int start_index;
  int stop_index;

  if (!absl::SimpleAtoi(ArgS(args, 2), &start_index)) {
    VLOG(1) << "Failed to parse array start index";
    cntx->SendError(kInvalidIntErr);
    return;
  }

  if (!absl::SimpleAtoi(ArgS(args, 3), &stop_index)) {
    VLOG(1) << "Failed to parse array stop index";
    cntx->SendError(kInvalidIntErr);
    return;
  }

  if (stop_index < 0) {
    cntx->SendError(kInvalidIntErr);
    return;
  }

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrTrim(t->GetOpArgs(shard), key, json_path, start_index, stop_index);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::ArrPop(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();
  int index = parser.NextOrDefault<int>(-1);

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrPop(t->GetOpArgs(shard), key, json_path, index);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::Clear(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpClear(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::StrAppend(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view value = ArgS(args, 2);

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  // We try parsing the value into json string object first.
  optional<JsonType> parsed_json = JsonFromString(value);
  if (!parsed_json || !parsed_json->is_string()) {
    return cntx->SendError("expected string value", kSyntaxErrType);
  };

  string_view json_string = parsed_json->as_string_view();
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrAppend(t->GetOpArgs(shard), key, json_path, json_string);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::ObjKeys(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpObjKeys(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::Del(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, path, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::NumIncrBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view num = ArgS(args, 2);

  double dnum;
  if (!ParseDouble(num, &dnum)) {
    cntx->SendError(kWrongTypeErr);
    return;
  }

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDoubleArithmetic(t->GetOpArgs(shard), key, json_path, dnum, OP_ADD);
  };

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::NumMultBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view num = ArgS(args, 2);

  double dnum;
  if (!ParseDouble(num, &dnum)) {
    cntx->SendError(kWrongTypeErr);
    return;
  }

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDoubleArithmetic(t->GetOpArgs(shard), key, json_path, dnum, OP_MULTIPLY);
  };

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::Toggle(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  if (json_path.IsLegacyModePath()) {
    ExecuteToggle<bool>(key, json_path, cntx);
  } else {
    ExecuteToggle<long>(key, json_path, cntx);
  }
}

void JsonFamily::Type(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpType(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::ArrLen(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrLen(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::ObjLen(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpObjLen(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::StrLen(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view path = parser.NextOrDefault();

  WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrLen(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  auto result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  reply_generic::Send(result, rb);
}

void JsonFamily::Get(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1U);

  facade::CmdArgParser parser{args};
  string_view key = parser.Next();

  std::optional<std::string> indent;
  std::optional<std::string> new_line;
  std::optional<std::string> space;

  vector<pair<string_view, WrappedJsonPath>> paths;

  while (parser.HasNext()) {
    if (parser.Check("SPACE")) {
      space = parser.Next();
      continue;
    }
    if (parser.Check("NEWLINE")) {
      new_line = parser.Next();
      continue;
    }
    if (parser.Check("INDENT")) {
      indent = parser.Next();
      continue;
    }

    string_view path_str = parser.Next();
    WrappedJsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path_str));

    paths.emplace_back(path_str, std::move(json_path));
  }

  if (auto err = parser.Error(); err)
    return cntx->SendError(err->MakeReply());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpJsonGet(t->GetOpArgs(shard), key, paths, indent, new_line, space);
  };

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result) {
    rb->SendBulkString(*result);
  } else {
    if (result == facade::OpStatus::KEY_NOTFOUND) {
      rb->SendNull();  // Match Redis
    } else {
      cntx->SendError(result.status());
    }
  }
}

#define HFUNC(x) SetHandler(&JsonFamily::x)

// Redis modules do not have acl categories, therefore they can not be used by default.
// However, we do not implement those as modules and therefore we can define our own
// sensible defaults.
// For now I introduced only the JSON category which will be the default.
// TODO: Add sensible defaults/categories to json commands

void JsonFamily::Register(CommandRegistry* registry) {
  constexpr size_t kMsetFlags =
      CO::WRITE | CO::DENYOOM | CO::FAST | CO::INTERLEAVED_KEYS | CO::NO_AUTOJOURNAL;
  registry->StartFamily();
  *registry << CI{"JSON.GET", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(Get);
  *registry << CI{"JSON.MGET", CO::READONLY | CO::FAST, -3, 1, -2, acl::JSON}.HFUNC(MGet);
  *registry << CI{"JSON.TYPE", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(Type);
  *registry << CI{"JSON.STRLEN", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(StrLen);
  *registry << CI{"JSON.OBJLEN", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(ObjLen);
  *registry << CI{"JSON.ARRLEN", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(ArrLen);
  *registry << CI{"JSON.TOGGLE", CO::WRITE | CO::FAST, 3, 1, 1, acl::JSON}.HFUNC(Toggle);
  *registry << CI{"JSON.NUMINCRBY", CO::WRITE | CO::FAST, 4, 1, 1, acl::JSON}.HFUNC(NumIncrBy);
  *registry << CI{"JSON.NUMMULTBY", CO::WRITE | CO::FAST, 4, 1, 1, acl::JSON}.HFUNC(NumMultBy);
  *registry << CI{"JSON.DEL", CO::WRITE, -2, 1, 1, acl::JSON}.HFUNC(Del);
  *registry << CI{"JSON.FORGET", CO::WRITE, -2, 1, 1, acl::JSON}.HFUNC(
      Del);  // An alias of JSON.DEL.
  *registry << CI{"JSON.OBJKEYS", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(ObjKeys);
  *registry << CI{"JSON.STRAPPEND", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, acl::JSON}.HFUNC(
      StrAppend);
  *registry << CI{"JSON.CLEAR", CO::WRITE | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(Clear);
  *registry << CI{"JSON.ARRPOP", CO::WRITE | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(ArrPop);
  *registry << CI{"JSON.ARRTRIM", CO::WRITE | CO::FAST, 5, 1, 1, acl::JSON}.HFUNC(ArrTrim);
  *registry << CI{"JSON.ARRINSERT", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::JSON}.HFUNC(
      ArrInsert);
  *registry << CI{"JSON.ARRAPPEND", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::JSON}.HFUNC(
      ArrAppend);
  *registry << CI{"JSON.ARRINDEX", CO::READONLY | CO::FAST, -4, 1, 1, acl::JSON}.HFUNC(ArrIndex);
  // TODO: Support negative first_key index to revive the debug sub-command
  *registry << CI{"JSON.DEBUG", CO::READONLY | CO::FAST, -3, 2, 2, acl::JSON}.HFUNC(Debug)
            << CI{"JSON.RESP", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(Resp)
            << CI{"JSON.SET", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::JSON}.HFUNC(Set)
            << CI{"JSON.MSET", kMsetFlags, -4, 1, -1, acl::JSON}.HFUNC(MSet)
            << CI{"JSON.MERGE", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, acl::JSON}.HFUNC(
                   Merge);
}

}  // namespace dfly
