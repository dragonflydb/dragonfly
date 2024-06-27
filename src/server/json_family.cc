// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <jsoncons/json.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>
#include <variant>

#include "base/expected.hpp"
#include "base/flags.h"
#include "base/logging.h"
#include "core/flatbuffers.h"
#include "core/json/json_object.h"
#include "core/json/path.h"
#include "core/string_or_view.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "facade/op_status.h"
#include "glog/logging.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/search/doc_index.h"
#include "server/string_family.h"
#include "server/table.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"

ABSL_FLAG(bool, jsonpathv2, true,
          "If true uses Dragonfly jsonpath implementation, "
          "otherwise uses legacy jsoncons implementation.");
ABSL_FLAG(bool, experimental_flat_json, false, "If true uses flat json implementation.");

namespace dfly {

using namespace std;
using namespace jsoncons;
using facade::kSyntaxErrType;
using facade::WrongNumArgsError;

using JsonExpression = jsonpath::jsonpath_expression<JsonType>;
using OptBool = optional<bool>;
using OptLong = optional<long>;
using OptSizeT = optional<size_t>;
using OptString = optional<string>;
using JsonReplaceVerify = std::function<void(JsonType&)>;
using CI = CommandId;

static const char DefaultJsonPath[] = "$";

namespace {

using ExprCallback = absl::FunctionRef<void(string_view, const JsonType&)>;

class JsonPath {
 public:
  static constexpr std::string_view kV1PathRootElement = "."sv;
  static constexpr std::string_view kV2PathRootElement = "$"sv;

  JsonPath(json::Path json_path, StringOrView path, bool is_legacy_mode_path)
      : parsed_path_(std::move(json_path)),
        path_(std::move(path)),
        is_legacy_mode_path_(is_legacy_mode_path) {
  }

  JsonPath(JsonExpression expression, StringOrView path, bool is_legacy_mode_path)
      : parsed_path_(std::move(expression)),
        path_(std::move(path)),
        is_legacy_mode_path_(is_legacy_mode_path) {
  }

  JsonPath(const JsonPath&) = delete;
  JsonPath(JsonPath&&) = default;

  JsonPath& operator=(const JsonPath&) = delete;
  JsonPath& operator=(JsonPath&&) = delete;

  void Evaluate(const JsonType* json_entry, ExprCallback cb) const;
  std::error_code Replace(JsonType* json_entry, json::MutateCallback cb) const;

  bool IsLegacyModePath() const;

  bool RefersToRootElement() const;

  bool HoldsJsonPath() const;
  const json::Path& AsJsonPath() const;
  const JsonExpression& AsJsonExpression() const;

 protected:
  std::variant<json::Path, JsonExpression> parsed_path_;
  StringOrView path_;
  bool is_legacy_mode_path_;
};

void JsonPath::Evaluate(const JsonType* json_entry, ExprCallback cb) const {
  if (HoldsJsonPath()) {
    const auto& json_path = AsJsonPath();
    json::EvaluatePath(json_path, *json_entry,
                       [&cb](optional<string_view> key, const JsonType& val) {
                         cb(key ? *key : string_view{}, val);
                       });
  } else {
    const auto& json_expression = AsJsonExpression();
    json_expression.evaluate(*json_entry, cb);
  }
}

std::error_code JsonPath::Replace(JsonType* json_entry, json::MutateCallback cb) const {
  if (HoldsJsonPath()) {
    const auto& json_path = AsJsonPath();
    json::MutatePath(json_path, cb, json_entry);
    return {};
  }

  using evaluator_t = jsonpath::detail::jsonpath_evaluator<JsonType, JsonType&>;
  using value_type = evaluator_t::value_type;
  using reference = evaluator_t::reference;
  using json_selector_t = evaluator_t::path_expression_type;

  jsonpath::custom_functions<JsonType> funcs = jsonpath::custom_functions<JsonType>();

  error_code ec;
  jsonpath::detail::static_resources<value_type, reference> static_resources(funcs);
  evaluator_t e;

  json_selector_t expr = e.compile(static_resources, path_.view(), ec);
  RETURN_ON_ERR(ec);

  jsonpath::detail::dynamic_resources<value_type, reference> resources;

  auto f = [&cb](const jsonpath::basic_path_node<char>& path, JsonType& val) {
    cb(jsonpath::to_string(path), &val);
  };

  expr.evaluate(resources, *json_entry, json_selector_t::path_node_type{}, *json_entry,
                std::move(f), jsonpath::result_options::nodups | jsonpath::result_options::path);
  return ec;
}

bool JsonPath::RefersToRootElement() const {
  auto path = path_.view();
  return path.empty() || path == kV1PathRootElement || path == kV2PathRootElement;
}

bool JsonPath::IsLegacyModePath() const {
  return is_legacy_mode_path_;
}

bool JsonPath::HoldsJsonPath() const {
  return std::holds_alternative<json::Path>(parsed_path_);
}

const json::Path& JsonPath::AsJsonPath() const {
  return std::get<json::Path>(parsed_path_);
}

const JsonExpression& JsonPath::AsJsonExpression() const {
  return std::get<JsonExpression>(parsed_path_);
}

template <typename T> using ParseResult = io::Result<T, std::string>;

ParseResult<JsonExpression> ParseJsonPathAsExpression(std::string_view path) {
  std::error_code ec;
  JsonExpression res = MakeJsonPathExpr(path, ec);
  if (ec)
    return nonstd::make_unexpected(kSyntaxErr);
  return res;
}

ParseResult<JsonPath> ParseJsonPath(StringOrView path, bool is_legacy_mode_path) {
  if (absl::GetFlag(FLAGS_jsonpathv2)) {
    auto path_result = json::ParsePath(path.view());
    RETURN_UNEXPECTED(path_result);
    return JsonPath{std::move(path_result).value(), std::move(path), is_legacy_mode_path};
  }

  auto expr_result = ParseJsonPathAsExpression(path.view());
  RETURN_UNEXPECTED(expr_result);
  return JsonPath{std::move(expr_result).value(), std::move(path), is_legacy_mode_path};
}

ParseResult<JsonPath> ParseJsonPathV1(std::string_view path) {
  if (path == JsonPath::kV1PathRootElement) {
    return ParseJsonPath(StringOrView::FromView(JsonPath::kV2PathRootElement), true);
  }

  std::string v2_path = absl::StrCat(JsonPath::kV2PathRootElement, path.front() != '.' ? "." : "",
                                     path);  // Convert to V2 path
  return ParseJsonPath(StringOrView::FromString(std::move(v2_path)), true);
}

ParseResult<JsonPath> ParseJsonPathV2(std::string_view path) {
  return ParseJsonPath(StringOrView::FromView(path), false);
}

bool IsJsonPathV2(std::string_view path) {
  return path.front() == '$';
}

ParseResult<JsonPath> ParseJsonPath(std::string_view path) {
  DCHECK(!path.empty());
  return IsJsonPathV2(path) ? ParseJsonPathV2(path) : ParseJsonPathV1(path);
}

facade::OpStatus SetJson(const OpArgs& op_args, string_view key, JsonType&& value) {
  auto& db_slice = op_args.shard->db_slice();

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

inline std::optional<JsonType> JsonFromString(std::string_view input) {
  return dfly::JsonFromString(input, CompactObj::memory_resource());
}

template <typename T>
void PrintOptVec(ConnectionContext* cntx, const OpResult<vector<optional<T>>>& result) {
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result->empty()) {
    rb->SendNullArray();
  } else {
    rb->StartArray(result->size());
    for (auto& it : *result) {
      if (it.has_value()) {
        if constexpr (is_floating_point_v<T>) {
          rb->SendDouble(*it);
        } else {
          static_assert(is_integral_v<T>, "Integral required.");
          rb->SendLong(*it);
        }
      } else {
        rb->SendNull();
      }
    }
  }
}

OpStatus UpdateEntry(const OpArgs& op_args, std::string_view key, const JsonPath& json_path,
                     json::MutateCallback cb, JsonReplaceVerify verify_op = {}) {
  auto it_res = op_args.shard->db_slice().FindMutable(op_args.db_cntx, key, OBJ_JSON);
  if (!it_res.ok()) {
    return it_res.status();
  }

  PrimeValue& pv = it_res->it->second;

  JsonType* json_val = pv.GetJson();
  DCHECK(json_val) << "should have a valid JSON object for key '" << key << "' the type for it is '"
                   << pv.ObjType() << "'";
  JsonType& json_entry = *json_val;

  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv);

  std::error_code ec = json_path.Replace(json_val, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::ERROR_OCCURED;
  }

  // Make sure that we don't have other internal issue with the operation
  if (verify_op) {
    verify_op(json_entry);
  }

  it_res->post_updater.Run();
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);

  return OpStatus::OK;
}

OpResult<JsonType*> GetJson(const OpArgs& op_args, string_view key) {
  auto it_res = op_args.shard->db_slice().FindReadOnly(op_args.db_cntx, key, OBJ_JSON);
  if (!it_res.ok())
    return it_res.status();

  JsonType* json_val = it_res.value()->second.GetJson();
  DCHECK(json_val) << "should have a valid JSON object for key " << key;

  return json_val;
}

// Returns the index of the next right bracket
optional<size_t> GetNextIndex(string_view str) {
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
      optional<size_t> end_val_idx = GetNextIndex(json_path);
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

void SendJsonValue(RedisReplyBuilder* rb, const JsonType& j) {
  if (j.is_double()) {
    rb->SendDouble(j.as_double());
  } else if (j.is_number()) {
    rb->SendLong(j.as_integer<long>());
  } else if (j.is_bool()) {
    rb->SendSimpleString(j.as_bool() ? "true" : "false");
  } else if (j.is_null()) {
    rb->SendNull();
  } else if (j.is_string()) {
    rb->SendBulkString(j.as_string_view());
  } else if (j.is_object()) {
    rb->StartArray(j.size() + 1);
    rb->SendSimpleString("{");
    for (const auto& item : j.object_range()) {
      rb->StartArray(2);
      rb->SendBulkString(item.key());
      SendJsonValue(rb, item.value());
    }
  } else if (j.is_array()) {
    rb->StartArray(j.size() + 1);
    rb->SendSimpleString("[");
    for (const auto& item : j.array_range()) {
      SendJsonValue(rb, item);
    }
  }
}

bool LegacyModeIsEnabled(const std::vector<std::pair<std::string_view, JsonPath>>& paths) {
  return std::all_of(paths.begin(), paths.end(),
                     [](auto& parsed_path) { return parsed_path.second.IsLegacyModePath(); });
}

OpResult<string> OpJsonGet(const OpArgs& op_args, string_view key,
                           const vector<pair<string_view, JsonPath>>& paths,
                           const OptString& indent, const OptString& new_line,
                           const OptString& space) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  const JsonType& json_entry = *(result.value());
  if (paths.empty()) {
    // this implicitly means that we're using $ which
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

  auto eval_wrapped = [&json_entry, legacy_mode_is_enabled](const JsonPath& json_path) -> JsonType {
    if (legacy_mode_is_enabled) {
      std::optional<JsonType> result;
      auto cb = [&result](const string_view&, const JsonType& val) {
        if (!result) {
          result = val;
        }
      };
      json_path.Evaluate(&json_entry, std::move(cb));
      return result;
    }

    JsonType result(json_array_arg);
    auto cb = [&result](const string_view&, const JsonType& val) { result.push_back(val); };
    json_path.Evaluate(&json_entry, std::move(cb));
    return result;
  };

  JsonType out{json_object_arg};  // see https://github.com/danielaparker/jsoncons/issues/482
  if (paths.size() == 1) {
    out = eval_wrapped(paths[0].second);
  } else {
    for (const auto& [path_str, path] : paths) {
      out[path_str] = eval_wrapped(path);
    }
  }

  json_printable jp(out, options, indenting::indent);
  std::stringstream ss;
  jp.dump(ss);
  return ss.str();
}

OpResult<vector<string>> OpType(const OpArgs& op_args, string_view key, const JsonPath& json_path) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  vector<string> vec;
  auto cb = [&vec](const string_view&, const JsonType& val) {
    vec.emplace_back(JsonTypeToName(val));
  };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

OpResult<vector<OptSizeT>> OpStrLen(const OpArgs& op_args, string_view key,
                                    const JsonPath& json_path) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view&, const JsonType& val) {
    if (val.is_string()) {
      vec.emplace_back(val.as_string_view().size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

OpResult<vector<OptSizeT>> OpObjLen(const OpArgs& op_args, string_view key,
                                    const JsonPath& json_path) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view&, const JsonType& val) {
    if (val.is_object()) {
      vec.emplace_back(val.size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

OpResult<vector<OptSizeT>> OpArrLen(const OpArgs& op_args, string_view key,
                                    const JsonPath& json_path) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view&, const JsonType& val) {
    if (val.is_array()) {
      vec.emplace_back(val.size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

OpResult<vector<OptBool>> OpToggle(const OpArgs& op_args, string_view key,
                                   const JsonPath& json_path) {
  vector<OptBool> vec;

  auto cb = [&vec](optional<string_view>, JsonType* val) {
    if (val->is_bool()) {
      bool next_val = val->as_bool() ^ true;
      *val = next_val;
      vec.emplace_back(next_val);
    } else {
      vec.emplace_back(nullopt);
    }
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, json_path, std::move(cb));
  RETURN_BAD_STATUS(status);
  return vec;
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
                                    const JsonPath& json_path, double num,
                                    ArithmeticOpType op_type) {
  bool is_result_overflow = false;
  double int_part;
  bool has_fractional_part = (modf(num, &int_part) != 0);
  JsonType output(json_array_arg);

  auto cb = [&](optional<string_view>, JsonType* val) {
    if (val->is_number()) {
      bool res = false;
      BinOpApply(num, has_fractional_part, op_type, val, &res);
      if (res) {
        is_result_overflow = true;
      } else {
        output.push_back(*val);
      }
    } else {
      output.push_back(JsonType::null());
    }
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, json_path, std::move(cb));

  if (is_result_overflow)
    return OpStatus::INVALID_NUMERIC_RESULT;

  RETURN_BAD_STATUS(status);
  return output.as_string();
}

// If expression is nullopt, then the whole key should be deleted, otherwise deletes
// items specified by the expression/path.
OpResult<long> OpDel(const OpArgs& op_args, string_view key,
                     const std::optional<JsonPath>& json_path) {
  if (!json_path || json_path->RefersToRootElement()) {
    auto& db_slice = op_args.shard->db_slice();
    auto it = db_slice.FindMutable(op_args.db_cntx, key).it;  // post_updater will run immediately
    return long(db_slice.Del(op_args.db_cntx.db_index, it));
  }

  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return 0;
  }

  if (json_path->HoldsJsonPath()) {
    const json::Path& path = json_path->AsJsonPath();
    long deletions = json::MutatePath(
        path, [](optional<string_view>, JsonType* val) { return true; }, *result);
    return deletions;
  }

  vector<string> deletion_items;
  auto cb = [&](const auto& path, JsonType* val) {
    deletion_items.emplace_back(*path);
    return false;
  };

  error_code ec = json_path->Replace(result.value(), std::move(cb));
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return 0;
  }

  if (deletion_items.empty()) {
    return 0;
  }

  long total_deletions = 0;
  JsonType patch(json_array_arg, {});
  reverse(deletion_items.begin(), deletion_items.end());  // deletion should finish at root keys.
  for (const auto& item : deletion_items) {
    string pointer = ConvertToJsonPointer(item);
    total_deletions++;
    JsonType patch_item(json_object_arg, {{"op", "remove"}, {"path", pointer}});
    patch.emplace_back(patch_item);
  }

  jsonpatch::apply_patch(*result.value(), patch, ec);
  if (ec) {
    VLOG(1) << "Failed to apply patch on json with error: " << ec.message();
    return 0;
  }

  // SetString(op_args, key, j.as_string());
  return total_deletions;
}

// Returns a vector of string vectors,
// keys within the same object are stored in the same string vector.
OpResult<vector<StringVec>> OpObjKeys(const OpArgs& op_args, string_view key,
                                      const JsonPath& json_path) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<StringVec> vec;
  auto cb = [&vec](const string_view& key, const JsonType& val) {
    // Aligned with ElastiCache flavor.
    DVLOG(2) << "key: " << key << " val: " << val.to_string();

    if (!val.is_object()) {
      vec.emplace_back();
      return;
    }

    auto& current_object = vec.emplace_back();
    for (const auto& member : val.object_range()) {
      current_object.emplace_back(member.key());
    }
  };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

// Retruns array of string lengths after a successful operation.
OpResult<vector<OptSizeT>> OpStrAppend(const OpArgs& op_args, string_view key, JsonPath& path,
                                       facade::ArgRange strs) {
  vector<OptSizeT> vec;

  auto cb = [&](const auto&, JsonType* val) {
    if (val->is_string()) {
      string new_val = val->as_string();
      for (string_view str : strs) {
        new_val += str;
      }

      *val = new_val;
      vec.emplace_back(new_val.size());
    } else {
      vec.emplace_back(nullopt);
    }
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, path, std::move(cb));
  RETURN_BAD_STATUS(status);
  return vec;
}

// Returns the numbers of values cleared.
// Clears containers(arrays or objects) and zeroing numbers.
OpResult<long> OpClear(const OpArgs& op_args, string_view key, JsonPath& path) {
  long clear_items = 0;

  auto cb = [&clear_items](const auto& path, JsonType* val) {
    if (!(val->is_object() || val->is_array() || val->is_number())) {
      return false;
    }

    if (val->is_object()) {
      val->erase(val->object_range().begin(), val->object_range().end());
    } else if (val->is_array()) {
      val->erase(val->array_range().begin(), val->array_range().end());
    } else if (val->is_number()) {
      *val = 0;
    }

    clear_items += 1;
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, path, std::move(cb));
  RETURN_BAD_STATUS(status);
  return clear_items;
}

void ArrayPop(std::optional<std::string_view>, int index, JsonType* val,
              vector<OptString>* result) {
  if (!val->is_array() || val->empty()) {
    result->emplace_back(nullopt);
    return;
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
    result->emplace_back(nullopt);
    return;
  }

  result->push_back(std::move(str));
  val->erase(it);
  return;
};

// Returns string vector that represents the pop out values.
OpResult<vector<OptString>> OpArrPop(const OpArgs& op_args, string_view key, JsonPath& path,
                                     int index) {
  vector<OptString> vec;

  auto cb = [&vec, index](optional<string_view>, JsonType* val) {
    ArrayPop(nullopt, index, val, &vec);
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, path, std::move(cb));
  RETURN_BAD_STATUS(status);
  return vec;
}

// Returns numeric vector that represents the new length of the array at each path.
OpResult<vector<OptSizeT>> OpArrTrim(const OpArgs& op_args, string_view key, JsonPath& path,
                                     int start_index, int stop_index) {
  vector<OptSizeT> vec;
  auto cb = [&](const auto&, JsonType* val) {
    if (!val->is_array()) {
      vec.emplace_back(nullopt);
      return false;
    }

    if (val->empty()) {
      vec.emplace_back(0);
      return false;
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
      vec.emplace_back(0);
      return false;
    }

    auto trim_start_it = std::next(val->array_range().begin(), trim_start_index);
    auto trim_end_it = val->array_range().end();
    if (trim_end_index < val->size()) {
      trim_end_it = std::next(val->array_range().begin(), trim_end_index + 1);
    }

    *val = json_array<JsonType>(trim_start_it, trim_end_it);
    vec.emplace_back(val->size());
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, path, std::move(cb));
  RETURN_BAD_STATUS(status);

  return vec;
}

// Returns numeric vector that represents the new length of the array at each path.
OpResult<vector<OptSizeT>> OpArrInsert(const OpArgs& op_args, string_view key,
                                       const JsonPath& json_path, int index,
                                       const vector<JsonType>& new_values) {
  bool out_of_boundaries_encountered = false;
  vector<OptSizeT> vec;

  // Insert user-supplied value into the supplied index that should be valid.
  // If at least one index isn't valid within an array in the json doc, the operation is discarded.
  // Negative indexes start from the end of the array.
  auto cb = [&](const auto&, JsonType* val) {
    if (out_of_boundaries_encountered) {
      return false;
    }

    if (!val->is_array()) {
      vec.emplace_back(nullopt);
      return false;
    }

    size_t removal_index;
    if (index < 0) {
      if (val->empty()) {
        out_of_boundaries_encountered = true;
        return false;
      }

      int temp_index = index + val->size();
      if (temp_index < 0) {
        out_of_boundaries_encountered = true;
        return false;
      }

      removal_index = temp_index;
    } else {
      if ((size_t)index > val->size()) {
        out_of_boundaries_encountered = true;
        return false;
      }

      removal_index = index;
    }

    auto it = next(val->array_range().begin(), removal_index);
    for (auto& new_val : new_values) {
      it = val->insert(it, new_val);
      it++;
    }

    vec.emplace_back(val->size());
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, json_path, std::move(cb));
  RETURN_BAD_STATUS(status);

  if (out_of_boundaries_encountered) {
    return OpStatus::OUT_OF_RANGE;
  }

  return vec;
}

// Returns numeric vector that represents the new length of the array at each path, or Null reply
// if the matching JSON value is not an array.
OpResult<vector<OptSizeT>> OpArrAppend(const OpArgs& op_args, string_view key, JsonPath& path,
                                       const vector<JsonType>& append_values) {
  vector<OptSizeT> vec;

  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  auto cb = [&](const auto&, JsonType* val) {
    if (!val->is_array()) {
      vec.emplace_back(nullopt);
      return false;
    }
    for (auto& new_val : append_values) {
      val->emplace_back(new_val);
    }
    vec.emplace_back(val->size());
    return false;
  };

  OpStatus status = UpdateEntry(op_args, key, path, std::move(cb));
  RETURN_BAD_STATUS(status);

  return vec;
}

// Returns a numeric vector representing each JSON value first index of the JSON scalar.
// An index value of -1 represents unfound in the array.
// JSON scalar has types of string, boolean, null, and number.
OpResult<vector<OptLong>> OpArrIndex(const OpArgs& op_args, string_view key,
                                     const JsonPath& json_path, const JsonType& search_val,
                                     int start_index, int end_index) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  vector<OptLong> vec;
  auto cb = [&](const string_view&, const JsonType& val) {
    if (!val.is_array()) {
      vec.emplace_back(nullopt);
      return;
    }

    if (val.empty()) {
      vec.emplace_back(-1);
      return;
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
      vec.emplace_back(-1);
      return;
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

    vec.emplace_back(pos);
  };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

// Returns string vector that represents the query result of each supplied key.
vector<OptString> OpJsonMGet(const JsonPath& json_path, const Transaction* t, EngineShard* shard) {
  ShardArgs args = t->GetShardArgs(shard->shard_id());
  DCHECK(!args.Empty());
  vector<OptString> response(args.Size());

  auto& db_slice = shard->db_slice();
  unsigned index = 0;
  for (string_view key : args) {
    auto it_res = db_slice.FindReadOnly(t->GetDbContext(), key, OBJ_JSON);
    auto& dest = response[index++];
    if (!it_res.ok())
      continue;

    dest.emplace();
    JsonType* json_val = it_res.value()->second.GetJson();
    DCHECK(json_val) << "should have a valid JSON object for key " << key;

    vector<JsonType> query_result;
    auto cb = [&query_result](const string_view&, const JsonType& val) {
      query_result.push_back(val);
    };

    json_path.Evaluate(json_val, std::move(cb));

    if (query_result.empty()) {
      continue;
    }

    JsonType arr(json_array_arg);
    arr.reserve(query_result.size());
    for (auto& s : query_result) {
      arr.push_back(s);
    }

    string str;
    error_code ec;
    arr.dump(str, {}, ec);
    if (ec) {
      VLOG(1) << "Failed to dump JSON array to string with the error: " << ec.message();
    }

    dest = std::move(str);
  }

  return response;
}

// Returns numeric vector that represents the number of fields of JSON value at each path.
OpResult<vector<OptSizeT>> OpFields(const OpArgs& op_args, string_view key,
                                    const JsonPath& json_path) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view&, const JsonType& val) {
    vec.emplace_back(CountJsonFields(val));
  };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

// Returns json vector that represents the result of the json query.
OpResult<vector<JsonType>> OpResp(const OpArgs& op_args, string_view key,
                                  const JsonPath& json_path) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  RETURN_ON_BAD_STATUS(result);

  vector<JsonType> vec;
  auto cb = [&vec](const string_view&, const JsonType& val) { vec.emplace_back(val); };

  json_path.Evaluate(result.value(), std::move(cb));
  return vec;
}

// Returns boolean that represents the result of the operation.
OpResult<bool> OpSet(const OpArgs& op_args, string_view key, string_view path,
                     const JsonPath& json_path, std::string_view json_str, bool is_nx_condition,
                     bool is_xx_condition) {
  std::optional<JsonType> parsed_json = JsonFromString(json_str);
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
      auto it_res = op_args.shard->db_slice().FindReadOnly(op_args.db_cntx, key, OBJ_JSON);
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
  auto cb = [&](const auto&, JsonType* val) {
    path_exists = true;
    if (!is_nx_condition) {
      operation_result = true;
      *val = new_json;
    }
    return false;
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
      jsonpointer::add(json, pointer, new_json, ec);
      if (ec) {
        VLOG(1) << "Failed to add a JSON value to the following path: " << path
                << " with the error: " << ec.message();
        return OpStatus::SYNTAX_ERR;
      }

      operation_result = true;
    }

    return OpStatus::OK;
  };

  OpStatus status = UpdateEntry(op_args, key, json_path, std::move(cb), inserter);
  RETURN_BAD_STATUS(status);
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

// Implements the recursive algorithm from
// https://datatracker.ietf.org/doc/html/rfc7386#section-2
void RecursiveMerge(const JsonType& patch, JsonType* dest) {
  if (!patch.is_object()) {
    *dest = patch;
    return;
  }

  if (!dest->is_object()) {
    *dest = JsonType(json_object_arg, dest->get_allocator());
  }

  for (const auto& k_v : patch.object_range()) {
    if (k_v.value().is_null()) {
      dest->erase(k_v.key());
    } else {
      RecursiveMerge(k_v.value(), &dest->at(k_v.key()));
    }
  }
}

OpStatus OpMerge(const OpArgs& op_args, string_view key, std::string_view json_str) {
  std::optional<JsonType> parsed_json = JsonFromString(json_str);
  if (!parsed_json) {
    VLOG(1) << "got invalid JSON string '" << json_str << "' cannot be saved";
    return OpStatus::SYNTAX_ERR;
  }

  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_JSON);
  if (it_res.ok()) {
    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, it_res->it->second);

    JsonType* obj = it_res->it->second.GetJson();
    RecursiveMerge(*parsed_json, obj);
    it_res->post_updater.Run();
    op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, it_res->it->second);
    return OpStatus::OK;
  }

  // We add a new key only with path being root.
  if (it_res.status() != OpStatus::KEY_NOTFOUND) {
    return it_res.status();
  }

  // Add a new key.
  return OpSet(op_args, key, "$", json_str, false, false).status();
}

}  // namespace

void JsonFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view json_str = ArgS(args, 2);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  bool is_nx_condition = false;
  bool is_xx_condition = false;
  string_view operation_opts;
  if (args.size() > 3) {
    operation_opts = ArgS(args, 3);
    if (absl::EqualsIgnoreCase(operation_opts, "NX")) {
      is_nx_condition = true;
    } else if (absl::EqualsIgnoreCase(operation_opts, "XX")) {
      is_xx_condition = true;
    } else {
      cntx->SendError(kSyntaxErr);
      return;
    }
  }

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
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view value = ArgS(args, 2);

  if (path != "." && path != "$") {
    return cntx->SendError("Only root path is supported", kSyntaxErrType);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpMerge(t->GetOpArgs(shard), key, value);
  };

  OpStatus status = cntx->transaction->ScheduleSingleHop(cb);
  if (status == OpStatus::OK)
    return cntx->SendOk();
  cntx->SendError(status);
}

void JsonFamily::Resp(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = DefaultJsonPath;
  if (args.size() > 1) {
    path = ArgS(args, 1);
  }

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpResp(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<JsonType>> result = trans->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result) {
    rb->StartArray(result->size());
    for (const auto& it : *result) {
      SendJsonValue(rb, it);
    }
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::Debug(CmdArgList args, ConnectionContext* cntx) {
  function<decltype(OpFields)> func;
  string_view command = ArgS(args, 0);
  // The 'MEMORY' sub-command is not supported yet, calling to operation function should be added
  // here.
  if (absl::EqualsIgnoreCase(command, "help")) {
    auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
    rb->StartArray(2);
    rb->SendBulkString(
        "JSON.DEBUG FIELDS <key> <path> - report number of fields in the JSON element.");
    rb->SendBulkString("JSON.DEBUG HELP - print help message.");
    return;

  } else if (absl::EqualsIgnoreCase(command, "fields")) {
    func = &OpFields;
  } else {
    cntx->SendError(facade::UnknownSubCmd(command, "JSON.DEBUG"), facade::kSyntaxErrType);
    return;
  }

  if (args.size() < 3) {
    cntx->SendError(facade::WrongNumArgsError(cntx->cid->name()), facade::kSyntaxErrType);
    return;
  }

  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return func(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::MGet(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1U);

  string_view path = ArgS(args, args.size() - 1);
  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  Transaction* transaction = cntx->transaction;
  unsigned shard_count = shard_set->size();
  std::vector<vector<OptString>> mget_resp(shard_count);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    mget_resp[sid] = OpJsonMGet(json_path, t, shard);
    return OpStatus::OK;
  };

  OpStatus result = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, result);

  std::vector<OptString> results(args.size() - 1);
  for (ShardId sid = 0; sid < shard_count; ++sid) {
    if (!transaction->IsActive(sid))
      continue;

    vector<OptString>& res = mget_resp[sid];
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
  rb->StartArray(results.size());
  for (auto& it : results) {
    if (!it) {
      rb->SendNull();
    } else {
      rb->SendBulkString(*it);
    }
  }
}

void JsonFamily::ArrIndex(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

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
  OpResult<vector<OptLong>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
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

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

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
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));
  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::ArrAppend(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

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
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));
  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
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

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrTrim(t->GetOpArgs(shard), key, json_path, start_index, stop_index);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));
  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::ArrPop(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  int index = -1;

  if (args.size() >= 3) {
    if (!absl::SimpleAtoi(ArgS(args, 2), &index)) {
      VLOG(1) << "Failed to convert the following value to numeric, pop out the last item"
              << ArgS(args, 2);
    }
  }

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrPop(t->GetOpArgs(shard), key, json_path, index);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptString>> result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result) {
    rb->StartArray(result->size());
    for (auto& it : *result) {
      if (!it) {
        rb->SendNull();
      } else {
        rb->SendSimpleString(*it);
      }
    }
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::Clear(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpClear(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    cntx->SendLong(*result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::StrAppend(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));
  auto strs = args.subspan(2);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrAppend(t->GetOpArgs(shard), key, json_path, facade::ArgRange{strs});
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::ObjKeys(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = "$";
  if (args.size() == 2) {
    path = ArgS(args, 1);
  }

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpObjKeys(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<StringVec>> result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result) {
    rb->StartArray(result->size());
    for (auto& it : *result) {
      if (it.empty()) {
        rb->SendNullArray();
      } else {
        rb->SendStringArr(it);
      }
    }
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::Del(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path;

  std::optional<JsonPath> json_path;

  if (args.size() > 1) {
    path = ArgS(args, 1);
    json_path.emplace(GET_OR_SEND_UNEXPECTED(ParseJsonPath(path)));
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(std::move(cb));
  cntx->SendLong(*result);
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

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDoubleArithmetic(t->GetOpArgs(shard), key, json_path, dnum, OP_ADD);
  };

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  if (result) {
    rb->SendBulkString(*result);
  } else {
    cntx->SendError(result.status());
  }
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

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDoubleArithmetic(t->GetOpArgs(shard), key, json_path, dnum, OP_MULTIPLY);
  };

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    rb->SendBulkString(*result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::Toggle(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpToggle(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptBool>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::Type(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpType(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<string>> result = trans->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (result) {
    if (result->empty()) {
      // When vector is empty, the path doesn't exist in the corresponding json.
      rb->SendNull();
    } else {
      rb->SendStringArr(*result);
    }
  } else {
    if (result.status() == OpStatus::KEY_NOTFOUND) {
      rb->SendNullArray();
    } else {
      cntx->SendError(result.status());
    }
  }
}

void JsonFamily::ArrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrLen(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::ObjLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpObjLen(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::StrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path));

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrLen(t->GetOpArgs(shard), key, json_path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    cntx->SendError(result.status());
  }
}

void JsonFamily::Get(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1U);

  facade::CmdArgParser parser{args};
  string_view key = parser.Next();

  OptString indent;
  OptString new_line;
  OptString space;

  vector<pair<string_view, JsonPath>> paths;

  while (parser.HasNext()) {
    if (parser.Check("SPACE").IgnoreCase().ExpectTail(1)) {
      space = parser.Next();
      continue;
    }
    if (parser.Check("NEWLINE").IgnoreCase().ExpectTail(1)) {
      new_line = parser.Next();
      continue;
    }
    if (parser.Check("INDENT").IgnoreCase().ExpectTail(1)) {
      indent = parser.Next();
      continue;
    }

    string_view path_str = parser.Next();
    JsonPath json_path = GET_OR_SEND_UNEXPECTED(ParseJsonPath(path_str));

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
  constexpr size_t kMsetFlags = CO::WRITE | CO::DENYOOM | CO::FAST | CO::INTERLEAVED_KEYS;
  registry->StartFamily();
  *registry << CI{"JSON.GET", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(Get);
  *registry << CI{"JSON.MGET", CO::READONLY | CO::FAST, -3, 1, -2, acl::JSON}.HFUNC(MGet);
  *registry << CI{"JSON.TYPE", CO::READONLY | CO::FAST, 3, 1, 1, acl::JSON}.HFUNC(Type);
  *registry << CI{"JSON.STRLEN", CO::READONLY | CO::FAST, 3, 1, 1, acl::JSON}.HFUNC(StrLen);
  *registry << CI{"JSON.OBJLEN", CO::READONLY | CO::FAST, 3, 1, 1, acl::JSON}.HFUNC(ObjLen);
  *registry << CI{"JSON.ARRLEN", CO::READONLY | CO::FAST, 3, 1, 1, acl::JSON}.HFUNC(ArrLen);
  *registry << CI{"JSON.TOGGLE", CO::WRITE | CO::FAST, 3, 1, 1, acl::JSON}.HFUNC(Toggle);
  *registry << CI{"JSON.NUMINCRBY", CO::WRITE | CO::FAST, 4, 1, 1, acl::JSON}.HFUNC(NumIncrBy);
  *registry << CI{"JSON.NUMMULTBY", CO::WRITE | CO::FAST, 4, 1, 1, acl::JSON}.HFUNC(NumMultBy);
  *registry << CI{"JSON.DEL", CO::WRITE, -2, 1, 1, acl::JSON}.HFUNC(Del);
  *registry << CI{"JSON.FORGET", CO::WRITE, -2, 1, 1, acl::JSON}.HFUNC(
      Del);  // An alias of JSON.DEL.
  *registry << CI{"JSON.OBJKEYS", CO::READONLY | CO::FAST, -2, 1, 1, acl::JSON}.HFUNC(ObjKeys);
  *registry << CI{"JSON.STRAPPEND", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, acl::JSON}.HFUNC(
      StrAppend);
  *registry << CI{"JSON.CLEAR", CO::WRITE | CO::FAST, 3, 1, 1, acl::JSON}.HFUNC(Clear);
  *registry << CI{"JSON.ARRPOP", CO::WRITE | CO::FAST, -3, 1, 1, acl::JSON}.HFUNC(ArrPop);
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
