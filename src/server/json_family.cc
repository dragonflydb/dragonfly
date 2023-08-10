// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

extern "C" {
#include "redis/object.h"
}

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

#include "base/logging.h"
#include "core/json_object.h"
#include "server/command_registry.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/search/doc_index.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace jsoncons;

using JsonExpression = jsonpath::jsonpath_expression<JsonType>;
using OptBool = optional<bool>;
using OptLong = optional<long>;
using OptSizeT = optional<size_t>;
using OptString = optional<string>;
using JsonReplaceCb = function<void(const string&, JsonType&)>;
using JsonReplaceVerify = std::function<OpStatus(JsonType&)>;
using CI = CommandId;

static const char DefaultJsonPath[] = "$";

namespace {

inline OpStatus JsonReplaceVerifyNoOp(JsonType&) {
  return OpStatus::OK;
}

void SetJson(const OpArgs& op_args, string_view key, JsonType&& value) {
  auto& db_slice = op_args.shard->db_slice();
  DbIndex db_index = op_args.db_cntx.db_index;
  auto [it_output, added] = db_slice.AddOrFind(op_args.db_cntx, key);

  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, it_output->second);
  db_slice.PreUpdate(db_index, it_output);

  it_output->second.SetJson(std::move(value));

  db_slice.PostUpdate(db_index, it_output, key);
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, it_output->second);
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

JsonExpression ParseJsonPath(string_view path, error_code* ec) {
  if (path == ".") {
    // RedisJson V1 uses the dot for root level access.
    // There are more incompatibilities with legacy paths which are not supported.
    path = "$"sv;
  }

  return jsonpath::make_expression<JsonType>(path, *ec);
}

template <typename T>
void PrintOptVec(ConnectionContext* cntx, const OpResult<vector<optional<T>>>& result) {
  if (result->empty()) {
    (*cntx)->SendNullArray();
  } else {
    (*cntx)->StartArray(result->size());
    for (auto& it : *result) {
      if (it.has_value()) {
        if constexpr (is_floating_point_v<T>) {
          (*cntx)->SendDouble(*it);
        } else {
          static_assert(is_integral_v<T>, "Integral required.");
          (*cntx)->SendLong(*it);
        }
      } else {
        (*cntx)->SendNull();
      }
    }
  }
}

error_code JsonReplace(JsonType& instance, string_view path, JsonReplaceCb callback) {
  using evaluator_t = jsoncons::jsonpath::detail::jsonpath_evaluator<JsonType, JsonType&>;
  using value_type = evaluator_t::value_type;
  using reference = evaluator_t::reference;
  using json_selector_t = evaluator_t::path_expression_type;
  using json_location_type = evaluator_t::json_location_type;
  jsonpath::custom_functions<JsonType> funcs = jsonpath::custom_functions<JsonType>();

  error_code ec;
  jsoncons::jsonpath::detail::static_resources<value_type, reference> static_resources(funcs);
  evaluator_t e;
  json_selector_t expr = e.compile(static_resources, path, ec);
  if (ec) {
    return ec;
  }

  jsoncons::jsonpath::detail::dynamic_resources<value_type, reference> resources;
  auto f = [&callback](const json_location_type& path, reference val) {
    callback(path.to_string(), val);
  };

  expr.evaluate(resources, instance, resources.root_path_node(), instance, f,
                jsonpath::result_options::nodups);
  return ec;
}

OpStatus UpdateEntry(const OpArgs& op_args, std::string_view key, std::string_view path,
                     JsonReplaceCb callback, JsonReplaceVerify verify_op = JsonReplaceVerifyNoOp) {
  OpResult<PrimeIterator> it_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_JSON);
  if (!it_res.ok()) {
    return it_res.status();
  }

  PrimeIterator entry_it = it_res.value();
  auto& db_slice = op_args.shard->db_slice();
  auto db_index = op_args.db_cntx.db_index;
  JsonType* json_val = entry_it->second.GetJson();
  DCHECK(json_val) << "should have a valid JSON object for key '" << key << "' the type for it is '"
                   << entry_it->second.ObjType() << "'";
  JsonType& json_entry = *json_val;

  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, entry_it->second);
  db_slice.PreUpdate(db_index, entry_it);

  // Run the update operation on this entry
  error_code ec = JsonReplace(json_entry, path, callback);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  // Make sure that we don't have other internal issue with the operation
  OpStatus res = verify_op(json_entry);
  if (res == OpStatus::OK) {
    db_slice.PostUpdate(db_index, entry_it, key);
    op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, entry_it->second);
  }

  return res;
}

OpResult<JsonType*> GetJson(const OpArgs& op_args, string_view key) {
  OpResult<PrimeIterator> it_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_JSON);
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

void SendJsonValue(ConnectionContext* cntx, const JsonType& j) {
  if (j.is_double()) {
    (*cntx)->SendDouble(j.as_double());
  } else if (j.is_number()) {
    (*cntx)->SendLong(j.as_integer<long>());
  } else if (j.is_bool()) {
    (*cntx)->SendSimpleString(j.as_bool() ? "true" : "false");
  } else if (j.is_null()) {
    (*cntx)->SendNull();
  } else if (j.is_string()) {
    (*cntx)->SendSimpleString(j.as_string_view());
  } else if (j.is_object()) {
    (*cntx)->StartArray(j.size() + 1);
    (*cntx)->SendSimpleString("{");
    for (const auto& item : j.object_range()) {
      (*cntx)->StartArray(2);
      (*cntx)->SendSimpleString(item.key());
      SendJsonValue(cntx, item.value());
    }
  } else if (j.is_array()) {
    (*cntx)->StartArray(j.size() + 1);
    (*cntx)->SendSimpleString("[");
    for (const auto& item : j.array_range()) {
      SendJsonValue(cntx, item);
    }
  }
}

OpResult<string> OpGet(const OpArgs& op_args, string_view key,
                       vector<pair<string_view, JsonExpression>> expressions) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  const JsonType& json_entry = *(result.value());
  if (expressions.empty()) {
    // this implicitly means that we're using $ which
    // means we just brings all values
    return json_entry.to_string();
  }
  if (expressions.size() == 1) {
    json out = expressions[0].second.evaluate(json_entry);
    return out.as<string>();
  }

  json out;
  for (auto& expr : expressions) {
    json eval = expr.second.evaluate(json_entry);
    out[expr.first] = eval;
  }

  return out.as<string>();
}

OpResult<vector<string>> OpType(const OpArgs& op_args, string_view key, JsonExpression expression) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  const JsonType& json_entry = *(result.value());
  vector<string> vec;
  auto cb = [&vec](const string_view& path, const JsonType& val) {
    vec.emplace_back(JsonTypeToName(val));
  };

  expression.evaluate(json_entry, cb);
  return vec;
}

OpResult<vector<OptSizeT>> OpStrLen(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }
  const JsonType& json_entry = *(result.value());
  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const JsonType& val) {
    if (val.is_string()) {
      vec.emplace_back(val.as_string_view().size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  expression.evaluate(json_entry, cb);
  return vec;
}

OpResult<vector<OptSizeT>> OpObjLen(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  const JsonType& json_entry = *(result.value());
  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const JsonType& val) {
    if (val.is_object()) {
      vec.emplace_back(val.size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  expression.evaluate(json_entry, cb);
  return vec;
}

OpResult<vector<OptSizeT>> OpArrLen(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  const JsonType& json_entry = *(result.value());
  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const JsonType& val) {
    if (val.is_array()) {
      vec.emplace_back(val.size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  expression.evaluate(json_entry, cb);
  return vec;
}

OpResult<vector<OptBool>> OpToggle(const OpArgs& op_args, string_view key, string_view path) {
  vector<OptBool> vec;
  auto cb = [&vec](const string& path, JsonType& val) {
    if (val.is_bool()) {
      bool current_val = val.as_bool() ^ true;
      val = current_val;
      vec.emplace_back(current_val);
    } else {
      vec.emplace_back(nullopt);
    }
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb);
  if (status != OpStatus::OK) {
    return status;
  }

  return vec;
}

template <typename Op>
OpResult<string> OpDoubleArithmetic(const OpArgs& op_args, string_view key, string_view path,
                                    double num, Op arithmetic_op) {
  bool is_result_overflow = false;
  double int_part;
  bool has_fractional_part = (modf(num, &int_part) != 0);
  json output(json_array_arg);

  auto cb = [&](const string& path, JsonType& val) {
    if (val.is_number()) {
      double result = arithmetic_op(val.as<double>(), num);
      if (isinf(result)) {
        is_result_overflow = true;
        return;
      }

      if (val.is_double() || has_fractional_part) {
        val = result;
      } else {
        val = (uint64_t)result;
      }
      output.push_back(val);
    } else {
      output.push_back(json::null());
    }
  };

  auto verifier = [&is_result_overflow](JsonType&) {
    if (is_result_overflow) {
      return OpStatus::INVALID_NUMERIC_RESULT;
    }
    return OpStatus::OK;
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb, verifier);
  if (status != OpStatus::OK) {
    return status;
  }

  return output.as_string();
}

OpResult<long> OpDel(const OpArgs& op_args, string_view key, string_view path) {
  long total_deletions = 0;
  if (path.empty()) {
    auto& db_slice = op_args.shard->db_slice();
    auto [it, _] = db_slice.FindExt(op_args.db_cntx, key);
    total_deletions += long(db_slice.Del(op_args.db_cntx.db_index, it));
    return total_deletions;
  }

  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return total_deletions;
  }

  vector<string> deletion_items;
  auto cb = [&](const string& path, JsonType& val) { deletion_items.emplace_back(path); };

  // json j = move(result.value());
  JsonType& json_entry = *(result.value());
  error_code ec = JsonReplace(json_entry, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return total_deletions;
  }

  if (deletion_items.empty()) {
    return total_deletions;
  }

  json patch(json_array_arg, {});
  reverse(deletion_items.begin(), deletion_items.end());  // deletion should finish at root keys.
  for (const auto& item : deletion_items) {
    string pointer = ConvertToJsonPointer(item);
    total_deletions++;
    json patch_item(json_object_arg, {{"op", "remove"}, {"path", pointer}});
    patch.emplace_back(patch_item);
  }

  jsonpatch::apply_patch(json_entry, patch, ec);
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
                                      JsonExpression expression) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<StringVec> vec;
  auto cb = [&vec](const string_view& path, const JsonType& val) {
    // Aligned with ElastiCache flavor.
    if (!val.is_object()) {
      vec.emplace_back();
      return;
    }

    auto& current_object = vec.emplace_back();
    for (const auto& member : val.object_range()) {
      current_object.emplace_back(member.key());
    }
  };
  JsonType& json_entry = *(result.value());

  expression.evaluate(json_entry, cb);
  return vec;
}

// Retruns array of string lengths after a successful operation.
OpResult<vector<OptSizeT>> OpStrAppend(const OpArgs& op_args, string_view key, string_view path,
                                       const vector<string_view>& strs) {
  vector<OptSizeT> vec;
  auto cb = [&](const string& path, JsonType& val) {
    if (val.is_string()) {
      string new_val = val.as_string();
      for (auto& str : strs) {
        new_val += str;
      }

      val = new_val;
      vec.emplace_back(new_val.size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb);
  if (status != OpStatus::OK) {
    return status;
  }

  return vec;
}

// Returns the numbers of values cleared.
// Clears containers(arrays or objects) and zeroing numbers.
OpResult<long> OpClear(const OpArgs& op_args, string_view key, string_view path) {
  long clear_items = 0;
  auto cb = [&clear_items](const string& path, JsonType& val) {
    if (!(val.is_object() || val.is_array() || val.is_number())) {
      return;
    }

    if (val.is_object()) {
      val.erase(val.object_range().begin(), val.object_range().end());
    } else if (val.is_array()) {
      val.erase(val.array_range().begin(), val.array_range().end());
    } else if (val.is_number()) {
      val = 0;
    }

    clear_items += 1;
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb);
  if (status != OpStatus::OK) {
    return status;
  }
  return clear_items;
}

// Returns string vector that represents the pop out values.
OpResult<vector<OptString>> OpArrPop(const OpArgs& op_args, string_view key, string_view path,
                                     int index) {
  vector<OptString> vec;
  auto cb = [&](const string& path, JsonType& val) {
    if (!val.is_array() || val.empty()) {
      vec.emplace_back(nullopt);
      return;
    }

    size_t removal_index;
    if (index < 0) {
      int temp_index = index + val.size();
      removal_index = abs(temp_index);
    } else {
      removal_index = index;
    }

    if (removal_index >= val.size()) {
      removal_index %= val.size();  // rounded to the array boundaries.
    }

    auto it = std::next(val.array_range().begin(), removal_index);
    string str;
    error_code ec;
    it->dump(str, {}, ec);
    if (ec) {
      VLOG(1) << "Failed to dump JSON to string with the error: " << ec.message();
      return;
    }

    vec.push_back(str);
    val.erase(it);
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb);
  if (status != OpStatus::OK) {
    return status;
  }

  return vec;
}

// Returns numeric vector that represents the new length of the array at each path.
OpResult<vector<OptSizeT>> OpArrTrim(const OpArgs& op_args, string_view key, string_view path,
                                     int start_index, int stop_index) {
  vector<OptSizeT> vec;
  auto cb = [&](const string& path, JsonType& val) {
    if (!val.is_array()) {
      vec.emplace_back(nullopt);
      return;
    }

    if (val.empty()) {
      vec.emplace_back(0);
      return;
    }

    size_t trim_start_index;
    if (start_index < 0) {
      trim_start_index = 0;
    } else {
      trim_start_index = start_index;
    }

    size_t trim_end_index;
    if ((size_t)stop_index >= val.size()) {
      trim_end_index = val.size();
    } else {
      trim_end_index = stop_index;
    }

    if (trim_start_index >= val.size() || trim_start_index > trim_end_index) {
      val.erase(val.array_range().begin(), val.array_range().end());
      vec.emplace_back(0);
      return;
    }

    auto trim_start_it = std::next(val.array_range().begin(), trim_start_index);
    auto trim_end_it = val.array_range().end();
    if (trim_end_index < val.size()) {
      trim_end_it = std::next(val.array_range().begin(), trim_end_index + 1);
    }

    val = json_array<JsonType>(trim_start_it, trim_end_it);
    vec.emplace_back(val.size());
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb);
  if (status != OpStatus::OK) {
    return status;
  }
  return vec;
}

// Returns numeric vector that represents the new length of the array at each path.
OpResult<vector<OptSizeT>> OpArrInsert(const OpArgs& op_args, string_view key, string_view path,
                                       int index, const vector<JsonType>& new_values) {
  bool out_of_boundaries_encountered = false;
  vector<OptSizeT> vec;
  // Insert user-supplied value into the supplied index that should be valid.
  // If at least one index isn't valid within an array in the json doc, the operation is discarded.
  // Negative indexes start from the end of the array.
  auto cb = [&](const string& path, JsonType& val) {
    if (out_of_boundaries_encountered) {
      return;
    }

    if (!val.is_array()) {
      vec.emplace_back(nullopt);
      return;
    }

    size_t removal_index;
    if (index < 0) {
      if (val.empty()) {
        out_of_boundaries_encountered = true;
        return;
      }

      int temp_index = index + val.size();
      if (temp_index < 0) {
        out_of_boundaries_encountered = true;
        return;
      }

      removal_index = temp_index;
    } else {
      if ((size_t)index > val.size()) {
        out_of_boundaries_encountered = true;
        return;
      }

      removal_index = index;
    }

    auto it = next(val.array_range().begin(), removal_index);
    for (auto& new_val : new_values) {
      it = val.insert(it, new_val);
      it++;
    }

    vec.emplace_back(val.size());
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb);
  if (status != OpStatus::OK) {
    return status;
  }

  if (out_of_boundaries_encountered) {
    return OpStatus::OUT_OF_RANGE;
  }

  return vec;
}

// Returns numeric vector that represents the new length of the array at each path, or Null reply
// if the matching JSON value is not an array.
OpResult<vector<OptSizeT>> OpArrAppend(const OpArgs& op_args, string_view key, string_view path,
                                       const vector<JsonType>& append_values) {
  vector<OptSizeT> vec;

  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  auto cb = [&](const string& path, JsonType& val) {
    if (!val.is_array()) {
      vec.emplace_back(nullopt);
      return;
    }
    for (auto& new_val : append_values) {
      val.emplace_back(new_val);
    }
    vec.emplace_back(val.size());
  };

  OpStatus status = UpdateEntry(op_args, key, path, cb);
  if (status != OpStatus::OK) {
    return status;
  }

  return vec;
}

// Returns a numeric vector representing each JSON value first index of the JSON scalar.
// An index value of -1 represents unfound in the array.
// JSON scalar has types of string, boolean, null, and number.
OpResult<vector<OptLong>> OpArrIndex(const OpArgs& op_args, string_view key,
                                     JsonExpression expression, const JsonType& search_val,
                                     int start_index, int end_index) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptLong> vec;
  auto cb = [&](const string_view& path, const JsonType& val) {
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
  JsonType& json_entry = *(result.value());
  expression.evaluate(json_entry, cb);
  return vec;
}

// Returns string vector that represents the query result of each supplied key.
vector<OptString> OpMGet(JsonExpression expression, const Transaction* t, EngineShard* shard) {
  auto args = t->GetShardArgs(shard->shard_id());
  DCHECK(!args.empty());
  vector<OptString> response(args.size());

  auto& db_slice = shard->db_slice();
  for (size_t i = 0; i < args.size(); ++i) {
    OpResult<PrimeIterator> it_res = db_slice.Find(t->GetDbContext(), args[i], OBJ_JSON);
    if (!it_res.ok())
      continue;

    auto& dest = response[i].emplace();
    JsonType* json_val = it_res.value()->second.GetJson();
    DCHECK(json_val) << "should have a valid JSON object for key " << args[i];

    vector<JsonType> query_result;
    auto cb = [&query_result](const string_view& path, const JsonType& val) {
      query_result.push_back(val);
    };

    const JsonType& json_entry = *(json_val);
    expression.evaluate(json_entry, cb);

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

    dest = move(str);
  }

  return response;
}

// Returns numeric vector that represents the number of fields of JSON value at each path.
OpResult<vector<OptSizeT>> OpFields(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const JsonType& val) {
    vec.emplace_back(CountJsonFields(val));
  };
  const JsonType& json_entry = *(result.value());
  expression.evaluate(json_entry, cb);
  return vec;
}

// Returns json vector that represents the result of the json query.
OpResult<vector<JsonType>> OpResp(const OpArgs& op_args, string_view key,
                                  JsonExpression expression) {
  OpResult<JsonType*> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<JsonType> vec;
  auto cb = [&vec](const string_view& path, const JsonType& val) { vec.emplace_back(val); };
  const JsonType& json_entry = *(result.value());
  expression.evaluate(json_entry, cb);
  return vec;
}

// Returns boolean that represents the result of the operation.
OpResult<bool> OpSet(const OpArgs& op_args, string_view key, string_view path,
                     std::string_view json_str, bool is_nx_condition, bool is_xx_condition) {
  std::optional<JsonType> parsed_json = JsonFromString(json_str);
  if (!parsed_json) {
    LOG(WARNING) << "got invalid JSON string '" << json_str << "' cannot be saved";
    return OpStatus::SYNTAX_ERR;
  }

  // The whole key should be replaced.
  // NOTE: unlike in Redis, we are overriding the value when the path is "$"
  // this is regardless of the current key type. In redis if the key exists
  // and its not JSON, it would return an error.
  if (path == "." || path == "$") {
    if (is_nx_condition || is_xx_condition) {
      OpResult<PrimeIterator> it_res =
          op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_JSON);
      bool key_exists = (it_res.status() != OpStatus::KEY_NOTFOUND);
      if (is_nx_condition && key_exists) {
        return false;
      }

      if (is_xx_condition && !key_exists) {
        return false;
      }
    }

    SetJson(op_args, key, std::move(parsed_json.value()));
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
  auto cb = [&](const string& path, JsonType& val) {
    path_exists = true;
    if (!is_nx_condition) {
      operation_result = true;
      val = new_json;
    }
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

  OpStatus status = UpdateEntry(op_args, key, path, cb, inserter);
  if (status != OpStatus::OK) {
    return status;
  }

  return operation_result;
}

}  // namespace

void JsonFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view json_str = ArgS(args, 2);
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
      (*cntx)->SendError(kSyntaxErr);
      return;
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, path, json_str, is_nx_condition, is_xx_condition);
  };

  Transaction* trans = cntx->transaction;
  OpResult<bool> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    if (*result) {
      (*cntx)->SendSimpleString("OK");
    } else {
      (*cntx)->SendNull();
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::Resp(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = DefaultJsonPath;
  if (args.size() > 1) {
    path = ArgS(args, 1);
  }

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpResp(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<JsonType>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    (*cntx)->StartArray(result->size());
    for (const auto& it : *result) {
      SendJsonValue(cntx, it);
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::Debug(CmdArgList args, ConnectionContext* cntx) {
  function<decltype(OpFields)> func;
  string_view command = ArgS(args, 0);
  // The 'MEMORY' sub-command is not supported yet, calling to operation function should be added
  // here.
  if (absl::EqualsIgnoreCase(command, "help")) {
    (*cntx)->StartArray(2);
    (*cntx)->SendSimpleString(
        "JSON.DEBUG FIELDS <key> <path> - report number of fields in the JSON element.");
    (*cntx)->SendSimpleString("JSON.DEBUG HELP - print help message.");
    return;

  } else if (absl::EqualsIgnoreCase(command, "fields")) {
    func = &OpFields;

  } else {
    (*cntx)->SendError(facade::UnknownSubCmd(command, "JSON.DEBUG"), facade::kSyntaxErrType);
    return;
  }

  if (args.size() < 3) {
    (*cntx)->SendError(facade::WrongNumArgsError(cntx->cid->name()), facade::kSyntaxErrType);
    return;
  }

  error_code ec;
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return func(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::MGet(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1U);

  error_code ec;
  string_view path = ArgS(args, args.size() - 1);
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  Transaction* transaction = cntx->transaction;
  unsigned shard_count = shard_set->size();
  std::vector<vector<OptString>> mget_resp(shard_count);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    mget_resp[sid] = OpMGet(ParseJsonPath(path, &ec), t, shard);
    return OpStatus::OK;
  };

  OpStatus result = transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, result);

  std::vector<OptString> results(args.size() - 1);
  for (ShardId sid = 0; sid < shard_count; ++sid) {
    if (!transaction->IsActive(sid))
      continue;

    vector<OptString>& res = mget_resp[sid];
    ArgSlice slice = transaction->GetShardArgs(sid);

    DCHECK(!slice.empty());
    DCHECK_EQ(slice.size(), res.size());

    for (size_t j = 0; j < slice.size(); ++j) {
      if (!res[j])
        continue;

      uint32_t indx = transaction->ReverseArgIndex(sid, j);
      results[indx] = move(res[j]);
    }
  }

  (*cntx)->StartArray(results.size());
  for (auto& it : results) {
    if (!it) {
      (*cntx)->SendNull();
    } else {
      (*cntx)->SendBulkString(*it);
    }
  }
}

void JsonFamily::ArrIndex(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  optional<JsonType> search_value = JsonFromString(ArgS(args, 2));
  if (!search_value) {
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  if (search_value->is_object() || search_value->is_array()) {
    (*cntx)->SendError(kWrongTypeErr);
    return;
  }

  int start_index = 0;
  if (args.size() >= 4) {
    if (!absl::SimpleAtoi(ArgS(args, 3), &start_index)) {
      VLOG(1) << "Failed to convert the start index to numeric" << ArgS(args, 3);
      (*cntx)->SendError(kInvalidIntErr);
      return;
    }
  }

  int end_index = 0;
  if (args.size() >= 5) {
    if (!absl::SimpleAtoi(ArgS(args, 4), &end_index)) {
      VLOG(1) << "Failed to convert the stop index to numeric" << ArgS(args, 4);
      (*cntx)->SendError(kInvalidIntErr);
      return;
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrIndex(t->GetOpArgs(shard), key, move(expression), *search_value, start_index,
                      end_index);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptLong>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::ArrInsert(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  int index = -1;

  if (!absl::SimpleAtoi(ArgS(args, 2), &index)) {
    VLOG(1) << "Failed to convert the following value to numeric: " << ArgS(args, 2);
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  vector<JsonType> new_values;
  for (size_t i = 3; i < args.size(); i++) {
    optional<JsonType> val = JsonFromString(ArgS(args, i));
    if (!val) {
      (*cntx)->SendError(kSyntaxErr);
      return;
    }

    new_values.emplace_back(move(*val));
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrInsert(t->GetOpArgs(shard), key, path, index, new_values);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));
  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::ArrAppend(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  vector<JsonType> append_values;
  for (size_t i = 2; i < args.size(); ++i) {
    optional<JsonType> converted_val = JsonFromString(ArgS(args, i));
    if (!converted_val) {
      (*cntx)->SendError(kSyntaxErr);
      return;
    }

    if (converted_val->is_object()) {
      (*cntx)->SendError(kWrongTypeErr);
      return;
    }

    append_values.emplace_back(converted_val);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrAppend(t->GetOpArgs(shard), key, path, append_values);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));
  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::ArrTrim(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  int start_index;
  int stop_index;

  if (!absl::SimpleAtoi(ArgS(args, 2), &start_index)) {
    VLOG(1) << "Failed to parse array start index";
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  if (!absl::SimpleAtoi(ArgS(args, 3), &stop_index)) {
    VLOG(1) << "Failed to parse array stop index";
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  if (stop_index < 0) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrTrim(t->GetOpArgs(shard), key, path, start_index, stop_index);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));
  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
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

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrPop(t->GetOpArgs(shard), key, path, index);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptString>> result = trans->ScheduleSingleHopT(move(cb));
  if (result) {
    (*cntx)->StartArray(result->size());
    for (auto& it : *result) {
      if (!it) {
        (*cntx)->SendNull();
      } else {
        (*cntx)->SendSimpleString(*it);
      }
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::Clear(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpClear(t->GetOpArgs(shard), key, path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::StrAppend(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  vector<string_view> strs;
  for (size_t i = 2; i < args.size(); ++i) {
    strs.emplace_back(ArgS(args, i));
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrAppend(t->GetOpArgs(shard), key, path, strs);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::ObjKeys(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpObjKeys(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<StringVec>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    (*cntx)->StartArray(result->size());
    for (auto& it : *result) {
      if (it.empty()) {
        (*cntx)->SendNullArray();
      } else {
        (*cntx)->SendStringArr(it);
      }
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::Del(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path;
  if (args.size() > 1) {
    path = ArgS(args, 1);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(move(cb));
  (*cntx)->SendLong(*result);
}

void JsonFamily::NumIncrBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view num = ArgS(args, 2);

  double dnum;
  if (!ParseDouble(num, &dnum)) {
    (*cntx)->SendError(kWrongTypeErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDoubleArithmetic(t->GetOpArgs(shard), key, path, dnum, plus<double>{});
  };

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    (*cntx)->SendSimpleString(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::NumMultBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);
  string_view num = ArgS(args, 2);

  double dnum;
  if (!ParseDouble(num, &dnum)) {
    (*cntx)->SendError(kWrongTypeErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDoubleArithmetic(t->GetOpArgs(shard), key, path, dnum, multiplies<double>{});
  };

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    (*cntx)->SendSimpleString(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::Toggle(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpToggle(t->GetOpArgs(shard), key, path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptBool>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::Type(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpType(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<string>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    if (result->empty()) {
      // When vector is empty, the path doesn't exist in the corresponding json.
      (*cntx)->SendNull();
    } else {
      (*cntx)->SendStringArr(*result);
    }
  } else {
    if (result.status() == OpStatus::KEY_NOTFOUND) {
      (*cntx)->SendNullArray();
    } else {
      (*cntx)->SendError(result.status());
    }
  }
}

void JsonFamily::ArrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpArrLen(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::ObjLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpObjLen(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::StrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view path = ArgS(args, 1);

  error_code ec;
  JsonExpression expression = ParseJsonPath(path, &ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrLen(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    PrintOptVec(cntx, result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void JsonFamily::Get(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 1U);
  string_view key = ArgS(args, 0);

  vector<pair<string_view, JsonExpression>> expressions;

  for (size_t i = 1; i < args.size(); ++i) {
    string_view path = ArgS(args, i);
    error_code ec;
    JsonExpression expr = ParseJsonPath(path, &ec);

    if (ec) {
      LOG(WARNING) << "path '" << path << "': Invalid JSONPath syntax: " << ec.message();
      return (*cntx)->SendError(kSyntaxErr);
    }
    expressions.emplace_back(path, move(expr));
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGet(t->GetOpArgs(shard), key, move(expressions));
  };

  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    (*cntx)->SendBulkString(*result);
  } else {
    if (result == facade::OpStatus::KEY_NOTFOUND) {
      // Match what Redis returning
      (*cntx)->SendNull();
    } else {
      (*cntx)->SendError(result.status());
    }
  }
}

#define HFUNC(x) SetHandler(&JsonFamily::x)

void JsonFamily::Register(CommandRegistry* registry) {
  *registry << CI{"JSON.GET", CO::READONLY | CO::FAST, -2, 1, 1, 1}.HFUNC(Get);
  *registry << CI{"JSON.MGET", CO::READONLY | CO::FAST | CO::REVERSE_MAPPING, -3, 1, -2, 1}.HFUNC(
      MGet);
  *registry << CI{"JSON.TYPE", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(Type);
  *registry << CI{"JSON.STRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(StrLen);
  *registry << CI{"JSON.OBJLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ObjLen);
  *registry << CI{"JSON.ARRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ArrLen);
  *registry << CI{"JSON.TOGGLE", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Toggle);
  *registry << CI{"JSON.NUMINCRBY", CO::WRITE | CO::FAST, 4, 1, 1, 1}.HFUNC(NumIncrBy);
  *registry << CI{"JSON.NUMMULTBY", CO::WRITE | CO::FAST, 4, 1, 1, 1}.HFUNC(NumMultBy);
  *registry << CI{"JSON.DEL", CO::WRITE, -2, 1, 1, 1}.HFUNC(Del);
  *registry << CI{"JSON.FORGET", CO::WRITE, -2, 1, 1, 1}.HFUNC(Del);  // An alias of JSON.DEL.
  *registry << CI{"JSON.OBJKEYS", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ObjKeys);
  *registry << CI{"JSON.STRAPPEND", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, 1}.HFUNC(
      StrAppend);
  *registry << CI{"JSON.CLEAR", CO::WRITE | CO::FAST, 3, 1, 1, 1}.HFUNC(Clear);
  *registry << CI{"JSON.ARRPOP", CO::WRITE | CO::FAST, -3, 1, 1, 1}.HFUNC(ArrPop);
  *registry << CI{"JSON.ARRTRIM", CO::WRITE | CO::FAST, 5, 1, 1, 1}.HFUNC(ArrTrim);
  *registry << CI{"JSON.ARRINSERT", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, 1}.HFUNC(
      ArrInsert);
  *registry << CI{"JSON.ARRAPPEND", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, 1}.HFUNC(
      ArrAppend);
  *registry << CI{"JSON.ARRINDEX", CO::READONLY | CO::FAST, -4, 1, 1, 1}.HFUNC(ArrIndex);
  *registry << CI{"JSON.DEBUG", CO::READONLY | CO::FAST, -2, 1, 1, 1}.HFUNC(Debug);
  *registry << CI{"JSON.RESP", CO::READONLY | CO::FAST, -2, 1, 1, 1}.HFUNC(Resp);
  *registry << CI{"JSON.SET", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, 1}.HFUNC(Set);
}

}  // namespace dfly
