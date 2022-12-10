// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

extern "C" {
#include "redis/object.h"
}

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace jsoncons;

using JsonExpression = jsonpath::jsonpath_expression<json>;
using OptBool = optional<bool>;
using OptLong = optional<long>;
using OptSizeT = optional<size_t>;
using OptString = optional<string>;
using JsonReplaceCb = function<void(const string&, json&)>;
using CI = CommandId;

namespace {

string GetString(EngineShard* shard, const PrimeValue& pv) {
  string res;
  if (pv.IsExternal()) {
    auto* tiered = shard->tiered_storage();
    auto [offset, size] = pv.GetExternalPtr();
    res.resize(size);

    error_code ec = tiered->Read(offset, size, res.data());
    CHECK(!ec) << "TBD: " << ec;
  } else {
    pv.GetString(&res);
  }

  return res;
}

void SetString(const OpArgs& op_args, string_view key, const string& value) {
  auto& db_slice = op_args.shard->db_slice();
  DbIndex db_index = op_args.db_cntx.db_index;
  auto [it_output, added] = db_slice.AddOrFind(op_args.db_cntx, key);
  db_slice.PreUpdate(db_index, it_output);
  it_output->second.SetString(value);
  db_slice.PostUpdate(db_index, it_output, key);
}

string JsonType(const json& val) {
  if (val.is_null()) {
    return "null";
  } else if (val.is_bool()) {
    return "boolean";
  } else if (val.is_string()) {
    return "string";
  } else if (val.is_int64() || val.is_uint64()) {
    return "integer";
  } else if (val.is_number()) {
    return "number";
  } else if (val.is_object()) {
    return "object";
  } else if (val.is_array()) {
    return "array";
  }

  return "";
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

error_code JsonReplace(json& instance, string_view& path, JsonReplaceCb callback) {
  using evaluator_t = jsoncons::jsonpath::detail::jsonpath_evaluator<json, json&>;
  using value_type = evaluator_t::value_type;
  using reference = evaluator_t::reference;
  using json_selector_t = evaluator_t::path_expression_type;
  using json_location_type = evaluator_t::json_location_type;
  jsonpath::custom_functions<json> funcs = jsonpath::custom_functions<json>();

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

bool JsonErrorHandler(json_errc ec, const ser_context&) {
  VLOG(1) << "Error while decode JSON: " << make_error_code(ec).message();
  return false;
}

optional<json> ConstructJsonFromString(string_view val) {
  error_code ec;
  json_decoder<json> decoder;
  basic_json_parser<char> parser(basic_json_decode_options<char>{}, &JsonErrorHandler);

  parser.update(val);
  parser.finish_parse(decoder, ec);

  if (!decoder.is_valid()) {
    return nullopt;
  }

  return decoder.get_result();
}

OpResult<json> GetJson(const OpArgs& op_args, string_view key) {
  OpResult<PrimeIterator> it_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_STRING);
  if (!it_res.ok())
    return it_res.status();

  const PrimeValue& pv = it_res.value()->second;

  string val = GetString(op_args.shard, pv);
  optional<json> j = ConstructJsonFromString(val);
  if (!j) {
    return OpStatus::SYNTAX_ERR;
  }

  return *j;
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

size_t CountJsonFields(const json& j) {
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

void SendJsonValue(ConnectionContext* cntx, const json& j) {
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
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  if (expressions.size() == 1) {
    json out = expressions[0].second.evaluate(*result);
    return out.as<string>();
  }

  json out;
  for (auto& expr : expressions) {
    json eval = expr.second.evaluate(*result);
    out[expr.first] = eval;
  }

  return out.as<string>();
}

OpResult<vector<string>> OpType(const OpArgs& op_args, string_view key, JsonExpression expression) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<string> vec;
  auto cb = [&vec](const string_view& path, const json& val) { vec.emplace_back(JsonType(val)); };

  expression.evaluate(*result, cb);
  return vec;
}

OpResult<vector<OptSizeT>> OpStrLen(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const json& val) {
    if (val.is_string()) {
      vec.emplace_back(val.as_string_view().size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  expression.evaluate(*result, cb);
  return vec;
}

OpResult<vector<OptSizeT>> OpObjLen(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const json& val) {
    if (val.is_object()) {
      vec.emplace_back(val.object_value().size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  expression.evaluate(*result, cb);
  return vec;
}

OpResult<vector<OptSizeT>> OpArrLen(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const json& val) {
    if (val.is_array()) {
      vec.emplace_back(val.array_value().size());
    } else {
      vec.emplace_back(nullopt);
    }
  };

  expression.evaluate(*result, cb);
  return vec;
}

OpResult<vector<OptBool>> OpToggle(const OpArgs& op_args, string_view key, string_view path) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptBool> vec;
  auto cb = [&vec](const string& path, json& val) {
    if (val.is_bool()) {
      bool current_val = val.as_bool() ^ true;
      val = current_val;
      vec.emplace_back(current_val);
    } else {
      vec.emplace_back(nullopt);
    }
  };

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  SetString(op_args, key, j.as_string());
  return vec;
}

template <typename Op>
OpResult<string> OpDoubleArithmetic(const OpArgs& op_args, string_view key, string_view path,
                                    double num, Op arithmetic_op) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  bool is_result_overflow = false;
  double int_part;
  bool has_fractional_part = (modf(num, &int_part) != 0);
  json output(json_array_arg);

  auto cb = [&](const string& path, json& val) {
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

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  if (is_result_overflow) {
    return OpStatus::INVALID_NUMERIC_RESULT;
  }

  SetString(op_args, key, j.as_string());
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

  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return total_deletions;
  }

  vector<string> deletion_items;
  auto cb = [&](const string& path, json& val) { deletion_items.emplace_back(path); };

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaulate expression on json with error: " << ec.message();
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

  jsonpatch::apply_patch(j, patch, ec);
  if (ec) {
    VLOG(1) << "Failed to apply patch on json with error: " << ec.message();
    return 0;
  }

  SetString(op_args, key, j.as_string());
  return total_deletions;
}

// Returns a vector of string vectors,
// keys within the same object are stored in the same string vector.
OpResult<vector<StringVec>> OpObjKeys(const OpArgs& op_args, string_view key,
                                      JsonExpression expression) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<StringVec> vec;
  auto cb = [&vec](const string_view& path, const json& val) {
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

  expression.evaluate(*result, cb);
  return vec;
}

// Retruns array of string lengths after a successful operation.
OpResult<vector<OptSizeT>> OpStrAppend(const OpArgs& op_args, string_view key, string_view path,
                                       const vector<string_view>& strs) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptSizeT> vec;
  auto cb = [&](const string& path, json& val) {
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

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  SetString(op_args, key, j.as_string());
  return vec;
}

// Returns the numbers of values cleared.
// Clears containers(arrays or objects) and zeroing numbers.
OpResult<long> OpClear(const OpArgs& op_args, string_view key, string_view path) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  long clear_items = 0;
  auto cb = [&clear_items](const string& path, json& val) {
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

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  SetString(op_args, key, j.as_string());
  return clear_items;
}

// Returns string vector that represents the pop out values.
OpResult<vector<OptString>> OpArrPop(const OpArgs& op_args, string_view key, string_view path,
                                     int index) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptString> vec;
  auto cb = [&](const string& path, json& val) {
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

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  SetString(op_args, key, j.as_string());
  return vec;
}

// Returns numeric vector that represents the new length of the array at each path.
OpResult<vector<OptSizeT>> OpArrTrim(const OpArgs& op_args, string_view key, string_view path,
                                     int start_index, int stop_index) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptSizeT> vec;
  auto cb = [&](const string& path, json& val) {
    if (!val.is_array() || val.empty()) {
      vec.emplace_back(nullopt);
      return;
    }

    size_t trim_start_index;
    if (start_index < 0) {
      trim_start_index = 0;
    } else {
      trim_start_index = start_index;
    }

    size_t trim_stop_index;
    if ((size_t)stop_index >= val.size()) {
      trim_stop_index = val.size();
    } else {
      trim_stop_index = stop_index;
    }

    if (trim_start_index >= val.size() || trim_start_index > trim_stop_index) {
      val.erase(val.array_range().begin(), val.array_range().end());
      vec.emplace_back(val.size());
      return;
    }

    auto it = std::next(val.array_range().begin(), trim_start_index);
    while (it != val.array_range().end()) {
      if (trim_start_index++ == trim_stop_index) {
        break;
      }

      it = val.erase(it);
    }

    vec.emplace_back(val.size());
  };

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  SetString(op_args, key, j.as_string());
  return vec;
}

// Returns numeric vector that represents the new length of the array at each path.
OpResult<vector<OptSizeT>> OpArrInsert(const OpArgs& op_args, string_view key, string_view path,
                                       int index, const vector<json>& new_values) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  bool out_of_boundaries_encountered = false;
  vector<OptSizeT> vec;
  // Insert user-supplied value into the supplied index that should be valid.
  // If at least one index isn't valid within an array in the json doc, the operation is discarded.
  // Negative indexes start from the end of the array.
  auto cb = [&](const string& path, json& val) {
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

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  if (out_of_boundaries_encountered) {
    return OpStatus::OUT_OF_RANGE;
  }

  SetString(op_args, key, j.as_string());
  return vec;
}

// Returns numeric vector that represents the new length of the array at each path, or Null reply
// if the matching JSON value is not an array.
OpResult<vector<OptSizeT>> OpArrAppend(const OpArgs& op_args, string_view key, string_view path,
                                       const vector<json>& append_values) {
  vector<OptSizeT> vec;

  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  auto cb = [&](const string& path, json& val) {
    if (!val.is_array()) {
      vec.emplace_back(nullopt);
      return;
    }
    for (auto& new_val : append_values) {
      val.emplace_back(new_val);
    }
    vec.emplace_back(val.size());
  };

  json j = move(result.value());
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  SetString(op_args, key, j.as_string());
  return vec;
}

// Returns a numeric vector representing each JSON value first index of the JSON scalar.
// An index value of -1 represents unfound in the array.
// JSON scalar has types of string, boolean, null, and number.
OpResult<vector<OptLong>> OpArrIndex(const OpArgs& op_args, string_view key,
                                     JsonExpression expression, const json& search_val,
                                     int start_index, int end_index) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptLong> vec;
  auto cb = [&](const string_view& path, const json& val) {
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

  expression.evaluate(*result, cb);
  return vec;
}

// Returns string vector that represents the query result of each supplied key.
OpResult<vector<OptString>> OpMGet(const OpArgs& op_args, const vector<string_view>& keys,
                                   JsonExpression expression) {
  vector<OptString> vec;
  for (auto& it : keys) {
    OpResult<json> result = GetJson(op_args, it);
    if (!result) {
      vec.emplace_back();
      continue;
    }

    auto cb = [&vec](const string_view& path, const json& val) {
      string str;
      error_code ec;
      val.dump(str, {}, ec);
      if (ec) {
        VLOG(1) << "Failed to dump JSON to string with the error: " << ec.message();
        return;
      }

      vec.push_back(move(str));
    };

    expression.evaluate(*result, cb);
  }

  return vec;
}

// Returns numeric vector that represents the number of fields of JSON value at each path.
OpResult<vector<OptSizeT>> OpFields(const OpArgs& op_args, string_view key,
                                    JsonExpression expression) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<OptSizeT> vec;
  auto cb = [&vec](const string_view& path, const json& val) {
    vec.emplace_back(CountJsonFields(val));
  };

  expression.evaluate(*result, cb);
  return vec;
}

// Returns json vector that represents the result of the json query.
OpResult<vector<json>> OpResp(const OpArgs& op_args, string_view key, JsonExpression expression) {
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  vector<json> vec;
  auto cb = [&vec](const string_view& path, const json& val) { vec.emplace_back(val); };

  expression.evaluate(*result, cb);
  return vec;
}

// Returns boolean that represents the result of the operation.
OpResult<bool> OpSet(const OpArgs& op_args, string_view key, string_view path,
                     string_view json_str) {
  // Check if the supplied JSON is valid.
  optional<json> j = ConstructJsonFromString(json_str);
  if (!j) {
    return OpStatus::SYNTAX_ERR;
  }

  // The whole key should be replaced.
  if (path == "." || path == "$") {
    SetString(op_args, key, j->as_string());
    return true;
  }

  // Update the existing JSON.
  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return result.status();
  }

  json existing_json = move(result.value());
  bool path_exists = false;
  auto cb = [&](const string& path, json& val) {
    path_exists = true;
    val = *j;
  };

  error_code ec = JsonReplace(existing_json, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaluate expression on json with error: " << ec.message();
    return OpStatus::SYNTAX_ERR;
  }

  if (!path_exists) {
    return false;
  }

  SetString(op_args, key, existing_json.as_string());
  return true;
}

}  // namespace

void JsonFamily::Set(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  string_view json_str = ArgS(args, 3);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, path, json_str);
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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpResp(t->GetOpArgs(shard), key, move(expression));
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<json>> result = trans->ScheduleSingleHopT(move(cb));

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
  string_view command = ArgS(args, 1);
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

  if (args.size() < 4) {
    (*cntx)->SendError(facade::WrongNumArgsError(command), facade::kSyntaxErrType);
    return;
  }

  error_code ec;
  string_view key = ArgS(args, 2);
  string_view path = ArgS(args, 3);
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

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
  error_code ec;
  string_view path = ArgS(args, args.size() - 1);
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  vector<string_view> vec;
  for (auto i = 1U; i < args.size() - 1; i++) {
    vec.emplace_back(ArgS(args, i));
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpMGet(t->GetOpArgs(shard), vec, move(expression));
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

void JsonFamily::ArrIndex(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

  if (ec) {
    VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  optional<json> search_value = ConstructJsonFromString(ArgS(args, 3));
  if (!search_value) {
    (*cntx)->SendError(kSyntaxErr);
    return;
  }

  if (search_value->is_object() || search_value->is_array()) {
    (*cntx)->SendError(kWrongTypeErr);
    return;
  }

  int start_index = 0;
  if (args.size() >= 5) {
    if (!absl::SimpleAtoi(ArgS(args, 4), &start_index)) {
      VLOG(1) << "Failed to convert the start index to numeric" << ArgS(args, 4);
      (*cntx)->SendError(kInvalidIntErr);
      return;
    }
  }

  int end_index = 0;
  if (args.size() >= 6) {
    if (!absl::SimpleAtoi(ArgS(args, 5), &end_index)) {
      VLOG(1) << "Failed to convert the stop index to numeric" << ArgS(args, 5);
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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  int index = -1;

  if (!absl::SimpleAtoi(ArgS(args, 3), &index)) {
    VLOG(1) << "Failed to convert the following value to numeric: " << ArgS(args, 3);
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  vector<json> new_values;
  for (size_t i = 4; i < args.size(); i++) {
    optional<json> val = ConstructJsonFromString(ArgS(args, i));
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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  vector<json> append_values;
  for (size_t i = 3; i < args.size(); ++i) {
    optional<json> converted_val = ConstructJsonFromString(ArgS(args, i));
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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  int start_index;
  int stop_index;

  if (!absl::SimpleAtoi(ArgS(args, 3), &start_index)) {
    VLOG(1) << "Failed to parse array start index";
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  if (!absl::SimpleAtoi(ArgS(args, 4), &stop_index)) {
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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  int index = -1;

  if (args.size() >= 4) {
    if (!absl::SimpleAtoi(ArgS(args, 3), &index)) {
      VLOG(1) << "Failed to convert the following value to numeric, pop out the last item"
              << ArgS(args, 3);
    }
  }

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  vector<string_view> strs;
  for (size_t i = 3; i < args.size(); ++i) {
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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

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
  string_view key = ArgS(args, 1);
  string_view path;
  if (args.size() > 2) {
    path = ArgS(args, 2);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, path);
  };

  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(move(cb));
  (*cntx)->SendLong(*result);
}

void JsonFamily::NumIncrBy(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  string_view num = ArgS(args, 3);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);
  string_view num = ArgS(args, 3);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

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
  string_view key = ArgS(args, 1);
  string_view path = ArgS(args, 2);

  error_code ec;
  JsonExpression expression = jsonpath::make_expression<json>(path, ec);

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
  DCHECK_GE(args.size(), 3U);
  string_view key = ArgS(args, 1);

  vector<pair<string_view, JsonExpression>> expressions;
  for (size_t i = 2; i < args.size(); ++i) {
    string_view path = ArgS(args, i);

    error_code ec;
    JsonExpression expr = jsonpath::make_expression<json>(path, ec);

    if (ec) {
      VLOG(1) << "Invalid JSONPath syntax: " << ec.message();
      (*cntx)->SendError(kSyntaxErr);
      return;
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
    (*cntx)->SendError(result.status());
  }
}

#define HFUNC(x) SetHandler(&JsonFamily::x)

void JsonFamily::Register(CommandRegistry* registry) {
  *registry << CI{"JSON.GET", CO::READONLY | CO::FAST, -3, 1, 1, 1}.HFUNC(Get);
  *registry << CI{"JSON.MGET", CO::READONLY | CO::FAST, -3, 1, 1, 1}.HFUNC(MGet);
  *registry << CI{"JSON.TYPE", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(Type);
  *registry << CI{"JSON.STRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(StrLen);
  *registry << CI{"JSON.OBJLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ObjLen);
  *registry << CI{"JSON.ARRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ArrLen);
  *registry << CI{"JSON.TOGGLE", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(Toggle);
  *registry << CI{"JSON.NUMINCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(
      NumIncrBy);
  *registry << CI{"JSON.NUMMULTBY", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(
      NumMultBy);
  *registry << CI{"JSON.DEL", CO::WRITE, -2, 1, 1, 1}.HFUNC(Del);
  *registry << CI{"JSON.FORGET", CO::WRITE, -2, 1, 1, 1}.HFUNC(Del);  // An alias of JSON.DEL.
  *registry << CI{"JSON.OBJKEYS", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(ObjKeys);
  *registry << CI{"JSON.STRAPPEND", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, 1}.HFUNC(
      StrAppend);
  *registry << CI{"JSON.CLEAR", CO::WRITE | CO::DENYOOM | CO::FAST, 3, 1, 1, 1}.HFUNC(Clear);
  *registry << CI{"JSON.ARRPOP", CO::WRITE | CO::DENYOOM | CO::FAST, -3, 1, 1, 1}.HFUNC(ArrPop);
  *registry << CI{"JSON.ARRTRIM", CO::WRITE | CO::DENYOOM | CO::FAST, 5, 1, 1, 1}.HFUNC(ArrTrim);
  *registry << CI{"JSON.ARRINSERT", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, 1}.HFUNC(
      ArrInsert);
  *registry << CI{"JSON.ARRAPPEND", CO::WRITE | CO::DENYOOM | CO::FAST, -4, 1, 1, 1}.HFUNC(
      ArrAppend);
  *registry << CI{"JSON.ARRINDEX", CO::READONLY | CO::FAST, -4, 1, 1, 1}.HFUNC(ArrIndex);
  *registry << CI{"JSON.DEBUG", CO::READONLY | CO::FAST, -2, 1, 1, 1}.HFUNC(Debug);
  *registry << CI{"JSON.RESP", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(Resp);
  *registry << CI{"JSON.SET", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(Set);
}

}  // namespace dfly
