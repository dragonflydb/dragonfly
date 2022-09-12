// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

extern "C" {
#include "redis/object.h"
}

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
using OptSizeT = optional<size_t>;
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

inline void RecordJournal(const OpArgs& op_args, const PrimeKey& pkey, const PrimeKey& pvalue) {
  if (op_args.shard->journal()) {
    op_args.shard->journal()->RecordEntry(op_args.txid, pkey, pvalue);
  }
}

void SetString(const OpArgs& op_args, string_view key, const string& value) {
  auto& db_slice = op_args.shard->db_slice();
  auto [it_output, added] = db_slice.AddOrFind(op_args.db_ind, key);
  it_output->second.SetString(value);
  db_slice.PostUpdate(op_args.db_ind, it_output);
  RecordJournal(op_args, it_output->first, it_output->second);
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

OpResult<json> GetJson(const OpArgs& op_args, string_view key) {
  OpResult<PrimeIterator> it_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_STRING);
  if (!it_res.ok())
    return it_res.status();

  error_code ec;
  json_decoder<json> decoder;
  const PrimeValue& pv = it_res.value()->second;

  string val = GetString(op_args.shard, pv);
  basic_json_parser<char> parser(basic_json_decode_options<char>{}, &JsonErrorHandler);

  parser.update(val);
  parser.finish_parse(decoder, ec);

  if (!decoder.is_valid()) {
    return OpStatus::SYNTAX_ERR;
  }

  return decoder.get_result();
}

string ConvertToJsonPointer(string_view json_path) {
  if (json_path.empty() || json_path[0] != '$') {
    return "";
  }

  // except the supplied string is compatible with JSONPath syntax.
  // Each item in the string is a left bracket followed by
  // numeric or '<key>' and then a right bracket.
  vector<string> parts;
  bool invalid_syntax = false;
  std::size_t i = 1;
  while (i < json_path.size()) {
    bool is_array = false;
    bool is_object = false;

    // check string size is sufficient enough for at least one item.
    if (i + 2 >= json_path.size()) {
      invalid_syntax = true;
      break;
    }

    if (json_path[i] == '[') {
      if (json_path[i + 1] == '\'') {
        is_object = true;
        i += 2;
      } else if (isdigit(json_path[i + 1])) {
        is_array = true;
        i += 1;
      } else {
        invalid_syntax = true;
        break;
      }
    } else {
      invalid_syntax = true;
      break;
    }

    if (is_array) {
      auto end_pos = json_path.find(']', i);
      if (end_pos == string::npos) {
        invalid_syntax = true;
        break;
      }

      parts.emplace_back(json_path.substr(i, end_pos - i));
      i = end_pos + 1;

    } else if (is_object) {
      // find the index of the right bracket.
      auto end_val_idx = string::npos;
      auto current_idx = i;
      while (end_val_idx == string::npos) {
        if (current_idx + 1 >= json_path.size()) {
          break;
        }

        // ignore escaped character (e.g. \']).
        if (json_path[current_idx] == '\\') {
          current_idx += 2;
        }

        else if (json_path[current_idx] == '\'' && json_path[current_idx + 1] == ']') {
          end_val_idx = current_idx++;
        }

        else {
          current_idx++;
        }
      }

      if (end_val_idx == string::npos) {
        invalid_syntax = true;
        break;
      }

      parts.emplace_back(json_path.substr(i, end_val_idx - i));
      i = end_val_idx + 2;

    } else {
      invalid_syntax = true;
      break;
    }
  }

  if (invalid_syntax) {
    LOG(FATAL) << "Unexpected JSONPath syntax: " << json_path;
  }

  string result = absl::StrJoin(parts, "/");
  return '/' + result;  // add leading slash
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

  json j = result.value();
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaulate expression on json with error: " << ec.message();
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

  json j = result.value();
  error_code ec = JsonReplace(j, path, cb);
  if (ec) {
    VLOG(1) << "Failed to evaulate expression on json with error: " << ec.message();
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
    auto fres = db_slice.FindExt(op_args.db_ind, key);
    if (IsValid(fres.first)) {
      total_deletions += long(db_slice.Del(op_args.db_ind, fres.first));
    }

    return total_deletions;
  }

  OpResult<json> result = GetJson(op_args, key);
  if (!result) {
    return total_deletions;
  }

  vector<string> deletion_items;
  auto cb = [&](const string& path, json& val) { deletion_items.emplace_back(path); };

  json j = result.value();
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
  for (const auto& it : deletion_items) {
    string pointer = ConvertToJsonPointer(it);
    if (pointer.empty()) {
      VLOG(1) << "Failed to convert the following JSONPath to pointer: " << it;
      continue;
    }
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

}  // namespace

void JsonFamily::Del(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view path;
  if (args.size() > 2) {
    path = ArgS(args, 2);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, path);
  };

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<long> result = trans->ScheduleSingleHopT(move(cb));
  DVLOG(1) << "JSON.DEL " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.NUMINCRBY " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.NUMMULTBY " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<vector<OptBool>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.TOGGLE " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<vector<string>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.TYPE " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.ARRLEN " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.OBJLEN " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<vector<OptSizeT>> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.STRLEN " << trans->DebugId() << ": " << key;
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

  DVLOG(1) << "Before Get::ScheduleSingleHopT " << key;
  Transaction* trans = cntx->transaction;
  OpResult<string> result = trans->ScheduleSingleHopT(move(cb));

  if (result) {
    DVLOG(1) << "JSON.GET " << trans->DebugId() << ": " << key << " " << result.value();
    (*cntx)->SendBulkString(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

#define HFUNC(x) SetHandler(&JsonFamily::x)

void JsonFamily::Register(CommandRegistry* registry) {
  *registry << CI{"JSON.GET", CO::READONLY | CO::FAST, -3, 1, 1, 1}.HFUNC(Get);
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
}

}  // namespace dfly
