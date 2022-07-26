// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/json_family.h"

extern "C" {
#include "redis/object.h"
}

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/error.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <absl/strings/str_join.h>

namespace dfly {

using namespace std;
using namespace jsoncons;

using JsonExpression = jsonpath::jsonpath_expression<json>;
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

OpResult<string> OpGet(const OpArgs& op_args, string_view key, vector<pair<string_view, JsonExpression>> expressions) {
  OpResult<PrimeIterator> it_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_STRING);
  if (!it_res.ok())
    return it_res.status();

  const PrimeValue& pv = it_res.value()->second;
  string val = GetString(op_args.shard, pv);

  auto callback = [](json_errc ec, const ser_context&) {
    VLOG(1) << "Error while decode JSON: " << make_error_code(ec).message();
    return false;
  };

  error_code ec;
  json_decoder<json> decoder;
  basic_json_parser<char> parser(basic_json_decode_options<char>{}, callback);

  parser.update(val);
  parser.finish_parse(decoder, ec);

  if (!decoder.is_valid()) {
    return OpStatus::SYNTAX_ERR;
  }

  if (expressions.size() == 1) {
    json out = expressions[0].second.evaluate(decoder.get_result());
    return out.as<string>();
  }

  json out;
  json result = decoder.get_result();
  for (auto& expr: expressions) {
    json eval = expr.second.evaluate(result);
    out[expr.first] = eval;
  }

  return out.as<string>();
}

} // namespace

void JsonFamily::Get(CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 3U);
  string_view key = ArgS(args, 1);

  vector<pair<string_view, JsonExpression>> expressions;
  for (size_t i = 2; i < args.size(); ++i) {
    string_view path = ArgS(args, i);

    error_code ec;
    JsonExpression expr = jsonpath::make_expression<json>(path, ec);

    if (ec) {
      VLOG(1) << "Invalid JSONPath: " << ec.message();
      (*cntx)->SendError(kWrongTypeErr);
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
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        (*cntx)->SendError(kWrongTypeErr);
        break;
      case OpStatus::SYNTAX_ERR:
        (*cntx)->SendError(kSyntaxErr);
        break;
      default:
        DVLOG(1) << "JSON.GET " << key << " nil";
        (*cntx)->SendNull();
    }
  }
}

#define HFUNC(x) SetHandler(&JsonFamily::x)

void JsonFamily::Register(CommandRegistry* registry) {
  *registry << CI{"JSON.GET", CO::READONLY | CO::FAST, -3, 1, 1, 1}.HFUNC(Get);
}

}  // namespace dfly
