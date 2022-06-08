// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include <absl/strings/str_cat.h>

extern "C" {
#include "redis/object.h"
#include "redis/stream.h"
}

#include "base/logging.h"
#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

namespace {

struct Record {
  streamID id;
  vector<pair<string, string>> kv_arr;
};

using RecordVec = vector<Record>;

struct ParsedStreamId {
  streamID val;
  bool has_seq = false;   // Was an ID different than "ms-*" specified? for XADD only.
  bool id_given = false;  // Was an ID different than "*" specified? for XADD only.
};

struct RangeId {
  ParsedStreamId parsed_id;
  bool exclude = false;
};

bool ParseID(string_view strid, bool strict, uint64_t missing_seq, ParsedStreamId* dest) {
  if (strid.empty() || strid.size() > 127)
    return false;

  if (strid == "*")
    return true;

  dest->id_given = true;
  dest->has_seq = true;

  /* Handle the "-" and "+" special cases. */
  if (strid == "-" || strid == "+") {
    if (strict)
      return false;

    if (strid == "-") {
      dest->val.ms = 0;
      dest->val.seq = 0;
      return true;
    }

    dest->val.ms = UINT64_MAX;
    dest->val.seq = UINT64_MAX;
    return true;
  }

  /* Parse <ms>-<seq> form. */
  streamID result{.ms = 0, .seq = 0};

  size_t dash_pos = strid.find('-');
  if (!absl::SimpleAtoi(strid.substr(0, dash_pos), &result.ms))
    return false;

  if (dash_pos == string_view::npos) {
    result.seq = missing_seq;
  } else {
    if (dash_pos + 1 == strid.size())
      return false;

    if (dash_pos + 2 == strid.size() && strid[dash_pos + 1] == '*') {
      result.seq = 0;
      dest->has_seq = false;
    } else if (!absl::SimpleAtoi(strid.substr(dash_pos + 1), &result.seq)) {
      return false;
    }
  }

  dest->val = result;

  return true;
}

bool ParseRangeId(string_view id, RangeId* dest) {
  if (id.empty())
    return false;
  if (id[0] == '(') {
    dest->exclude = true;
    id.remove_prefix(1);
  }

  return ParseID(id, dest->exclude, 0, &dest->parsed_id);
}

OpResult<streamID> OpAdd(const OpArgs& op_args, string_view key, const ParsedStreamId& parsed_id,
                         CmdArgList args) {
  DCHECK(!args.empty() && args.size() % 2 == 0);
  auto& db_slice = op_args.shard->db_slice();
  pair<PrimeIterator, bool> add_res;

  try {
    add_res = db_slice.AddOrFind(op_args.db_ind, key);
  } catch (bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }

  robj* stream_obj = nullptr;
  PrimeIterator& it = add_res.first;

  if (add_res.second) {  // new key
    stream_obj = createStreamObject();

    it->second.ImportRObj(stream_obj);
  } else {
    if (it->second.ObjType() != OBJ_STREAM)
      return OpStatus::WRONG_TYPE;

    db_slice.PreUpdate(op_args.db_ind, it);
  }

  stream* str = (stream*)it->second.RObjPtr();

  // we should really get rid of this monstrousity and rewrite streamAppendItem ourselves here.
  unique_ptr<robj*[]> objs(new robj*[args.size()]);
  for (size_t i = 0; i < args.size(); ++i) {
    objs[i] = createStringObject(args[i].data(), args[i].size());
  }

  streamID result_id;
  streamID passed_id = parsed_id.val;
  int res = streamAppendItem(str, objs.get(), args.size() / 2, &result_id,
                             parsed_id.id_given ? &passed_id : nullptr, parsed_id.has_seq);
  if (res != C_OK) {
    if (errno == ERANGE)
      return OpStatus::OUT_OF_RANGE;

    return OpStatus::OUT_OF_MEMORY;
  }

  return result_id;
}

OpResult<RecordVec> OpRange(const OpArgs& op_args, string_view key, const ParsedStreamId& start,
                            const ParsedStreamId& end, uint32_t count) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_ind, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  RecordVec result;

  if (count == 0)
    return result;

  streamIterator si;
  int64_t numfields;
  streamID id;
  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  int rev = 0;
  streamID sstart = start.val, send = end.val;

  streamIteratorStart(&si, s, &sstart, &send, rev);
  while (streamIteratorGetID(&si, &id, &numfields)) {
    Record rec;
    rec.id = id;
    rec.kv_arr.reserve(numfields);

    /* Emit the field-value pairs. */
    while (numfields--) {
      unsigned char *key, *value;
      int64_t key_len, value_len;
      streamIteratorGetField(&si, &key, &value, &key_len, &value_len);
      string skey(reinterpret_cast<char*>(key), key_len);
      string sval(reinterpret_cast<char*>(value), value_len);

      rec.kv_arr.emplace_back(move(skey), move(sval));
    }

    result.push_back(move(rec));

    if (count == result.size())
      break;
  }

  streamIteratorStop(&si);

  return result;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_ind, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();
  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  return s->length;
}

inline string StreamIdRepr(const streamID& id) {
  return absl::StrCat(id.ms, "-", id.seq);
};

const char kInvalidStreamId[] = "Invalid stream ID specified as stream command argument";
}  // namespace

void StreamFamily::XAdd(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  // TODO: args parsing
  string_view id = ArgS(args, 2);
  ParsedStreamId parsed_id;

  args.remove_prefix(3);
  if (args.empty() || args.size() % 2 == 1) {
    return (*cntx)->SendError(WrongNumArgsError("XADD"), kSyntaxErrType);
  }

  if (!ParseID(id, true, 0, &parsed_id)) {
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpAdd(op_args, key, parsed_id, args);
  };

  OpResult<streamID> add_result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (add_result) {
    return (*cntx)->SendBulkString(StreamIdRepr(*add_result));
  }

  return (*cntx)->SendError(add_result.status());
}

void StreamFamily::XLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpLen(op_args, key);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendLong(*result);
  }

  return (*cntx)->SendError(result.status());
}

void StreamFamily::XRange(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view start = ArgS(args, 2);
  string_view end = ArgS(args, 3);
  uint32_t count = kuint32max;

  RangeId rs, re;
  if (!ParseRangeId(start, &rs) || !ParseRangeId(end, &re)) {
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  if (rs.exclude && streamIncrID(&rs.parsed_id.val) != C_OK) {
    return (*cntx)->SendError("invalid start ID for the interval", kSyntaxErrType);
  }

  if (re.exclude && streamDecrID(&re.parsed_id.val) != C_OK) {
    return (*cntx)->SendError("invalid end ID for the interval", kSyntaxErrType);
  }

  if (args.size() > 4) {
    if (args.size() != 6) {
      return (*cntx)->SendError(WrongNumArgsError("XRANGE"), kSyntaxErrType);
    }
    ToUpper(&args[4]);
    string_view opt = ArgS(args, 4);
    string_view val = ArgS(args, 5);

    if (opt != "COUNT" || !absl::SimpleAtoi(val, &count)) {
      return (*cntx)->SendError(kSyntaxErr);
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpRange(op_args, key, rs.parsed_id, re.parsed_id, count);
  };

  OpResult<RecordVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    (*cntx)->StartArray(result->size());
    for (const auto& item : *result) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(StreamIdRepr(item.id));
      (*cntx)->StartArray(item.kv_arr.size() * 2);
      for (const auto& k_v : item.kv_arr) {
        (*cntx)->SendBulkString(k_v.first);
        (*cntx)->SendBulkString(k_v.second);
      }
    }
  }

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->StartArray(0);
  }
  return (*cntx)->SendError(result.status());
}

#define HFUNC(x) SetHandler(&StreamFamily::x)

void StreamFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"XADD", CO::WRITE | CO::FAST, -5, 1, 1, 1}.HFUNC(XAdd)
            << CI{"XLEN", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(XLen)
            << CI{"XRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(XRange);
}

}  // namespace dfly