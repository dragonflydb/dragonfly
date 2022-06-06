// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include <absl/strings/str_cat.h>

#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

namespace {

using StreamId = pair<int64_t, int64_t>;

struct Record {
  StreamId id;
  string field;
  string value;
};

using RecordVec = vector<Record>;

OpResult<StreamId> OpAdd(const OpArgs& op_args, string_view key, string_view id, CmdArgList args) {
  return OpStatus::WRONG_TYPE;  // TBD
}

OpResult<RecordVec> OpRange(const OpArgs& op_args, string_view key, string_view start,
                            string_view end) {
  return OpStatus::WRONG_TYPE;  // TBD
}

inline string StreamIdRepr(const StreamId& id) {
  return absl::StrCat(id.first, "-", id.second);
};

}  // namespace

void StreamFamily::XAdd(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  // TODO: args parsing
  string_view id = ArgS(args, 2);
  args.remove_prefix(2);
  if (args.empty() || args.size() % 2 == 1) {
    return (*cntx)->SendError(WrongNumArgsError("XADD"), kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpAdd(op_args, key, id, args);
  };

  OpResult<StreamId> add_result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (add_result) {
    return (*cntx)->SendBulkString(StreamIdRepr(*add_result));
  }

  return (*cntx)->SendError(add_result.status());
}

void StreamFamily::XRange(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view start = ArgS(args, 2);
  string_view end = ArgS(args, 1);

  // TODO: parse options

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args{shard, t->db_index()};
    return OpRange(op_args, key, start, end);
  };

  OpResult<RecordVec> add_result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (add_result) {
    (*cntx)->StartArray(add_result->size());
    for (const auto& item : *add_result) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(StreamIdRepr(item.id));
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(item.field);
      (*cntx)->SendBulkString(item.value);
    }
  }

  return (*cntx)->SendError(add_result.status());
}

#define HFUNC(x) SetHandler(&StreamFamily::x)

void StreamFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"XADD", CO::WRITE | CO::FAST, -5, 1, 1, 1}.HFUNC(XAdd)
            << CI{"XRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(XRange);
}

}  // namespace dfly