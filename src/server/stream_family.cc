// Copyright 2022, DragonflyDB authors.  All rights reserved.
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

struct AddOpts {
  ParsedStreamId parsed_id;
  uint32_t max_limit = kuint32max;
  bool max_limit_approx = false;
};

struct GroupInfo {
  string name;
  size_t consumer_size;
  size_t pending_size;
  streamID last_id;
};

struct RangeOpts {
  ParsedStreamId start;
  ParsedStreamId end;
  bool is_rev = false;
  uint32_t count = kuint32max;
};

const char kInvalidStreamId[] = "Invalid stream ID specified as stream command argument";
const char kXGroupKeyNotFound[] =
    "The XGROUP subcommand requires the key to exist. "
    "Note that for CREATE you may want to use the MKSTREAM option to create "
    "an empty stream automatically.";

inline string StreamIdRepr(const streamID& id) {
  return absl::StrCat(id.ms, "-", id.seq);
};

inline string NoGroupError(string_view key, string_view cgroup) {
  return absl::StrCat("-NOGROUP No such consumer group '", cgroup, "' for key name '", key, "'");
}

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
  streamID result{.ms = 0, .seq = missing_seq};

  size_t dash_pos = strid.find('-');
  if (!absl::SimpleAtoi(strid.substr(0, dash_pos), &result.ms))
    return false;

  if (dash_pos != string_view::npos) {
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

OpResult<streamID> OpAdd(const OpArgs& op_args, string_view key, const AddOpts& opts,
                         CmdArgList args) {
  DCHECK(!args.empty() && args.size() % 2 == 0);
  auto& db_slice = op_args.shard->db_slice();
  pair<PrimeIterator, bool> add_res;

  try {
    add_res = db_slice.AddOrFind(op_args.db_cntx, key);
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

    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  }

  stream* stream_inst = (stream*)it->second.RObjPtr();

  // we should really get rid of this monstrousity and rewrite streamAppendItem ourselves here.
  unique_ptr<robj*[]> objs(new robj*[args.size()]);
  for (size_t i = 0; i < args.size(); ++i) {
    objs[i] = createStringObject(args[i].data(), args[i].size());
  }

  streamID result_id;
  const auto& parsed_id = opts.parsed_id;
  streamID passed_id = parsed_id.val;
  int res = streamAppendItem(stream_inst, objs.get(), args.size() / 2, &result_id,
                             parsed_id.id_given ? &passed_id : nullptr, parsed_id.has_seq);

  for (size_t i = 0; i < args.size(); ++i) {
    decrRefCount(objs[i]);
  }

  if (res != C_OK) {
    if (errno == ERANGE)
      return OpStatus::OUT_OF_RANGE;
    if (errno == EDOM)
      return OpStatus::STREAM_ID_SMALL;

    return OpStatus::OUT_OF_MEMORY;
  }

  if (opts.max_limit < kuint32max) {
    /* Notify xtrim event if needed. */
    streamTrimByLength(stream_inst, opts.max_limit, opts.max_limit_approx);
    // TODO: when replicating, we should propagate it as exact limit in case of trimming.
  }
  return result_id;
}

OpResult<RecordVec> OpRange(const OpArgs& op_args, string_view key, const RangeOpts& opts) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  RecordVec result;

  if (opts.count == 0)
    return result;

  streamIterator si;
  int64_t numfields;
  streamID id;
  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  streamID sstart = opts.start.val, send = opts.end.val;

  streamIteratorStart(&si, s, &sstart, &send, opts.is_rev);
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

    if (opts.count == result.size())
      break;
  }

  streamIteratorStop(&si);

  return result;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();
  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  return s->length;
}

OpResult<vector<GroupInfo>> OpListGroups(const DbContext& db_cntx, string_view key,
                                         EngineShard* shard) {
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  vector<GroupInfo> result;
  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();

  if (s->cgroups) {
    result.reserve(raxSize(s->cgroups));

    raxIterator ri;
    raxStart(&ri, s->cgroups);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri)) {
      streamCG* cg = (streamCG*)ri.data;
      GroupInfo ginfo;
      ginfo.name.assign(reinterpret_cast<char*>(ri.key), ri.key_len);
      ginfo.consumer_size = raxSize(cg->consumers);
      ginfo.pending_size = raxSize(cg->pel);
      ginfo.last_id = cg->last_id;
      result.push_back(std::move(ginfo));
    }
    raxStop(&ri);
  }

  return result;
}

constexpr uint8_t kCreateOptMkstream = 1 << 0;

struct CreateOpts {
  string_view gname;
  string_view id;
  uint8_t flags = 0;
};

OpStatus OpCreate(const OpArgs& op_args, string_view key, const CreateOpts& opts) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it) {
    if (opts.flags & kCreateOptMkstream) {
      // MKSTREAM is enabled, so create the stream
      res_it = db_slice.AddNew(op_args.db_cntx, key, PrimeValue{}, 0);
      if (!res_it)
        return res_it.status();

      robj* stream_obj = createStreamObject();
      (*res_it)->second.ImportRObj(stream_obj);
    } else {
      return res_it.status();
    }
  }

  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  streamID id;
  ParsedStreamId parsed_id;
  if (opts.id == "$") {
    id = s->last_id;
  } else {
    if (ParseID(opts.id, true, 0, &parsed_id)) {
      id = parsed_id.val;
    } else {
      return OpStatus::SYNTAX_ERR;
    }
  }

  streamCG* cg = streamCreateCG(s, opts.gname.data(), opts.gname.size(), &id, 0);
  if (cg) {
    return OpStatus::OK;
  }
  return OpStatus::BUSY_GROUP;
}

OpResult<pair<stream*, streamCG*>> FindGroup(const OpArgs& op_args, string_view key,
                                             string_view gname) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  CompactObj& cobj = (*res_it)->second;
  pair<stream*, streamCG*> res;
  res.first = (stream*)cobj.RObjPtr();
  shard->tmp_str1 = sdscpylen(shard->tmp_str1, gname.data(), gname.size());
  res.second = streamLookupCG(res.first, shard->tmp_str1);

  return res;
}

// XGROUP DESTROY key groupname
OpStatus OpDestroyGroup(const OpArgs& op_args, string_view key, string_view gname) {
  OpResult<pair<stream*, streamCG*>> cgr_res = FindGroup(op_args, key, gname);
  if (!cgr_res)
    return cgr_res.status();

  stream* s = cgr_res->first;
  streamCG* scg = cgr_res->second;

  if (scg) {
    raxRemove(s->cgroups, (uint8_t*)(gname.data()), gname.size(), NULL);
    streamFreeCG(scg);
    return OpStatus::OK;
  }

  return OpStatus::SKIPPED;
}

// XGROUP DELCONSUMER key groupname consumername
OpResult<uint32_t> OpDelConsumer(const OpArgs& op_args, string_view key, string_view gname,
                                 string_view consumer_name) {
  OpResult<pair<stream*, streamCG*>> cgroup_res = FindGroup(op_args, key, gname);
  if (!cgroup_res)
    return cgroup_res.status();

  streamCG* cg = cgroup_res->second;
  if (cg == nullptr)
    return OpStatus::SKIPPED;

  long long pending = 0;
  auto* shard = op_args.shard;

  shard->tmp_str1 = sdscpylen(shard->tmp_str1, consumer_name.data(), consumer_name.size());
  streamConsumer* consumer = streamLookupConsumer(cg, shard->tmp_str1, SLC_NO_REFRESH);
  if (consumer) {
    pending = raxSize(consumer->pel);
    streamDelConsumer(cg, consumer);
  }

  return pending;
}

OpStatus OpSetId(const OpArgs& op_args, string_view key, string_view gname, string_view id) {
  OpResult<pair<stream*, streamCG*>> cgr_res = FindGroup(op_args, key, gname);
  if (!cgr_res)
    return cgr_res.status();

  streamCG* cg = cgr_res->second;
  if (cg == nullptr)
    return OpStatus::SKIPPED;

  streamID sid;
  ParsedStreamId parsed_id;
  if (id == "$") {
    sid = cgr_res->first->last_id;
  } else {
    if (ParseID(id, true, 0, &parsed_id)) {
      sid = parsed_id.val;
    } else {
      return OpStatus::SYNTAX_ERR;
    }
  }
  cg->last_id = sid;

  return OpStatus::OK;
}

OpStatus OpSetId2(const OpArgs& op_args, string_view key, const streamID& sid) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  CompactObj& cobj = (*res_it)->second;
  stream* stream_inst = (stream*)cobj.RObjPtr();
  long long entries_added = -1;
  streamID max_xdel_id{0, 0};

  /* If the stream has at least one item, we want to check that the user
   * is setting a last ID that is equal or greater than the current top
   * item, otherwise the fundamental ID monotonicity assumption is violated. */
  if (stream_inst->length > 0) {
    streamID maxid;
    streamID id = sid;
    streamLastValidID(stream_inst, &maxid);

    if (streamCompareID(&id, &maxid) < 0) {
      return OpStatus::STREAM_ID_SMALL;
    }

    /* If an entries_added was provided, it can't be lower than the length. */
    if (entries_added != -1 && stream_inst->length > uint64_t(entries_added)) {
      return OpStatus::ENTRIES_ADDED_SMALL;
    }
  }

  stream_inst->last_id = sid;
  if (entries_added != -1)
    stream_inst->entries_added = entries_added;
  if (!streamIDEqZero(&max_xdel_id))
    stream_inst->max_deleted_entry_id = max_xdel_id;

  return OpStatus::OK;
}

OpResult<uint32_t> OpDel(const OpArgs& op_args, string_view key, absl::Span<streamID> ids) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  CompactObj& cobj = (*res_it)->second;
  stream* stream_inst = (stream*)cobj.RObjPtr();

  uint32_t deleted = 0;
  bool first_entry = false;

  for (size_t j = 0; j < ids.size(); j++) {
    streamID id = ids[j];
    if (!streamDeleteItem(stream_inst, &id))
      continue;

    /* We want to know if the first entry in the stream was deleted
     * so we can later set the new one. */
    if (streamCompareID(&id, &stream_inst->first_id) == 0) {
      first_entry = 1;
    }
    /* Update the stream's maximal tombstone if needed. */
    if (streamCompareID(&id, &stream_inst->max_deleted_entry_id) > 0) {
      stream_inst->max_deleted_entry_id = id;
    }
    deleted++;
  }

  /* Update the stream's first ID. */
  if (deleted) {
    if (stream_inst->length == 0) {
      stream_inst->first_id.ms = 0;
      stream_inst->first_id.seq = 0;
    } else if (first_entry) {
      streamGetEdgeID(stream_inst, 1, 1, &stream_inst->first_id);
    }
  }

  return deleted;
}

void CreateGroup(CmdArgList args, string_view key, ConnectionContext* cntx) {
  if (args.size() < 2)
    return (*cntx)->SendError(UnknownSubCmd("CREATE", "XGROUP"));

  CreateOpts opts;
  opts.gname = ArgS(args, 0);
  opts.id = ArgS(args, 1);
  if (args.size() >= 3) {
    ToUpper(&args[2]);
    if (ArgS(args, 2) == "MKSTREAM")
      opts.flags |= kCreateOptMkstream;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCreate(t->GetOpArgs(shard), key, opts);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result);
  }
}

void DestroyGroup(string_view key, string_view gname, ConnectionContext* cntx) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDestroyGroup(t->GetOpArgs(shard), key, gname);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::OK:
      return (*cntx)->SendLong(1);
    case OpStatus::SKIPPED:
      return (*cntx)->SendLong(0);
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result);
  }
}

void DelConsumer(string_view key, string_view gname, string_view consumer,
                 ConnectionContext* cntx) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDelConsumer(t->GetOpArgs(shard), key, gname, consumer);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::OK:
      return (*cntx)->SendLong(*result);
    case OpStatus::SKIPPED:
      return (*cntx)->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result.status());
  }
}

void SetId(string_view key, string_view gname, CmdArgList args, ConnectionContext* cntx) {
  string_view id = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSetId(t->GetOpArgs(shard), key, gname, id);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::SKIPPED:
      return (*cntx)->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result);
  }
}

void XGroupHelp(CmdArgList args, ConnectionContext* cntx) {
  string_view help_arr[] = {
      "CREATE <key> <groupname> <id|$> [option]",
      "    Create a new consumer group. Options are:",
      "    * MKSTREAM",
      "      Create the empty stream if it does not exist.",
      "CREATECONSUMER <key> <groupname> <consumer>",
      "    Create a new consumer in the specified group.",
      "DELCONSUMER <key> <groupname> <consumer>",
      "    Remove the specified consumer.",
      "DESTROY <key> <groupname>"
      "    Remove the specified group.",
      "SETID <key> <groupname> <id|$>",
      "    Set the current group ID.",
  };
  return (*cntx)->SendSimpleStrArr(help_arr);
}

}  // namespace

void StreamFamily::XAdd(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  unsigned id_indx = 1;
  AddOpts add_opts;

  for (; id_indx < args.size(); ++id_indx) {
    ToUpper(&args[id_indx]);
    string_view arg = ArgS(args, id_indx);
    if (arg == "MAXLEN") {
      if (id_indx + 2 >= args.size()) {
        return (*cntx)->SendError(kSyntaxErr);
      }
      ++id_indx;
      if (ArgS(args, id_indx) == "~") {
        add_opts.max_limit_approx = true;
        ++id_indx;
      }
      arg = ArgS(args, id_indx);
      if (!absl::SimpleAtoi(arg, &add_opts.max_limit)) {
        return (*cntx)->SendError(kSyntaxErr);
      }
    } else {
      break;
    }
  }

  args.remove_prefix(id_indx);
  if (args.size() < 2 || args.size() % 2 == 0) {
    return (*cntx)->SendError(WrongNumArgsError("XADD"), kSyntaxErrType);
  }

  string_view id = ArgS(args, 0);

  if (!ParseID(id, true, 0, &add_opts.parsed_id)) {
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, add_opts, args);
  };

  OpResult<streamID> add_result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (add_result) {
    return (*cntx)->SendBulkString(StreamIdRepr(*add_result));
  }

  if (add_result.status() == OpStatus::STREAM_ID_SMALL) {
    return (*cntx)->SendError(
        "The ID specified in XADD is equal or smaller than "
        "the target stream top item");
  }

  return (*cntx)->SendError(add_result.status());
}

void StreamFamily::XDel(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  absl::InlinedVector<streamID, 8> ids(args.size());

  for (size_t i = 0; i < args.size(); ++i) {
    ParsedStreamId parsed_id;
    string_view str_id = ArgS(args, i);
    if (!ParseID(str_id, true, 0, &parsed_id)) {
      return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
    }
    ids[i] = parsed_id.val;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, absl::Span{ids.data(), ids.size()});
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendLong(*result);
  }

  (*cntx)->SendError(result.status());
}

void StreamFamily::XGroup(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (args.size() >= 2) {
    string_view key = ArgS(args, 1);
    if (sub_cmd == "CREATE") {
      args.remove_prefix(2);
      return CreateGroup(std::move(args), key, cntx);
    }

    if (sub_cmd == "DESTROY" && args.size() == 3) {
      string_view gname = ArgS(args, 2);
      return DestroyGroup(key, gname, cntx);
    }

    if (sub_cmd == "DELCONSUMER" && args.size() == 4) {
      string_view gname = ArgS(args, 2);
      string_view cname = ArgS(args, 3);
      return DelConsumer(key, gname, cname, cntx);
    }

    if (sub_cmd == "SETID" && args.size() >= 4) {
      string_view gname = ArgS(args, 2);
      args.remove_prefix(3);
      return SetId(key, gname, std::move(args), cntx);
    }
  }

  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "XGROUP"));
}

void StreamFamily::XInfo(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  if (sub_cmd == "HELP") {
    string_view help_arr[] = {
        "CONSUMERS <key> <groupname>",
        "    Show consumers of <groupname>.",
        "GROUPS <key>",
        "    Show the stream consumer groups.",
        "STREAM <key> [FULL [COUNT <count>]",
        "    Show information about the stream.",
    };
    return (*cntx)->SendSimpleStrArr(help_arr);
  }

  if (args.size() >= 2) {
    string_view key = ArgS(args, 1);
    ShardId sid = Shard(key, shard_set->size());

    if (sub_cmd == "GROUPS") {
      // We do not use transactional xemantics for xinfo since it's informational command.
      auto cb = [&]() {
        EngineShard* shard = EngineShard::tlocal();
        DbContext db_context{.db_index = cntx->db_index(), .time_now_ms = GetCurrentTimeMs()};
        return OpListGroups(db_context, key, shard);
      };

      OpResult<vector<GroupInfo>> result = shard_set->Await(sid, std::move(cb));
      if (result) {
        (*cntx)->StartArray(result->size());
        for (const auto& ginfo : *result) {
          absl::AlphaNum an1(ginfo.consumer_size);
          absl::AlphaNum an2(ginfo.pending_size);
          string last_id = StreamIdRepr(ginfo.last_id);
          string_view arr[8] = {"name",    ginfo.name,  "consumers",         an1.Piece(),
                                "pending", an2.Piece(), "last-delivered-id", last_id};

          (*cntx)->SendStringArr(absl::Span<string_view>{arr, 8}, RedisReplyBuilder::MAP);
        }
        return;
      }
      return (*cntx)->SendError(result.status());
    }
  }
  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "XINFO"));
}

void StreamFamily::XLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendLong(*result);
  }

  return (*cntx)->SendError(result.status());
}

void StreamFamily::XRange(CmdArgList args, ConnectionContext* cntx) {
  XRangeGeneric(std::move(args), false, cntx);
}

void StreamFamily::XRevRange(CmdArgList args, ConnectionContext* cntx) {
  XRangeGeneric(std::move(args), true, cntx);
}

void StreamFamily::XSetId(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view idstr = ArgS(args, 1);

  ParsedStreamId parsed_id;
  if (!ParseID(idstr, true, 0, &parsed_id)) {
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSetId2(t->GetOpArgs(shard), key, parsed_id.val);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::STREAM_ID_SMALL:
      return (*cntx)->SendError(
          "The ID specified in XSETID is smaller than the target stream top item");
    case OpStatus::ENTRIES_ADDED_SMALL:
      return (*cntx)->SendError(
          "The entries_added specified in XSETID is smaller than "
          "the target stream length");
    default:
      return (*cntx)->SendError(result);
  }
}

void StreamFamily::XRangeGeneric(CmdArgList args, bool is_rev, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view start = ArgS(args, 1);
  string_view end = ArgS(args, 2);
  RangeOpts range_opts;
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

  if (args.size() > 3) {
    if (args.size() != 5) {
      return (*cntx)->SendError(WrongNumArgsError("XRANGE"), kSyntaxErrType);
    }
    ToUpper(&args[3]);
    string_view opt = ArgS(args, 3);
    string_view val = ArgS(args, 4);

    if (opt != "COUNT" || !absl::SimpleAtoi(val, &range_opts.count)) {
      return (*cntx)->SendError(kSyntaxErr);
    }
  }

  range_opts.start = rs.parsed_id;
  range_opts.end = re.parsed_id;
  range_opts.is_rev = is_rev;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(t->GetOpArgs(shard), key, range_opts);
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
    return;
  }

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendEmptyArray();
  }
  return (*cntx)->SendError(result.status());
}

#define HFUNC(x) SetHandler(&StreamFamily::x)

void StreamFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"XADD", CO::WRITE | CO::FAST, -5, 1, 1, 1}.HFUNC(XAdd)
            << CI{"XDEL", CO::WRITE | CO::FAST, -3, 1, 1, 1}.HFUNC(XDel)
            << CI{"XGROUP", CO::WRITE | CO::DENYOOM, -3, 2, 2, 1}.HFUNC(XGroup)
            << CI{"XINFO", CO::READONLY | CO::NOSCRIPT, -2, 0, 0, 0}.HFUNC(XInfo)
            << CI{"XLEN", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(XLen)
            << CI{"XRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(XRange)
            << CI{"XREVRANGE", CO::READONLY, -4, 1, 1, 1}.HFUNC(XRevRange)
            << CI{"XSETID", CO::WRITE | CO::DENYOOM, 3, 1, 1, 1}.HFUNC(XSetId)
            << CI{"_XGROUP_HELP", CO::NOSCRIPT | CO::HIDDEN, 2, 0, 0, 0}.SetHandler(XGroupHelp);
}

}  // namespace dfly
