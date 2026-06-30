// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cuckoo.h"
#include "facade/cmd_arg_parser.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/error.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

namespace {

constexpr uint64_t kDefaultCapacity = 1024;

struct CuckooInfo {
  size_t size = 0;
  uint64_t num_buckets = 0;
  size_t num_filters = 0;
  size_t num_items = 0;
  uint64_t num_deletes = 0;
  uint8_t bucket_size = 0;
  uint16_t expansion = 0;
  uint16_t max_iterations = 0;
};

OpResult<bool> OpAdd(const OpArgs& op_args, string_view key, string_view item) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue& pv = op_res->it->second;
  if (op_res->is_new) {
    pv.SetCuckooFilter(CuckooFilterOptions{.capacity = kDefaultCapacity});
  }

  if (!pv.GetCuckooFilter()->Insert(CuckooFilter::Hash(item)))
    return OpStatus::CUCKOO_FILTER_FULL;
  return true;
}

OpResult<bool> OpAddNx(const OpArgs& op_args, string_view key, string_view item) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue& pv = op_res->it->second;
  if (op_res->is_new) {
    pv.SetCuckooFilter(CuckooFilterOptions{.capacity = kDefaultCapacity});
  }

  uint64_t hash = CuckooFilter::Hash(item);
  if (pv.GetCuckooFilter()->Exists(hash))
    return false;

  if (!pv.GetCuckooFilter()->Insert(hash))
    return OpStatus::CUCKOO_FILTER_FULL;
  return true;
}

OpResult<vector<bool>> OpExists(const OpArgs& op_args, string_view key, ParsedArgs items) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  if (!op_res)
    return op_res.status();

  const CuckooFilter* cf = op_res.value()->second.GetCuckooFilter();
  vector<bool> result(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    result[i] = cf->Exists(CuckooFilter::Hash(items[i]));
  }
  return result;
}

OpResult<CuckooInfo> OpInfo(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  if (!op_res)
    return op_res.status();

  const CuckooFilter* cf = op_res.value()->second.GetCuckooFilter();
  return CuckooInfo{cf->MallocUsed(), cf->NumBuckets(),     cf->NumFilters(), cf->NumItems(),
                    cf->NumDeletes(), cf->SlotsPerBucket(), cf->Expansion(),  cf->MaxIterations()};
}

OpResult<size_t> OpCount(const OpArgs& op_args, string_view key, string_view item) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  if (!op_res)
    return op_res.status();

  const CuckooFilter* cf = op_res.value()->second.GetCuckooFilter();
  return cf->Count(CuckooFilter::Hash(item));
}

OpResult<bool> OpDel(const OpArgs& op_args, string_view key, string_view item) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  CuckooFilter* cf = op_res->it->second.GetCuckooFilter();
  bool deleted = cf->Delete(CuckooFilter::Hash(item));
  // auto-compact once deletes exceed 10% of items (mirrors RedisBloom's threshold)
  if (deleted && cf->NumFilters() > 1 && cf->NumDeletes() > cf->NumItems() / 10)
    cf->Compact(/*cont=*/false);
  return deleted;
}

OpStatus OpCompact(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  // cont=true: unlike Delete()'s automatic compaction, CF.COMPACT keeps trying older
  // sub-filters even if a newer one couldn't be fully emptied.
  op_res->it->second.GetCuckooFilter()->Compact(/*cont=*/true);
  return OpStatus::OK;
}

struct InsertOptions {
  uint64_t capacity = kDefaultCapacity;
  bool nocreate = false;
};

// Shared op for CF.INSERT and CF.INSERTNX. Returns one integer per item:
//   1  — item inserted
//   0  — item already exists (nx only)
//  -1  — filter is full, item could not be inserted
// Returns KEY_NOTFOUND if nocreate is set and the key does not exist.
OpResult<vector<int>> OpInsert(const OpArgs& op_args, string_view key, ParsedArgs items,
                               const InsertOptions& opts, bool nx) {
  auto& db_slice = op_args.GetDbSlice();

  DbSlice::ItAndUpdater it_and_updater;
  if (opts.nocreate) {
    auto find_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
    if (!find_res)
      return find_res.status();
    it_and_updater = std::move(*find_res);
  } else {
    auto add_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
    RETURN_ON_BAD_STATUS(add_res);
    if (add_res->is_new) {
      add_res->it->second.SetCuckooFilter(CuckooFilterOptions{.capacity = opts.capacity});
    }
    it_and_updater = std::move(*add_res);
  }

  CuckooFilter* cf = it_and_updater.it->second.GetCuckooFilter();
  vector<int> result(items.size());
  for (size_t i = 0; i < items.size(); ++i) {
    const uint64_t hash = CuckooFilter::Hash(items[i]);
    if (nx) {
      if (cf->Exists(hash)) {
        result[i] = 0;
      } else {
        result[i] = cf->Insert(hash) ? 1 : -1;
      }
    } else {
      result[i] = cf->Insert(hash) ? 1 : -1;
    }
  }
  return result;
}

OpStatus OpReserve(const OpArgs& op_args, string_view key, const CuckooFilterOptions& options) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  if (!op_res->is_new)
    return OpStatus::KEY_EXISTS;

  op_res->it->second.SetCuckooFilter(options);
  return OpStatus::OK;
}

void CmdReserve(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  uint64_t capacity = parser.Next<uint64_t>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, rb);

  if (capacity == 0) {
    return rb->SendError("CF: capacity must be greater than 0");
  }

  uint8_t bucket_size = CuckooFilterOptions::kDefaultSlotsPerBucket;
  uint16_t max_iterations = CuckooFilterOptions::kDefaultMaxIterations;
  uint16_t expansion = CuckooFilterOptions::kDefaultExpansion;

  parser.Apply(Tag("BUCKETSIZE", &bucket_size), Tag("MAXITERATIONS", &max_iterations),
               Tag("EXPANSION", &expansion));

  if (!parser.Finalize()) {
    return rb->SendError(parser.TakeError().MakeReply());
  }

  // The parser already rejects values that overflow the field width above (e.g. bucketsize
  // 256) with the standard "value is not an integer or out of range" error. Only the
  // business-rule bounds below (zero, and CF's tighter expansion cap) need manual checks.
  if (bucket_size == 0) {
    return rb->SendError("CF: bucket size must be between 1 and 255");
  }
  if (max_iterations == 0) {
    return rb->SendError("CF: max iterations must be between 1 and 65535");
  }
  if (expansion > 32767) {
    return rb->SendError("CF: expansion must be between 0 and 32767");
  }

  CuckooFilterOptions options{capacity, bucket_size, max_iterations, expansion};

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), key, options);
  };

  OpStatus res = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::KEY_EXISTS) {
    return rb->SendError("item exists");
  }
  if (res == OpStatus::OK) {
    return rb->SendOk();
  }
  return rb->SendError(res);
}

void CmdAdd(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  string_view item = parser.Next();

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), key, item);
  };

  OpResult<bool> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(*res);
}

void CmdAddNx(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  string_view item = parser.Next();

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAddNx(t->GetOpArgs(shard), key, item);
  };

  OpResult<bool> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(*res);
}

OpResult<vector<bool>> RunExists(CommandContext* cmd_cntx, string_view key, ParsedArgs items) {
  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExists(t->GetOpArgs(shard), key, items);
  };
  return cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
}

void CmdExists(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  ParsedArgs items = parser.UnparsedArgs();

  OpResult<vector<bool>> res = RunExists(cmd_cntx, key, items);
  if (!res && res.status() != OpStatus::KEY_NOTFOUND && res.status() != OpStatus::WRONG_TYPE)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(res ? res->front() : 0);
}

void CmdMExists(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  ParsedArgs items = parser.UnparsedArgs();

  OpResult<vector<bool>> res = RunExists(cmd_cntx, key, items);
  if (!res && res.status() != OpStatus::KEY_NOTFOUND && res.status() != OpStatus::WRONG_TYPE)
    return cmd_cntx->SendError(res.status());

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RedisReplyBuilder::ArrayScope scope{rb, items.size()};
  for (size_t i = 0; i < items.size(); ++i) {
    rb->SendLong(res ? static_cast<long>((*res)[i]) : 0);
  }
}

void CmdInfo(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInfo(t->GetOpArgs(shard), key);
  };

  OpResult<CuckooInfo> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res)
    return rb->SendError(res.status());

  SinkReplyBuilder::ReplyScope scope(rb);
  rb->StartArray(16);
  rb->SendBulkString("Size");
  rb->SendLong(static_cast<long>(res->size));
  rb->SendBulkString("Number of buckets");
  rb->SendLong(static_cast<long>(res->num_buckets));
  rb->SendBulkString("Number of filters");
  rb->SendLong(static_cast<long>(res->num_filters));
  rb->SendBulkString("Number of items inserted");
  rb->SendLong(static_cast<long>(res->num_items));
  rb->SendBulkString("Number of items deleted");
  rb->SendLong(static_cast<long>(res->num_deletes));
  rb->SendBulkString("Bucket size");
  rb->SendLong(res->bucket_size);
  rb->SendBulkString("Expansion rate");
  rb->SendLong(res->expansion);
  rb->SendBulkString("Max iterations");
  rb->SendLong(res->max_iterations);
}

void CmdCount(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  string_view item = parser.Next();

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCount(t->GetOpArgs(shard), key, item);
  };

  OpResult<size_t> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res && res.status() != OpStatus::KEY_NOTFOUND && res.status() != OpStatus::WRONG_TYPE)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(res ? static_cast<long>(*res) : 0);
}

void CmdDel(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  string_view item = parser.Next();

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, item);
  };

  OpResult<bool> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(*res);
}

void CmdInsertImpl(CmdArgParser parser, CommandContext* cmd_cntx, bool nx) {
  string_view key = parser.Next();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, rb);

  InsertOptions opts;
  parser.Apply(Tag("CAPACITY", &opts.capacity), Exist("NOCREATE", &opts.nocreate));
  RETURN_ON_PARSE_ERROR(parser, rb);

  if (!opts.nocreate && opts.capacity == 0)
    return rb->SendError("CF: capacity must be greater than 0");

  if (!parser.Check("ITEMS")) {
    return rb->SendError("CF.INSERT requires ITEMS keyword");
  }
  ParsedArgs items = parser.UnparsedArgs();
  if (items.empty()) {
    return rb->SendError("CF.INSERT requires at least one item");
  }

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInsert(t->GetOpArgs(shard), key, items, opts, nx);
  };

  OpResult<vector<int>> res = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (!res)
    return rb->SendError(res.status());

  RedisReplyBuilder::ArrayScope scope{rb, res->size()};
  for (int v : *res) {
    rb->SendLong(v);
  }
}

void CmdInsert(CmdArgParser parser, CommandContext* cmd_cntx) {
  CmdInsertImpl(std::move(parser), cmd_cntx, false);
}

void CmdInsertNx(CmdArgParser parser, CommandContext* cmd_cntx) {
  CmdInsertImpl(std::move(parser), cmd_cntx, true);
}

void CmdCompact(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCompact(t->GetOpArgs(shard), key);
  };

  OpStatus res = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));
  if (res == OpStatus::OK)
    return cmd_cntx->SendOk();
  return cmd_cntx->SendError(res);
}

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterCuckooFilterFamily(CommandRegistry* registry) {
  registry->StartFamily(acl::CUCKOO_FILTER);

  *registry << CI{"CF.RESERVE", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1}.HFUNC(Reserve)
            << CI{"CF.ADD", CO::JOURNALED | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(Add)
            << CI{"CF.ADDNX", CO::JOURNALED | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(AddNx)
            << CI{"CF.EXISTS", CO::READONLY | CO::FAST, 3, 1, 1}.HFUNC(Exists)
            << CI{"CF.MEXISTS", CO::READONLY | CO::FAST, -3, 1, 1}.HFUNC(MExists)
            << CI{"CF.INFO", CO::READONLY | CO::FAST, 2, 1, 1}.HFUNC(Info)
            << CI{"CF.COUNT", CO::READONLY | CO::FAST, 3, 1, 1}.HFUNC(Count)
            << CI{"CF.DEL", CO::JOURNALED | CO::DENYOOM | CO::FAST, 3, 1, 1}.HFUNC(Del)
            << CI{"CF.INSERT", CO::JOURNALED | CO::DENYOOM | CO::FAST, -4, 1, 1}.HFUNC(Insert)
            << CI{"CF.INSERTNX", CO::JOURNALED | CO::DENYOOM | CO::FAST, -4, 1, 1}.HFUNC(InsertNx)
            << CI{"CF.COMPACT", CO::JOURNALED, 2, 1, 1}.HFUNC(Compact);
}

}  // namespace dfly
