// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/flags.h"
#include "core/cuckoo.h"
#include "facade/cmd_arg_parser.h"
#include "facade/reply_builder.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/config_registry.h"
#include "server/conn_context.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/transaction.h"

// Defaults used when a filter is auto-created without explicit BUCKETSIZE/CAPACITY/MAXITERATIONS/
// EXPANSION (CF.ADD, CF.ADDNX, CF.INSERT without CAPACITY), and the cap on sub-filter growth
// enforced by CF.ADD/CF.ADDNX/CF.INSERT/CF.INSERTNX. All are also settable via CONFIG SET; see
// the RegisterMutable calls below for the cached values hot-path code actually reads.
ABSL_FLAG(uint32_t, cf_bucket_size, 2, "Default cuckoo filter bucket size (slots per bucket)");
ABSL_FLAG(uint64_t, cf_initial_size, 1024, "Default cuckoo filter initial capacity");
ABSL_FLAG(uint32_t, cf_max_iterations, 20,
          "Default cuckoo filter max relocation iterations before an insert fails");
ABSL_FLAG(uint32_t, cf_expansion_factor, 1, "Default cuckoo filter sub-filter growth factor");
ABSL_FLAG(uint32_t, cf_max_expansions, 32,
          "Maximum number of sub-filters a cuckoo filter can grow to via CF.ADD/CF.ADDNX/"
          "CF.INSERT/CF.INSERTNX before those commands start failing");

namespace dfly {

using namespace facade;
using namespace std;

namespace {

constexpr uint64_t kMaxCapacity = 1ULL << 30;

constexpr char kCapacityErr[] = "CF: capacity must be in the range [2 * bucket size, 1073741824]";
constexpr char kBucketSizeErr[] = "CF: bucket size must be between 1 and 255";
constexpr char kMaxIterationsErr[] = "CF: max iterations must be between 1 and 65535";
constexpr char kExpansionErr[] = "CF: expansion must be between 0 and 32768";

// Per-thread cached copies of the flags above, seeded from the flag at each thread's startup.
// Every proactor thread in shard_set->pool() also handles connections, so these cover both
// command parsing and shard execution. Refreshed on all threads via ConfigRegistry callbacks (see
// RegisterCuckooFilterConfig) on CONFIG SET, so hot-path command code never calls
// absl::GetFlag() directly.
thread_local uint8_t tl_cf_bucket_size = static_cast<uint8_t>(absl::GetFlag(FLAGS_cf_bucket_size));
thread_local uint64_t tl_cf_initial_size = absl::GetFlag(FLAGS_cf_initial_size);
thread_local uint16_t tl_cf_max_iterations =
    static_cast<uint16_t>(absl::GetFlag(FLAGS_cf_max_iterations));
thread_local uint16_t tl_cf_expansion_factor =
    static_cast<uint16_t>(absl::GetFlag(FLAGS_cf_expansion_factor));
thread_local uint32_t tl_cf_max_expansions = absl::GetFlag(FLAGS_cf_max_expansions);

// capacity must be at least 2*bucket_size (two buckets worth of room) and at most kMaxCapacity.
bool CapacityInRange(uint64_t capacity, uint8_t bucket_size) {
  return capacity >= 2ULL * bucket_size && capacity <= kMaxCapacity;
}

CuckooFilterOptions NewFilterOptions(uint64_t capacity) {
  return CuckooFilterOptions{
      .capacity = capacity,
      .slots_per_bucket = tl_cf_bucket_size,
      .max_iterations = tl_cf_max_iterations,
      .expansion = tl_cf_expansion_factor,
  };
}

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
    pv.SetCuckooFilter(NewFilterOptions(tl_cf_initial_size));
  }

  CuckooFilter* cf = pv.GetCuckooFilter();
  if (cf->NumFilters() >= tl_cf_max_expansions)
    return OpStatus::CUCKOO_FILTER_MAX_EXPANSIONS;

  if (!cf->Insert(CuckooFilter::Hash(item)))
    return OpStatus::CUCKOO_FILTER_FULL;
  return true;
}

OpResult<bool> OpAddNx(const OpArgs& op_args, string_view key, string_view item) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_CUCKOOFILTER);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue& pv = op_res->it->second;
  if (op_res->is_new) {
    pv.SetCuckooFilter(NewFilterOptions(tl_cf_initial_size));
  }

  CuckooFilter* cf = pv.GetCuckooFilter();
  uint64_t hash = CuckooFilter::Hash(item);
  if (cf->Exists(hash))
    return false;

  if (cf->NumFilters() >= tl_cf_max_expansions)
    return OpStatus::CUCKOO_FILTER_MAX_EXPANSIONS;

  if (!cf->Insert(hash))
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
  // auto-compact once deletes exceed 10% of items
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
  string_view key;
  uint64_t capacity = tl_cf_initial_size;
  bool nocreate = false;
};

constexpr auto kInsertGrammar =
    Compile(Args(&InsertOptions::key), Options(Field("CAPACITY", &InsertOptions::capacity),
                                               Exist("NOCREATE", &InsertOptions::nocreate)));

// Shared op for CF.INSERT and CF.INSERTNX. Returns one integer per item:
//   1  — item inserted
//   0  — item already exists (nx only)
//  -1  — filter is full, item could not be inserted
// Returns KEY_NOTFOUND if nocreate is set and the key does not exist.
OpResult<vector<int>> OpInsert(const OpArgs& op_args, ParsedArgs items, const InsertOptions& opts,
                               bool nx) {
  auto& db_slice = op_args.GetDbSlice();

  DbSlice::ItAndUpdater it_and_updater;
  if (opts.nocreate) {
    auto find_res = db_slice.FindMutable(op_args.db_cntx, opts.key, OBJ_CUCKOOFILTER);
    if (!find_res)
      return find_res.status();
    it_and_updater = std::move(*find_res);
  } else {
    auto add_res = db_slice.AddOrFind(op_args.db_cntx, opts.key, OBJ_CUCKOOFILTER);
    RETURN_ON_BAD_STATUS(add_res);
    if (add_res->is_new) {
      add_res->it->second.SetCuckooFilter(NewFilterOptions(opts.capacity));
    }
    it_and_updater = std::move(*add_res);
  }

  CuckooFilter* cf = it_and_updater.it->second.GetCuckooFilter();
  if (cf->NumFilters() >= tl_cf_max_expansions)
    return OpStatus::CUCKOO_FILTER_MAX_EXPANSIONS;

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

struct ReserveOpts {
  string_view key;
  uint64_t capacity = 0;
  Validated<uint8_t, NotEq<uint8_t{0}, kBucketSizeErr>> bucket_size{tl_cf_bucket_size};
  Validated<uint16_t, NotEq<uint16_t{0}, kMaxIterationsErr>> max_iterations{tl_cf_max_iterations};
  Validated<uint16_t, ClosedRange<0, 32768, kExpansionErr>> expansion{tl_cf_expansion_factor};
};

constexpr auto kReserveGrammar =
    Compile(Args(&ReserveOpts::key, &ReserveOpts::capacity),
            Options(Field("BUCKETSIZE", &ReserveOpts::bucket_size),
                    Field("MAXITERATIONS", &ReserveOpts::max_iterations),
                    Field("EXPANSION", &ReserveOpts::expansion)));

void CmdReserve(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  ReserveOpts opts;
  kReserveGrammar.Apply(&parser, &opts);
  if (!parser.Finalize()) {
    return rb->SendError(parser.TakeError().MakeReply());
  }

  if (!CapacityInRange(opts.capacity, opts.bucket_size))
    return rb->SendError(kCapacityErr);

  CuckooFilterOptions options{opts.capacity, opts.bucket_size, opts.max_iterations, opts.expansion};

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), opts.key, options);
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
  ParsedArgs items = parser.RemainingRange(kSyntaxErr);
  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  OpResult<vector<bool>> res = RunExists(cmd_cntx, key, items);
  if (!res && res.status() != OpStatus::KEY_NOTFOUND && res.status() != OpStatus::WRONG_TYPE)
    return cmd_cntx->SendError(res.status());
  cmd_cntx->SendLong(res ? res->front() : 0);
}

void CmdMExists(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  ParsedArgs items = parser.RemainingRange(kSyntaxErr);
  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

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
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  InsertOptions opts;
  kInsertGrammar.Apply(&parser, &opts);
  RETURN_ON_PARSE_ERROR(parser, rb);

  // Validated unconditionally, regardless of NOCREATE or whether the key already exists —
  // CF.INSERT accepts CAPACITY only to describe how an about-to-be-created filter should look,
  // but a bogus value is rejected up front either way.
  if (!CapacityInRange(opts.capacity, tl_cf_bucket_size))
    return rb->SendError(kCapacityErr);

  if (!parser.Check("ITEMS")) {
    return rb->SendError("CF.INSERT requires ITEMS keyword");
  }
  ParsedArgs items = parser.RemainingRange("CF.INSERT requires at least one item");
  RETURN_ON_PARSE_ERROR(parser, rb);

  const auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInsert(t->GetOpArgs(shard), items, opts, nx);
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

void RegisterCuckooFilterConfig() {
  auto* pool = shard_set->pool();

  config_registry.RegisterMutable("cf_bucket_size", [pool](const absl::CommandLineFlag& flag) {
    auto val = flag.TryGet<uint32_t>();
    if (!val || *val < 1 || *val > 255)
      return false;
    pool->AwaitBrief([v = static_cast<uint8_t>(*val)](unsigned, auto*) { tl_cf_bucket_size = v; });
    return true;
  });

  config_registry.RegisterMutable("cf_initial_size", [pool](const absl::CommandLineFlag& flag) {
    auto val = flag.TryGet<uint64_t>();
    if (!val || *val < 4 || *val > kMaxCapacity)
      return false;
    pool->AwaitBrief([v = *val](unsigned, auto*) { tl_cf_initial_size = v; });
    return true;
  });

  config_registry.RegisterMutable("cf_max_iterations", [pool](const absl::CommandLineFlag& flag) {
    auto val = flag.TryGet<uint32_t>();
    if (!val || *val < 1 || *val > 65535)
      return false;
    pool->AwaitBrief(
        [v = static_cast<uint16_t>(*val)](unsigned, auto*) { tl_cf_max_iterations = v; });
    return true;
  });

  config_registry.RegisterMutable("cf_expansion_factor", [pool](const absl::CommandLineFlag& flag) {
    auto val = flag.TryGet<uint32_t>();
    if (!val || *val > 32768)
      return false;
    uint16_t v = static_cast<uint16_t>(*val);
    pool->AwaitBrief([v](unsigned, auto*) { tl_cf_expansion_factor = v; });
    return true;
  });

  config_registry.RegisterMutable("cf_max_expansions", [pool](const absl::CommandLineFlag& flag) {
    auto val = flag.TryGet<uint32_t>();
    if (!val || *val < 1 || *val > 65536)
      return false;
    pool->AwaitBrief([v = *val](unsigned, auto*) { tl_cf_max_expansions = v; });
    return true;
  });
}

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterCuckooFilterFamily(CommandRegistry* registry) {
  RegisterCuckooFilterConfig();

  registry->StartFamily(acl::CUCKOO_FILTER);

  *registry << CI{"CF.RESERVE", CO::JOURNALED | CO::DENYOOM | CO::FAST, -3, 1, 1}.HFUNC(Reserve)
            // It should really be fast but I am keeping compat.
            << CI{"CF.ADD", CO::JOURNALED | CO::DENYOOM, 3, 1, 1}.HFUNC(Add)
            << CI{"CF.ADDNX", CO::JOURNALED | CO::DENYOOM, 3, 1, 1}.HFUNC(AddNx)
            << CI{"CF.EXISTS", CO::READONLY | CO::FAST, 3, 1, 1}.HFUNC(Exists)
            << CI{"CF.MEXISTS", CO::READONLY | CO::FAST, -3, 1, 1}.HFUNC(MExists)
            << CI{"CF.INFO", CO::READONLY | CO::FAST, 2, 1, 1}.HFUNC(Info)
            << CI{"CF.COUNT", CO::READONLY | CO::FAST, 3, 1, 1}.HFUNC(Count)
            << CI{"CF.DEL", CO::JOURNALED | CO::FAST, 3, 1, 1}.HFUNC(Del)
            << CI{"CF.INSERT", CO::JOURNALED | CO::DENYOOM, -4, 1, 1}.HFUNC(Insert)
            << CI{"CF.INSERTNX", CO::JOURNALED | CO::DENYOOM, -4, 1, 1}.HFUNC(InsertNx)
            // They mark it fast and read only. It's neither.
            << CI{"CF.COMPACT", CO::JOURNALED, 2, 1, 1}.HFUNC(Compact);
}

}  // namespace dfly
