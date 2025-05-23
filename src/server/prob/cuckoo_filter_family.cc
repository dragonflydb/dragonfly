// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/prob/cuckoo_filter_family.h"

#include "absl/functional/function_ref.h"
#include "core/prob/cuckoo_filter.h"
#include "facade/cmd_arg_parser.h"
#include "facade/op_status.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/db_slice.h"
#include "server/error.h"
#include "server/transaction.h"

namespace dfly {
using namespace facade;
using CI = CommandId;

namespace {

OpStatus InitCuckooFilter(CompactObj* obj, const prob::CuckooReserveParams& params) {
  auto cuckoo_filter = prob::CuckooFilter::Init(params, obj->memory_resource());
  if (!cuckoo_filter) {
    return OpStatus::OUT_OF_MEMORY;
  }

  obj->SetCuckooFilter(std::move(cuckoo_filter).value());
  return OpStatus::OK;
}

OpResult<bool> OpReserve(const OpArgs& op_args, std::string_view key,
                         const prob::CuckooReserveParams& params) {
  auto res_it = op_args.GetDbSlice().AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(res_it);

  if (!res_it->is_new) {
    return OpStatus::KEY_EXISTS;
  }

  auto status = InitCuckooFilter(&res_it->it->second, params);
  if (status != OpStatus::OK) {
    return status;
  }
  return true;
}

OpResult<bool> OpAddCommon(const OpArgs& op_args, std::string_view key, std::string_view item,
                           bool is_nx) {
  auto res_it = op_args.GetDbSlice().AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(res_it);

  auto& obj = res_it->it->second;
  if (res_it->is_new) {
    auto status = InitCuckooFilter(&obj, {});
    if (status != OpStatus::OK) {
      return status;
    }
  }

  auto* filter = obj.GetCuckooFilter();
  const uint64_t hash = prob::CuckooFilter::GetHash(item);
  if (is_nx && filter->Exists(hash)) {
    // TODO: improve this to not to call Exists and Insert
    // We should add InsertUnique method to the CuckooFilter
    return false;
  }

  return filter->Insert(hash);
}

OpResult<std::vector<bool>> OpInsertCommon(const OpArgs& op_args, std::string_view key, bool is_nx,
                                           const prob::CuckooReserveParams& params,
                                           bool create_cuckoo_if_not_exists,
                                           absl::Span<const std::string_view> items) {
  OpResult<DbSlice::ItAndUpdater> res_it;
  if (create_cuckoo_if_not_exists) {
    res_it = op_args.GetDbSlice().AddOrFind(op_args.db_cntx, key);
  } else {
    res_it = op_args.GetDbSlice().FindMutable(op_args.db_cntx, key, OBJ_CUCKOO_FILTER);
  }

  RETURN_ON_BAD_STATUS(res_it);

  auto& obj = res_it->it->second;
  if (res_it->is_new) {
    DCHECK(create_cuckoo_if_not_exists);
    auto status = InitCuckooFilter(&obj, params);
    if (status != OpStatus::OK) {
      return status;
    }
  }

  auto* filter = obj.GetCuckooFilter();

  auto insert = [&](std::string_view item) {
    const auto hash = prob::CuckooFilter::GetHash(item);
    return filter->Insert(hash);
  };

  auto insert_with_exists = [&](std::string_view item) {
    const auto hash = prob::CuckooFilter::GetHash(item);
    if (filter->Exists(hash)) {
      return false;
    }
    return filter->Insert(hash);
  };

  auto cb =
      is_nx ? static_cast<absl::FunctionRef<bool(std::string_view)>>(insert_with_exists) : insert;

  std::vector<bool> result(items.size());
  for (size_t i = 0; i < items.size(); i++) {
    result[i] = cb(items[i]);
  }

  return result;
}

OpResult<std::vector<bool>> OpExistsCommon(const OpArgs& op_args, std::string_view key,
                                           absl::Span<const std::string_view> items) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_CUCKOO_FILTER);
  RETURN_ON_BAD_STATUS(res_it);

  const auto* filter = res_it->GetInnerIt()->second.GetCuckooFilter();
  std::vector<bool> results(items.size());
  for (size_t i = 0; i < items.size(); i++) {
    results[i] = filter->Exists(items[i]);
  }

  return results;
}

OpResult<bool> OpDel(const OpArgs& op_args, std::string_view key, std::string_view item) {
  auto res_it = op_args.GetDbSlice().FindMutable(op_args.db_cntx, key, OBJ_CUCKOO_FILTER);
  RETURN_ON_BAD_STATUS(res_it);

  auto* filter = res_it->it->second.GetCuckooFilter();
  return filter->Delete(item);
}

OpResult<uint64_t> OpCount(const OpArgs& op_args, std::string_view key, std::string_view item) {
  auto res_it = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_CUCKOO_FILTER);
  RETURN_ON_BAD_STATUS(res_it);

  const auto* filter = res_it->GetInnerIt()->second.GetCuckooFilter();
  return filter->Count(item);
}

void AddImplCommon(CmdArgList args, const CommandContext& cmd_cntx, bool is_nx) {
  CmdArgParser parser{args};
  std::string_view key = parser.Next();
  std::string_view item = parser.Next();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAddCommon(t->GetOpArgs(shard), key, item, is_nx);
  };

  auto result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    rb->SendLong(result.value() ? 1 : 0);
  } else {
    rb->SendError(result.status());
  }
}

void InsertImplCommon(CmdArgList args, const CommandContext& cmd_cntx, bool is_nx) {
  CmdArgParser parser{args};
  std::string_view key = parser.Next();

  /* TODO: improve agruments parsing.
     If CAPACITY and NOCREATE are specified at the same time -> error. */
  prob::CuckooReserveParams params;
  bool create_cuckoo_if_not_exists = true;
  while (parser.HasNext()) {
    if (parser.Check("CAPACITY")) {
      params.capacity = parser.Next<uint64_t>();
    } else if (parser.Check("NOCREATE")) {
      create_cuckoo_if_not_exists = false;
    } else if (parser.Check("ITEMS")) {
      break;
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInsertCommon(t->GetOpArgs(shard), key, is_nx, params, create_cuckoo_if_not_exists,
                          parser.Tail());
  };

  auto result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    const auto& result_array = result.value();
    rb->StartArray(result_array.size());
    for (const auto& was_added : result_array) {
      rb->SendLong(was_added ? 1 : 0);
    }
  } else {
    rb->SendError(result.status());
  }
}

void ExistsImplCommon(CmdArgList args, const CommandContext& cmd_cntx, bool is_multi) {
  CmdArgParser parser{args};
  std::string_view key = parser.Next();

  std::vector<std::string_view> items;
  if (is_multi) {
    while (parser.HasNext()) {
      items.push_back(parser.Next());
    }
  } else {
    items.push_back(parser.Next());
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExistsCommon(t->GetOpArgs(shard), key, items);
  };

  auto result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    if (is_multi) {
      rb->StartArray(result->size());
      for (auto res : *result) {
        rb->SendLong(res);
      }
    } else {
      DCHECK(result->size() == 1);
      rb->SendLong((*result)[0]);
    }
  } else {
    rb->SendError(result.status());
  }
}

}  // anonymous namespace

void CuckooFilterFamily::Reserve(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  std::string_view key = parser.Next();

  prob::CuckooReserveParams params{.capacity = parser.Next<uint64_t>()};
  while (parser.HasNext()) {
    if (parser.Check("BUCKETSIZE")) {
      params.bucket_size = parser.Next<uint8_t>();
    } else if (parser.Check("MAXITERATIONS")) {
      params.max_iterations = parser.Next<uint16_t>();
    } else if (parser.Check("EXPANSION")) {
      params.expansion = parser.Next<uint16_t>();
    } else {
      break;
    }
  }

  if (!parser.Finalize()) {
    return cmd_cntx.rb->SendError(parser.Error()->MakeReply());
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpReserve(t->GetOpArgs(shard), key, params);
  };

  auto result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    rb->SendOk();
  } else {
    rb->SendError(result.status());
  }
}

void CuckooFilterFamily::Add(CmdArgList args, const CommandContext& cmd_cntx) {
  return AddImplCommon(args, cmd_cntx, false);
}

void CuckooFilterFamily::AddNx(CmdArgList args, const CommandContext& cmd_cntx) {
  return AddImplCommon(args, cmd_cntx, true);
}

void CuckooFilterFamily::Insert(CmdArgList args, const CommandContext& cmd_cntx) {
  return InsertImplCommon(args, cmd_cntx, false);
}

void CuckooFilterFamily::InsertNx(CmdArgList args, const CommandContext& cmd_cntx) {
  return InsertImplCommon(args, cmd_cntx, true);
}

void CuckooFilterFamily::Exists(CmdArgList args, const CommandContext& cmd_cntx) {
  return ExistsImplCommon(args, cmd_cntx, false);
}

void CuckooFilterFamily::MExists(CmdArgList args, const CommandContext& cmd_cntx) {
  return ExistsImplCommon(args, cmd_cntx, true);
}

void CuckooFilterFamily::Del(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  std::string_view key = parser.Next();
  std::string_view item = parser.Next();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, item);
  };

  auto result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    rb->SendLong(result.value() ? 1 : 0);
  } else {
    rb->SendError(result.status());
  }
}

void CuckooFilterFamily::Count(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  std::string_view key = parser.Next();
  std::string_view item = parser.Next();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCount(t->GetOpArgs(shard), key, item);
  };

  auto result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    rb->SendLong(result.value());
  } else {
    rb->SendError(result.status());
  }
}

#define HFUNC(x) SetHandler(&CuckooFilterFamily::x)

void CuckooFilterFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();

  *registry
      << CI{"CF.RESERVE", CO::WRITE | CO::DENYOOM, -3, 1, 1, acl::CUCKOO_FILTER}.HFUNC(Reserve)
      << CI{"CF.ADD", CO::WRITE | CO::DENYOOM, 3, 1, 1, acl::CUCKOO_FILTER}.HFUNC(Add)
      << CI{"CF.ADDNX", CO::WRITE | CO::DENYOOM, 3, 1, 1, acl::CUCKOO_FILTER}.HFUNC(AddNx)
      << CI{"CF.INSERT", CO::WRITE | CO::DENYOOM, -4, 1, 1, acl::CUCKOO_FILTER}.HFUNC(Insert)
      << CI{"CF.INSERTNX", CO::WRITE | CO::DENYOOM, -4, 1, 1, acl::CUCKOO_FILTER}.HFUNC(InsertNx)
      << CI{"CF.EXISTS", CO::READONLY | CO::FAST, 3, 1, 1, acl::CUCKOO_FILTER}.HFUNC(Exists)
      << CI{"CF.MEXISTS", CO::READONLY | CO::FAST, -3, 1, 1, acl::CUCKOO_FILTER}.HFUNC(MExists)
      << CI{"CF.DEL", CO::WRITE, 3, 1, 1, acl::CUCKOO_FILTER}.HFUNC(Del)
      << CI{"CF.COUNT", CO::READONLY | CO::FAST, 3, 1, 1, acl::CUCKOO_FILTER}.HFUNC(Count);
};

}  // namespace dfly
