// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/list_family.h"

extern "C" {
#include "redis/object.h"
#include "redis/sds.h"
}

#include <absl/strings/numbers.h>

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/server_state.h"
#include "server/transaction.h"

/**
 * The number of entries allowed per internal list node can be specified
 * as a fixed maximum size or a maximum number of elements.
 * For a fixed maximum size, use -5 through -1, meaning:
 * -5: max size: 64 Kb  <-- not recommended for normal workloads
 * -4: max size: 32 Kb  <-- not recommended
 * -3: max size: 16 Kb  <-- probably not recommended
 * -2: max size: 8 Kb   <-- good
 * -1: max size: 4 Kb   <-- good
 * Positive numbers mean store up to _exactly_ that number of elements
 * per list node.
 * The highest performing option is usually -2 (8 Kb size) or -1 (4 Kb size),
 * but if your use case is unique, adjust the settings as necessary.
 *
 */
DEFINE_int32(list_max_listpack_size, -2, "Maximum listpack size, default is 8kb");

/**
 * Lists may also be compressed.
 * Compress depth is the number of quicklist listpack nodes from *each* side of
 * the list to *exclude* from compression.  The head and tail of the list
 * are always uncompressed for fast push/pop operations.  Settings are:
 * 0: disable all list compression
 * 1: depth 1 means "don't start compressing until after 1 node into the list,
 *    going from either the head or tail"
 *    So: [head]->node->node->...->node->[tail]
 *    [head], [tail] will always be uncompressed; inner nodes will compress.
 * 2: [head]->[next]->node->node->...->node->[prev]->[tail]
 *    2 here means: don't compress head or head->next or tail->prev or tail,
 *    but compress all nodes between them.
 * 3: [head]->[next]->[next]->node->node->...->node->[prev]->[prev]->[tail]
 * etc.
 *
 */

DEFINE_int32(list_compress_depth, 0, "Compress depth of the list. Default is no compression");

namespace dfly {

using namespace std;
using namespace facade;

namespace {

quicklistEntry QLEntry() {
  quicklistEntry res{.quicklist = NULL,
                     .node = NULL,
                     .zi = NULL,
                     .value = NULL,
                     .longval = 0,
                     .sz = 0,
                     .offset = 0};
  return res;
}

quicklist* GetQL(const PrimeValue& mv) {
  return (quicklist*)mv.RObjPtr();
}

void* listPopSaver(unsigned char* data, size_t sz) {
  return createStringObject((char*)data, sz);
}

string ListPop(ListDir dir, quicklist* ql) {
  long long vlong;
  robj* value = NULL;

  int ql_where = (dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;

  // Empty list automatically removes the key (see below).
  CHECK_EQ(1,
           quicklistPopCustom(ql, ql_where, (unsigned char**)&value, NULL, &vlong, listPopSaver));
  string res;
  if (value) {
    DCHECK(value->encoding == OBJ_ENCODING_EMBSTR || value->encoding == OBJ_ENCODING_RAW);
    sds s = (sds)(value->ptr);
    res = string{s, sdslen(s)};
    decrRefCount(value);
  } else {
    res = absl::StrCat(vlong);
  }

  return res;
}

class BPopper {
 public:
  explicit BPopper(ListDir dir);

  // Returns WRONG_TYPE, OK.
  // If OK is returned then use result() to fetch the value.
  OpStatus Run(Transaction* t, unsigned msec);

  auto result() const {
    return make_pair<string_view, string_view>(key_, value_);
  }

  bool found() const {
    return found_;
  }

 private:
  OpStatus Pop(Transaction* t, EngineShard* shard);

  ListDir dir_;

  bool found_ = false;
  PrimeIterator find_it_;
  ShardId find_sid_ = std::numeric_limits<ShardId>::max();

  string key_;
  string value_;
};

BPopper::BPopper(ListDir dir) : dir_(dir) {
}

OpStatus BPopper::Run(Transaction* t, unsigned msec) {
  OpResult<Transaction::FindFirstResult> result;
  using time_point = Transaction::time_point;

  time_point tp =
      msec ? chrono::steady_clock::now() + chrono::milliseconds(msec) : time_point::max();
  bool is_multi = t->IsMulti();
  if (!is_multi) {
    t->Schedule();
  }

  auto* stats = ServerState::tl_connection_stats();

  while (true) {
    result = t->FindFirst();

    if (result)
      break;

    if (result.status() != OpStatus::KEY_NOTFOUND) {  // Some error occurred.
      // We could be registered in the queue due to previous iterations.
      t->UnregisterWatch();
      break;
    }

    if (is_multi) {
      auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      t->Execute(std::move(cb), true);

      return OpStatus::TIMED_OUT;
    }

    ++stats->num_blocked_clients;
    bool wait_succeeded = t->WaitOnWatch(tp);
    --stats->num_blocked_clients;

    if (!wait_succeeded)
      return OpStatus::TIMED_OUT;
  }

  if (!result)
    return result.status();

  VLOG(1) << "Popping an element";
  find_sid_ = result->sid;
  find_it_ = result->find_res;
  found_ = true;

  auto cb = [this](Transaction* t, EngineShard* shard) { return Pop(t, shard); };
  t->Execute(std::move(cb), true);

  return OpStatus::OK;
}

OpStatus BPopper::Pop(Transaction* t, EngineShard* shard) {
  DCHECK(found());

  if (shard->shard_id() == find_sid_) {
    find_it_->first.GetString(&key_);

    quicklist* ql = GetQL(find_it_->second);
    value_ = ListPop(dir_, ql);

    if (quicklistCount(ql) == 0) {
      CHECK(shard->db_slice().Del(t->db_index(), find_it_));
    }
  }
  return OpStatus::OK;
}

}  // namespace

void ListFamily::LPush(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::LEFT, false, std::move(args), cntx);
}

void ListFamily::LPushX(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::LEFT, true, std::move(args), cntx);
}

void ListFamily::LPop(CmdArgList args, ConnectionContext* cntx) {
  return PopGeneric(ListDir::LEFT, std::move(args), cntx);
}

void ListFamily::RPush(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::RIGHT, false, std::move(args), cntx);
}

void ListFamily::RPushX(CmdArgList args, ConnectionContext* cntx) {
  return PushGeneric(ListDir::RIGHT, true, std::move(args), cntx);
}

void ListFamily::RPop(CmdArgList args, ConnectionContext* cntx) {
  return PopGeneric(ListDir::RIGHT, std::move(args), cntx);
}

void ListFamily::LLen(CmdArgList args, ConnectionContext* cntx) {
  auto key = ArgS(args, 1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpLen(OpArgs{shard, t->db_index()}, key);
  };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    (*cntx)->SendLong(0);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void ListFamily::LIndex(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view index_str = ArgS(args, 2);
  int32_t index;
  if (!absl::SimpleAtoi(index_str, &index)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIndex(OpArgs{shard, t->db_index()}, key, index);
  };
  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendBulkString(result.value());
  } else {
    (*cntx)->SendNull();
  }
}

void ListFamily::LTrim(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view s_str = ArgS(args, 2);
  std::string_view e_str = ArgS(args, 3);
  int32_t start, end;

  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpTrim(OpArgs{shard, t->db_index()}, key, start, end);
  };
  cntx->transaction->ScheduleSingleHop(std::move(cb));
  (*cntx)->SendOk();
}

void ListFamily::LRange(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view s_str = ArgS(args, 2);
  std::string_view e_str = ArgS(args, 3);
  int32_t start, end;

  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(OpArgs{shard, t->db_index()}, key, start, end);
  };

  auto res = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (!res && res.status() != OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendError(res.status());
  }

  (*cntx)->SendStringArr(*res);
}

// lrem key 5 foo, will remove foo elements from the list if exists at most 5 times.
void ListFamily::LRem(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view index_str = ArgS(args, 2);
  std::string_view elem = ArgS(args, 3);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(OpArgs{shard, t->db_index()}, key, elem, count);
  };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
  } else {
    (*cntx)->SendLong(0);
  }
}

void ListFamily::LSet(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view index_str = ArgS(args, 2);
  std::string_view elem = ArgS(args, 3);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(OpArgs{shard, t->db_index()}, key, elem, count);
  };
  OpResult<void> result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  if (result) {
    (*cntx)->SendOk();
  } else {
    (*cntx)->SendError(result.status());
  }
}

void ListFamily::BLPop(CmdArgList args, ConnectionContext* cntx) {
  BPopGeneric(ListDir::LEFT, std::move(args), cntx);
}

void ListFamily::BRPop(CmdArgList args, ConnectionContext* cntx) {
  BPopGeneric(ListDir::RIGHT, std::move(args), cntx);
}

void ListFamily::BPopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 3u);

  float timeout;
  auto timeout_str = ArgS(args, args.size() - 1);
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return (*cntx)->SendError("timeout is not a float or out of range");
  }
  if (timeout < 0) {
    return (*cntx)->SendError("timeout is negative");
  }
  VLOG(1) << "BLPop start " << timeout;

  Transaction* transaction = cntx->transaction;
  BPopper popper(dir);
  OpStatus result = popper.Run(transaction, unsigned(timeout * 1000));

  switch (result) {
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    case OpStatus::OK:
      break;
    case OpStatus::TIMED_OUT:
      return (*cntx)->SendNullArray();
    default:
      LOG(FATAL) << "Unexpected error " << result;
  }

  CHECK(popper.found());
  VLOG(1) << "BLPop returned ";

  auto res = popper.result();
  std::string_view str_arr[2] = {res.first, res.second};
  return (*cntx)->SendStringArr(str_arr);
}

void ListFamily::PushGeneric(ListDir dir, bool skip_notexists, CmdArgList args,
                             ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  vector<std::string_view> vals(args.size() - 2);
  for (size_t i = 2; i < args.size(); ++i) {
    vals[i - 2] = ArgS(args, i);
  }
  absl::Span<std::string_view> span{vals.data(), vals.size()};
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPush(OpArgs{shard, t->db_index()}, key, dir, skip_notexists, span);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE) {
    return (*cntx)->SendError(kWrongTypeErr);
  }

  return (*cntx)->SendLong(result.value());
}

void ListFamily::PopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  int32_t count = 1;
  bool return_arr = false;

  if (args.size() > 2) {
    if (args.size() > 3) {
      ToLower(&args[0]);
      return (*cntx)->SendError(WrongNumArgsError(ArgS(args, 0)));
    }

    string_view count_s = ArgS(args, 2);
    if (!absl::SimpleAtoi(count_s, &count)) {
      return (*cntx)->SendError(kInvalidIntErr);
    }

    if (count < 0) {
      return (*cntx)->SendError(kUintErr);
    }
    return_arr = true;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPop(OpArgs{shard, t->db_index()}, key, dir, count);
  };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendNull();
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    default:;
  }

  if (return_arr) {
    if (result->empty()) {
      (*cntx)->SendNullArray();
    } else {
      (*cntx)->StartArray(result->size());
      for (const auto& k : *result) {
        (*cntx)->SendBulkString(k);
      }
    }
  } else {
    DCHECK_EQ(1u, result->size());
    (*cntx)->SendBulkString(result->front());
  }
}

OpResult<uint32_t> ListFamily::OpPush(const OpArgs& op_args, std::string_view key, ListDir dir,
                                      bool skip_notexist, absl::Span<std::string_view> vals) {
  EngineShard* es = op_args.shard;
  PrimeIterator it;
  bool new_key = false;

  if (skip_notexist) {
    auto it_res = es->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
    if (!it_res)
      return it_res.status();
    it = *it_res;
  } else {
    tie(it, new_key) = es->db_slice().AddOrFind(op_args.db_ind, key);
  }
  quicklist* ql;

  if (new_key) {
    robj* o = createQuicklistObject();
    ql = (quicklist*)o->ptr;
    quicklistSetOptions(ql, FLAGS_list_max_listpack_size, FLAGS_list_compress_depth);
    it->second.ImportRObj(o);
  } else {
    if (it->second.ObjType() != OBJ_LIST)
      return OpStatus::WRONG_TYPE;
    es->db_slice().PreUpdate(op_args.db_ind, it);
    ql = GetQL(it->second);
  }

  // Left push is LIST_HEAD.
  int pos = (dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;

  for (auto v : vals) {
    es->tmp_str1 = sdscpylen(es->tmp_str1, v.data(), v.size());
    quicklistPush(ql, es->tmp_str1, sdslen(es->tmp_str1), pos);
  }

  if (new_key) {
    // TODO: to use PrimeKey for watched table.
    string tmp;
    string_view key = it->first.GetSlice(&tmp);
    es->AwakeWatched(op_args.db_ind, key);
  } else {
    es->db_slice().PostUpdate(op_args.db_ind, it);
  }

  return quicklistCount(ql);
}

OpResult<StringVec> ListFamily::OpPop(const OpArgs& op_args, string_view key, ListDir dir,
                                      uint32_t count) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> it_res = db_slice.Find(op_args.db_ind, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);
  db_slice.PreUpdate(op_args.db_ind, it);

  StringVec res;
  if (quicklistCount(ql) < count) {
    count = quicklistCount(ql);
  }
  res.reserve(count);

  for (unsigned i = 0; i < count; ++i) {
    res.push_back(ListPop(dir, ql));
  }
  db_slice.PostUpdate(op_args.db_ind, it);

  if (quicklistCount(ql) == 0) {
    CHECK(db_slice.Del(op_args.db_ind, it));
  }

  return res;
}

OpResult<uint32_t> ListFamily::OpLen(const OpArgs& op_args, std::string_view key) {
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();

  quicklist* ql = GetQL(res.value()->second);

  return quicklistCount(ql);
}

OpResult<string> ListFamily::OpIndex(const OpArgs& op_args, std::string_view key, long index) {
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();
  quicklist* ql = GetQL(res.value()->second);
  quicklistEntry entry = QLEntry();
  quicklistIter* iter = quicklistGetIteratorAtIdx(ql, AL_START_TAIL, index);
  if (!iter)
    return OpStatus::KEY_NOTFOUND;

  quicklistNext(iter, &entry);
  string str;

  if (entry.value) {
    str.assign(reinterpret_cast<char*>(entry.value), entry.sz);
  } else {
    str = absl::StrCat(entry.longval);
  }
  quicklistReleaseIterator(iter);

  return str;
}

OpResult<uint32_t> ListFamily::OpRem(const OpArgs& op_args, std::string_view key,
                                     std::string_view elem, long count) {
  DCHECK(!elem.empty());
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();
  quicklist* ql = GetQL(res.value()->second);

  int iter_direction = AL_START_HEAD;
  long long index = 0;
  if (count < 0) {
    count = -count;
    iter_direction = AL_START_TAIL;
    index = -1;
  }

  quicklistIter* qiter = quicklistGetIteratorAtIdx(ql, iter_direction, index);
  quicklistEntry entry;
  unsigned removed = 0;
  const uint8_t* elem_ptr = reinterpret_cast<const uint8_t*>(elem.data());
  while (quicklistNext(qiter, &entry)) {
    if (quicklistCompare(&entry, elem_ptr, elem.size())) {
      quicklistDelEntry(qiter, &entry);
      removed++;
      if (count && removed == count)
        break;
    }
  }
  quicklistReleaseIterator(qiter);

  if (quicklistCount(ql) == 0) {
    CHECK(op_args.shard->db_slice().Del(op_args.db_ind, res.value()));
  }

  return removed;
}

OpStatus ListFamily::OpSet(const OpArgs& op_args, std::string_view key, std::string_view elem,
                           long index) {
  DCHECK(!elem.empty());
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();
  quicklist* ql = GetQL(res.value()->second);

  int replaced = quicklistReplaceAtIndex(ql, index, elem.data(), elem.size());
  if (!replaced) {
    return OpStatus::OUT_OF_RANGE;
  }
  return OpStatus::OK;
}

OpStatus ListFamily::OpTrim(const OpArgs& op_args, std::string_view key, long start, long end) {
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();
  quicklist* ql = GetQL(res.value()->second);
  long llen = quicklistCount(ql);

  /* convert negative indexes */
  if (start < 0)
    start = llen + start;
  if (end < 0)
    end = llen + end;
  if (start < 0)
    start = 0;

  long ltrim, rtrim;

  /* Invariant: start >= 0, so this test will be true when end < 0.
   * The range is empty when start > end or start >= length. */
  if (start > end || start >= llen) {
    /* Out of range start or start > end result in empty list */
    ltrim = llen;
    rtrim = 0;
  } else {
    if (end >= llen)
      end = llen - 1;
    ltrim = start;
    rtrim = llen - end - 1;
  }
  quicklistDelRange(ql, 0, ltrim);
  quicklistDelRange(ql, -rtrim, rtrim);

  if (quicklistCount(ql) == 0) {
    CHECK(op_args.shard->db_slice().Del(op_args.db_ind, res.value()));
  }
  return OpStatus::OK;
}

OpResult<StringVec> ListFamily::OpRange(const OpArgs& op_args, std::string_view key, long start,
                                        long end) {
  auto res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_LIST);
  if (!res)
    return res.status();

  quicklist* ql = GetQL(res.value()->second);
  long llen = quicklistCount(ql);

  /* convert negative indexes */
  if (start < 0)
    start = llen + start;
  if (end < 0)
    end = llen + end;
  if (start < 0)
    start = 0;

  /* Invariant: start >= 0, so this test will be true when end < 0.
   * The range is empty when start > end or start >= length. */
  if (start > end || start >= llen) {
    /* Out of range start or start > end result in empty list */
    return StringVec{};
  }

  if (end >= llen)
    end = llen - 1;

  unsigned lrange = end - start + 1;
  quicklistIter* qiter = quicklistGetIteratorAtIdx(ql, AL_START_HEAD, start);
  quicklistEntry entry = QLEntry();
  StringVec str_vec;

  unsigned cnt = 0;
  while (cnt < lrange && quicklistNext(qiter, &entry)) {
    if (entry.value)
      str_vec.emplace_back(reinterpret_cast<char*>(entry.value), entry.sz);
    else
      str_vec.push_back(absl::StrCat(entry.longval));
    ++cnt;
  }
  quicklistReleaseIterator(qiter);

  return str_vec;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&ListFamily::x)

void ListFamily::Register(CommandRegistry* registry) {
  *registry << CI{"LPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(LPush)
            << CI{"LPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(LPushX)
            << CI{"LPOP", CO::WRITE | CO::FAST | CO::DENYOOM, -2, 1, 1, 1}.HFUNC(LPop)
            << CI{"RPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(RPush)
            << CI{"RPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(RPushX)
            << CI{"RPOP", CO::WRITE | CO::FAST | CO::DENYOOM, -2, 1, 1, 1}.HFUNC(RPop)
            << CI{"BLPOP", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING, -3, 1, -2, 1}.HFUNC(BLPop)
            << CI{"BRPOP", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING, -3, 1, -2, 1}.HFUNC(BRPop)
            << CI{"LLEN", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(LLen)
            << CI{"LINDEX", CO::READONLY, 3, 1, 1, 1}.HFUNC(LIndex)
            << CI{"LRANGE", CO::READONLY, 4, 1, 1, 1}.HFUNC(LRange)
            << CI{"LSET", CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(LSet)
            << CI{"LTRIM", CO::WRITE, 4, 1, 1, 1}.HFUNC(LTrim)
            << CI{"LREM", CO::WRITE, 4, 1, 1, 1}.HFUNC(LRem);
}

}  // namespace dfly
