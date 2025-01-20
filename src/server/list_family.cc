// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/list_family.h"

#include "facade/cmd_arg_parser.h"
#include "server/acl/acl_commands_def.h"

extern "C" {
#include "redis/sds.h"
}

#include <absl/strings/numbers.h>

#include "base/flags.h"
#include "base/logging.h"
#include "core/qlist.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
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
ABSL_FLAG(int32_t, list_max_listpack_size, -2, "Maximum listpack size, default is 8kb");

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

ABSL_FLAG(int32_t, list_compress_depth, 0, "Compress depth of the list. Default is no compression");
ABSL_FLAG(bool, list_experimental_v2, true,
          "Enables dragonfly specific implementation of quicklist");

namespace dfly {

using namespace std;
using namespace facade;
using absl::GetFlag;
using time_point = Transaction::time_point;

namespace {

quicklist* GetQL(const PrimeValue& mv) {
  return (quicklist*)mv.RObjPtr();
}

QList* GetQLV2(const PrimeValue& mv) {
  return (QList*)mv.RObjPtr();
}

void* listPopSaver(unsigned char* data, size_t sz) {
  return new string((char*)data, sz);
}

QList::Where ToWhere(ListDir dir) {
  return dir == ListDir::LEFT ? QList::HEAD : QList::TAIL;
}

enum InsertParam { INSERT_BEFORE, INSERT_AFTER };

string ListPop(ListDir dir, quicklist* ql) {
  long long vlong;
  string* pop_str = nullptr;

  int ql_where = (dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;

  // Empty list automatically removes the key (see below).
  CHECK_EQ(1,
           quicklistPopCustom(ql, ql_where, (unsigned char**)&pop_str, NULL, &vlong, listPopSaver));
  string res;
  if (pop_str) {
    pop_str->swap(res);
    delete pop_str;
  } else {
    res = absl::StrCat(vlong);
  }

  return res;
}

ListDir ParseDir(facade::CmdArgParser* parser) {
  return parser->MapNext("LEFT", ListDir::LEFT, "RIGHT", ListDir::RIGHT);
}

string_view DirToSv(ListDir dir) {
  switch (dir) {
    case ListDir::LEFT:
      return "LEFT"sv;
    case ListDir::RIGHT:
      return "RIGHT"sv;
  }
  return ""sv;
}

bool ElemCompare(const quicklistEntry& entry, string_view elem) {
  if (entry.value) {
    return entry.sz == elem.size() &&
           (entry.sz == 0 || memcmp(entry.value, elem.data(), entry.sz) == 0);
  }

  absl::AlphaNum an(entry.longval);
  return elem == an.Piece();
}

class BPopPusher {
 public:
  BPopPusher(string_view pop_key, string_view push_key, ListDir popdir, ListDir pushdir);

  // Returns WRONG_TYPE, OK.
  // If OK is returned then use result() to fetch the value.
  OpResult<string> Run(unsigned limit_ms, Transaction* tx, ConnectionContext* cntx);

 private:
  OpResult<string> RunSingle(time_point tp, Transaction* tx, ConnectionContext* cntx);
  OpResult<string> RunPair(time_point tp, Transaction* tx, ConnectionContext* cntx);

  string_view pop_key_, push_key_;
  ListDir popdir_, pushdir_;
};

// Called as a callback from MKBlocking after we've determined which key to pop.
std::string OpBPop(Transaction* t, EngineShard* shard, std::string_view key, ListDir dir) {
  DVLOG(2) << "popping from " << key << " " << t->DebugId();

  auto& db_slice = t->GetDbSlice(shard->shard_id());
  auto it_res = db_slice.FindMutable(t->GetDbContext(), key, OBJ_LIST);

  CHECK(it_res) << t->DebugId() << " " << key;  // must exist and must be ok.

  auto it = it_res->it;
  std::string value;
  size_t len;

  if (it->second.Encoding() == OBJ_ENCODING_QUICKLIST) {
    quicklist* ql = GetQL(it->second);

    value = ListPop(dir, ql);
    len = quicklistCount(ql);
  } else {
    QList* ql = GetQLV2(it->second);
    QList::Where where = ToWhere(dir);
    value = ql->Pop(where);
    len = ql->Size();
  }

  it_res->post_updater.Run();

  OpArgs op_args = t->GetOpArgs(shard);
  if (len == 0) {
    DVLOG(1) << "deleting key " << key << " " << t->DebugId();
    op_args.GetDbSlice().Del(op_args.db_cntx, it);
  }

  if (op_args.shard->journal()) {
    string command = dir == ListDir::LEFT ? "LPOP" : "RPOP";
    RecordJournal(op_args, command, ArgSlice{key}, 1);
  }

  return value;
}

OpResult<string> OpMoveSingleShard(const OpArgs& op_args, string_view src, string_view dest,
                                   ListDir src_dir, ListDir dest_dir) {
  auto& db_slice = op_args.GetDbSlice();
  auto src_res = db_slice.FindMutable(op_args.db_cntx, src, OBJ_LIST);
  if (!src_res)
    return src_res.status();

  auto src_it = src_res->it;
  quicklist* src_ql = nullptr;
  QList* srcql_v2 = nullptr;
  quicklist* dest_ql = nullptr;
  QList* destql_v2 = nullptr;
  string val;
  size_t prev_len = 0;

  if (src_it->second.Encoding() == OBJ_ENCODING_QUICKLIST) {
    src_ql = GetQL(src_it->second);
    prev_len = quicklistCount(src_ql);
  } else {
    DCHECK_EQ(src_it->second.Encoding(), kEncodingQL2);
    srcql_v2 = GetQLV2(src_it->second);
    prev_len = srcql_v2->Size();
  }

  if (src == dest) {  // simple case.
    if (src_ql) {
      val = ListPop(src_dir, src_ql);
      int pos = (dest_dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
      quicklistPush(src_ql, val.data(), val.size(), pos);
    } else {
      val = srcql_v2->Pop(ToWhere(src_dir));
      srcql_v2->Push(val, ToWhere(dest_dir));
    }

    return val;
  }

  src_res->post_updater.Run();

  auto op_res = db_slice.AddOrFind(op_args.db_cntx, dest);
  RETURN_ON_BAD_STATUS(op_res);
  auto& dest_res = *op_res;

  // Insertion of dest could invalidate src_it. Find it again.
  src_res = db_slice.FindMutable(op_args.db_cntx, src, OBJ_LIST);
  src_it = src_res->it;

  if (dest_res.is_new) {
    if (absl::GetFlag(FLAGS_list_experimental_v2)) {
      destql_v2 = CompactObj::AllocateMR<QList>(GetFlag(FLAGS_list_max_listpack_size),
                                                GetFlag(FLAGS_list_compress_depth));
      dest_res.it->second.InitRobj(OBJ_LIST, kEncodingQL2, destql_v2);
    } else {
      dest_ql = quicklistCreate();
      quicklistSetOptions(dest_ql, GetFlag(FLAGS_list_max_listpack_size),
                          GetFlag(FLAGS_list_compress_depth));
      dest_res.it->second.InitRobj(OBJ_LIST, OBJ_ENCODING_QUICKLIST, dest_ql);
    }
  } else {
    if (dest_res.it->second.ObjType() != OBJ_LIST)
      return OpStatus::WRONG_TYPE;
    if (dest_res.it->second.Encoding() == kEncodingQL2) {
      destql_v2 = GetQLV2(dest_res.it->second);
    } else {
      DCHECK_EQ(dest_res.it->second.Encoding(), OBJ_ENCODING_QUICKLIST);
      dest_ql = GetQL(dest_res.it->second);
    }
  }

  if (src_ql) {
    DCHECK(dest_ql);
    val = ListPop(src_dir, src_ql);
    int pos = (dest_dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    quicklistPush(dest_ql, val.data(), val.size(), pos);
  } else {
    DCHECK(srcql_v2);
    DCHECK(destql_v2);
    val = srcql_v2->Pop(ToWhere(src_dir));
    destql_v2->Push(val, ToWhere(dest_dir));
  }

  src_res->post_updater.Run();
  dest_res.post_updater.Run();

  if (prev_len == 1) {
    db_slice.Del(op_args.db_cntx, src_it);
  }

  return val;
}

// Read-only peek operation that determines whether the list exists and optionally
// returns the first from left/right value without popping it from the list.
OpResult<string> Peek(const OpArgs& op_args, string_view key, ListDir dir, bool fetch) {
  auto it_res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res) {
    return it_res.status();
  }

  if (!fetch)
    return OpStatus::OK;

  const PrimeValue& pv = it_res.value()->second;
  DCHECK_GT(pv.Size(), 0u);  // should be not-empty.

  if (pv.Encoding() == OBJ_ENCODING_QUICKLIST) {
    quicklist* ql = GetQL(it_res.value()->second);
    quicklistEntry entry = container_utils::QLEntry();
    quicklistIter* iter =
        quicklistGetIterator(ql, (dir == ListDir::LEFT) ? AL_START_HEAD : AL_START_TAIL);

    CHECK(quicklistNext(iter, &entry));
    quicklistReleaseIterator(iter);

    return (entry.value) ? string(reinterpret_cast<char*>(entry.value), entry.sz)
                         : absl::StrCat(entry.longval);
  }

  DCHECK_EQ(pv.Encoding(), kEncodingQL2);
  QList* ql = GetQLV2(pv);
  auto it = ql->GetIterator(ToWhere(dir));
  CHECK(it.Next());

  return it.Get().to_string();
}

OpResult<uint32_t> OpPush(const OpArgs& op_args, std::string_view key, ListDir dir,
                          bool skip_notexist, facade::ArgRange vals, bool journal_rewrite) {
  EngineShard* es = op_args.shard;
  DbSlice::AddOrFindResult res;

  if (skip_notexist) {
    auto tmp_res = op_args.GetDbSlice().FindMutable(op_args.db_cntx, key, OBJ_LIST);
    if (tmp_res == OpStatus::KEY_NOTFOUND)
      return 0;  // Redis returns 0 for nonexisting keys for the *PUSHX actions.
    RETURN_ON_BAD_STATUS(tmp_res);
    res = std::move(*tmp_res);
  } else {
    auto op_res = op_args.GetDbSlice().AddOrFind(op_args.db_cntx, key);
    RETURN_ON_BAD_STATUS(op_res);
    res = std::move(*op_res);
  }

  size_t len = 0;
  DVLOG(1) << "OpPush " << key << " new_key " << res.is_new;
  quicklist* ql = nullptr;
  QList* ql_v2 = nullptr;

  if (res.is_new) {
    if (absl::GetFlag(FLAGS_list_experimental_v2)) {
      ql_v2 = CompactObj::AllocateMR<QList>(GetFlag(FLAGS_list_max_listpack_size),
                                            GetFlag(FLAGS_list_compress_depth));
      res.it->second.InitRobj(OBJ_LIST, kEncodingQL2, ql_v2);
    } else {
      ql = quicklistCreate();
      quicklistSetOptions(ql, GetFlag(FLAGS_list_max_listpack_size),
                          GetFlag(FLAGS_list_compress_depth));
      res.it->second.InitRobj(OBJ_LIST, OBJ_ENCODING_QUICKLIST, ql);
    }
  } else {
    if (res.it->second.ObjType() != OBJ_LIST)
      return OpStatus::WRONG_TYPE;
    if (res.it->second.Encoding() == kEncodingQL2) {
      ql_v2 = GetQLV2(res.it->second);
    } else {
      ql = GetQL(res.it->second);
    }
  }

  if (ql) {
    // Left push is LIST_HEAD.
    int pos = (dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    for (string_view v : vals) {
      auto vsds = WrapSds(v);
      quicklistPush(ql, vsds, sdslen(vsds), pos);
    }
    len = quicklistCount(ql);
  } else {
    QList::Where where = ToWhere(dir);
    for (string_view v : vals) {
      ql_v2->Push(v, where);
    }
    len = ql_v2->Size();
  }

  if (res.is_new) {
    auto blocking_controller = op_args.db_cntx.ns->GetBlockingController(es->shard_id());
    if (blocking_controller) {
      string tmp;
      string_view key = res.it->first.GetSlice(&tmp);

      blocking_controller->AwakeWatched(op_args.db_cntx.db_index, key);
    }
  }

  if (journal_rewrite && op_args.shard->journal()) {
    string command = dir == ListDir::LEFT ? "LPUSH" : "RPUSH";
    vector<string_view> mapped(vals.Size() + 1);
    mapped[0] = key;
    std::copy(vals.begin(), vals.end(), mapped.begin() + 1);
    RecordJournal(op_args, command, mapped, 2);
  }

  return len;
}

OpResult<StringVec> OpPop(const OpArgs& op_args, string_view key, ListDir dir, uint32_t count,
                          bool return_results, bool journal_rewrite) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  if (count == 0)
    return StringVec{};

  auto it = it_res->it;
  size_t prev_len = 0;
  StringVec res;

  if (it->second.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(it->second);
    prev_len = ql->Size();

    if (prev_len < count) {
      count = prev_len;
    }

    if (return_results) {
      res.reserve(count);
    }

    QList::Where where = ToWhere(dir);
    for (unsigned i = 0; i < count; ++i) {
      string val = ql->Pop(where);
      if (return_results) {
        res.push_back(std::move(val));
      }
    }
  } else {
    quicklist* ql = GetQL(it->second);
    prev_len = quicklistCount(ql);

    if (prev_len < count) {
      count = prev_len;
    }

    if (return_results) {
      res.reserve(count);
    }

    for (unsigned i = 0; i < count; ++i) {
      string val = ListPop(dir, ql);
      if (return_results) {
        res.push_back(std::move(val));
      }
    }
  }
  it_res->post_updater.Run();

  if (count == prev_len) {
    db_slice.Del(op_args.db_cntx, it);
  }

  if (op_args.shard->journal() && journal_rewrite) {
    string command = dir == ListDir::LEFT ? "LPOP" : "RPOP";
    RecordJournal(op_args, command, ArgSlice{key}, 2);
  }
  return res;
}

OpResult<string> MoveTwoShards(Transaction* trans, string_view src, string_view dest,
                               ListDir src_dir, ListDir dest_dir, bool conclude_on_error) {
  DCHECK_EQ(2u, trans->GetUniqueShardCnt());

  OpResult<string> find_res[2];
  OpResult<string> result;

  // Transaction is comprised of 2 hops:
  // 1 - check for entries existence, their types and if possible -
  //     read the value we may move from the source list.
  // 2.  If everything is ok, pop from source and push the peeked value into
  //     the destination.
  //
  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto args = t->GetShardArgs(shard->shard_id());
    DCHECK_EQ(1u, args.Size());
    bool is_dest = args.Front() == dest;
    find_res[is_dest] = Peek(t->GetOpArgs(shard), args.Front(), src_dir, !is_dest);
    return OpStatus::OK;
  };

  trans->Execute(std::move(cb), false);

  if (!find_res[0] || find_res[1].status() == OpStatus::WRONG_TYPE) {
    result = find_res[0] ? find_res[1] : find_res[0];
    if (conclude_on_error)
      trans->Conclude();
  } else {
    // Everything is ok, lets proceed with the mutations.
    auto cb = [&](Transaction* t, EngineShard* shard) {
      auto args = t->GetShardArgs(shard->shard_id());
      auto key = args.Front();
      bool is_dest = (key == dest);
      OpArgs op_args = t->GetOpArgs(shard);

      if (is_dest) {
        string_view val{find_res[0].value()};
        DVLOG(1) << "Pushing value: " << val << " to list: " << dest;

        OpPush(op_args, key, dest_dir, false, ArgSlice{val}, true);

        // blocking_controller does not have to be set with non-blocking transactions.
        auto blocking_controller = t->GetNamespace().GetBlockingController(shard->shard_id());
        if (blocking_controller) {
          // hack, again. since we hacked which queue we are waiting on (see RunPair)
          // we must clean-up src key here manually. See RunPair why we do this.
          // in short- we suspended on "src" on both shards.
          blocking_controller->FinalizeWatched(ArgSlice({src}), t);
        }
      } else {
        DVLOG(1) << "Popping value from list: " << key;
        OpPop(op_args, key, src_dir, 1, false, true);
      }

      return OpStatus::OK;
    };
    trans->Execute(std::move(cb), true);
    result = std::move(find_res[0].value());
  }

  return result;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, std::string_view key) {
  auto res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_LIST);
  if (!res)
    return res.status();

  if (res.value()->second.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(res.value()->second);
    return ql->Size();
  }

  quicklist* ql = GetQL(res.value()->second);
  return quicklistCount(ql);
}

OpResult<string> OpIndex(const OpArgs& op_args, std::string_view key, long index) {
  auto res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_LIST);
  if (!res)
    return res.status();

  string str;
  if (res.value()->second.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(res.value()->second);
    auto it = ql->GetIterator(index);
    if (!it.Next())
      return OpStatus::KEY_NOTFOUND;
    str = it.Get().to_string();
  } else {
    quicklist* ql = GetQL(res.value()->second);
    quicklistEntry entry = container_utils::QLEntry();
    quicklistIter* iter = quicklistGetIteratorAtIdx(ql, AL_START_TAIL, index);
    if (!iter)
      return OpStatus::KEY_NOTFOUND;

    quicklistNext(iter, &entry);

    if (entry.value) {
      str.assign(reinterpret_cast<char*>(entry.value), entry.sz);
    } else {
      str = absl::StrCat(entry.longval);
    }
    quicklistReleaseIterator(iter);
  }
  return str;
}

OpResult<vector<uint32_t>> OpPos(const OpArgs& op_args, string_view key, string_view element,
                                 int rank, uint32_t count, uint32_t max_len) {
  DCHECK(key.data() && element.data());
  DCHECK_NE(rank, 0);

  auto it_res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res.ok())
    return it_res.status();

  const PrimeValue& pv = (*it_res)->second;
  vector<uint32_t> matches;

  if (pv.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(pv);
    QList::Where where = QList::HEAD;
    if (rank < 0) {
      rank = -rank;
      where = QList::TAIL;
    }

    auto it = ql->GetIterator(where);
    unsigned index = 0;
    while (it.Next() && (max_len == 0 || index < max_len)) {
      if (it.Get() == element) {
        if (rank == 1) {
          auto k = (where == QList::HEAD) ? index : ql->Size() - index - 1;
          matches.push_back(k);
          if (count && matches.size() >= count)
            break;
        } else {
          rank--;
        }
      }
      index++;
    }
  } else {
    int direction = AL_START_HEAD;
    if (rank < 0) {
      rank = -rank;
      direction = AL_START_TAIL;
    }

    quicklist* ql = GetQL(it_res.value()->second);
    quicklistIter* ql_iter = quicklistGetIterator(ql, direction);
    quicklistEntry entry;

    unsigned index = 0;
    int matched = 0;
    string str;

    while (quicklistNext(ql_iter, &entry) && (max_len == 0 || index < max_len)) {
      if (entry.value) {
        str.assign(reinterpret_cast<char*>(entry.value), entry.sz);
      } else {
        str = absl::StrCat(entry.longval);
      }
      if (str == element) {
        matched++;
        auto k = (direction == AL_START_TAIL) ? ql->count - index - 1 : index;
        if (matched >= rank) {
          matches.push_back(k);
          if (count && unsigned(matched - rank + 1) >= count) {
            break;
          }
        }
      }
      index++;
    }
    quicklistReleaseIterator(ql_iter);
  }
  return matches;
}

OpResult<int> OpInsert(const OpArgs& op_args, string_view key, string_view pivot, string_view elem,
                       InsertParam insert_param) {
  DCHECK(key.data() && pivot.data() && elem.data());

  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeValue& pv = it_res->it->second;

  int res = -1;

  if (pv.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(pv);
    QList::InsertOpt insert_opt = (insert_param == INSERT_BEFORE) ? QList::BEFORE : QList::AFTER;
    if (ql->Insert(pivot, elem, insert_opt)) {
      res = ql->Size();
    }
  } else {
    quicklist* ql = GetQL(pv);
    quicklistEntry entry = container_utils::QLEntry();
    quicklistIter* qiter = quicklistGetIterator(ql, AL_START_HEAD);
    bool found = false;

    while (quicklistNext(qiter, &entry)) {
      if (ElemCompare(entry, pivot)) {
        found = true;
        break;
      }
    }

    if (found) {
      if (insert_param == INSERT_AFTER) {
        quicklistInsertAfter(qiter, &entry, elem.data(), elem.size());
      } else {
        DCHECK_EQ(INSERT_BEFORE, insert_param);
        quicklistInsertBefore(qiter, &entry, elem.data(), elem.size());
      }
      res = quicklistCount(ql);
    }
    quicklistReleaseIterator(qiter);
  }

  return res;
}

OpResult<uint32_t> OpRem(const OpArgs& op_args, string_view key, string_view elem, long count) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  auto it = it_res->it;
  size_t len = 0;
  unsigned removed = 0;

  int64_t ival;

  // try parsing the element into an integer.
  int is_int = lpStringToInt64(elem.data(), elem.size(), &ival);

  if (it->second.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(it->second);
    QList::Where where = QList::HEAD;

    if (count < 0) {
      count = -count;
      where = QList::TAIL;
    }

    auto it = ql->GetIterator(where);
    auto is_match = [&](const QList::Entry& entry) {
      return is_int ? entry.is_int() && entry.ival() == ival : entry == elem;
    };

    while (it.Next()) {
      QList::Entry entry = it.Get();
      if (is_match(entry)) {
        it = ql->Erase(it);
        removed++;
        if (count && removed == count)
          break;
      }
    }
    len = ql->Size();
  } else {
    quicklist* ql = GetQL(it->second);

    int iter_direction = AL_START_HEAD;
    long long index = 0;
    if (count < 0) {
      count = -count;
      iter_direction = AL_START_TAIL;
      index = -1;
    }

    quicklistIter qiter;
    quicklistInitIterator(&qiter, ql, iter_direction, index);
    quicklistEntry entry;

    auto is_match = [&](const quicklistEntry& entry) {
      if (is_int != (entry.value == nullptr))
        return false;

      return is_int ? entry.longval == ival : ElemCompare(entry, elem);
    };

    while (quicklistNext(&qiter, &entry)) {
      if (is_match(entry)) {
        quicklistDelEntry(&qiter, &entry);
        removed++;
        if (count && removed == count)
          break;
      }
    }
    quicklistCompressIterator(&qiter);
    len = quicklistCount(ql);
  }
  it_res->post_updater.Run();

  if (len == 0) {
    db_slice.Del(op_args.db_cntx, it);
  }

  return removed;
}

OpStatus OpSet(const OpArgs& op_args, string_view key, string_view elem, long index) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  auto it = it_res->it;
  OpStatus status = OpStatus::OUT_OF_RANGE;
  if (it->second.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(it->second);
    if (ql->Replace(index, elem))
      status = OpStatus::OK;
  } else {
    DCHECK_EQ(it->second.Encoding(), OBJ_ENCODING_QUICKLIST);
    quicklist* ql = GetQL(it->second);

    int replaced = quicklistReplaceAtIndex(ql, index, elem.data(), elem.size());
    if (replaced) {
      status = OpStatus::OK;
    }
  }
  return status;
}

OpStatus OpTrim(const OpArgs& op_args, string_view key, long start, long end) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  auto it = it_res->it;

  long llen = it->second.Size();

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

  if (it->second.Encoding() == kEncodingQL2) {
    QList* ql = GetQLV2(it->second);
    ql->Erase(0, ltrim);
    ql->Erase(-rtrim, rtrim);
  } else {
    quicklist* ql = GetQL(it->second);
    quicklistDelRange(ql, 0, ltrim);
    quicklistDelRange(ql, -rtrim, rtrim);
  }
  it_res->post_updater.Run();

  if (it->second.Size() == 0) {
    db_slice.Del(op_args.db_cntx, it);
  }
  return OpStatus::OK;
}

OpResult<StringVec> OpRange(const OpArgs& op_args, std::string_view key, long start, long end) {
  auto res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_LIST);
  if (!res)
    return res.status();

  const PrimeValue& pv = (*res)->second;
  long llen = pv.Size();

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

  StringVec str_vec;
  container_utils::IterateList(
      pv,
      [&str_vec](container_utils::ContainerEntry ce) {
        str_vec.emplace_back(ce.ToString());
        return true;
      },
      start, end);
  return str_vec;
}

void MoveGeneric(string_view src, string_view dest, ListDir src_dir, ListDir dest_dir,
                 Transaction* tx, SinkReplyBuilder* builder) {
  OpResult<string> result;

  if (tx->GetUniqueShardCnt() == 1) {
    tx->ReviveAutoJournal();  // On single shard we can use the auto journal flow.
    auto cb = [&](Transaction* t, EngineShard* shard) {
      return OpMoveSingleShard(t->GetOpArgs(shard), src, dest, src_dir, dest_dir);
    };
    result = tx->ScheduleSingleHopT(std::move(cb));
  } else {
    result = MoveTwoShards(tx, src, dest, src_dir, dest_dir, true);
  }

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (result) {
    return rb->SendBulkString(*result);
  }

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      rb->SendNull();
      break;

    default:
      builder->SendError(result.status());
      break;
  }
}

void RPopLPush(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view src = ArgS(args, 0);
  string_view dest = ArgS(args, 1);

  MoveGeneric(src, dest, ListDir::RIGHT, ListDir::LEFT, cmd_cntx.tx, cmd_cntx.rb);
}

void BRPopLPush(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};
  auto [src, dest] = parser.Next<string_view, string_view>();
  float timeout = parser.Next<float>();
  auto* builder = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

  if (timeout < 0)
    return builder->SendError("timeout is negative");

  BPopPusher bpop_pusher(src, dest, ListDir::RIGHT, ListDir::LEFT);
  OpResult<string> op_res =
      bpop_pusher.Run(unsigned(timeout * 1000), cmd_cntx.tx, cmd_cntx.conn_cntx);

  if (op_res) {
    return builder->SendBulkString(*op_res);
  }

  switch (op_res.status()) {
    case OpStatus::CANCELLED:
    case OpStatus::TIMED_OUT:
      return builder->SendNull();
      break;

    default:
      return builder->SendError(op_res.status());
      break;
  }
}

void BLMove(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};
  auto [src, dest] = parser.Next<string_view, string_view>();
  ListDir src_dir = ParseDir(&parser);
  ListDir dest_dir = ParseDir(&parser);
  float timeout = parser.Next<float>();
  auto* builder = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

  if (timeout < 0)
    return builder->SendError("timeout is negative");

  BPopPusher bpop_pusher(src, dest, src_dir, dest_dir);
  OpResult<string> op_res =
      bpop_pusher.Run(unsigned(timeout * 1000), cmd_cntx.tx, cmd_cntx.conn_cntx);

  if (op_res) {
    return builder->SendBulkString(*op_res);
  }

  switch (op_res.status()) {
    case OpStatus::CANCELLED:
    case OpStatus::TIMED_OUT:
      return builder->SendNull();
      break;

    default:
      return builder->SendError(op_res.status());
      break;
  }
}

BPopPusher::BPopPusher(string_view pop_key, string_view push_key, ListDir popdir, ListDir pushdir)
    : pop_key_(pop_key), push_key_(push_key), popdir_(popdir), pushdir_(pushdir) {
}

OpResult<string> BPopPusher::Run(unsigned limit_ms, Transaction* tx, ConnectionContext* cntx) {
  time_point tp =
      limit_ms ? chrono::steady_clock::now() + chrono::milliseconds(limit_ms) : time_point::max();

  if (tx->GetUniqueShardCnt() == 1) {
    return RunSingle(tp, tx, cntx);
  }

  return RunPair(tp, tx, cntx);
}

OpResult<string> BPopPusher::RunSingle(time_point tp, Transaction* tx, ConnectionContext* cntx) {
  OpResult<string> op_res;
  bool is_multi = tx->IsMulti();
  auto cb_move = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args = t->GetOpArgs(shard);
    op_res = OpMoveSingleShard(op_args, pop_key_, push_key_, popdir_, pushdir_);
    if (op_res) {
      if (op_args.shard->journal()) {
        std::array<string_view, 4> arr = {pop_key_, push_key_, DirToSv(popdir_), DirToSv(pushdir_)};
        RecordJournal(op_args, "LMOVE", arr, 1);
      }
      auto blocking_controller = t->GetNamespace().GetBlockingController(shard->shard_id());
      if (blocking_controller) {
        string tmp;

        blocking_controller->AwakeWatched(op_args.db_cntx.db_index, push_key_);
      }
    }

    return OpStatus::OK;
  };
  tx->Execute(cb_move, false);

  if (is_multi || op_res.status() != OpStatus::KEY_NOTFOUND) {
    if (op_res.status() == OpStatus::KEY_NOTFOUND) {
      op_res = OpStatus::TIMED_OUT;
    }
    tx->Conclude();
    return op_res;
  }

  auto wcb = [&](Transaction* t, EngineShard* shard) { return ArgSlice(&pop_key_, 1); };

  const auto key_checker = [](EngineShard* owner, const DbContext& context, Transaction*,
                              std::string_view key) -> bool {
    return context.GetDbSlice(owner->shard_id()).FindReadOnly(context, key, OBJ_LIST).ok();
  };
  // Block
  auto status = tx->WaitOnWatch(tp, std::move(wcb), key_checker, &(cntx->blocked), &(cntx->paused));
  if (status != OpStatus::OK)
    return status;

  tx->Execute(cb_move, true);
  return op_res;
}

OpResult<string> BPopPusher::RunPair(time_point tp, Transaction* tx, ConnectionContext* cntx) {
  bool is_multi = tx->IsMulti();
  OpResult<string> op_res = MoveTwoShards(tx, pop_key_, push_key_, popdir_, pushdir_, false);

  if (is_multi || op_res.status() != OpStatus::KEY_NOTFOUND) {
    if (op_res.status() == OpStatus::KEY_NOTFOUND) {
      op_res = OpStatus::TIMED_OUT;
    }
    return op_res;
  }

  // a hack: we watch in both shards for pop_key but only in the source shard it's relevant.
  // Therefore we follow the regular flow of watching the key but for the destination shard it
  // will never be triggerred.
  // This allows us to run Transaction::Execute on watched transactions in both shards.
  auto wcb = [&](Transaction* t, EngineShard* shard) { return ArgSlice(&this->pop_key_, 1); };

  const auto key_checker = [](EngineShard* owner, const DbContext& context, Transaction*,
                              std::string_view key) -> bool {
    return context.GetDbSlice(owner->shard_id()).FindReadOnly(context, key, OBJ_LIST).ok();
  };

  if (auto status = tx->WaitOnWatch(tp, std::move(wcb), key_checker, &cntx->blocked, &cntx->paused);
      status != OpStatus::OK)
    return status;

  return MoveTwoShards(tx, pop_key_, push_key_, popdir_, pushdir_, true);
}

void PushGeneric(ListDir dir, bool skip_notexists, CmdArgList args, Transaction* tx,
                 SinkReplyBuilder* builder) {
  std::string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPush(t->GetOpArgs(shard), key, dir, skip_notexists, args.subspan(1), false);
  };

  OpResult<uint32_t> result = tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    return builder->SendLong(result.value());
  }

  return builder->SendError(result.status());
}

void PopGeneric(ListDir dir, CmdArgList args, Transaction* tx, SinkReplyBuilder* builder) {
  facade::CmdArgParser parser{args};
  string_view key = parser.Next();

  uint32_t count = 1;
  bool return_arr = false;
  if (parser.HasNext()) {
    count = parser.Next<uint32_t>();
    return_arr = true;
  }

  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPop(t->GetOpArgs(shard), key, dir, count, true, false);
  };

  OpResult<StringVec> result = tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      return rb->SendNull();
    case OpStatus::WRONG_TYPE:
      return builder->SendError(kWrongTypeErr);
    default:;
  }

  if (return_arr) {
    rb->StartArray(result->size());
    for (const auto& k : *result) {
      rb->SendBulkString(k);
    }
  } else {
    DCHECK_EQ(1u, result->size());
    rb->SendBulkString(result->front());
  }
}

void BPopGeneric(ListDir dir, CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                 ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 2u);

  float timeout;
  auto timeout_str = ArgS(args, args.size() - 1);
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return builder->SendError("timeout is not a float or out of range");
  }
  if (timeout < 0) {
    return builder->SendError("timeout is negative");
  }
  VLOG(1) << "BPop timeout(" << timeout << ")";

  std::string popped_value;
  auto cb = [dir, &popped_value](Transaction* t, EngineShard* shard, std::string_view key) {
    popped_value = OpBPop(t, shard, key, dir);
  };

  OpResult<string> popped_key = container_utils::RunCbOnFirstNonEmptyBlocking(
      tx, OBJ_LIST, std::move(cb), unsigned(timeout * 1000), &cntx->blocked, &cntx->paused);

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (popped_key) {
    DVLOG(1) << "BPop " << tx->DebugId() << " popped from key " << popped_key;  // key.
    std::string_view str_arr[2] = {*popped_key, popped_value};
    return rb->SendBulkStrArr(str_arr);
  }

  DVLOG(1) << "result for " << tx->DebugId() << " is " << popped_key.status();

  switch (popped_key.status()) {
    case OpStatus::WRONG_TYPE:
      return builder->SendError(kWrongTypeErr);
    case OpStatus::CANCELLED:
    case OpStatus::TIMED_OUT:
      return rb->SendNullArray();
    case OpStatus::KEY_MOVED: {
      auto error = cluster::SlotOwnershipError(*tx->GetUniqueSlotId());
      CHECK(!error.status.has_value() || error.status.value() != facade::OpStatus::OK);
      return builder->SendError(std::move(error));
    }
    default:
      LOG(ERROR) << "Unexpected error " << popped_key.status();
  }
  return rb->SendNullArray();
}

}  // namespace

void ListFamily::LPush(CmdArgList args, const CommandContext& cmd_cntx) {
  return PushGeneric(ListDir::LEFT, false, std::move(args), cmd_cntx.tx, cmd_cntx.rb);
}

void ListFamily::LPushX(CmdArgList args, const CommandContext& cmd_cntx) {
  return PushGeneric(ListDir::LEFT, true, std::move(args), cmd_cntx.tx, cmd_cntx.rb);
}

void ListFamily::LPop(CmdArgList args, const CommandContext& cmd_cntx) {
  return PopGeneric(ListDir::LEFT, std::move(args), cmd_cntx.tx, cmd_cntx.rb);
}

void ListFamily::RPush(CmdArgList args, const CommandContext& cmd_cntx) {
  return PushGeneric(ListDir::RIGHT, false, std::move(args), cmd_cntx.tx, cmd_cntx.rb);
}

void ListFamily::RPushX(CmdArgList args, const CommandContext& cmd_cntx) {
  return PushGeneric(ListDir::RIGHT, true, std::move(args), cmd_cntx.tx, cmd_cntx.rb);
}

void ListFamily::RPop(CmdArgList args, const CommandContext& cmd_cntx) {
  return PopGeneric(ListDir::RIGHT, std::move(args), cmd_cntx.tx, cmd_cntx.rb);
}

void ListFamily::LLen(CmdArgList args, const CommandContext& cmd_cntx) {
  auto key = ArgS(args, 0);
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };
  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    cmd_cntx.rb->SendLong(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    cmd_cntx.rb->SendLong(0);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void ListFamily::LPos(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};
  auto [key, elem] = parser.Next<string_view, string_view>();

  int rank = 1;
  uint32_t count = 1;
  uint32_t max_len = 0;
  bool skip_count = true;

  while (parser.HasNext()) {
    if (parser.Check("RANK")) {
      rank = parser.Next<int>();
      continue;
    }

    if (parser.Check("COUNT")) {
      count = parser.Next<uint32_t>();
      skip_count = false;
      continue;
    }

    if (parser.Check("MAXLEN")) {
      max_len = parser.Next<uint32_t>();
      continue;
    }

    parser.Skip(1);
  }

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (rank == 0)
    return rb->SendError(kInvalidIntErr);

  if (auto err = parser.Error(); err)
    return rb->SendError(err->MakeReply());

  auto cb = [&, &key = key, &elem = elem](Transaction* t, EngineShard* shard) {
    return OpPos(t->GetOpArgs(shard), key, elem, rank, count, max_len);
  };

  Transaction* trans = cmd_cntx.tx;
  OpResult<vector<uint32_t>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    return rb->SendError(result.status());
  } else if (result.status() == OpStatus::INVALID_VALUE) {
    return rb->SendError(result.status());
  }

  if (skip_count) {
    if (result->empty()) {
      rb->SendNull();
    } else {
      rb->SendLong((*result)[0]);
    }
  } else {
    SinkReplyBuilder::ReplyAggregator agg(rb);
    rb->StartArray(result->size());
    const auto& array = result.value();
    for (const auto& v : array) {
      rb->SendLong(v);
    }
  }
}

void ListFamily::LIndex(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view index_str = ArgS(args, 1);
  int32_t index;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (!absl::SimpleAtoi(index_str, &index)) {
    rb->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIndex(t->GetOpArgs(shard), key, index);
  };

  OpResult<string> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    rb->SendBulkString(result.value());
  } else if (result.status() == OpStatus::WRONG_TYPE) {
    rb->SendError(result.status());
  } else {
    rb->SendNull();
  }
}

/* LINSERT <key> (BEFORE|AFTER) <pivot> <element> */
void ListFamily::LInsert(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};
  string_view key = parser.Next();
  InsertParam where = parser.MapNext("AFTER", INSERT_AFTER, "BEFORE", INSERT_BEFORE);
  auto [pivot, elem] = parser.Next<string_view, string_view>();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (auto err = parser.Error(); err)
    return rb->SendError(err->MakeReply());

  DCHECK(pivot.data() && elem.data());

  auto cb = [&, &pivot = pivot, &elem = elem](Transaction* t, EngineShard* shard) {
    return OpInsert(t->GetOpArgs(shard), key, pivot, elem, where);
  };

  OpResult<int> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result || result == OpStatus::KEY_NOTFOUND) {
    return rb->SendLong(result.value_or(0));
  }

  rb->SendError(result.status());
}

void ListFamily::LTrim(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view s_str = ArgS(args, 1);
  string_view e_str = ArgS(args, 2);
  int32_t start, end;

  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    cmd_cntx.rb->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpTrim(t->GetOpArgs(shard), key, start, end);
  };
  OpStatus st = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  if (st == OpStatus::KEY_NOTFOUND)
    st = OpStatus::OK;
  cmd_cntx.rb->SendError(st);
}

void ListFamily::LRange(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view s_str = ArgS(args, 1);
  std::string_view e_str = ArgS(args, 2);
  int32_t start, end;

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    rb->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(t->GetOpArgs(shard), key, start, end);
  };

  auto res = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (!res && res.status() != OpStatus::KEY_NOTFOUND) {
    return rb->SendError(res.status());
  }

  rb->SendBulkStrArr(*res);
}

// lrem key 5 foo, will remove foo elements from the list if exists at most 5 times.
void ListFamily::LRem(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view index_str = ArgS(args, 1);
  std::string_view elem = ArgS(args, 2);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    cmd_cntx.rb->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(t->GetOpArgs(shard), key, elem, count);
  };
  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result || result == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx.rb->SendLong(result.value_or(0));
  }
  cmd_cntx.rb->SendError(result.status());
}

void ListFamily::LSet(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view index_str = ArgS(args, 1);
  std::string_view elem = ArgS(args, 2);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    cmd_cntx.rb->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, elem, count);
  };
  OpResult<void> result = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  if (result) {
    cmd_cntx.rb->SendOk();
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void ListFamily::BLPop(CmdArgList args, const CommandContext& cmd_cntx) {
  BPopGeneric(ListDir::LEFT, std::move(args), cmd_cntx.tx, cmd_cntx.rb, cmd_cntx.conn_cntx);
}

void ListFamily::BRPop(CmdArgList args, const CommandContext& cmd_cntx) {
  BPopGeneric(ListDir::RIGHT, std::move(args), cmd_cntx.tx, cmd_cntx.rb, cmd_cntx.conn_cntx);
}

void ListFamily::LMove(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};
  auto [src, dest] = parser.Next<string_view, string_view>();
  ListDir src_dir = ParseDir(&parser);
  ListDir dest_dir = ParseDir(&parser);

  if (auto err = parser.Error(); err)
    return cmd_cntx.rb->SendError(err->MakeReply());

  MoveGeneric(src, dest, src_dir, dest_dir, cmd_cntx.tx, cmd_cntx.rb);
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&ListFamily::x)

namespace acl {
constexpr uint32_t kLPush = WRITE | LIST | FAST;
constexpr uint32_t kLPushX = WRITE | LIST | FAST;
constexpr uint32_t kLPop = WRITE | LIST | FAST;
constexpr uint32_t kRPush = WRITE | LIST | FAST;
constexpr uint32_t kRPushX = WRITE | LIST | FAST;
constexpr uint32_t kRPop = WRITE | LIST | FAST;
constexpr uint32_t kRPopLPush = WRITE | LIST | SLOW;
constexpr uint32_t kBRPopLPush = WRITE | LIST | SLOW | BLOCKING;
constexpr uint32_t kBLPop = WRITE | LIST | SLOW | BLOCKING;
constexpr uint32_t kBRPop = WRITE | LIST | SLOW | BLOCKING;
constexpr uint32_t kLLen = READ | LIST | FAST;
constexpr uint32_t kLPos = READ | LIST | SLOW;
constexpr uint32_t kLIndex = READ | LIST | SLOW;
constexpr uint32_t kLInsert = READ | LIST | SLOW;
constexpr uint32_t kLRange = READ | LIST | SLOW;
constexpr uint32_t kLSet = WRITE | LIST | SLOW;
constexpr uint32_t kLTrim = WRITE | LIST | SLOW;
constexpr uint32_t kLRem = WRITE | LIST | SLOW;
constexpr uint32_t kLMove = WRITE | LIST | SLOW;
constexpr uint32_t kBLMove = READ | LIST | SLOW | BLOCKING;
}  // namespace acl

void ListFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();
  *registry
      << CI{"LPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, acl::kLPush}.HFUNC(LPush)
      << CI{"LPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, acl::kLPushX}.HFUNC(LPushX)
      << CI{"LPOP", CO::WRITE | CO::FAST, -2, 1, 1, acl::kLPop}.HFUNC(LPop)
      << CI{"RPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, acl::kRPush}.HFUNC(RPush)
      << CI{"RPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, acl::kRPushX}.HFUNC(RPushX)
      << CI{"RPOP", CO::WRITE | CO::FAST, -2, 1, 1, acl::kRPop}.HFUNC(RPop)
      << CI{"RPOPLPUSH", CO::WRITE | CO::FAST | CO::NO_AUTOJOURNAL, 3, 1, 2, acl::kRPopLPush}
             .SetHandler(RPopLPush)
      << CI{"BRPOPLPUSH",    CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, 4, 1, 2,
            acl::kBRPopLPush}
             .SetHandler(BRPopLPush)
      << CI{"BLPOP",    CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2,
            acl::kBLPop}
             .HFUNC(BLPop)
      << CI{"BRPOP",    CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2,
            acl::kBRPop}
             .HFUNC(BRPop)
      << CI{"LLEN", CO::READONLY | CO::FAST, 2, 1, 1, acl::kLLen}.HFUNC(LLen)
      << CI{"LPOS", CO::READONLY | CO::FAST, -3, 1, 1, acl::kLPos}.HFUNC(LPos)
      << CI{"LINDEX", CO::READONLY, 3, 1, 1, acl::kLIndex}.HFUNC(LIndex)
      << CI{"LINSERT", CO::WRITE | CO::DENYOOM, 5, 1, 1, acl::kLInsert}.HFUNC(LInsert)
      << CI{"LRANGE", CO::READONLY, 4, 1, 1, acl::kLRange}.HFUNC(LRange)
      << CI{"LSET", CO::WRITE | CO::DENYOOM, 4, 1, 1, acl::kLSet}.HFUNC(LSet)
      << CI{"LTRIM", CO::WRITE, 4, 1, 1, acl::kLTrim}.HFUNC(LTrim)
      << CI{"LREM", CO::WRITE, 4, 1, 1, acl::kLRem}.HFUNC(LRem)
      << CI{"LMOVE", CO::WRITE | CO::NO_AUTOJOURNAL, 5, 1, 2, acl::kLMove}.HFUNC(LMove)
      << CI{"BLMOVE", CO::WRITE | CO::NO_AUTOJOURNAL | CO::BLOCKING, 6, 1, 2, acl::kBLMove}
             .SetHandler(BLMove);
}

}  // namespace dfly
