// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/list_family.h"

extern "C" {
#include "redis/object.h"
#include "redis/sds.h"
}

#include <absl/strings/numbers.h>

#include "base/flags.h"
#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
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

namespace dfly {

using namespace std;
using namespace facade;
using absl::GetFlag;
using time_point = Transaction::time_point;

namespace {

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

optional<ListDir> ParseDir(string_view arg) {
  if (arg == "LEFT") {
    return ListDir::LEFT;
  }
  if (arg == "RIGHT") {
    return ListDir::RIGHT;
  }

  return nullopt;
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

// Circular buffer for string messages
struct CircularMessages {
  CircularMessages(size_t size) : current{0}, messages{size} {
  }

  string* Next() {
    string* next = &messages[current];
    current = (current + 1) % messages.size();
    next->clear();
    return next;
  }

  vector<string> All() {
    vector<string> out;
    out.insert(out.end(), messages.begin() + current, messages.end());
    out.insert(out.end(), messages.begin(), messages.begin() + current);
    out.erase(std::remove_if(out.begin(), out.end(), [&](const auto& msg) { return msg.empty(); }),
              out.end());
    return out;
  }

  int current = 0;
  vector<string> messages;
};

// Temporary debug measures. Trace what happens with list keys on given shard.
// Used to recover logs for BLPOP failures. See OpBPop.
thread_local CircularMessages debugMessages{50};

class BPopPusher {
 public:
  BPopPusher(string_view pop_key, string_view push_key, ListDir popdir, ListDir pushdir);

  // Returns WRONG_TYPE, OK.
  // If OK is returned then use result() to fetch the value.
  OpResult<string> Run(Transaction* t, unsigned limit_ms);

 private:
  OpResult<string> RunSingle(Transaction* t, time_point tp);
  OpResult<string> RunPair(Transaction* t, time_point tp);

  string_view pop_key_, push_key_;
  ListDir popdir_, pushdir_;
};

// Called as a callback from MKBlocking after we've determined which key to pop.
std::string OpBPop(Transaction* t, EngineShard* shard, std::string_view key, ListDir dir) {
  DVLOG(2) << "popping from " << key << " " << t->DebugId();

  auto& db_slice = shard->db_slice();
  auto it_res = db_slice.Find(t->GetDbContext(), key, OBJ_LIST);

  if (!it_res) {
    auto messages = debugMessages.All();
    stringstream out;
    out << "CRASH REPORT" << endl;
    out << "key: " << key << " tx: " << t->DebugId() << "\n";
    out << "===\n";
    for (auto msg : messages)
      out << msg << "\n";
    out << "===" << endl;
    LOG(ERROR) << out.str();
    LOG(FATAL)
        << "Encountered critical error. Please open a github issue or message us on discord with "
           "attached report. https://github.com/dragonflydb/dragonfly";
  }

  CHECK(it_res) << t->DebugId() << " " << key;  // must exist and must be ok.

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);

  absl::StrAppend(debugMessages.Next(), "OpBPop: ", key, " by ", t->DebugId());

  db_slice.PreUpdate(t->GetDbIndex(), it);
  std::string value = ListPop(dir, ql);
  db_slice.PostUpdate(t->GetDbIndex(), it, key);

  if (quicklistCount(ql) == 0) {
    DVLOG(1) << "deleting key " << key << " " << t->DebugId();
    absl::StrAppend(debugMessages.Next(), "OpBPop Del: ", key, " by ", t->DebugId());

    CHECK(shard->db_slice().Del(t->GetDbIndex(), it));
  }

  if (OpArgs op_args = t->GetOpArgs(shard); op_args.shard->journal()) {
    string command = dir == ListDir::LEFT ? "LPOP" : "RPOP";
    RecordJournal(op_args, command, ArgSlice{key}, 1);
  }

  return value;
}

OpResult<string> OpMoveSingleShard(const OpArgs& op_args, string_view src, string_view dest,
                                   ListDir src_dir, ListDir dest_dir) {
  auto& db_slice = op_args.shard->db_slice();
  auto src_res = db_slice.Find(op_args.db_cntx, src, OBJ_LIST);
  if (!src_res)
    return src_res.status();

  PrimeIterator src_it = *src_res;
  quicklist* src_ql = GetQL(src_it->second);

  if (src == dest) {  // simple case.
    db_slice.PreUpdate(op_args.db_cntx.db_index, src_it);
    string val = ListPop(src_dir, src_ql);

    int pos = (dest_dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
    quicklistPush(src_ql, val.data(), val.size(), pos);
    db_slice.PostUpdate(op_args.db_cntx.db_index, src_it, src);

    return val;
  }

  quicklist* dest_ql = nullptr;
  PrimeIterator dest_it;
  bool new_key = false;
  try {
    tie(dest_it, new_key) = db_slice.AddOrFind(op_args.db_cntx, dest);
  } catch (bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }

  if (new_key) {
    robj* obj = createQuicklistObject();
    dest_ql = (quicklist*)obj->ptr;
    quicklistSetOptions(dest_ql, GetFlag(FLAGS_list_max_listpack_size),
                        GetFlag(FLAGS_list_compress_depth));
    dest_it->second.ImportRObj(obj);

    // Insertion of dest could invalidate src_it. Find it again.
    src_it = db_slice.GetTables(op_args.db_cntx.db_index).first->Find(src);
  } else {
    if (dest_it->second.ObjType() != OBJ_LIST)
      return OpStatus::WRONG_TYPE;

    dest_ql = GetQL(dest_it->second);
    db_slice.PreUpdate(op_args.db_cntx.db_index, dest_it);
  }

  db_slice.PreUpdate(op_args.db_cntx.db_index, src_it);

  string val = ListPop(src_dir, src_ql);
  int pos = (dest_dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;
  quicklistPush(dest_ql, val.data(), val.size(), pos);

  db_slice.PostUpdate(op_args.db_cntx.db_index, src_it, src);
  db_slice.PostUpdate(op_args.db_cntx.db_index, dest_it, dest, !new_key);

  if (quicklistCount(src_ql) == 0) {
    CHECK(db_slice.Del(op_args.db_cntx.db_index, src_it));
  }

  return val;
}

// Read-only peek operation that determines whether the list exists and optionally
// returns the first from left/right value without popping it from the list.
OpResult<string> Peek(const OpArgs& op_args, string_view key, ListDir dir, bool fetch) {
  auto it_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res) {
    return it_res.status();
  }

  if (!fetch)
    return OpStatus::OK;

  quicklist* ql = GetQL(it_res.value()->second);
  quicklistEntry entry = container_utils::QLEntry();
  quicklistIter* iter = (dir == ListDir::LEFT) ? quicklistGetIterator(ql, AL_START_HEAD)
                                               : quicklistGetIterator(ql, AL_START_TAIL);
  CHECK(quicklistNext(iter, &entry));
  quicklistReleaseIterator(iter);

  if (entry.value)
    return string(reinterpret_cast<char*>(entry.value), entry.sz);
  else
    return absl::StrCat(entry.longval);
}

OpResult<uint32_t> OpPush(const OpArgs& op_args, std::string_view key, ListDir dir,
                          bool skip_notexist, ArgSlice vals, bool journal_rewrite) {
  EngineShard* es = op_args.shard;
  PrimeIterator it;
  bool new_key = false;

  if (skip_notexist) {
    auto it_res = es->db_slice().Find(op_args.db_cntx, key, OBJ_LIST);
    if (!it_res)
      return 0;  // Redis returns 0 for nonexisting keys for the *PUSHX actions.
    it = *it_res;
  } else {
    try {
      tie(it, new_key) = es->db_slice().AddOrFind(op_args.db_cntx, key);
    } catch (bad_alloc&) {
      return OpStatus::OUT_OF_MEMORY;
    }
  }

  quicklist* ql = nullptr;
  DVLOG(1) << "OpPush " << key << " new_key " << new_key;

  if (new_key) {
    robj* o = createQuicklistObject();
    ql = (quicklist*)o->ptr;
    quicklistSetOptions(ql, GetFlag(FLAGS_list_max_listpack_size),
                        GetFlag(FLAGS_list_compress_depth));
    it->second.ImportRObj(o);
  } else {
    if (it->second.ObjType() != OBJ_LIST)
      return OpStatus::WRONG_TYPE;
    es->db_slice().PreUpdate(op_args.db_cntx.db_index, it);
    ql = GetQL(it->second);
  }

  // Left push is LIST_HEAD.
  int pos = (dir == ListDir::LEFT) ? QUICKLIST_HEAD : QUICKLIST_TAIL;

  for (auto v : vals) {
    es->tmp_str1 = sdscpylen(es->tmp_str1, v.data(), v.size());
    quicklistPush(ql, es->tmp_str1, sdslen(es->tmp_str1), pos);
  }

  if (new_key) {
    if (es->blocking_controller()) {
      string tmp;
      string_view key = it->first.GetSlice(&tmp);

      bool res = es->blocking_controller()->AwakeWatched(op_args.db_cntx.db_index, key);
      absl::StrAppend(debugMessages.Next(), "OpPush AwakeWatched: ", key, " by ",
                      op_args.tx->DebugId(), " res: ", res);
    }
  } else {
    es->db_slice().PostUpdate(op_args.db_cntx.db_index, it, key, true);
  }

  if (journal_rewrite && op_args.shard->journal()) {
    string command = dir == ListDir::LEFT ? "LPUSH" : "RPUSH";
    vector<string_view> mapped(vals.size() + 1);
    mapped[0] = key;
    std::copy(vals.begin(), vals.end(), mapped.begin() + 1);
    RecordJournal(op_args, command, mapped, 2);
  }

  return quicklistCount(ql);
}

OpResult<StringVec> OpPop(const OpArgs& op_args, string_view key, ListDir dir, uint32_t count,
                          bool return_results, bool journal_rewrite) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> it_res = db_slice.Find(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);
  db_slice.PreUpdate(op_args.db_cntx.db_index, it);

  StringVec res;
  if (quicklistCount(ql) < count) {
    count = quicklistCount(ql);
  }
  res.reserve(count);

  if (return_results) {
    for (unsigned i = 0; i < count; ++i) {
      res.push_back(ListPop(dir, ql));
    }
  } else {
    for (unsigned i = 0; i < count; ++i) {
      ListPop(dir, ql);
    }
  }

  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);

  if (quicklistCount(ql) == 0) {
    absl::StrAppend(debugMessages.Next(), "OpPop Del: ", key, " by ", op_args.tx->DebugId());
    CHECK(db_slice.Del(op_args.db_cntx.db_index, it));
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
    DCHECK_EQ(1u, args.size());
    bool is_dest = args.front() == dest;
    find_res[is_dest] = Peek(t->GetOpArgs(shard), args.front(), src_dir, !is_dest);
    return OpStatus::OK;
  };

  trans->Execute(move(cb), false);

  if (!find_res[0] || find_res[1].status() == OpStatus::WRONG_TYPE) {
    result = find_res[0] ? find_res[1] : find_res[0];
    if (conclude_on_error) {
      auto cb = [&](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
      trans->Execute(move(cb), true);
    }
  } else {
    // Everything is ok, lets proceed with the mutations.
    auto cb = [&](Transaction* t, EngineShard* shard) {
      auto args = t->GetShardArgs(shard->shard_id());
      auto key = args.front();
      bool is_dest = (key == dest);
      OpArgs op_args = t->GetOpArgs(shard);

      if (is_dest) {
        string_view val{find_res[0].value()};
        DVLOG(1) << "Pushing value: " << val << " to list: " << dest;

        ArgSlice span{&val, 1};
        OpPush(op_args, key, dest_dir, false, span, true);

        // blocking_controller does not have to be set with non-blocking transactions.
        if (shard->blocking_controller()) {
          // hack, again. since we hacked which queue we are waiting on (see RunPair)
          // we must clean-up src key here manually. See RunPair why we do this.
          // in short- we suspended on "src" on both shards.
          shard->blocking_controller()->FinalizeWatched(ArgSlice{&src, 1}, t);
        }
      } else {
        DVLOG(1) << "Popping value from list: " << key;
        OpPop(op_args, key, src_dir, 1, false, true);
      }

      return OpStatus::OK;
    };
    trans->Execute(move(cb), true);
    result = std::move(find_res[0].value());
  }

  return result;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, std::string_view key) {
  auto res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_LIST);
  if (!res)
    return res.status();

  quicklist* ql = GetQL(res.value()->second);

  return quicklistCount(ql);
}

OpResult<string> OpIndex(const OpArgs& op_args, std::string_view key, long index) {
  auto res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_LIST);
  if (!res)
    return res.status();
  quicklist* ql = GetQL(res.value()->second);
  quicklistEntry entry = container_utils::QLEntry();
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

OpResult<vector<uint32_t>> OpPos(const OpArgs& op_args, std::string_view key,
                                 std::string_view element, int rank, int count, int max_len) {
  OpResult<PrimeIterator> it_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res.ok())
    return it_res.status();

  int direction = AL_START_HEAD;
  if (rank < 0) {
    rank = -rank;
    direction = AL_START_TAIL;
  }

  quicklist* ql = GetQL(it_res.value()->second);
  quicklistIter* ql_iter = quicklistGetIterator(ql, direction);
  quicklistEntry entry;

  int index = 0;
  int matched = 0;
  vector<uint32_t> matches;
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
        if (count && matched - rank + 1 >= count) {
          break;
        }
      }
    }
    index++;
  }
  quicklistReleaseIterator(ql_iter);
  return matches;
}

OpResult<int> OpInsert(const OpArgs& op_args, string_view key, string_view pivot, string_view elem,
                       int insert_param) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  quicklist* ql = GetQL(it_res.value()->second);
  quicklistEntry entry = container_utils::QLEntry();
  quicklistIter* qiter = quicklistGetIterator(ql, AL_START_HEAD);
  bool found = false;

  while (quicklistNext(qiter, &entry)) {
    if (ElemCompare(entry, pivot)) {
      found = true;
      break;
    }
  }

  int res = -1;
  if (found) {
    db_slice.PreUpdate(op_args.db_cntx.db_index, *it_res);
    if (insert_param == LIST_TAIL) {
      quicklistInsertAfter(qiter, &entry, elem.data(), elem.size());
    } else {
      DCHECK_EQ(LIST_HEAD, insert_param);
      quicklistInsertBefore(qiter, &entry, elem.data(), elem.size());
    }
    db_slice.PostUpdate(op_args.db_cntx.db_index, *it_res, key);
    res = quicklistCount(ql);
  }
  quicklistReleaseIterator(qiter);
  return res;
}

OpResult<uint32_t> OpRem(const OpArgs& op_args, string_view key, string_view elem, long count) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);

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

  db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  while (quicklistNext(qiter, &entry)) {
    if (quicklistCompare(&entry, elem_ptr, elem.size())) {
      quicklistDelEntry(qiter, &entry);
      removed++;
      if (count && removed == count)
        break;
    }
  }
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);

  quicklistReleaseIterator(qiter);

  if (quicklistCount(ql) == 0) {
    CHECK(db_slice.Del(op_args.db_cntx.db_index, it));
  }

  return removed;
}

OpStatus OpSet(const OpArgs& op_args, string_view key, string_view elem, long index) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);

  db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  int replaced = quicklistReplaceAtIndex(ql, index, elem.data(), elem.size());
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);

  if (!replaced) {
    return OpStatus::OUT_OF_RANGE;
  }
  return OpStatus::OK;
}

OpStatus OpTrim(const OpArgs& op_args, string_view key, long start, long end) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_LIST);
  if (!it_res)
    return it_res.status();

  PrimeIterator it = *it_res;
  quicklist* ql = GetQL(it->second);
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

  db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  quicklistDelRange(ql, 0, ltrim);
  quicklistDelRange(ql, -rtrim, rtrim);
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);

  if (quicklistCount(ql) == 0) {
    CHECK(db_slice.Del(op_args.db_cntx.db_index, it));
  }
  return OpStatus::OK;
}

OpResult<StringVec> OpRange(const OpArgs& op_args, std::string_view key, long start, long end) {
  auto res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_LIST);
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

  StringVec str_vec;
  container_utils::IterateList(
      res.value()->second,
      [&str_vec](container_utils::ContainerEntry ce) {
        str_vec.emplace_back(ce.ToString());
        return true;
      },
      start, end);

  return str_vec;
}

void MoveGeneric(ConnectionContext* cntx, string_view src, string_view dest, ListDir src_dir,
                 ListDir dest_dir) {
  OpResult<string> result;

  if (cntx->transaction->GetUniqueShardCnt() == 1) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      auto ec = OpMoveSingleShard(t->GetOpArgs(shard), src, dest, src_dir, dest_dir);
      // On single shard we can use the auto journal flow.
      t->RenableAutoJournal();
      return ec;
    };

    result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  } else {
    cntx->transaction->Schedule();
    result = MoveTwoShards(cntx->transaction, src, dest, src_dir, dest_dir, true);
  }

  if (result) {
    return (*cntx)->SendBulkString(*result);
  }

  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      (*cntx)->SendNull();
      break;

    default:
      (*cntx)->SendError(result.status());
      break;
  }
}

void RPopLPush(CmdArgList args, ConnectionContext* cntx) {
  string_view src = ArgS(args, 0);
  string_view dest = ArgS(args, 1);

  MoveGeneric(cntx, src, dest, ListDir::RIGHT, ListDir::LEFT);
}

void BRPopLPush(CmdArgList args, ConnectionContext* cntx) {
  string_view src = ArgS(args, 0);
  string_view dest = ArgS(args, 1);
  string_view timeout_str = ArgS(args, 2);

  float timeout;
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return (*cntx)->SendError("timeout is not a float or out of range");
  }

  if (timeout < 0) {
    return (*cntx)->SendError("timeout is negative");
  }

  BPopPusher bpop_pusher(src, dest, ListDir::RIGHT, ListDir::LEFT);
  OpResult<string> op_res = bpop_pusher.Run(cntx->transaction, unsigned(timeout * 1000));

  if (op_res) {
    return (*cntx)->SendBulkString(*op_res);
  }

  switch (op_res.status()) {
    case OpStatus::TIMED_OUT:
      return (*cntx)->SendNull();
      break;

    default:
      return (*cntx)->SendError(op_res.status());
      break;
  }
}

void BLMove(CmdArgList args, ConnectionContext* cntx) {
  string_view src = ArgS(args, 0);
  string_view dest = ArgS(args, 1);
  string_view timeout_str = ArgS(args, 4);

  float timeout;
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return (*cntx)->SendError("timeout is not a float or out of range");
  }

  if (timeout < 0) {
    return (*cntx)->SendError("timeout is negative");
  }

  ToUpper(&args[2]);
  ToUpper(&args[3]);

  optional<ListDir> src_dir = ParseDir(ArgS(args, 2));
  optional<ListDir> dest_dir = ParseDir(ArgS(args, 3));
  if (!src_dir || !dest_dir) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  BPopPusher bpop_pusher(src, dest, *src_dir, *dest_dir);
  OpResult<string> op_res = bpop_pusher.Run(cntx->transaction, unsigned(timeout * 1000));

  if (op_res) {
    return (*cntx)->SendBulkString(*op_res);
  }

  switch (op_res.status()) {
    case OpStatus::TIMED_OUT:
      return (*cntx)->SendNull();
      break;

    default:
      return (*cntx)->SendError(op_res.status());
      break;
  }
}

BPopPusher::BPopPusher(string_view pop_key, string_view push_key, ListDir popdir, ListDir pushdir)
    : pop_key_(pop_key), push_key_(push_key), popdir_(popdir), pushdir_(pushdir) {
}

OpResult<string> BPopPusher::Run(Transaction* t, unsigned limit_ms) {
  time_point tp =
      limit_ms ? chrono::steady_clock::now() + chrono::milliseconds(limit_ms) : time_point::max();

  t->Schedule();

  if (t->GetUniqueShardCnt() == 1) {
    return RunSingle(t, tp);
  }

  return RunPair(t, tp);
}

OpResult<string> BPopPusher::RunSingle(Transaction* t, time_point tp) {
  OpResult<string> op_res;
  bool is_multi = t->IsMulti();
  auto cb_move = [&](Transaction* t, EngineShard* shard) {
    OpArgs op_args = t->GetOpArgs(shard);
    op_res = OpMoveSingleShard(op_args, pop_key_, push_key_, popdir_, pushdir_);
    if (op_res && op_args.shard->journal()) {
      std::array<string_view, 4> arr = {pop_key_, push_key_, DirToSv(popdir_), DirToSv(pushdir_)};
      RecordJournal(op_args, "LMOVE", arr, 1);
    }
    return OpStatus::OK;
  };
  t->Execute(cb_move, false);

  if (is_multi || op_res.status() != OpStatus::KEY_NOTFOUND) {
    if (op_res.status() == OpStatus::KEY_NOTFOUND) {
      op_res = OpStatus::TIMED_OUT;
    }
    auto cb = [](Transaction* t, EngineShard* shard) { return OpStatus::OK; };
    t->Execute(std::move(cb), true);
    return op_res;
  }

  auto wcb = [&](Transaction* t, EngineShard* shard) { return ArgSlice{&this->pop_key_, 1}; };

  // Block
  bool wait_succeeded = t->WaitOnWatch(tp, std::move(wcb));
  if (!wait_succeeded)
    return OpStatus::TIMED_OUT;

  t->Execute(cb_move, true);
  return op_res;
}

OpResult<string> BPopPusher::RunPair(Transaction* t, time_point tp) {
  bool is_multi = t->IsMulti();
  OpResult<string> op_res = MoveTwoShards(t, pop_key_, push_key_, popdir_, pushdir_, false);

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
  auto wcb = [&](Transaction* t, EngineShard* shard) { return ArgSlice{&this->pop_key_, 1}; };

  bool wait_succeeded = t->WaitOnWatch(tp, std::move(wcb));
  if (!wait_succeeded)
    return OpStatus::TIMED_OUT;

  return MoveTwoShards(t, pop_key_, push_key_, popdir_, pushdir_, true);
}

}  // namespace

void _DebugAppend(string_view msg) {
  debugMessages.Next()->assign(msg);
}

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
  auto key = ArgS(args, 0);
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    (*cntx)->SendLong(0);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void ListFamily::LPos(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view elem = ArgS(args, 1);

  int rank = 1;
  uint32_t count = 1;
  uint32_t max_len = 0;
  bool skip_count = true;

  for (size_t i = 2; i < args.size(); i++) {
    ToUpper(&args[i]);
    const auto& arg_v = ArgS(args, i);
    if (arg_v == "RANK") {
      if (!absl::SimpleAtoi(ArgS(args, (i + 1)), &rank) || rank == 0) {
        return (*cntx)->SendError(kInvalidIntErr);
      }
    }
    if (arg_v == "COUNT") {
      if (!absl::SimpleAtoi(ArgS(args, (i + 1)), &count)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }
      skip_count = false;
    }
    if (arg_v == "MAXLEN") {
      if (!absl::SimpleAtoi(ArgS(args, (i + 1)), &max_len)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPos(t->GetOpArgs(shard), key, elem, rank, count, max_len);
  };

  Transaction* trans = cntx->transaction;
  OpResult<vector<uint32_t>> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::WRONG_TYPE) {
    return (*cntx)->SendError(result.status());
  } else if (result.status() == OpStatus::INVALID_VALUE) {
    return (*cntx)->SendError(result.status());
  }

  if (skip_count) {
    if (result->empty()) {
      (*cntx)->SendNull();
    } else {
      (*cntx)->SendLong((*result)[0]);
    }
  } else {
    SinkReplyBuilder::ReplyAggregator agg(cntx->reply_builder());
    (*cntx)->StartArray(result->size());
    const auto& array = result.value();
    for (const auto& v : array) {
      (*cntx)->SendLong(v);
    }
  }
}

void ListFamily::LIndex(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view index_str = ArgS(args, 1);
  int32_t index;
  if (!absl::SimpleAtoi(index_str, &index)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIndex(t->GetOpArgs(shard), key, index);
  };

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendBulkString(result.value());
  } else if (result.status() == OpStatus::WRONG_TYPE) {
    (*cntx)->SendError(result.status());
  } else {
    (*cntx)->SendNull();
  }
}

/* LINSERT <key> (BEFORE|AFTER) <pivot> <element> */
void ListFamily::LInsert(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view param = ArgS(args, 1);
  string_view pivot = ArgS(args, 2);
  string_view elem = ArgS(args, 3);
  int where;

  ToUpper(&args[1]);
  if (param == "AFTER") {
    where = LIST_TAIL;
  } else if (param == "BEFORE") {
    where = LIST_HEAD;
  } else {
    return (*cntx)->SendError(kSyntaxErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpInsert(t->GetOpArgs(shard), key, pivot, elem, where);
  };

  OpResult<int> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    return (*cntx)->SendLong(result.value());
  }

  (*cntx)->SendError(result.status());
}

void ListFamily::LTrim(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view s_str = ArgS(args, 1);
  string_view e_str = ArgS(args, 2);
  int32_t start, end;

  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpTrim(t->GetOpArgs(shard), key, start, end);
  };
  cntx->transaction->ScheduleSingleHop(std::move(cb));
  (*cntx)->SendOk();
}

void ListFamily::LRange(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view s_str = ArgS(args, 1);
  std::string_view e_str = ArgS(args, 2);
  int32_t start, end;

  if (!absl::SimpleAtoi(s_str, &start) || !absl::SimpleAtoi(e_str, &end)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(t->GetOpArgs(shard), key, start, end);
  };

  auto res = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (!res && res.status() != OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendError(res.status());
  }

  (*cntx)->SendStringArr(*res);
}

// lrem key 5 foo, will remove foo elements from the list if exists at most 5 times.
void ListFamily::LRem(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view index_str = ArgS(args, 1);
  std::string_view elem = ArgS(args, 2);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRem(t->GetOpArgs(shard), key, elem, count);
  };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
  } else {
    (*cntx)->SendLong(0);
  }
}

void ListFamily::LSet(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view index_str = ArgS(args, 1);
  std::string_view elem = ArgS(args, 2);
  int32_t count;

  if (!absl::SimpleAtoi(index_str, &count)) {
    (*cntx)->SendError(kInvalidIntErr);
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, elem, count);
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

void ListFamily::LMove(CmdArgList args, ConnectionContext* cntx) {
  std::string_view src = ArgS(args, 0);
  std::string_view dest = ArgS(args, 1);
  std::string_view src_dir_str = ArgS(args, 2);
  std::string_view dest_dir_str = ArgS(args, 3);

  ToUpper(&args[2]);
  ToUpper(&args[3]);

  optional<ListDir> src_dir = ParseDir(src_dir_str);
  optional<ListDir> dest_dir = ParseDir(dest_dir_str);
  if (!src_dir || !dest_dir) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  MoveGeneric(cntx, src, dest, *src_dir, *dest_dir);
}

void ListFamily::BPopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx) {
  DCHECK_GE(args.size(), 2u);

  float timeout;
  auto timeout_str = ArgS(args, args.size() - 1);
  if (!absl::SimpleAtof(timeout_str, &timeout)) {
    return (*cntx)->SendError("timeout is not a float or out of range");
  }
  if (timeout < 0) {
    return (*cntx)->SendError("timeout is negative");
  }
  VLOG(1) << "BPop timeout(" << timeout << ")";

  Transaction* transaction = cntx->transaction;

  std::string popped_value;
  auto cb = [dir, &popped_value](Transaction* t, EngineShard* shard, std::string_view key) {
    popped_value = OpBPop(t, shard, key, dir);
  };

  cntx->conn_state.is_blocking = true;
  OpResult<string> popped_key = container_utils::RunCbOnFirstNonEmptyBlocking(
      transaction, OBJ_LIST, move(cb), unsigned(timeout * 1000));
  cntx->conn_state.is_blocking = false;
  if (popped_key) {
    DVLOG(1) << "BPop " << transaction->DebugId() << " popped from key " << popped_key;  // key.
    std::string_view str_arr[2] = {*popped_key, popped_value};
    return (*cntx)->SendStringArr(str_arr);
  }

  DVLOG(1) << "result for " << transaction->DebugId() << " is " << popped_key.status();

  switch (popped_key.status()) {
    case OpStatus::WRONG_TYPE:
      return (*cntx)->SendError(kWrongTypeErr);
    case OpStatus::TIMED_OUT:
      return (*cntx)->SendNullArray();
    default:
      LOG(ERROR) << "Unexpected error " << popped_key.status();
  }
  return (*cntx)->SendNullArray();
}

void ListFamily::PushGeneric(ListDir dir, bool skip_notexists, CmdArgList args,
                             ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  vector<std::string_view> vals(args.size() - 1);
  for (size_t i = 1; i < args.size(); ++i) {
    vals[i - 1] = ArgS(args, i);
  }
  absl::Span<std::string_view> span{vals.data(), vals.size()};
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPush(t->GetOpArgs(shard), key, dir, skip_notexists, span, false);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    return (*cntx)->SendLong(result.value());
  }

  return (*cntx)->SendError(result.status());
}

void ListFamily::PopGeneric(ListDir dir, CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  int32_t count = 1;
  bool return_arr = false;

  if (args.size() > 1) {
    if (args.size() > 2) {
      return (*cntx)->SendError(WrongNumArgsError(cntx->cid->name()));
    }

    string_view count_s = ArgS(args, 1);
    if (!absl::SimpleAtoi(count_s, &count)) {
      return (*cntx)->SendError(kInvalidIntErr);
    }

    if (count < 0) {
      return (*cntx)->SendError(kUintErr);
    }
    return_arr = true;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPop(t->GetOpArgs(shard), key, dir, count, true, false);
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

using CI = CommandId;

#define HFUNC(x) SetHandler(&ListFamily::x)

void ListFamily::Register(CommandRegistry* registry) {
  *registry
      << CI{"LPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(LPush)
      << CI{"LPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(LPushX)
      << CI{"LPOP", CO::WRITE | CO::FAST | CO::DENYOOM, -2, 1, 1, 1}.HFUNC(LPop)
      << CI{"RPUSH", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(RPush)
      << CI{"RPUSHX", CO::WRITE | CO::FAST | CO::DENYOOM, -3, 1, 1, 1}.HFUNC(RPushX)
      << CI{"RPOP", CO::WRITE | CO::FAST | CO::DENYOOM, -2, 1, 1, 1}.HFUNC(RPop)
      << CI{"RPOPLPUSH", CO::WRITE | CO::FAST | CO::DENYOOM | CO::NO_AUTOJOURNAL, 3, 1, 2, 1}
             .SetHandler(RPopLPush)
      << CI{"BRPOPLPUSH", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, 4, 1, 2, 1}
             .SetHandler(BRPopLPush)
      << CI{"BLPOP", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2, 1}
             .HFUNC(BLPop)
      << CI{"BRPOP", CO::WRITE | CO::NOSCRIPT | CO::BLOCKING | CO::NO_AUTOJOURNAL, -3, 1, -2, 1}
             .HFUNC(BRPop)
      << CI{"LLEN", CO::READONLY | CO::FAST, 2, 1, 1, 1}.HFUNC(LLen)
      << CI{"LPOS", CO::READONLY | CO::FAST, -3, 1, 1, 1}.HFUNC(LPos)
      << CI{"LINDEX", CO::READONLY, 3, 1, 1, 1}.HFUNC(LIndex)
      << CI{"LINSERT", CO::WRITE, 5, 1, 1, 1}.HFUNC(LInsert)
      << CI{"LRANGE", CO::READONLY, 4, 1, 1, 1}.HFUNC(LRange)
      << CI{"LSET", CO::WRITE | CO::DENYOOM, 4, 1, 1, 1}.HFUNC(LSet)
      << CI{"LTRIM", CO::WRITE, 4, 1, 1, 1}.HFUNC(LTrim)
      << CI{"LREM", CO::WRITE, 4, 1, 1, 1}.HFUNC(LRem)
      << CI{"LMOVE", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL, 5, 1, 2, 1}.HFUNC(LMove)
      << CI{"BLMOVE", CO::WRITE | CO::DENYOOM | CO::NO_AUTOJOURNAL | CO::BLOCKING, 6, 1, 2, 1}
             .SetHandler(BLMove);
}

}  // namespace dfly
