// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/redis_aux.h"
}

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

using namespace std;

namespace dfly {

namespace {

constexpr size_t kMaxListPackLen = 1024;

bool IsGoodForListpack(CmdArgList args, const uint8_t* lp) {
  size_t sum = 0;
  for (auto s : args) {
    if (s.size() > server.hash_max_listpack_value)
      return false;
    sum += s.size();
  }

  return lpBytes(const_cast<uint8_t*>(lp)) + sum < kMaxListPackLen;
}

string LpGetVal(uint8_t* lp_it) {
  int64_t ele_len;
  uint8_t* ptr = lpGet(lp_it, &ele_len, NULL);
  if (ptr) {
    return string(reinterpret_cast<char*>(ptr), ele_len);
  }

  return absl::StrCat(ele_len);
}

// returns a new pointer to lp. Returns true if field was inserted or false it it already existed.
pair<uint8_t*, bool> lpInsertElem(uint8_t* lp, string_view field, string_view val) {
  uint8_t* vptr;

  uint8_t* fptr = lpFirst(lp);
  uint8_t* fsrc = (uint8_t*)field.data();
  uint8_t* vsrc = (uint8_t*)val.data();

  bool updated = false;

  if (fptr) {
    fptr = lpFind(lp, fptr, fsrc, field.size(), 1);
    if (fptr) {
      /* Grab pointer to the value (fptr points to the field) */
      vptr = lpNext(lp, fptr);
      updated = true;

      /* Replace value */
      lp = lpReplace(lp, &vptr, vsrc, val.size());
    }
  }

  if (!updated) {
    /* Push new field/value pair onto the tail of the listpack */
    // TODO: we should at least allocate once for both elements.
    lp = lpAppend(lp, fsrc, field.size());
    lp = lpAppend(lp, vsrc, val.size());
  }

  return make_pair(lp, !updated);
}

}  // namespace

void HSetFamily::HDel(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  args.remove_prefix(2);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(OpArgs{shard, t->db_index()}, key, args);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpLen(OpArgs{shard, t->db_index()}, key);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HExists(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view field = ArgS(args, 2);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<int> {
    auto& db_slice = shard->db_slice();
    auto it_res = db_slice.Find(t->db_index(), key, OBJ_HASH);

    if (it_res) {
      robj* hset = (*it_res)->second.AsRObj();
      shard->tmp_str1 = sdscpylen(shard->tmp_str1, field.data(), field.size());

      return hashTypeExists(hset, shard->tmp_str1);
    }
    if (it_res.status() == OpStatus::KEY_NOTFOUND)
      return 0;
    return it_res.status();
  };

  OpResult<int> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HMGet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  args.remove_prefix(2);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpMGet(OpArgs{shard, t->db_index()}, key, args);
  };

  OpResult<vector<OptStr>> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    (*cntx)->StartArray(result->size());
    for (const auto& val : *result) {
      if (val) {
        (*cntx)->SendBulkString(*val);
      } else {
        (*cntx)->SendNull();
      }
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HGet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view field = ArgS(args, 2);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGet(OpArgs{shard, t->db_index()}, key, field);
  };

  OpResult<string> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendBulkString(*result);
  } else {
    if (result.status() == OpStatus::KEY_NOTFOUND) {
      (*cntx)->SendNull();
    } else {
      (*cntx)->SendError(result.status());
    }
  }
}

void HSetFamily::HIncrBy(CmdArgList args, ConnectionContext* cntx) {
  LOG(DFATAL) << "TBD";
}

void HSetFamily::HKeys(CmdArgList args, ConnectionContext* cntx) {
  HGetGeneric(args, cntx, FIELDS);
}

void HSetFamily::HVals(CmdArgList args, ConnectionContext* cntx) {
  HGetGeneric(args, cntx, VALUES);
}

void HSetFamily::HGetAll(CmdArgList args, ConnectionContext* cntx) {
  HGetGeneric(args, cntx, GetAllMode::FIELDS | GetAllMode::VALUES);
}

void HSetFamily::HGetGeneric(CmdArgList args, ConnectionContext* cntx, uint8_t getall_mask) {
  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGetAll(OpArgs{shard, t->db_index()}, key, getall_mask);
  };

  OpResult<vector<string>> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    (*cntx)->StartArray(result->size());
    for (const auto& s : *result) {
      (*cntx)->SendBulkString(s);
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HSet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  args.remove_prefix(2);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(OpArgs{shard, t->db_index()}, key, args, false);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HMSet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  args.remove_prefix(2);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(OpArgs{shard, t->db_index()}, key, args, false);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  // Returns "OK", that's all the difference from HSet.
  (*cntx)->SendError(result.status());
}

void HSetFamily::HSetNx(CmdArgList args, ConnectionContext* cntx) {
  LOG(DFATAL) << "TBD";
}

void HSetFamily::HStrLen(CmdArgList args, ConnectionContext* cntx) {
  LOG(DFATAL) << "TBD";
}

OpResult<uint32_t> HSetFamily::OpSet(const OpArgs& op_args, string_view key, CmdArgList values,
                                     bool skip_if_exists) {
  DCHECK(!values.empty() && 0 == values.size() % 2);

  auto& db_slice = op_args.shard->db_slice();
  const auto [it, inserted] = db_slice.AddOrFind(op_args.db_ind, key);

  DbSlice::InternalDbStats* stats = db_slice.MutableStats(op_args.db_ind);

  if (inserted) {
    robj* ro = createHashObject();
    it->second.ImportRObj(ro);
    stats->listpack_blob_cnt++;
    stats->listpack_bytes += lpBytes((uint8_t*)ro->ptr);
  } else {
    if (it->second.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;
  }

  robj* hset = it->second.AsRObj();
  uint8_t* lp = (uint8_t*)hset->ptr;

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    stats->listpack_bytes -= lpBytes(lp);

    if (!IsGoodForListpack(values, lp)) {
      stats->listpack_blob_cnt--;
      hashTypeConvert(hset, OBJ_ENCODING_HT);
    }
  }

  unsigned created = 0;

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    bool inserted;
    for (size_t i = 0; i < values.size(); i += 2) {
      tie(lp, inserted) = lpInsertElem(lp, ArgS(values, i), ArgS(values, i + 1));
      created += inserted;
    }
    hset->ptr = lp;
    stats->listpack_bytes += lpBytes(lp);
  } else {
    DCHECK_EQ(OBJ_ENCODING_HT, hset->encoding);

    // Dictionary
    for (size_t i = 0; i < values.size(); i += 2) {
      sds fs = sdsnewlen(values[i].data(), values[i].size());
      sds vs = sdsnewlen(values[i + 1].data(), values[i + 1].size());

      // hashTypeSet checks for hash_max_listpack_entries and converts into dictionary
      // if it goes beyond.
      created += !hashTypeSet(hset, fs, vs, HASH_SET_TAKE_FIELD | HASH_SET_TAKE_VALUE);
    }
  }
  it->second.SyncRObj();

  return created;
}

OpResult<uint32_t> HSetFamily::OpDel(const OpArgs& op_args, string_view key, CmdArgList values) {
  DCHECK(!values.empty());

  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_HASH);

  if (!it_res)
    return it_res.status();

  CompactObj& co = (*it_res)->second;
  robj* hset = co.AsRObj();
  unsigned deleted = 0;
  bool key_remove = false;
  DbSlice::InternalDbStats* stats = db_slice.MutableStats(op_args.db_ind);

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    stats->listpack_bytes -= lpBytes((uint8_t*)hset->ptr);
  }

  for (auto s : values) {
    op_args.shard->tmp_str1 = sdscpylen(op_args.shard->tmp_str1, s.data(), s.size());

    if (hashTypeDelete(hset, op_args.shard->tmp_str1)) {
      ++deleted;
      if (hashTypeLength(hset) == 0) {
        key_remove = true;
        break;
      }
    }
  }

  co.SyncRObj();

  if (key_remove) {
    if (hset->encoding == OBJ_ENCODING_LISTPACK) {
      stats->listpack_blob_cnt--;
    }
    db_slice.Del(op_args.db_ind, *it_res);
  } else if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    stats->listpack_bytes += lpBytes((uint8_t*)hset->ptr);
  }

  return deleted;
}

auto HSetFamily::OpMGet(const OpArgs& op_args, std::string_view key, CmdArgList fields)
    -> OpResult<vector<OptStr>> {
  DCHECK(!fields.empty());

  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_HASH);

  if (!it_res)
    return it_res.status();

  CompactObj& co = (*it_res)->second;
  robj* hset = co.AsRObj();

  std::vector<OptStr> result(fields.size());

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* lp = (uint8_t*)hset->ptr;
    absl::flat_hash_map<string_view, unsigned> reverse;
    reverse.reserve(fields.size() + 1);
    for (size_t i = 0; i < fields.size(); ++i) {
      reverse.emplace(ArgS(fields, i), i);   // map fields to their index.
    }

    char ibuf[32];
    uint8_t* lp_elem = lpFirst(lp);
    int64_t ele_len;
    string_view key;
    DCHECK(lp_elem);  // empty containers are not allowed.

    // We do single pass on listpack for this operation.
    do {
      uint8_t* elem = lpGet(lp_elem, &ele_len, NULL);
      if (elem) {
        key = string_view{reinterpret_cast<char*>(elem), size_t(ele_len)};
      } else {
        char* next = absl::numbers_internal::FastIntToBuffer(ele_len, ibuf);
        key = string_view{ibuf, size_t(next - ibuf)};
      }
      lp_elem = lpNext(lp, lp_elem);  // switch to value
      DCHECK(lp_elem);

      auto it = reverse.find(key);
      if (it != reverse.end()) {
        DCHECK_LT(it->second, result.size());
        result[it->second].emplace(LpGetVal(lp_elem));  // populate found items.
      }

      lp_elem = lpNext(lp, lp_elem);  // switch to the next key
    } while(lp_elem);
  } else {
    DCHECK_EQ(OBJ_ENCODING_HT, hset->encoding);
    dict* d = (dict*)hset->ptr;
    for (size_t i = 0; i < fields.size(); ++i) {
      op_args.shard->tmp_str1 =
          sdscpylen(op_args.shard->tmp_str1, fields[i].data(), fields[i].size());
      dictEntry* de = dictFind(d, op_args.shard->tmp_str1);
      if (de) {
        sds val = (sds)dictGetVal(de);
        result[i]->assign(val, sdslen(val));
      }
    }
  }

  return result;
}

OpResult<uint32_t> HSetFamily::OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_HASH);

  if (it_res) {
    robj* hset = (*it_res)->second.AsRObj();
    return hashTypeLength(hset);
  }
  if (it_res.status() == OpStatus::KEY_NOTFOUND)
    return 0;
  return it_res.status();
}

OpResult<string> HSetFamily::OpGet(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_HASH);
  if (!it_res)
    return it_res.status();

  robj* hset = (*it_res)->second.AsRObj();

  op_args.shard->tmp_str1 = sdscpylen(op_args.shard->tmp_str1, field.data(), field.size());

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    unsigned char* vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    int ret = hashTypeGetFromListpack(hset, op_args.shard->tmp_str1, &vstr, &vlen, &vll);
    if (ret < 0) {
      return OpStatus::KEY_NOTFOUND;
    }
    if (vstr) {
      const char* src = reinterpret_cast<const char*>(vstr);
      return string{src, vlen};
    }

    return absl::StrCat(vll);
  }
  DCHECK_EQ(hset->encoding, OBJ_ENCODING_HT);

  dictEntry* de = dictFind((dict*)hset->ptr, op_args.shard->tmp_str1);
  if (!de)
    return OpStatus::KEY_NOTFOUND;

  sds val = (sds)dictGetVal(de);

  return string(val, sdslen(val));
}

OpResult<vector<string>> HSetFamily::OpGetAll(const OpArgs& op_args, string_view key,
                                              uint8_t mask) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_HASH);
  if (!it_res) {
    if (it_res.status() == OpStatus::KEY_NOTFOUND)
      return vector<string>{};
    return it_res.status();
  }

  robj* hset = (*it_res)->second.AsRObj();
  hashTypeIterator* hi = hashTypeInitIterator(hset);

  vector<string> res;
  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    while (hashTypeNext(hi) != C_ERR) {
      if (mask & FIELDS) {
        res.push_back(LpGetVal(hi->fptr));
      }

      if (mask & VALUES) {
        res.push_back(LpGetVal(hi->vptr));
      }
    }
  } else {
    while (hashTypeNext(hi) != C_ERR) {
      if (mask & FIELDS) {
        sds key = (sds)dictGetKey(hi->de);
        res.emplace_back(key, sdslen(key));
      }

      if (mask & VALUES) {
        sds val = (sds)dictGetVal(hi->de);
        res.emplace_back(val, sdslen(val));
      }
    }
  }

  hashTypeReleaseIterator(hi);

  return res;
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&HSetFamily::x)

void HSetFamily::Register(CommandRegistry* registry) {
  *registry << CI{"HDEL", CO::FAST | CO::WRITE, -3, 1, 1, 1}.HFUNC(HDel)
            << CI{"HLEN", CO::FAST | CO::READONLY, 2, 1, 1, 1}.HFUNC(HLen)
            << CI{"HEXISTS", CO::FAST | CO::READONLY, 3, 1, 1, 1}.HFUNC(HExists)
            << CI{"HGET", CO::FAST | CO::READONLY, 3, 1, 1, 1}.HFUNC(HGet)
            << CI{"HGETALL", CO::FAST | CO::READONLY, 2, 1, 1, 1}.HFUNC(HGetAll)
            << CI{"HMGET", CO::FAST | CO::READONLY, -3, 1, 1, 1}.HFUNC(HMGet)
            << CI{"HINCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(HIncrBy)
            << CI{"HKEYS", CO::READONLY, 2, 1, 1, 1}.HFUNC(HKeys)
            << CI{"HVALS", CO::READONLY, 2, 1, 1, 1}.HFUNC(HVals)
            << CI{"HSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(HSet)
            << CI{"HSETNX", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(HSetNx)
            << CI{"HSTRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(HStrLen);
}

}  // namespace dfly
