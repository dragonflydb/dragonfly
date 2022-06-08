// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
}

#include "base/logging.h"
#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

using namespace std;

namespace dfly {

using namespace facade;

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
// skip_exists controls what happens if the field already existed. If skip_exists = true,
// then val does not override the value and listpack is not changed. Otherwise, the corresponding
// value is overriden by val.
pair<uint8_t*, bool> LpInsert(uint8_t* lp, string_view field, string_view val, bool skip_exists) {
  uint8_t* vptr;

  uint8_t* fptr = lpFirst(lp);
  uint8_t* fsrc = field.empty() ? lp : (uint8_t*)field.data();

  // if we vsrc is NULL then lpReplace will delete the element, which is not what we want.
  // therefore, for an empty val we set it to some other valid address so that lpReplace
  // will do the right thing and encode empty string instead of deleting the element.
  uint8_t* vsrc = val.empty() ? lp : (uint8_t*)val.data();

  bool updated = false;

  if (fptr) {
    fptr = lpFind(lp, fptr, fsrc, field.size(), 1);
    if (fptr) {
      if (skip_exists) {
        return make_pair(lp, false);
      }
      /* Grab pointer to the value (fptr points to the field) */
      vptr = lpNext(lp, fptr);
      updated = true;

      /* Replace value */
      lp = lpReplace(lp, &vptr, vsrc, val.size());
      DCHECK_EQ(0u, lpLength(lp) % 2);
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
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
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
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    (*cntx)->StartArray(args.size());
    for (unsigned i = 0; i < args.size(); ++i) {
      (*cntx)->SendNull();
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
  string_view key = ArgS(args, 1);
  string_view field = ArgS(args, 2);
  string_view incrs = ArgS(args, 3);
  int64_t ival = 0;

  if (!absl::SimpleAtoi(incrs, &ival)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  IncrByParam param{ival};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(OpArgs{shard, t->db_index()}, key, field, &param);
  };

  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OK) {
    (*cntx)->SendLong(get<int64_t>(param));
  } else {
    switch (status) {
      case OpStatus::INVALID_VALUE:
        (*cntx)->SendError("hash value is not an integer");
        break;
      case OpStatus::OUT_OF_RANGE:
        (*cntx)->SendError(kIncrOverflow);
        break;
      default:
        (*cntx)->SendError(status);
        break;
    }
  }
}

void HSetFamily::HIncrByFloat(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view field = ArgS(args, 2);
  string_view incrs = ArgS(args, 3);
  double dval = 0;

  if (!absl::SimpleAtod(incrs, &dval)) {
    return (*cntx)->SendError(kInvalidFloatErr);
  }

  IncrByParam param{dval};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(OpArgs{shard, t->db_index()}, key, field, &param);
  };

  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OK) {
    (*cntx)->SendDouble(get<double>(param));
  } else {
    switch (status) {
      case OpStatus::INVALID_VALUE:
        (*cntx)->SendError("hash value is not a float");
        break;
      default:
        (*cntx)->SendError(status);
        break;
    }
  }
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

void HSetFamily::HScan(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 1);
  std::string_view token = ArgS(args, 2);

  uint64_t cursor = 0;

  if (!absl::SimpleAtoi(token, &cursor)) {
    return (*cntx)->SendError("invalid cursor");
  }

  if (args.size() > 3) {
    return (*cntx)->SendError("scan options are not supported yet");
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScan(OpArgs{shard, t->db_index()}, key, &cursor);
  };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() != OpStatus::WRONG_TYPE) {
    (*cntx)->StartArray(2);
    (*cntx)->SendSimpleString(absl::StrCat(cursor));
    (*cntx)->StartArray(result->size());
    for (const auto& k : *result) {
      (*cntx)->SendBulkString(k);
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HSet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  ToLower(&args[0]);

  string_view cmd = ArgS(args, 0);

  if (args.size() % 2 != 0) {
    return (*cntx)->SendError(facade::WrongNumArgsError(cmd), kSyntaxErrType);
  }

  args.remove_prefix(2);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(OpArgs{shard, t->db_index()}, key, args, false);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result && cmd == "hset") {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HSetNx(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  args.remove_prefix(2);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(OpArgs{shard, t->db_index()}, key, args, true);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HStrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);
  string_view field = ArgS(args, 2);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrLen(OpArgs{shard, t->db_index()}, key, field);
  };

  OpResult<size_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HRandField(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<StringVec> {
    auto& db_slice = shard->db_slice();
    auto it_res = db_slice.Find(t->db_index(), key, OBJ_HASH);

    if (!it_res)
      return it_res.status();

    const PrimeValue& pv = it_res.value()->second;
    StringVec str_vec;

    if (pv.Encoding() == OBJ_ENCODING_HT) {
      dict* this_dict = (dict*)pv.RObjPtr();
      dictEntry* de = dictGetFairRandomKey(this_dict);
      sds key = (sds)de->key;
      str_vec.emplace_back(key, sdslen(key));
    } else if (pv.Encoding() == OBJ_ENCODING_LISTPACK) {
      uint8_t* lp = (uint8_t*)pv.RObjPtr();
      size_t lplen = lpLength(lp);
      CHECK(lplen > 0 && lplen % 2 == 0);

      size_t hlen = lplen / 2;
      listpackEntry key;

      lpRandomPair(lp, hlen, &key, NULL);
      if (key.sval) {
        str_vec.emplace_back(reinterpret_cast<char*>(key.sval), key.slen);
      } else {
        str_vec.emplace_back(absl::StrCat(key.lval));
      }
    } else {
      LOG(ERROR) << "Invalid encoding " << pv.Encoding();
      return OpStatus::INVALID_VALUE;
    }
    return str_vec;
  };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    CHECK_EQ(1u, result->size());  // TBD: to support count and withvalues.
    (*cntx)->SendBulkString(result->front());
  } else {
    (*cntx)->SendError(result.status());
  }
}

OpResult<uint32_t> HSetFamily::OpSet(const OpArgs& op_args, string_view key, CmdArgList values,
                                     bool skip_if_exists) {
  DCHECK(!values.empty() && 0 == values.size() % 2);

  auto& db_slice = op_args.shard->db_slice();
  pair<PrimeIterator, bool> add_res;
  try {
    add_res = db_slice.AddOrFind(op_args.db_ind, key);
  } catch(bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }

  DbTableStats* stats = db_slice.MutableStats(op_args.db_ind);

  robj* hset = nullptr;
  uint8_t* lp = nullptr;
  PrimeIterator& it = add_res.first;

  if (add_res.second) {  // new key
    hset = createHashObject();
    lp = (uint8_t*)hset->ptr;

    it->second.ImportRObj(hset);
    stats->listpack_blob_cnt++;
    stats->listpack_bytes += lpBytes(lp);
  } else {
    if (it->second.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;

    db_slice.PreUpdate(op_args.db_ind, it);
  }
  hset = it->second.AsRObj();

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    lp = (uint8_t*)hset->ptr;
    stats->listpack_bytes -= lpBytes(lp);

    if (!IsGoodForListpack(values, lp)) {
      stats->listpack_blob_cnt--;
      hashTypeConvert(hset, OBJ_ENCODING_HT);
      lp = nullptr;
    }
  }

  unsigned created = 0;

  if (lp) {
    bool inserted;
    for (size_t i = 0; i < values.size(); i += 2) {
      tie(lp, inserted) = LpInsert(lp, ArgS(values, i), ArgS(values, i + 1), skip_if_exists);
      created += inserted;
    }
    hset->ptr = lp;
    stats->listpack_bytes += lpBytes(lp);
  } else {
    DCHECK_EQ(OBJ_ENCODING_HT, hset->encoding);
    dict* this_dict = (dict*)hset->ptr;

    // Dictionary
    for (size_t i = 0; i < values.size(); i += 2) {
      sds fs = sdsnewlen(values[i].data(), values[i].size());
      dictEntry* existing;
      dictEntry* de = dictAddRaw(this_dict, fs, &existing);
      if (de) {
        ++created;
      } else {  // already exists
        sdsfree(fs);
        if (skip_if_exists)
          continue;

        de = existing;
        dictFreeVal(this_dict, existing);
      }

      sds vs = sdsnewlen(values[i + 1].data(), values[i + 1].size());
      dictSetVal(this_dict, de, vs);
    }
  }
  it->second.SyncRObj();
  db_slice.PostUpdate(op_args.db_ind, it);

  return created;
}

OpResult<uint32_t> HSetFamily::OpDel(const OpArgs& op_args, string_view key, CmdArgList values) {
  DCHECK(!values.empty());

  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_HASH);

  if (!it_res)
    return it_res.status();

  db_slice.PreUpdate(op_args.db_ind, *it_res);
  CompactObj& co = (*it_res)->second;
  robj* hset = co.AsRObj();
  unsigned deleted = 0;
  bool key_remove = false;
  DbTableStats* stats = db_slice.MutableStats(op_args.db_ind);

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

  db_slice.PostUpdate(op_args.db_ind, *it_res);
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
      reverse.emplace(ArgS(fields, i), i);  // map fields to their index.
    }

    char ibuf[32];
    uint8_t* lp_elem = lpFirst(lp);
    int64_t ele_len;
    string_view key;
    DCHECK(lp_elem);  // empty containers are not allowed.
    size_t lplen = lpLength(lp);
    DCHECK(lplen > 0 && lplen % 2 == 0);

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
    } while (lp_elem);
  } else {
    DCHECK_EQ(OBJ_ENCODING_HT, hset->encoding);
    dict* d = (dict*)hset->ptr;
    for (size_t i = 0; i < fields.size(); ++i) {
      op_args.shard->tmp_str1 =
          sdscpylen(op_args.shard->tmp_str1, fields[i].data(), fields[i].size());
      dictEntry* de = dictFind(d, op_args.shard->tmp_str1);
      if (de) {
        sds val = (sds)dictGetVal(de);
        result[i].emplace(val, sdslen(val));
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

OpResult<size_t> HSetFamily::OpStrLen(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_ind, key, OBJ_HASH);

  if (!it_res) {
    if (it_res.status() == OpStatus::KEY_NOTFOUND)
      return 0;
    return it_res.status();
  }

  robj* hset = (*it_res)->second.AsRObj();
  size_t field_len = 0;
  op_args.shard->tmp_str1 = sdscpylen(op_args.shard->tmp_str1, field.data(), field.size());

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    unsigned char* vstr = NULL;
    unsigned int vlen = UINT_MAX;
    long long vll = LLONG_MAX;

    if (hashTypeGetFromListpack(hset, op_args.shard->tmp_str1, &vstr, &vlen, &vll) == 0)
      field_len = vstr ? vlen : sdigits10(vll);

    return field_len;
  }

  DCHECK_EQ(hset->encoding, OBJ_ENCODING_HT);

  dictEntry* de = dictFind((dict*)hset->ptr, op_args.shard->tmp_str1);
  return de ? sdslen((sds)de->v.val) : 0;
}

OpStatus HSetFamily::OpIncrBy(const OpArgs& op_args, string_view key, string_view field,
                              IncrByParam* param) {
  auto& db_slice = op_args.shard->db_slice();
  const auto [it, inserted] = db_slice.AddOrFind(op_args.db_ind, key);

  DbTableStats* stats = db_slice.MutableStats(op_args.db_ind);

  robj* hset = nullptr;
  size_t lpb = 0;

  if (inserted) {
    hset = createHashObject();
    it->second.ImportRObj(hset);
    stats->listpack_blob_cnt++;
    hset = it->second.AsRObj();
  } else {
    if (it->second.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;

    db_slice.PreUpdate(op_args.db_ind, it);
    hset = it->second.AsRObj();

    if (hset->encoding == OBJ_ENCODING_LISTPACK) {
      lpb = lpBytes((uint8_t*)hset->ptr);
      stats->listpack_bytes -= lpb;

      if (lpb >= kMaxListPackLen) {
        stats->listpack_blob_cnt--;
        hashTypeConvert(hset, OBJ_ENCODING_HT);
      }
    }
  }

  unsigned char* vstr = NULL;
  unsigned int vlen = UINT_MAX;
  long long old_val = 0;

  op_args.shard->tmp_str1 = sdscpylen(op_args.shard->tmp_str1, field.data(), field.size());

  int exist_res = hashTypeGetValue(hset, op_args.shard->tmp_str1, &vstr, &vlen, &old_val);

  if (holds_alternative<double>(*param)) {
    long double value;
    double incr = get<double>(*param);
    if (exist_res == C_OK) {
      if (vstr) {
        const char* exist_val = reinterpret_cast<char*>(vstr);

        if (!string2ld(exist_val, vlen, &value)) {
          stats->listpack_bytes += lpb;

          return OpStatus::INVALID_VALUE;
        }
      } else {
        value = old_val;
      }
      value += incr;

      if (isnan(value) || isinf(value)) {
        return OpStatus::INVALID_FLOAT;
      }
    } else {
      value = incr;
    }

    char buf[128];
    char* str = RedisReplyBuilder::FormatDouble(value, buf, sizeof(buf));
    string_view sval{str};

    if (hset->encoding == OBJ_ENCODING_LISTPACK) {
      uint8_t* lp = (uint8_t*)hset->ptr;

      lp = LpInsert(lp, field, sval, false).first;
      hset->ptr = lp;
      stats->listpack_bytes += lpBytes(lp);
    } else {
      sds news = sdsnewlen(str, sval.size());
      hashTypeSet(hset, op_args.shard->tmp_str1, news, HASH_SET_TAKE_VALUE);
    }
    param->emplace<double>(value);
  } else {
    if (exist_res == C_OK && vstr) {
      const char* exist_val = reinterpret_cast<char*>(vstr);
      if (!string2ll(exist_val, vlen, &old_val)) {
        stats->listpack_bytes += lpb;

        return OpStatus::INVALID_VALUE;
      }
    }
    int64_t incr = get<int64_t>(*param);
    if ((incr < 0 && old_val < 0 && incr < (LLONG_MIN - old_val)) ||
        (incr > 0 && old_val > 0 && incr > (LLONG_MAX - old_val))) {
      stats->listpack_bytes += lpb;

      return OpStatus::OUT_OF_RANGE;
    }

    int64_t new_val = old_val + incr;

    if (hset->encoding == OBJ_ENCODING_LISTPACK) {
      char buf[32];
      char* next = absl::numbers_internal::FastIntToBuffer(new_val, buf);
      string_view sval{buf, size_t(next - buf)};
      uint8_t* lp = (uint8_t*)hset->ptr;

      lp = LpInsert(lp, field, sval, false).first;
      hset->ptr = lp;
      stats->listpack_bytes += lpBytes(lp);
    } else {
      sds news = sdsfromlonglong(new_val);
      hashTypeSet(hset, op_args.shard->tmp_str1, news, HASH_SET_TAKE_VALUE);
    }
    param->emplace<int64_t>(new_val);
  }

  it->second.SyncRObj();
  db_slice.PostUpdate(op_args.db_ind, it);

  return OpStatus::OK;
}

OpResult<StringVec> HSetFamily::OpScan(const OpArgs& op_args, std::string_view key,
                                       uint64_t* cursor) {
  OpResult<PrimeIterator> find_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_HASH);

  if (!find_res)
    return find_res.status();

  PrimeIterator it = find_res.value();
  StringVec res;
  uint32_t count = 20;
  robj* hset = it->second.AsRObj();

  if (hset->encoding == OBJ_ENCODING_LISTPACK) {
    uint8_t* lp = (uint8_t*)hset->ptr;
    uint8_t* lp_elem = lpFirst(lp);

    DCHECK(lp_elem);  // empty containers are not allowed.

    int64_t ele_len;
    unsigned char intbuf[LP_INTBUF_SIZE];

    // We do single pass on listpack for this operation.
    do {
      uint8_t* elem = lpGet(lp_elem, &ele_len, intbuf);
      DCHECK(elem);
      res.emplace_back(reinterpret_cast<char*>(elem), size_t(ele_len));
      lp_elem = lpNext(lp, lp_elem);  // switch to value
    } while (lp_elem);
    *cursor = 0;
  } else {
    dict* ht = (dict*)hset->ptr;
    long maxiterations = count * 10;
    void* privdata = &res;
    auto scanCb = [](void* privdata, const dictEntry* de) {
      StringVec* res = (StringVec*)privdata;
      sds val = (sds)de->key;
      res->emplace_back(val, sdslen(val));
      val = (sds)de->v.val;
      res->emplace_back(val, sdslen(val));
    };

    do {
      *cursor = dictScan(ht, *cursor, scanCb, NULL, privdata);
    } while (*cursor && maxiterations-- && res.size() < count);
  }

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
            << CI{"HMSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(HSet)
            << CI{"HINCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(HIncrBy)
            << CI{"HINCRBYFLOAT", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(
                   HIncrByFloat)
            << CI{"HKEYS", CO::READONLY, 2, 1, 1, 1}.HFUNC(HKeys)

            // TODO: add options support
            << CI{"HRANDFIELD", CO::READONLY, 2, 1, 1, 1}.HFUNC(HRandField)
            << CI{"HSCAN", CO::READONLY | CO::RANDOM, -3, 1, 1, 1}.HFUNC(HScan)
            << CI{"HSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(HSet)
            << CI{"HSETNX", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(HSetNx)
            << CI{"HSTRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(HStrLen)
            << CI{"HVALS", CO::READONLY, 2, 1, 1, 1}.HFUNC(HVals);
}

uint32_t HSetFamily::MaxListPackLen() {
  return kMaxListPackLen;
}

}  // namespace dfly
