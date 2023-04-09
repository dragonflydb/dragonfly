// Copyright 2022, DragonflyDB authors.  All rights reserved.
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
#include "core/string_map.h"
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
using IncrByParam = std::variant<double, int64_t>;
using OptStr = std::optional<std::string>;
enum GetAllMode : uint8_t { FIELDS = 1, VALUES = 2 };

bool IsGoodForListpack(CmdArgList args, const uint8_t* lp) {
  size_t sum = 0;
  for (auto s : args) {
    if (s.size() > server.hash_max_listpack_value)
      return false;
    sum += s.size();
  }

  return lpBytes(const_cast<uint8_t*>(lp)) + sum < kMaxListPackLen;
}

inline StringMap* GetStringMap(const PrimeValue& pv, const DbContext& db_context) {
  StringMap* res = (StringMap*)pv.RObjPtr();
  uint32_t map_time = MemberTimeSeconds(db_context.time_now_ms);
  res->set_time(map_time);
  return res;
}

inline string_view LpGetView(uint8_t* lp_it, uint8_t int_buf[]) {
  int64_t ele_len = 0;
  uint8_t* elem = lpGet(lp_it, &ele_len, int_buf);
  DCHECK(elem);
  return string_view{reinterpret_cast<char*>(elem), size_t(ele_len)};
}

optional<string_view> LpFind(uint8_t* lp, string_view key, uint8_t int_buf[]) {
  uint8_t* fptr = lpFirst(lp);
  DCHECK(fptr);

  fptr = lpFind(lp, fptr, (unsigned char*)key.data(), key.size(), 1);
  if (!fptr)
    return nullopt;
  uint8_t* vptr = lpNext(lp, fptr);
  return LpGetView(vptr, int_buf);
}

pair<uint8_t*, bool> LpDelete(uint8_t* lp, string_view field) {
  uint8_t* fptr = lpFirst(lp);
  DCHECK(fptr);
  fptr = lpFind(lp, fptr, (unsigned char*)field.data(), field.size(), 1);
  if (fptr == NULL) {
    return make_pair(lp, false);
  }

  /* Delete both of the key and the value. */
  lp = lpDeleteRangeWithEntry(lp, &fptr, 2);
  return make_pair(lp, true);
}

// returns a new pointer to lp. Returns true if field was inserted or false it it already existed.
// skip_exists controls what happens if the field already existed. If skip_exists = true,
// then val does not override the value and listpack is not changed. Otherwise, the corresponding
// value is overridden by val.
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

size_t HMapLength(const DbContext& db_cntx, const CompactObj& co) {
  void* ptr = co.RObjPtr();
  if (co.Encoding() == kEncodingStrMap2) {
    StringMap* sm = GetStringMap(co, db_cntx);
    return sm->Size();
  }

  DCHECK_EQ(kEncodingListPack, co.Encoding());
  return lpLength((uint8_t*)ptr) / 2;
}

OpStatus IncrementValue(optional<string_view> prev_val, IncrByParam* param) {
  if (holds_alternative<double>(*param)) {
    double incr = get<double>(*param);
    long double value = 0;

    if (prev_val) {
      if (!string2ld(prev_val->data(), prev_val->size(), &value)) {
        return OpStatus::INVALID_VALUE;
      }
    }
    value += incr;
    if (isnan(value) || isinf(value)) {
      return OpStatus::INVALID_FLOAT;
    }

    param->emplace<double>(value);

    return OpStatus::OK;
  }

  // integer increment
  long long old_val = 0;
  if (prev_val) {
    if (!string2ll(prev_val->data(), prev_val->size(), &old_val)) {
      return OpStatus::INVALID_VALUE;
    }
  }

  int64_t incr = get<int64_t>(*param);
  if ((incr < 0 && old_val < 0 && incr < (LLONG_MIN - old_val)) ||
      (incr > 0 && old_val > 0 && incr > (LLONG_MAX - old_val))) {
    return OpStatus::OUT_OF_RANGE;
  }

  int64_t new_val = old_val + incr;
  param->emplace<int64_t>(new_val);

  return OpStatus::OK;
};

OpStatus OpIncrBy(const OpArgs& op_args, string_view key, string_view field, IncrByParam* param) {
  auto& db_slice = op_args.shard->db_slice();
  const auto [it, inserted] = db_slice.AddOrFind(op_args.db_cntx, key);

  DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);

  size_t lpb = 0;

  PrimeValue& pv = it->second;
  if (inserted) {
    pv.InitRobj(OBJ_HASH, kEncodingListPack, lpNew(0));
    stats->listpack_blob_cnt++;
  } else {
    if (pv.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;

    db_slice.PreUpdate(op_args.db_cntx.db_index, it);

    if (pv.Encoding() == kEncodingListPack) {
      uint8_t* lp = (uint8_t*)pv.RObjPtr();
      lpb = lpBytes(lp);
      stats->listpack_bytes -= lpb;

      if (lpb >= kMaxListPackLen) {
        stats->listpack_blob_cnt--;
        StringMap* sm = HSetFamily::ConvertToStrMap(lp);
        pv.InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
      }
    }
  }

  unsigned enc = pv.Encoding();

  if (enc == kEncodingListPack) {
    uint8_t intbuf[LP_INTBUF_SIZE];
    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    optional<string_view> res;

    if (!inserted)
      res = LpFind(lp, field, intbuf);

    OpStatus status = IncrementValue(res, param);
    if (status != OpStatus::OK) {
      stats->listpack_bytes += lpb;
      return status;
    }

    if (holds_alternative<double>(*param)) {
      double new_val = get<double>(*param);
      char buf[128];
      char* str = RedisReplyBuilder::FormatDouble(new_val, buf, sizeof(buf));
      lp = LpInsert(lp, field, str, false).first;
    } else {  // integer increment
      int64_t new_val = get<int64_t>(*param);
      absl::AlphaNum an(new_val);
      lp = LpInsert(lp, field, an.Piece(), false).first;
    }

    pv.SetRObjPtr(lp);
    stats->listpack_bytes += lpBytes(lp);
  } else {
    DCHECK_EQ(enc, kEncodingStrMap2);
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);

    sds val = nullptr;
    if (!inserted) {
      val = sm->Find(field);
    }

    optional<string_view> sv;
    if (val) {
      sv.emplace(val, sdslen(val));
    }

    OpStatus status = IncrementValue(sv, param);
    if (status != OpStatus::OK) {
      return status;
    }

    if (holds_alternative<double>(*param)) {
      double new_val = get<double>(*param);

      char buf[128];
      char* str = RedisReplyBuilder::FormatDouble(new_val, buf, sizeof(buf));
      sm->AddOrUpdate(field, str);
    } else {  // integer increment
      int64_t new_val = get<int64_t>(*param);
      absl::AlphaNum an(new_val);
      sm->AddOrUpdate(field, an.Piece());
    }
  }

  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);

  return OpStatus::OK;
}

OpResult<StringVec> OpScan(const OpArgs& op_args, std::string_view key, uint64_t* cursor,
                           const ScanOpts& scan_op) {
  constexpr size_t HASH_TABLE_ENTRIES_FACTOR = 2;  // return key/value

  /* We set the max number of iterations to ten times the specified
   * COUNT, so if the hash table is in a pathological state (very
   * sparsely populated) we avoid to block too much time at the cost
   * of returning no or very few elements. (taken from redis code at db.c line 904 */
  constexpr size_t INTERATION_FACTOR = 10;

  OpResult<PrimeIterator> find_res = op_args.shard->db_slice().Find(op_args.db_cntx, key, OBJ_HASH);

  if (!find_res) {
    DVLOG(1) << "ScanOp: find failed: " << find_res << ", baling out";
    return find_res.status();
  }

  PrimeIterator it = find_res.value();
  StringVec res;
  uint32_t count = scan_op.limit * HASH_TABLE_ENTRIES_FACTOR;
  PrimeValue& pv = it->second;

  if (pv.Encoding() == kEncodingListPack) {
    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    uint8_t* lp_elem = lpFirst(lp);

    DCHECK(lp_elem);  // empty containers are not allowed.

    uint8_t intbuf[LP_INTBUF_SIZE];

    // We do single pass on listpack for this operation - ignore any limits.
    do {
      string_view key = LpGetView(lp_elem, intbuf);
      lp_elem = lpNext(lp, lp_elem);  // switch to value
      DCHECK(lp_elem);

      if (scan_op.Matches(key)) {
        res.emplace_back(key);
        res.emplace_back(LpGetView(lp_elem, intbuf));
      }
      lp_elem = lpNext(lp, lp_elem);  // switch to next key
    } while (lp_elem);

    *cursor = 0;
  } else {
    DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);

    long max_iterations = count * INTERATION_FACTOR;

    // note about this lambda - don't capture here! it should be convertible to C function!
    auto scanCb = [&](const void* obj) {
      sds val = (sds)obj;
      size_t len = sdslen(val);
      if (scan_op.Matches(string_view(val, len))) {
        res.emplace_back(val, len);
        val += (len + 1);
        val = (sds)absl::little_endian::Load64(val);
        res.emplace_back(val, sdslen(val));
      }
    };

    do {
      *cursor = sm->Scan(*cursor, scanCb);
    } while (*cursor && max_iterations-- && res.size() < count);
  }

  return res;
}

OpResult<uint32_t> OpDel(const OpArgs& op_args, string_view key, CmdArgList values) {
  DCHECK(!values.empty());

  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_HASH);

  if (!it_res)
    return it_res.status();

  db_slice.PreUpdate(op_args.db_cntx.db_index, *it_res);
  PrimeValue& pv = (*it_res)->second;
  unsigned deleted = 0;
  bool key_remove = false;
  DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);
  unsigned enc = pv.Encoding();

  if (enc == kEncodingListPack) {
    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    stats->listpack_bytes -= lpBytes(lp);
    for (auto s : values) {
      auto res = LpDelete(lp, ToSV(s));
      if (res.second) {
        ++deleted;
        lp = res.first;
        if (lpLength(lp) == 0) {
          key_remove = true;
          break;
        }
      }
    }
    pv.SetRObjPtr(lp);
  } else {
    DCHECK_EQ(enc, kEncodingStrMap2);
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);

    for (auto s : values) {
      bool res = sm->Erase(ToSV(s));
      if (res) {
        ++deleted;
        if (sm->Size() == 0) {
          key_remove = true;
          break;
        }
      }
    }
  }

  db_slice.PostUpdate(op_args.db_cntx.db_index, *it_res, key);
  if (key_remove) {
    if (enc == kEncodingListPack) {
      stats->listpack_blob_cnt--;
    }
    db_slice.Del(op_args.db_cntx.db_index, *it_res);
  } else if (enc == kEncodingListPack) {
    stats->listpack_bytes += lpBytes((uint8_t*)pv.RObjPtr());
  }

  return deleted;
}

OpResult<vector<OptStr>> OpMGet(const OpArgs& op_args, std::string_view key, CmdArgList fields) {
  DCHECK(!fields.empty());

  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_HASH);

  if (!it_res)
    return it_res.status();

  PrimeValue& pv = (*it_res)->second;

  std::vector<OptStr> result(fields.size());

  if (pv.Encoding() == kEncodingListPack) {
    uint8_t* lp = (uint8_t*)pv.RObjPtr();

    absl::flat_hash_map<string_view, unsigned> reverse;
    reverse.reserve(fields.size() + 1);
    for (size_t i = 0; i < fields.size(); ++i) {
      reverse.emplace(ArgS(fields, i), i);  // map fields to their index.
    }

    size_t lplen = lpLength(lp);
    DCHECK(lplen > 0 && lplen % 2 == 0);

    uint8_t ibuf[32];
    string_view key;

    uint8_t* lp_elem = lpFirst(lp);
    DCHECK(lp_elem);  // empty containers are not allowed.

    // We do single pass on listpack for this operation.
    do {
      key = LpGetView(lp_elem, ibuf);
      lp_elem = lpNext(lp, lp_elem);  // switch to value
      DCHECK(lp_elem);

      auto it = reverse.find(key);
      if (it != reverse.end()) {
        DCHECK_LT(it->second, result.size());
        result[it->second].emplace(LpGetView(lp_elem, ibuf));  // populate found items.
      }

      lp_elem = lpNext(lp, lp_elem);  // switch to the next key
    } while (lp_elem);
  } else {
    DCHECK_EQ(kEncodingStrMap2, pv.Encoding());
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);

    for (size_t i = 0; i < fields.size(); ++i) {
      sds val = sm->Find(ToSV(fields[i]));
      if (val) {
        result[i].emplace(val, sdslen(val));
      }
    }
  }

  return result;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_HASH);

  if (it_res) {
    return HMapLength(op_args.db_cntx, (*it_res)->second);
  }

  if (it_res.status() == OpStatus::KEY_NOTFOUND)
    return 0;
  return it_res.status();
}

OpResult<int> OpExist(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_HASH);

  if (!it_res) {
    if (it_res.status() == OpStatus::KEY_NOTFOUND)
      return 0;
    return it_res.status();
  }

  const PrimeValue& pv = (*it_res)->second;
  void* ptr = pv.RObjPtr();
  if (pv.Encoding() == kEncodingListPack) {
    uint8_t intbuf[LP_INTBUF_SIZE];
    optional<string_view> res = LpFind((uint8_t*)ptr, field, intbuf);
    return res.has_value();
  }

  DCHECK_EQ(kEncodingStrMap2, pv.Encoding());
  StringMap* sm = GetStringMap(pv, op_args.db_cntx);

  return sm->Find(field) ? 1 : 0;
};

OpResult<string> OpGet(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_HASH);
  if (!it_res)
    return it_res.status();

  const PrimeValue& pv = (*it_res)->second;
  void* ptr = pv.RObjPtr();

  if (pv.Encoding() == kEncodingListPack) {
    uint8_t intbuf[LP_INTBUF_SIZE];
    optional<string_view> res = LpFind((uint8_t*)ptr, field, intbuf);
    if (!res) {
      return OpStatus::KEY_NOTFOUND;
    }
    return string(*res);
  }

  DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
  StringMap* sm = GetStringMap(pv, op_args.db_cntx);
  sds val = sm->Find(field);

  if (!val)
    return OpStatus::KEY_NOTFOUND;

  return string(val, sdslen(val));
}

OpResult<vector<string>> OpGetAll(const OpArgs& op_args, string_view key, uint8_t mask) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_HASH);
  if (!it_res) {
    if (it_res.status() == OpStatus::KEY_NOTFOUND)
      return vector<string>{};
    return it_res.status();
  }

  const PrimeValue& pv = (*it_res)->second;

  vector<string> res;
  bool keyval = (mask == (FIELDS | VALUES));
  unsigned index = 0;

  if (pv.Encoding() == kEncodingListPack) {
    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    res.resize(lpLength(lp) / (keyval ? 1 : 2));

    uint8_t* fptr = lpFirst(lp);
    uint8_t intbuf[LP_INTBUF_SIZE];

    while (fptr) {
      if (mask & FIELDS) {
        res[index++] = LpGetView(fptr, intbuf);
      }
      fptr = lpNext(lp, fptr);
      if (mask & VALUES) {
        res[index++] = LpGetView(fptr, intbuf);
      }
      fptr = lpNext(lp, fptr);
    }
  } else {
    DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);

    res.resize(sm->Size() * (keyval ? 2 : 1));
    for (const auto& k_v : *sm) {
      if (mask & FIELDS) {
        res[index++].assign(k_v.first, sdslen(k_v.first));
      }

      if (mask & VALUES) {
        res[index++].assign(k_v.second, sdslen(k_v.second));
      }
    }
  }

  return res;
}

OpResult<size_t> OpStrLen(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.shard->db_slice();
  auto it_res = db_slice.Find(op_args.db_cntx, key, OBJ_HASH);

  if (!it_res) {
    if (it_res.status() == OpStatus::KEY_NOTFOUND)
      return 0;
    return it_res.status();
  }

  const PrimeValue& pv = (*it_res)->second;
  void* ptr = pv.RObjPtr();
  if (pv.Encoding() == kEncodingListPack) {
    uint8_t intbuf[LP_INTBUF_SIZE];
    optional<string_view> res = LpFind((uint8_t*)ptr, field, intbuf);

    return res ? res->size() : 0;
  }

  DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
  StringMap* sm = GetStringMap(pv, op_args.db_cntx);

  sds res = sm->Find(field);
  return res ? sdslen(res) : 0;
}

struct OpSetParams {
  bool skip_if_exists = false;
  uint32_t ttl = UINT32_MAX;
};

OpResult<uint32_t> OpSet(const OpArgs& op_args, string_view key, CmdArgList values,
                         const OpSetParams& op_sp = OpSetParams{}) {
  DCHECK(!values.empty() && 0 == values.size() % 2);

  auto& db_slice = op_args.shard->db_slice();
  pair<PrimeIterator, bool> add_res;
  try {
    add_res = db_slice.AddOrFind(op_args.db_cntx, key);
  } catch (bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }

  DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);

  uint8_t* lp = nullptr;
  PrimeIterator& it = add_res.first;
  PrimeValue& pv = it->second;

  if (add_res.second) {  // new key
    if (op_sp.ttl == UINT32_MAX) {
      lp = lpNew(0);
      pv.InitRobj(OBJ_HASH, kEncodingListPack, lp);

      stats->listpack_blob_cnt++;
      stats->listpack_bytes += lpBytes(lp);
    } else {
      StringMap* sm = new StringMap(CompactObj::memory_resource());
      pv.InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
    }
  } else {
    if (pv.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;

    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  }

  if (pv.Encoding() == kEncodingListPack) {
    lp = (uint8_t*)pv.RObjPtr();
    stats->listpack_bytes -= lpBytes(lp);

    if (op_sp.ttl != UINT32_MAX || !IsGoodForListpack(values, lp)) {
      stats->listpack_blob_cnt--;
      StringMap* sm = HSetFamily::ConvertToStrMap(lp);
      pv.InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
      lp = nullptr;
    }
  }

  unsigned created = 0;

  if (lp) {
    bool inserted;
    for (size_t i = 0; i < values.size(); i += 2) {
      tie(lp, inserted) = LpInsert(lp, ArgS(values, i), ArgS(values, i + 1), op_sp.skip_if_exists);
      created += inserted;
    }
    pv.SetRObjPtr(lp);
    stats->listpack_bytes += lpBytes(lp);
  } else {
    DCHECK_EQ(kEncodingStrMap2, pv.Encoding());  // Dictionary
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);

    bool added;

    for (size_t i = 0; i < values.size(); i += 2) {
      if (op_sp.skip_if_exists)
        added = sm->AddOrSkip(ToSV(values[i]), ToSV(values[i + 1]), op_sp.ttl);
      else
        added = sm->AddOrUpdate(ToSV(values[i]), ToSV(values[i + 1]), op_sp.ttl);

      created += unsigned(added);
    }
  }
  db_slice.PostUpdate(op_args.db_cntx.db_index, it, key);

  return created;
}

void HGetGeneric(CmdArgList args, ConnectionContext* cntx, uint8_t getall_mask) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGetAll(t->GetOpArgs(shard), key, getall_mask);
  };

  OpResult<vector<string>> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));

  if (result) {
    bool is_map = (getall_mask == (VALUES | FIELDS));
    (*cntx)->SendStringArr(absl::Span<const string>{*result},
                           is_map ? RedisReplyBuilder::MAP : RedisReplyBuilder::ARRAY);
  } else {
    (*cntx)->SendError(result.status());
  }
}

// HSETEX key tll_sec field value field value ...
void HSetEx(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() % 2 != 0) {
    return (*cntx)->SendError(facade::WrongNumArgsError(cntx->cid->name()), kSyntaxErrType);
  }

  string_view key = ArgS(args, 0);
  string_view ttl_str = ArgS(args, 1);
  uint32_t ttl_sec;
  constexpr uint32_t kMaxTtl = (1UL << 26);

  if (!absl::SimpleAtoi(ttl_str, &ttl_sec) || ttl_sec == 0 || ttl_sec > kMaxTtl) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  args.remove_prefix(2);
  OpSetParams op_sp;
  op_sp.ttl = ttl_sec;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args, op_sp);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

}  // namespace

void HSetFamily::HDel(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, args);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HExists(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<int> {
    return OpExist(t->GetOpArgs(shard), key, field);
  };

  OpResult<int> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HMGet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpMGet(t->GetOpArgs(shard), key, args);
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
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGet(t->GetOpArgs(shard), key, field);
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
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);
  string_view incrs = ArgS(args, 2);
  int64_t ival = 0;

  if (!absl::SimpleAtoi(incrs, &ival)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  IncrByParam param{ival};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, field, &param);
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
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);
  string_view incrs = ArgS(args, 2);
  double dval = 0;

  if (!absl::SimpleAtod(incrs, &dval)) {
    return (*cntx)->SendError(kInvalidFloatErr);
  }

  IncrByParam param{dval};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, field, &param);
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

void HSetFamily::HScan(CmdArgList args, ConnectionContext* cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view token = ArgS(args, 1);

  uint64_t cursor = 0;

  if (!absl::SimpleAtoi(token, &cursor)) {
    return (*cntx)->SendError("invalid cursor");
  }

  // HSCAN key cursor [MATCH pattern] [COUNT count]
  if (args.size() > 6) {
    DVLOG(1) << "got " << args.size() << " this is more than it should be";
    return (*cntx)->SendError(kSyntaxErr);
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(2));
  if (!ops) {
    DVLOG(1) << "HScan invalid args - return " << ops << " to the user";
    return (*cntx)->SendError(ops.status());
  }

  ScanOpts scan_op = ops.value();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScan(t->GetOpArgs(shard), key, &cursor, scan_op);
  };

  OpResult<StringVec> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result.status() != OpStatus::WRONG_TYPE) {
    (*cntx)->StartArray(2);
    (*cntx)->SendBulkString(absl::StrCat(cursor));
    (*cntx)->StartArray(result->size());  // Within scan the page type is array
    for (const auto& k : *result) {
      (*cntx)->SendBulkString(k);
    }
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HSet(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  ToLower(&args[0]);

  if (args.size() % 2 != 1) {
    return (*cntx)->SendError(facade::WrongNumArgsError("hset"), kSyntaxErrType);
  }

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  string_view cmd{cntx->cid->name()};

  if (result && cmd == "HSET") {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HSetNx(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args, OpSetParams{.skip_if_exists = true});
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HStrLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrLen(t->GetOpArgs(shard), key, field);
  };

  OpResult<size_t> result = cntx->transaction->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(*result);
  } else {
    (*cntx)->SendError(result.status());
  }
}

void HSetFamily::HRandField(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<StringVec> {
    auto& db_slice = shard->db_slice();
    DbContext db_context = t->GetDbContext();
    auto it_res = db_slice.Find(db_context, key, OBJ_HASH);

    if (!it_res)
      return it_res.status();

    const PrimeValue& pv = it_res.value()->second;
    StringVec str_vec;

    if (pv.Encoding() == kEncodingStrMap2) {
      // TODO: to create real random logic.
      StringMap* string_map = (StringMap*)pv.RObjPtr();

      sds key = string_map->begin()->first;
      str_vec.emplace_back(key, sdslen(key));
    } else if (pv.Encoding() == kEncodingListPack) {
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
            << CI{"HSCAN", CO::READONLY, -3, 1, 1, 1}.HFUNC(HScan)
            << CI{"HSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, 1}.HFUNC(HSet)
            << CI{"HSETEX", CO::WRITE | CO::FAST | CO::DENYOOM, -5, 1, 1, 1}.SetHandler(HSetEx)
            << CI{"HSETNX", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, 1}.HFUNC(HSetNx)
            << CI{"HSTRLEN", CO::READONLY | CO::FAST, 3, 1, 1, 1}.HFUNC(HStrLen)
            << CI{"HVALS", CO::READONLY, 2, 1, 1, 1}.HFUNC(HVals);
}

uint32_t HSetFamily::MaxListPackLen() {
  return kMaxListPackLen;
}

StringMap* HSetFamily::ConvertToStrMap(uint8_t* lp) {
  StringMap* sm = new StringMap(CompactObj::memory_resource());
  size_t lplen = lpLength(lp);
  if (lplen == 0)
    return sm;

  sm->Reserve(lplen / 2);

  uint8_t* lp_elem = lpFirst(lp);
  uint8_t intbuf[2][LP_INTBUF_SIZE];

  DCHECK(lp_elem);  // empty containers are not allowed.

  do {
    string_view key = LpGetView(lp_elem, intbuf[0]);
    lp_elem = lpNext(lp, lp_elem);  // switch to value
    DCHECK(lp_elem);
    string_view value = LpGetView(lp_elem, intbuf[1]);
    lp_elem = lpNext(lp, lp_elem);       // switch to next key
    CHECK(sm->AddOrUpdate(key, value));  // must be unique
  } while (lp_elem);

  return sm;
}
}  // namespace dfly
