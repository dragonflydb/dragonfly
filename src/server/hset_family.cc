// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

#include "server/family_utils.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
#include "redis/zmalloc.h"
}

#include "base/logging.h"
#include "core/string_map.h"
#include "facade/cmd_arg_parser.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/search/doc_index.h"
#include "server/transaction.h"

using namespace std;

namespace dfly {

using namespace facade;
using absl::SimpleAtoi;

namespace {

using IncrByParam = std::variant<double, int64_t>;
using OptStr = std::optional<std::string>;
enum GetAllMode : uint8_t { FIELDS = 1, VALUES = 2 };

bool IsGoodForListpack(CmdArgList args, const uint8_t* lp) {
  size_t sum = 0;
  for (auto s : args) {
    if (s.size() > server.max_map_field_len)
      return false;
    sum += s.size();
  }

  return lpBytes(const_cast<uint8_t*>(lp)) + sum < server.max_listpack_map_bytes;
}

using container_utils::GetStringMap;
using container_utils::LpFind;
using container_utils::LpGetView;

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

size_t EstimateListpackMinBytes(CmdArgList members) {
  size_t bytes = 0;
  for (const auto& member : members) {
    bytes += (member.size() + 1);  // string + at least 1 byte for string header.
  }
  return bytes;
}

size_t HMapLength(const DbContext& db_cntx, const CompactObj& co) {
  void* ptr = co.RObjPtr();
  if (co.Encoding() == kEncodingStrMap2) {
    StringMap* sm = GetStringMap(co, db_cntx);
    return sm->UpperBoundSize();
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
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  if (!op_res) {
    return op_res.status();
  }
  auto& add_res = *op_res;

  DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);

  size_t lpb = 0;

  PrimeValue& pv = add_res.it->second;
  if (add_res.is_new) {
    pv.InitRobj(OBJ_HASH, kEncodingListPack, lpNew(0));
    stats->listpack_blob_cnt++;
  } else {
    if (pv.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;

    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, add_res.it->second);

    if (pv.Encoding() == kEncodingListPack) {
      uint8_t* lp = (uint8_t*)pv.RObjPtr();
      lpb = lpBytes(lp);
      stats->listpack_bytes -= lpb;

      if (lpb >= server.max_listpack_map_bytes) {
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

    if (!add_res.is_new)
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
    if (!add_res.is_new) {
      auto it = sm->Find(field);
      if (it != sm->end()) {
        val = it->second;
      }
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

  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);

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

  auto find_res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_HASH);

  if (!find_res) {
    DVLOG(1) << "ScanOp: find failed: " << find_res << ", baling out";
    return find_res.status();
  }

  auto it = find_res.value();
  StringVec res;
  uint32_t count = scan_op.limit * HASH_TABLE_ENTRIES_FACTOR;
  const PrimeValue& pv = it->second;

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

  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH);

  if (!it_res)
    return it_res.status();

  PrimeValue& pv = it_res->it->second;
  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv);

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
        if (sm->UpperBoundSize() == 0) {
          key_remove = true;
          break;
        }
      }
    }
  }

  it_res->post_updater.Run();

  if (!key_remove)
    op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);

  if (key_remove) {
    if (enc == kEncodingListPack) {
      stats->listpack_blob_cnt--;
    }
    db_slice.Del(op_args.db_cntx, it_res->it);
  } else if (enc == kEncodingListPack) {
    stats->listpack_bytes += lpBytes((uint8_t*)pv.RObjPtr());
  }

  return deleted;
}

OpResult<vector<OptStr>> OpHMGet(const OpArgs& op_args, std::string_view key, CmdArgList fields) {
  DCHECK(!fields.empty());

  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);

  if (!it_res)
    return it_res.status();

  const PrimeValue& pv = (*it_res)->second;

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
      auto it = sm->Find(ToSV(fields[i]));
      if (it != sm->end()) {
        result[i].emplace(it->second, sdslen(it->second));
      }
    }
  }

  return result;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);

  if (it_res) {
    return HMapLength(op_args.db_cntx, (*it_res)->second);
  }

  if (it_res.status() == OpStatus::KEY_NOTFOUND)
    return 0;
  return it_res.status();
}

OpResult<int> OpExist(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);

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

  return sm->Contains(field) ? 1 : 0;
};

OpResult<string> OpGet(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);
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
  auto it = sm->Find(field);

  if (it == sm->end())
    return OpStatus::KEY_NOTFOUND;

  return string(it->second, sdslen(it->second));
}

OpResult<vector<string>> OpGetAll(const OpArgs& op_args, string_view key, uint8_t mask) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);
  if (!it_res) {
    if (it_res.status() == OpStatus::KEY_NOTFOUND)
      return vector<string>{};
    return it_res.status();
  }

  const PrimeValue& pv = (*it_res)->second;

  vector<string> res;
  bool keyval = (mask == (FIELDS | VALUES));

  if (pv.Encoding() == kEncodingListPack) {
    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    res.resize(lpLength(lp) / (keyval ? 1 : 2));

    uint8_t* fptr = lpFirst(lp);
    uint8_t intbuf[LP_INTBUF_SIZE];

    unsigned index = 0;
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

    res.reserve(sm->UpperBoundSize() * (keyval ? 2 : 1));
    for (const auto& k_v : *sm) {
      if (mask & FIELDS) {
        res.emplace_back(k_v.first, sdslen(k_v.first));
      }

      if (mask & VALUES) {
        res.emplace_back(k_v.second, sdslen(k_v.second));
      }
    }
  }

  // Empty hashmaps must be deleted, this case only triggers for expired values
  // and the enconding is guaranteed to be a DenseSet since we only support expiring
  // value with that enconding.
  if (res.empty()) {
    // post_updater will run immediately
    auto it = db_slice.FindMutable(op_args.db_cntx, key).it;

    db_slice.Del(op_args.db_cntx, it);
  }

  return res;
}

OpResult<size_t> OpStrLen(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);

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

  auto it = sm->Find(field);
  return it != sm->end() ? sdslen(it->second) : 0;
}

struct OpSetParams {
  bool skip_if_exists = false;
  uint32_t ttl = UINT32_MAX;
};

OpResult<uint32_t> OpSet(const OpArgs& op_args, string_view key, CmdArgList values,
                         const OpSetParams& op_sp = OpSetParams{}) {
  DCHECK(!values.empty() && 0 == values.size() % 2);
  VLOG(2) << "OpSet(" << key << ")";

  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;

  DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);

  uint8_t* lp = nullptr;
  auto& it = add_res.it;
  PrimeValue& pv = it->second;

  if (add_res.is_new) {
    if (op_sp.ttl == UINT32_MAX) {
      lp = lpNew(0);
      pv.InitRobj(OBJ_HASH, kEncodingListPack, lp);

      stats->listpack_blob_cnt++;
      stats->listpack_bytes += lpBytes(lp);
    } else {
      pv.InitRobj(OBJ_HASH, kEncodingStrMap2, CompactObj::AllocateMR<StringMap>());
    }
  } else {
    if (pv.ObjType() != OBJ_HASH)
      return OpStatus::WRONG_TYPE;

    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, it->second);
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
    size_t malloc_reserved = zmalloc_size(lp);
    size_t min_sz = EstimateListpackMinBytes(values);
    if (min_sz > malloc_reserved) {
      lp = (uint8_t*)zrealloc(lp, min_sz);
    }
    for (size_t i = 0; i < values.size(); i += 2) {
      tie(lp, inserted) = LpInsert(lp, ArgS(values, i), ArgS(values, i + 1), op_sp.skip_if_exists);
      created += inserted;
    }
    pv.SetRObjPtr(lp);
    stats->listpack_bytes += lpBytes(lp);
  } else {
    DCHECK_EQ(kEncodingStrMap2, pv.Encoding());  // Dictionary
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);
    sm->Reserve(values.size() / 2);
    bool added;

    for (size_t i = 0; i < values.size(); i += 2) {
      string_view field = ToSV(values[i]);
      string_view value = ToSV(values[i + 1]);
      if (op_sp.skip_if_exists)
        added = sm->AddOrSkip(field, value, op_sp.ttl);
      else
        added = sm->AddOrUpdate(field, value, op_sp.ttl);

      created += unsigned(added);
    }
  }

  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);

  return created;
}

void HGetGeneric(CmdArgList args, uint8_t getall_mask, Transaction* tx, SinkReplyBuilder* builder) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGetAll(t->GetOpArgs(shard), key, getall_mask);
  };

  OpResult<vector<string>> result = tx->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (result) {
    bool is_map = (getall_mask == (VALUES | FIELDS));
    rb->SendBulkStrArr(absl::Span<const string>{*result},
                       is_map ? RedisReplyBuilder::MAP : RedisReplyBuilder::ARRAY);
  } else {
    builder->SendError(result.status());
  }
}

OpResult<vector<long>> OpHExpire(const OpArgs& op_args, string_view key, uint32_t ttl_sec,
                                 CmdArgList values) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH);

  if (!op_res) {
    if (op_res.status() == OpStatus::KEY_NOTFOUND) {
      std::vector<long> res(values.size(), -2);
      return res;
    }
    return op_res.status();
  }

  PrimeValue* pv = &((*op_res).it->second);
  return HSetFamily::SetFieldsExpireTime(op_args, ttl_sec, key, values, pv);
}

// HSETEX key [NX] tll_sec field value field value ...
void HSetEx(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};

  string_view key = parser.Next();

  bool skip_if_exists = static_cast<bool>(parser.Check("NX"sv));
  string_view ttl_str = parser.Next();

  uint32_t ttl_sec;
  constexpr uint32_t kMaxTtl = (1UL << 26);

  if (!absl::SimpleAtoi(ttl_str, &ttl_sec) || ttl_sec == 0 || ttl_sec > kMaxTtl) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  CmdArgList fields = parser.Tail();

  if (fields.size() % 2 != 0) {
    return cmd_cntx.rb->SendError(facade::WrongNumArgsError(cmd_cntx.conn_cntx->cid->name()),
                                  kSyntaxErrType);
  }

  OpSetParams op_sp{skip_if_exists, ttl_sec};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, fields, op_sp);
  };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    cmd_cntx.rb->SendLong(*result);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

}  // namespace

void HSetFamily::HDel(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, args);
  };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    cmd_cntx.rb->SendLong(*result);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HLen(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    cmd_cntx.rb->SendLong(*result);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HExists(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<int> {
    return OpExist(t->GetOpArgs(shard), key, field);
  };

  OpResult<int> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    cmd_cntx.rb->SendLong(*result);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HExpire(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};
  string_view key = parser.Next();
  string_view ttl_str = parser.Next();
  uint32_t ttl_sec;
  constexpr uint32_t kMaxTtl = (1UL << 26);
  if (!absl::SimpleAtoi(ttl_str, &ttl_sec) || ttl_sec == 0 || ttl_sec > kMaxTtl) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }
  if (!static_cast<bool>(parser.Check("FIELDS"sv))) {
    return cmd_cntx.rb->SendError(
        "Mandatory argument FIELDS is missing or not at the right position", kSyntaxErrType);
  }

  string_view numFieldsStr = parser.Next();
  uint32_t numFields;
  if (!absl::SimpleAtoi(numFieldsStr, &numFields) || numFields == 0) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  CmdArgList fields = parser.Tail();
  if (fields.size() != numFields) {
    return cmd_cntx.rb->SendError("The `numfields` parameter must match the number of arguments",
                                  kSyntaxErrType);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpHExpire(t->GetOpArgs(shard), key, ttl_sec, fields);
  };

  OpResult<vector<long>> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    rb->StartArray(result->size());
    const auto& array = result.value();
    for (const auto& v : array) {
      rb->SendLong(v);
    }
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HMGet(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpHMGet(t->GetOpArgs(shard), key, args);
  };

  OpResult<vector<OptStr>> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (result) {
    SinkReplyBuilder::ReplyAggregator agg(rb);
    rb->StartArray(result->size());
    for (const auto& val : *result) {
      if (val) {
        rb->SendBulkString(*val);
      } else {
        rb->SendNull();
      }
    }
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    SinkReplyBuilder::ReplyAggregator agg(rb);

    rb->StartArray(args.size());
    for (unsigned i = 0; i < args.size(); ++i) {
      rb->SendNull();
    }
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HGet(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpGet(t->GetOpArgs(shard), key, field);
  };

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  OpResult<string> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    rb->SendBulkString(*result);
  } else {
    if (result.status() == OpStatus::KEY_NOTFOUND) {
      rb->SendNull();
    } else {
      cmd_cntx.rb->SendError(result.status());
    }
  }
}

void HSetFamily::HIncrBy(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);
  string_view incrs = ArgS(args, 2);
  int64_t ival = 0;

  if (!absl::SimpleAtoi(incrs, &ival)) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  IncrByParam param{ival};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, field, &param);
  };

  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OK) {
    cmd_cntx.rb->SendLong(get<int64_t>(param));
  } else {
    switch (status) {
      case OpStatus::INVALID_VALUE:
        cmd_cntx.rb->SendError("hash value is not an integer");
        break;
      case OpStatus::OUT_OF_RANGE:
        cmd_cntx.rb->SendError(kIncrOverflow);
        break;
      default:
        cmd_cntx.rb->SendError(status);
        break;
    }
  }
}

void HSetFamily::HIncrByFloat(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);
  string_view incrs = ArgS(args, 2);
  double dval = 0;

  if (!absl::SimpleAtod(incrs, &dval)) {
    return cmd_cntx.rb->SendError(kInvalidFloatErr);
  }

  IncrByParam param{dval};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, field, &param);
  };

  OpStatus status = cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OK) {
    auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
    rb->SendDouble(get<double>(param));
  } else {
    switch (status) {
      case OpStatus::INVALID_VALUE:
        cmd_cntx.rb->SendError("hash value is not a float");
        break;
      default:
        cmd_cntx.rb->SendError(status);
        break;
    }
  }
}

void HSetFamily::HKeys(CmdArgList args, const CommandContext& cmd_cntx) {
  HGetGeneric(args, FIELDS, cmd_cntx.tx, cmd_cntx.rb);
}

void HSetFamily::HVals(CmdArgList args, const CommandContext& cmd_cntx) {
  HGetGeneric(args, VALUES, cmd_cntx.tx, cmd_cntx.rb);
}

void HSetFamily::HGetAll(CmdArgList args, const CommandContext& cmd_cntx) {
  HGetGeneric(args, GetAllMode::FIELDS | GetAllMode::VALUES, cmd_cntx.tx, cmd_cntx.rb);
}

void HSetFamily::HScan(CmdArgList args, const CommandContext& cmd_cntx) {
  std::string_view key = ArgS(args, 0);
  std::string_view token = ArgS(args, 1);

  uint64_t cursor = 0;

  if (!absl::SimpleAtoi(token, &cursor)) {
    return cmd_cntx.rb->SendError("invalid cursor");
  }

  // HSCAN key cursor [MATCH pattern] [COUNT count]
  if (args.size() > 6) {
    DVLOG(1) << "got " << args.size() << " this is more than it should be";
    return cmd_cntx.rb->SendError(kSyntaxErr);
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(2));
  if (!ops) {
    DVLOG(1) << "HScan invalid args - return " << ops << " to the user";
    return cmd_cntx.rb->SendError(ops.status());
  }

  ScanOpts scan_op = ops.value();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScan(t->GetOpArgs(shard), key, &cursor, scan_op);
  };

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  OpResult<StringVec> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() != OpStatus::WRONG_TYPE) {
    rb->StartArray(2);
    rb->SendBulkString(absl::StrCat(cursor));
    rb->StartArray(result->size());  // Within scan the page type is array
    for (const auto& k : *result) {
      rb->SendBulkString(k);
    }
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HSet(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  string_view cmd{cmd_cntx.conn_cntx->cid->name()};

  if (args.size() % 2 != 1) {
    return cmd_cntx.rb->SendError(facade::WrongNumArgsError(cmd), kSyntaxErrType);
  }

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args);
  };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));

  if (result && cmd == "HSET") {
    cmd_cntx.rb->SendLong(*result);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HSetNx(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args, OpSetParams{.skip_if_exists = true});
  };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    cmd_cntx.rb->SendLong(*result);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void HSetFamily::HStrLen(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrLen(t->GetOpArgs(shard), key, field);
  };

  OpResult<size_t> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    cmd_cntx.rb->SendLong(*result);
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

void StrVecEmplaceBack(StringVec& str_vec, const listpackEntry& lp) {
  if (lp.sval) {
    str_vec.emplace_back(reinterpret_cast<char*>(lp.sval), lp.slen);
    return;
  }
  str_vec.emplace_back(absl::StrCat(lp.lval));
}

void HSetFamily::HRandField(CmdArgList args, const CommandContext& cmd_cntx) {
  if (args.size() > 3) {
    DVLOG(1) << "Wrong number of command arguments: " << args.size();
    return cmd_cntx.rb->SendError(kSyntaxErr);
  }

  string_view key = ArgS(args, 0);
  int32_t count;
  bool with_values = false;

  if ((args.size() > 1) && (!SimpleAtoi(ArgS(args, 1), &count))) {
    return cmd_cntx.rb->SendError("count value is not an integer", kSyntaxErrType);
  }

  if (args.size() == 3) {
    string arg = absl::AsciiStrToUpper(ArgS(args, 2));
    if (arg != "WITHVALUES")
      return cmd_cntx.rb->SendError(kSyntaxErr);
    else
      with_values = true;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<StringVec> {
    auto& db_slice = t->GetDbSlice(shard->shard_id());
    DbContext db_context = t->GetDbContext();
    auto it_res = db_slice.FindReadOnly(db_context, key, OBJ_HASH);

    if (!it_res)
      return it_res.status();

    const PrimeValue& pv = it_res.value()->second;
    StringVec str_vec;

    if (pv.Encoding() == kEncodingStrMap2) {
      StringMap* string_map = GetStringMap(pv, db_context);

      if (args.size() == 1) {
        auto opt_pair = string_map->RandomPair();
        if (opt_pair.has_value()) {
          auto [key, value] = *opt_pair;
          str_vec.emplace_back(key, sdslen(key));
        }
      } else {
        size_t actual_count =
            (count >= 0) ? std::min(size_t(count), string_map->UpperBoundSize()) : abs(count);
        std::vector<sds> keys, vals;
        if (count >= 0) {
          string_map->RandomPairsUnique(actual_count, keys, vals, with_values);
        } else {
          string_map->RandomPairs(actual_count, keys, vals, with_values);
        }
        for (size_t i = 0; i < actual_count; ++i) {
          str_vec.emplace_back(keys[i], sdslen(keys[i]));
          if (with_values) {
            str_vec.emplace_back(vals[i], sdslen(vals[i]));
          }
        }
      }

      if (string_map->Empty()) {  // Can happen if we use a TTL on hash members.
        // post_updater will run immediately
        auto it = db_slice.FindMutable(db_context, key).it;
        db_slice.Del(db_context, it);
        return facade::OpStatus::KEY_NOTFOUND;
      }
    } else if (pv.Encoding() == kEncodingListPack) {
      uint8_t* lp = (uint8_t*)pv.RObjPtr();
      size_t lplen = lpLength(lp);
      CHECK(lplen > 0 && lplen % 2 == 0);
      size_t hlen = lplen / 2;
      if (args.size() == 1) {
        listpackEntry key;
        lpRandomPair(lp, hlen, &key, NULL);
        StrVecEmplaceBack(str_vec, key);
      } else {
        size_t actual_count = (count >= 0) ? std::min(size_t(count), hlen) : abs(count);
        std::unique_ptr<listpackEntry[]> keys = nullptr, vals = nullptr;
        keys = std::make_unique<listpackEntry[]>(actual_count);
        if (with_values)
          vals = std::make_unique<listpackEntry[]>(actual_count);

        // count has been specified.
        if (count >= 0)
          // always returns unique entries.
          lpRandomPairsUnique(lp, actual_count, keys.get(), vals.get());
        else
          // allows non-unique entries.
          lpRandomPairs(lp, actual_count, keys.get(), vals.get());

        for (size_t i = 0; i < actual_count; ++i) {
          StrVecEmplaceBack(str_vec, keys[i]);
          if (with_values) {
            StrVecEmplaceBack(str_vec, vals[i]);
          }
        }
      }
    } else {
      LOG(FATAL) << "Invalid encoding " << pv.Encoding();
    }
    return str_vec;
  };

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  OpResult<StringVec> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result) {
    if ((result->size() == 1) && (args.size() == 1))
      rb->SendBulkString(result->front());
    else
      rb->SendBulkStrArr(*result, facade::RedisReplyBuilder::ARRAY);
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    if (args.size() == 1)
      rb->SendNull();
    else
      rb->SendEmptyArray();
  } else {
    cmd_cntx.rb->SendError(result.status());
  }
}

using CI = CommandId;

#define HFUNC(x) SetHandler(&HSetFamily::x)

namespace acl {
constexpr uint32_t kHDel = WRITE | HASH | FAST;
constexpr uint32_t kHLen = READ | HASH | FAST;
constexpr uint32_t kHExists = READ | HASH | FAST;
constexpr uint32_t kHGet = READ | HASH | FAST;
constexpr uint32_t kHGetAll = READ | HASH | SLOW;
constexpr uint32_t kHMGet = READ | HASH | FAST;
constexpr uint32_t kHMSet = WRITE | HASH | FAST;
constexpr uint32_t kHIncrBy = WRITE | HASH | FAST;
constexpr uint32_t kHIncrByFloat = WRITE | HASH | FAST;
constexpr uint32_t kHKeys = READ | HASH | SLOW;
constexpr uint32_t kHRandField = READ | HASH | SLOW;
constexpr uint32_t kHScan = READ | HASH | SLOW;
constexpr uint32_t kHSet = WRITE | HASH | FAST;
constexpr uint32_t kHSetEx = WRITE | HASH | FAST;
constexpr uint32_t kHSetNx = WRITE | HASH | FAST;
constexpr uint32_t kHStrLen = READ | HASH | FAST;
constexpr uint32_t kHExpire = WRITE | HASH | FAST;
constexpr uint32_t kHVals = READ | HASH | SLOW;
}  // namespace acl

void HSetFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();
  *registry
      << CI{"HDEL", CO::FAST | CO::WRITE, -3, 1, 1, acl::kHDel}.HFUNC(HDel)
      << CI{"HLEN", CO::FAST | CO::READONLY, 2, 1, 1, acl::kHLen}.HFUNC(HLen)
      << CI{"HEXISTS", CO::FAST | CO::READONLY, 3, 1, 1, acl::kHExists}.HFUNC(HExists)
      << CI{"HGET", CO::FAST | CO::READONLY, 3, 1, 1, acl::kHGet}.HFUNC(HGet)
      << CI{"HGETALL", CO::FAST | CO::READONLY, 2, 1, 1, acl::kHGetAll}.HFUNC(HGetAll)
      << CI{"HMGET", CO::FAST | CO::READONLY, -3, 1, 1, acl::kHMGet}.HFUNC(HMGet)
      << CI{"HMSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, acl::kHMSet}.HFUNC(HSet)
      << CI{"HINCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, acl::kHIncrBy}.HFUNC(HIncrBy)
      << CI{"HINCRBYFLOAT", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, acl::kHIncrByFloat}.HFUNC(
             HIncrByFloat)
      << CI{"HKEYS", CO::READONLY, 2, 1, 1, acl::kHKeys}.HFUNC(HKeys)
      << CI{"HEXPIRE", CO::WRITE | CO::FAST | CO::DENYOOM, -5, 1, 1, acl::kHExpire}.HFUNC(HExpire)
      << CI{"HRANDFIELD", CO::READONLY, -2, 1, 1, acl::kHRandField}.HFUNC(HRandField)
      << CI{"HSCAN", CO::READONLY, -3, 1, 1, acl::kHScan}.HFUNC(HScan)
      << CI{"HSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1, acl::kHSet}.HFUNC(HSet)
      << CI{"HSETEX", CO::WRITE | CO::FAST | CO::DENYOOM, -5, 1, 1, acl::kHSetEx}.SetHandler(HSetEx)
      << CI{"HSETNX", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1, acl::kHSetNx}.HFUNC(HSetNx)
      << CI{"HSTRLEN", CO::READONLY | CO::FAST, 3, 1, 1, acl::kHStrLen}.HFUNC(HStrLen)
      << CI{"HVALS", CO::READONLY, 2, 1, 1, acl::kHVals}.HFUNC(HVals);
}

StringMap* HSetFamily::ConvertToStrMap(uint8_t* lp) {
  StringMap* sm = CompactObj::AllocateMR<StringMap>();
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
    lp_elem = lpNext(lp, lp_elem);  // switch to next key

    // must be unique
    if (!sm->AddOrUpdate(key, value)) {
      uint8_t tmpbuf[LP_INTBUF_SIZE];

      uint8_t* it = lpFirst(lp);
      LOG(ERROR) << "Internal error while converting listpack to stringmap when inserting key: "
                 << key << " , listpack keys are:";
      do {
        string_view key = LpGetView(it, tmpbuf);
        LOG(ERROR) << "Listpack key: " << key;
        it = lpNext(lp, it);
        CHECK(it);  // value must exist
        it = lpNext(lp, it);
      } while (it);
      LOG(ERROR) << "Internal error, report to Dragonfly team! ------------";
    }
  } while (lp_elem);

  return sm;
}

// returns -1 if no expiry is associated with the field, -3 if no field is found.
int32_t HSetFamily::FieldExpireTime(const DbContext& db_context, const PrimeValue& pv,
                                    std::string_view field) {
  DCHECK_EQ(OBJ_HASH, pv.ObjType());

  if (pv.Encoding() == kEncodingListPack) {
    uint8_t intbuf[LP_INTBUF_SIZE];
    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    optional<string_view> res = LpFind(lp, field, intbuf);
    return res ? -1 : -3;
  } else {
    StringMap* string_map = (StringMap*)pv.RObjPtr();
    string_map->set_time(MemberTimeSeconds(db_context.time_now_ms));
    auto it = string_map->Find(field);
    if (it == string_map->end())
      return -3;
    return it.HasExpiry() ? it.ExpiryTime() : -1;
  }
}

vector<long> HSetFamily::SetFieldsExpireTime(const OpArgs& op_args, uint32_t ttl_sec,
                                             string_view key, CmdArgList values, PrimeValue* pv) {
  DCHECK_EQ(OBJ_HASH, pv->ObjType());
  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, *pv);

  if (pv->Encoding() == kEncodingListPack) {
    // a valid result can never be a listpack, since it doesnt keep ttl
    uint8_t* lp = (uint8_t*)pv->RObjPtr();
    auto& db_slice = op_args.GetDbSlice();
    DbTableStats* stats = db_slice.MutableStats(op_args.db_cntx.db_index);
    stats->listpack_bytes -= lpBytes(lp);
    stats->listpack_blob_cnt--;
    StringMap* sm = HSetFamily::ConvertToStrMap(lp);
    pv->InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
  }

  // This needs to be explicitly fetched again since the pv might have changed.
  StringMap* sm = container_utils::GetStringMap(*pv, op_args.db_cntx);
  vector<long> res = ExpireElements(sm, values, ttl_sec);
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, *pv);
  return res;
}

}  // namespace dfly
