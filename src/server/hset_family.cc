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
#include "core/detail/listpack_wrap.h"
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

OpStatus IncrementValue(optional<string_view> prev_val, IncrByParam* param) {
  if (holds_alternative<double>(*param)) {
    double incr = get<double>(*param);
    double value = 0;

    if (prev_val) {
      if (!ParseDouble(*prev_val, &value)) {
        return OpStatus::INVALID_VALUE;
      }
    }
    value += incr;
    if (isnan(value) || isinf(value)) {
      return OpStatus::NAN_OR_INF_DURING_INCR;
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
}

OpStatus OpIncrBy(const OpArgs& op_args, string_view key, string_view field, IncrByParam* param) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(op_res);

  auto& add_res = *op_res;
  PrimeValue& pv = add_res.it->second;
  if (add_res.is_new) {
    pv.InitRobj(OBJ_HASH, kEncodingListPack, lpNew(0));
  } else {
    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, add_res.it->second);

    if (pv.Encoding() == kEncodingListPack) {
      uint8_t* lp = (uint8_t*)pv.RObjPtr();
      size_t lpb = lpBytes(lp);

      if (lpb >= server.max_listpack_map_bytes) {
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

// TODO: Make fully automatic + absl cleanup
struct KeyCleanup {
  using CleanupFuncT = std::function<void(std::string_view)>;
  explicit KeyCleanup(CleanupFuncT func, const std::string_view key_view)
      : f{std::move(func)}, key{key_view} {
  }
  ~KeyCleanup() {
    if (armed) {
      f(key);
    }
  }

  void arm() {
    armed = true;
  }

  CleanupFuncT f;
  std::string key;
  bool armed{false};
};

void DeleteKey(DbSlice& db_slice, const OpArgs& op_args, std::string_view key) {
  if (auto del_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH); del_it) {
    del_it->post_updater.Run();
    db_slice.Del(op_args.db_cntx, del_it->it);
    if (op_args.shard->journal()) {
      RecordJournal(op_args, "DEL"sv, {key});
    }
  }
}

std::pair<OpResult<DbSlice::ConstIterator>, KeyCleanup> FindReadOnly(DbSlice& db_slice,
                                                                     const OpArgs& op_args,
                                                                     std::string_view key) {
  return std::pair{db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH),
                   KeyCleanup{[&](const auto& k) { DeleteKey(db_slice, op_args, k); }, key}};
}

// The find and contains functions perform the usual search on string maps, with the added argument
// KeyCleanup. This object is armed if the string map becomes empty during search due to keys being
// expired. An armed object on destruction removes the key which has just become empty.
StringMap::iterator Find(StringMap* sm, const std::string_view field, KeyCleanup& defer_cleanup) {
  auto it = sm->Find(field);
  if (sm->Empty()) {
    defer_cleanup.arm();
  }
  return it;
}

bool Contains(StringMap* sm, const std::string_view field, KeyCleanup& defer_cleanup) {
  auto result = sm->Contains(field);
  if (sm->Empty()) {
    defer_cleanup.arm();
  }
  return result;
}

OpResult<StringVec> OpScan(const OpArgs& op_args, std::string_view key, uint64_t* cursor,
                           const ScanOpts& scan_op) {
  constexpr size_t HASH_TABLE_ENTRIES_FACTOR = 2;  // return key/value

  /* We set the max number of iterations to ten times the specified
   * COUNT, so if the hash table is in a pathological state (very
   * sparsely populated) we avoid to block too much time at the cost
   * of returning no or very few elements. (taken from redis code at db.c line 904 */
  constexpr size_t INTERATION_FACTOR = 10;

  DbSlice& db_slice = op_args.GetDbSlice();
  auto [find_res, defer_cleanup] = FindReadOnly(db_slice, op_args, key);

  if (!find_res) {
    DVLOG(1) << "ScanOp: find failed: " << find_res << ", baling out";
    *cursor = 0;
    return find_res.status();
  }

  auto it = find_res.value();
  StringVec res;
  uint32_t count = scan_op.limit * HASH_TABLE_ENTRIES_FACTOR;
  const PrimeValue& pv = it->second;

  if (pv.Encoding() == kEncodingListPack) {
    // TODO: Optimize unnecessary value reads
    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    for (const auto [key, value] : lw) {
      if (scan_op.Matches(key)) {
        res.emplace_back(key);
        res.emplace_back(value);
      }
    }
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
        val = StringMap::GetValue(val);
        res.emplace_back(val, sdslen(val));
      }
    };

    do {
      *cursor = sm->Scan(*cursor, scanCb);
    } while (*cursor && max_iterations-- && res.size() < count);

    if (sm->Empty()) {
      defer_cleanup.arm();
    }
  }

  return res;
}

OpResult<uint32_t> OpDel(const OpArgs& op_args, string_view key, CmdArgList values) {
  DCHECK(!values.empty());

  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(it_res);

  PrimeValue& pv = it_res->it->second;
  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv);

  unsigned deleted = 0;
  bool key_remove = false;
  unsigned enc = pv.Encoding();

  if (enc == kEncodingListPack) {
    uint8_t* lp = (uint8_t*)pv.RObjPtr();
    for (auto s : values) {
      auto res = LpDelete(lp, s);
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
      if (sm->Erase(s)) {
        ++deleted;
      }

      // Even if the previous Erase op did not erase anything, it can remove expired fields as a
      // side effect.
      if (sm->Empty()) {
        key_remove = true;
        break;
      }
    }
  }

  it_res->post_updater.Run();

  if (!key_remove)
    op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);

  if (key_remove) {
    db_slice.Del(op_args.db_cntx, it_res->it);
  }

  return deleted;
}

OpResult<vector<OptStr>> OpHMGet(const OpArgs& op_args, std::string_view key, CmdArgList fields) {
  DCHECK(!fields.empty());

  auto& db_slice = op_args.GetDbSlice();
  auto [it_res, defer_cleanup] = FindReadOnly(db_slice, op_args, key);
  RETURN_ON_BAD_STATUS(it_res);

  const PrimeValue& pv = (*it_res)->second;

  std::vector<OptStr> result(fields.size());

  if (pv.Encoding() == kEncodingListPack) {
    absl::flat_hash_map<string_view, absl::InlinedVector<size_t, 3>> reverse;
    reverse.reserve(fields.size() + 1);
    for (size_t i = 0; i < fields.size(); ++i) {
      reverse[ArgS(fields, i)].push_back(i);  // map fields to their index.
    }

    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    for (const auto [key, value] : lw) {
      if (auto it = reverse.find(key); it != reverse.end()) {
        for (size_t index : it->second) {
          DCHECK_LT(index, result.size());
          result[index].emplace(value);
        }
      }
    }
  } else {
    DCHECK_EQ(kEncodingStrMap2, pv.Encoding());
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);

    for (size_t i = 0; i < fields.size(); ++i) {
      if (auto it = Find(sm, fields[i], defer_cleanup); it != sm->end()) {
        result[i].emplace(it->second, sdslen(it->second));
      }
    }
  }

  return result;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(it_res);

  const auto& co = (*it_res)->second;
  if (co.Encoding() == kEncodingStrMap2) {
    StringMap* sm = GetStringMap(co, op_args.db_cntx);
    return sm->UpperBoundSize();
  } else {
    DCHECK_EQ(kEncodingListPack, co.Encoding());
    return detail::ListpackWrap{static_cast<uint8_t*>(co.RObjPtr())}.size();
  }
}

OpResult<uint32_t> OpExist(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.GetDbSlice();
  auto [it_res, defer_cleanup] = FindReadOnly(db_slice, op_args, key);
  RETURN_ON_BAD_STATUS(it_res);

  const PrimeValue& pv = (*it_res)->second;
  if (pv.Encoding() == kEncodingListPack) {
    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    return lw.Find(field) != lw.end() ? 1 : 0;
  } else {
    DCHECK_EQ(kEncodingStrMap2, pv.Encoding());
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);
    return Contains(sm, field, defer_cleanup) ? 1 : 0;
  }
}

OpResult<uint32_t> OpStrLen(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.GetDbSlice();
  auto [it_res, defer_cleanup] = FindReadOnly(db_slice, op_args, key);
  RETURN_ON_BAD_STATUS(it_res);

  const PrimeValue& pv = (*it_res)->second;
  if (pv.Encoding() == kEncodingListPack) {
    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    if (auto it = lw.Find(field); it != lw.end())
      return (*it).second.size();
  } else {
    DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);
    if (const auto it = Find(sm, field, defer_cleanup); it != sm->end())
      return sdslen(it->second);
  }
  return OpStatus::KEY_NOTFOUND;
}

OpResult<string> OpGet(const OpArgs& op_args, string_view key, string_view field) {
  auto& db_slice = op_args.GetDbSlice();
  auto [it_res, defer_cleanup] = FindReadOnly(db_slice, op_args, key);
  RETURN_ON_BAD_STATUS(it_res);

  const PrimeValue& pv = (*it_res)->second;
  if (pv.Encoding() == kEncodingListPack) {
    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    if (auto it = lw.Find(field); it != lw.end())
      return string{(*it).second};
  } else {
    DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);
    if (const auto it = Find(sm, field, defer_cleanup); it != sm->end())
      return string(it->second, sdslen(it->second));
  }
  return OpStatus::KEY_NOTFOUND;
}

OpResult<vector<string>> OpGetAll(const OpArgs& op_args, string_view key, uint8_t mask) {
  auto& db_slice = op_args.GetDbSlice();
  auto it_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(it_res);

  const PrimeValue& pv = (*it_res)->second;

  vector<string> res;
  bool keyval = (mask == (FIELDS | VALUES));

  if (pv.Encoding() == kEncodingListPack) {
    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    res.resize(lw.size() * (keyval ? 2 : 1));
    size_t index = 0;
    for (const auto [key, value] : lw) {
      if (mask & FIELDS)
        res[index++] = key;
      if (mask & VALUES)
        res[index++] = value;
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
    DeleteKey(db_slice, op_args, key);
  }

  return res;
}

struct OpSetParams {
  bool skip_if_exists = false;
  uint32_t ttl = UINT32_MAX;
  bool keepttl = false;
};

OpResult<uint32_t> OpSet(const OpArgs& op_args, string_view key, CmdArgList values,
                         const OpSetParams& op_sp = OpSetParams{}) {
  DCHECK(!values.empty() && 0 == values.size() % 2);
  VLOG(2) << "OpSet(" << key << ")";

  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.AddOrFind(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;

  uint8_t* lp = nullptr;
  auto& it = add_res.it;
  PrimeValue& pv = it->second;

  if (add_res.is_new) {
    if (op_sp.ttl == UINT32_MAX) {
      lp = lpNew(0);
      pv.InitRobj(OBJ_HASH, kEncodingListPack, lp);
    } else {
      pv.InitRobj(OBJ_HASH, kEncodingStrMap2, CompactObj::AllocateMR<StringMap>());
    }
  } else {
    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, it->second);
  }

  if (pv.Encoding() == kEncodingListPack) {
    lp = (uint8_t*)pv.RObjPtr();

    if (op_sp.ttl != UINT32_MAX || !IsGoodForListpack(values, lp)) {
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
  } else {
    DCHECK_EQ(kEncodingStrMap2, pv.Encoding());  // Dictionary
    StringMap* sm = GetStringMap(pv, op_args.db_cntx);
    sm->Reserve(values.size() / 2);
    bool added;

    for (size_t i = 0; i < values.size(); i += 2) {
      string_view field = values[i];
      string_view value = values[i + 1];
      if (op_sp.skip_if_exists)
        added = sm->AddOrSkip(field, value, op_sp.ttl);
      else
        added = sm->AddOrUpdate(field, value, op_sp.ttl, op_sp.keepttl);

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
  OpResult<vector<string>> result = tx->ScheduleSingleHopT(cb);

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  switch (result.status()) {
    case OpStatus::OK:
    case OpStatus::KEY_NOTFOUND: {
      bool is_map = (getall_mask == (VALUES | FIELDS));
      return rb->SendBulkStrArr(*result,
                                is_map ? RedisReplyBuilder::MAP : RedisReplyBuilder::ARRAY);
    }
    default:
      return rb->SendError(result.status());
  };
}

OpResult<vector<long>> OpHExpire(const OpArgs& op_args, string_view key, uint32_t ttl_sec,
                                 CmdArgList values) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue* pv = &((*op_res).it->second);
  return HSetFamily::SetFieldsExpireTime(op_args, ttl_sec, key, values, pv);
}

// HSETEX key [NX] [KEEPTTL] tll_sec field value field value ...
void HSetEx(CmdArgList args, const CommandContext& cmd_cntx) {
  CmdArgParser parser{args};

  string_view key = parser.Next();
  OpSetParams op_sp;

  const auto option_already_set = [&cmd_cntx] {
    return cmd_cntx.rb->SendError(WrongNumArgsError(cmd_cntx.conn_cntx->cid->name()));
  };

  while (true) {
    if (parser.Check("NX")) {
      if (op_sp.skip_if_exists) {
        return option_already_set();
      }
      op_sp.skip_if_exists = true;
    } else if (parser.Check("KEEPTTL")) {
      if (op_sp.keepttl) {
        return option_already_set();
      }
      op_sp.keepttl = true;
    } else {
      break;
    }
  }

  op_sp.ttl = parser.Next<uint32_t>();

  if (parser.HasError()) {
    return cmd_cntx.rb->SendError(parser.TakeError().MakeReply());
  }

  constexpr uint32_t kMaxTtl = (1UL << 26);
  if (op_sp.ttl == 0 || op_sp.ttl > kMaxTtl) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }

  CmdArgList fields = parser.Tail();

  if (fields.size() % 2 != 0) {
    return cmd_cntx.rb->SendError(facade::WrongNumArgsError(cmd_cntx.conn_cntx->cid->name()),
                                  kSyntaxErrType);
  }

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

struct HSetReplies {
  void Send(OpResult<uint32_t> result) const {
    switch (result.status()) {
      case OpStatus::OK:
      case OpStatus::KEY_NOTFOUND:
        return rb->SendLong(result.value_or(0));
      default:
        return rb->SendError(result.status());
    };
  }

  facade::SinkReplyBuilder* rb;
};

}  // namespace

void HSetFamily::HDel(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, args.subspan(1));
  };
  HSetReplies{cmd_cntx.rb}.Send(cmd_cntx.tx->ScheduleSingleHopT(cb));
}

void HSetFamily::HLen(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };
  HSetReplies{cmd_cntx.rb}.Send(cmd_cntx.tx->ScheduleSingleHopT(cb));
}

void HSetFamily::HExists(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpExist(t->GetOpArgs(shard), key, field);
  };
  HSetReplies{cmd_cntx.rb}.Send(cmd_cntx.tx->ScheduleSingleHopT(cb));
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

  if (auto err = parser.TakeError(); err)
    return cmd_cntx.rb->SendError(err.MakeReply());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpHExpire(t->GetOpArgs(shard), key, ttl_sec, fields);
  };
  OpResult<vector<long>> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  switch (result.status()) {
    case OpStatus::OK:
      return rb->SendLongArr(absl::MakeConstSpan(result.value()));
    case OpStatus::KEY_NOTFOUND:
      return rb->SendLongArr(absl::MakeConstSpan(vector<long>(numFields, -2)));
    default:
      return rb->SendError(result.status());
  };
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
    RedisReplyBuilder::ArrayScope scope{rb, result->size()};
    for (const auto& val : *result) {
      if (val) {
        rb->SendBulkString(*val);
      } else {
        rb->SendNull();
      }
    }
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    RedisReplyBuilder::ArrayScope scope{rb, args.size()};
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
  OpResult<string> result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  switch (result.status()) {
    case OpStatus::OK:
      return rb->SendBulkString(*result);
    case OpStatus::KEY_NOTFOUND:
      return rb->SendNull();
    default:
      return rb->SendError(result.status());
  };
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

  const ScanOpts& scan_op = ops.value();

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpScan(t->GetOpArgs(shard), key, &cursor, scan_op);
  };

  OpResult<StringVec> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (result.status() == OpStatus::WRONG_TYPE)
    return cmd_cntx.rb->SendError(result.status());

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  RedisReplyBuilder::ArrayScope scope{rb, 2};
  rb->SendBulkString(absl::StrCat(cursor));
  rb->SendBulkStrArr(*result);
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

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args.subspan(1), OpSetParams{.skip_if_exists = true});
  };
  HSetReplies{cmd_cntx.rb}.Send(cmd_cntx.tx->ScheduleSingleHopT(cb));
}

void HSetFamily::HStrLen(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpStrLen(t->GetOpArgs(shard), key, field);
  };
  HSetReplies{cmd_cntx.rb}.Send(cmd_cntx.tx->ScheduleSingleHopT(cb));
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
    if (result->size() == 1 && args.size() == 1)
      rb->SendBulkString(result->front());
    else if (with_values) {
      const auto result_size = result->size();
      DCHECK(result_size % 2 == 0)
          << "unexpected size of strings " << result_size << ", expected pairs";
      SinkReplyBuilder::ReplyScope scope{rb};
      const bool is_resp3 = rb->IsResp3();
      rb->StartArray(is_resp3 ? result_size / 2 : result_size);
      for (size_t i = 0; i < result_size; i += 2) {
        if (is_resp3)
          rb->StartArray(2);
        rb->SendBulkString((*result)[i]);
        rb->SendBulkString((*result)[i + 1]);
      }
    } else
      rb->SendBulkStrArr(*result, RedisReplyBuilder::ARRAY);
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

void HSetFamily::Register(CommandRegistry* registry) {
  registry->StartFamily(acl::HASH);
  *registry << CI{"HDEL", CO::FAST | CO::WRITE, -3, 1, 1}.HFUNC(HDel)
            << CI{"HLEN", CO::FAST | CO::READONLY, 2, 1, 1}.HFUNC(HLen)
            << CI{"HEXISTS", CO::FAST | CO::READONLY, 3, 1, 1}.HFUNC(HExists)
            << CI{"HGET", CO::FAST | CO::READONLY, 3, 1, 1}.HFUNC(HGet)
            << CI{"HGETALL", CO::FAST | CO::READONLY, 2, 1, 1}.HFUNC(HGetAll)
            << CI{"HMGET", CO::FAST | CO::READONLY, -3, 1, 1}.HFUNC(HMGet)
            << CI{"HMSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1}.HFUNC(HSet)
            << CI{"HINCRBY", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(HIncrBy)
            << CI{"HINCRBYFLOAT", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(HIncrByFloat)
            << CI{"HKEYS", CO::READONLY, 2, 1, 1}.HFUNC(HKeys)
            << CI{"HEXPIRE", CO::WRITE | CO::FAST | CO::DENYOOM, -5, 1, 1}.HFUNC(HExpire)
            << CI{"HRANDFIELD", CO::READONLY, -2, 1, 1}.HFUNC(HRandField)
            << CI{"HSCAN", CO::READONLY, -3, 1, 1}.HFUNC(HScan)
            << CI{"HSET", CO::WRITE | CO::FAST | CO::DENYOOM, -4, 1, 1}.HFUNC(HSet)
            << CI{"HSETEX", CO::WRITE | CO::FAST | CO::DENYOOM, -5, 1, 1}.SetHandler(HSetEx)
            << CI{"HSETNX", CO::WRITE | CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(HSetNx)
            << CI{"HSTRLEN", CO::READONLY | CO::FAST, 3, 1, 1}.HFUNC(HStrLen)
            << CI{"HVALS", CO::READONLY, 2, 1, 1}.HFUNC(HVals);
}

StringMap* HSetFamily::ConvertToStrMap(uint8_t* lp) {
  StringMap* sm = CompactObj::AllocateMR<StringMap>();

  detail::ListpackWrap lw{lp};
  sm->Reserve(lw.size());

  for (const auto [key, value] : lw) {
    if (!sm->AddOrUpdate(key, value)) {  // Must be unique
      LOG(ERROR) << "Internal error while converting listpack to stringmap when inserting key: "
                 << key << " , listpack keys are:";
      for (const auto [key2, _] : lw)
        LOG(ERROR) << key2;
      LOG(FATAL) << "Internal error, report to Dragonfly team! ------------";
    }
  }
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
