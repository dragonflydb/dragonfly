// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hset_family.h"

#include <absl/strings/ascii.h>

extern "C" {
#include "redis/listpack.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
#include "redis/zmalloc.h"
}

#include "base/logging.h"
#include "core/detail/listpack_wrap.h"
#include "core/overloaded.h"
#include "core/string_map.h"
#include "facade/cmd_arg_parser.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
#include "server/tiered_storage.h"
#include "server/tiering/decoders.h"
#include "server/tiering/serialized_map.h"
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

// Generic wrapper for multiple underlying map <string, string> types
// holding a variant of:
// 1. Listpack
// 2. StringMap
// 3. SerializedMap (tiered)
struct HMapWrap {
 private:
  template <typename F> decltype(auto) VisitRef(F f) const {  // Cast T* to T&
    return std::visit(Overloaded{[&f](auto* s) { return f(*s); }, f}, impl_);
  }

  template <typename F> decltype(auto) VisitMut(F& f) {
    auto serialized_bust = [&](tiering::SerializedMap* s) {
      ABSL_UNREACHABLE();                          // Serialized maps should never be mutable
      return f(static_cast<StringMap*>(nullptr));  // purely for same return type
    };
    return std::visit(Overloaded{f, serialized_bust}, impl_);
  }

 public:
  HMapWrap(const PrimeValue& pv, DbContext db_cntx) {
    DCHECK(!pv.IsExternal() || pv.IsCool());
    if (pv.Encoding() == kEncodingListPack)
      impl_ = detail::ListpackWrap{static_cast<uint8_t*>(pv.RObjPtr())};
    else
      impl_ = GetStringMap(pv, db_cntx);
  }

  explicit HMapWrap(tiering::SerializedMap* sm) : impl_{sm} {
  }

  size_t Length() const {
    Overloaded ov{
        [](StringMap* s) { return s->UpperBoundSize(); },
        [](const detail::ListpackWrap& lw) { return lw.size(); },
        [](tiering::SerializedMap* s) { return s->size(); },
    };
    return visit(ov, impl_);
  }

  auto Find(std::string_view key) const {
    using RT = optional<pair<string_view, string_view>>;
    return VisitRef([key](auto& h) -> RT {
      if (auto it = h.Find(key); it != h.end())
        return *it;
      return std::nullopt;
    });
  }

  auto Range() const {
    auto f = [](auto p) -> pair<string_view, string_view> { return p; };  // implicit conversion
    using IT = base::it::CompoundIterator<decltype(f), detail::ListpackWrap::Iterator,
                                          StringMap::iterator, tiering::SerializedMap::Iterator>;
    auto cb = [f](auto& h) -> std::pair<IT, IT> {
      return {{f, h.begin()}, {std::nullopt, h.end()}};
    };
    return base::it::Range(VisitRef(cb));
  }

  bool Erase(std::string_view key) {
    Overloaded ov{[key](StringMap* s) { return s->Erase(key); },
                  [key](detail::ListpackWrap& lw) { return lw.Delete(key); }};
    return VisitMut(ov);
  }

  void AddOrUpdate(std::string_view key, std::string_view value) {
    Overloaded ov{[&](StringMap* sm) { sm->AddOrUpdate(key, value, UINT32_MAX, true); },
                  [&](detail::ListpackWrap& lw) { lw.Insert(key, value, false); }};
    VisitMut(ov);
  }

  void Launder(PrimeValue& pv) {
    Overloaded ov{
        [](StringMap* s) {},
        [&](detail::ListpackWrap& lw) { pv.SetRObjPtr(lw.GetPointer()); },
    };
    VisitMut(ov);
  }

  template <typename T> optional<T> Get() const {
    if (holds_alternative<T>(impl_))
      return get<T>(impl_);
    return nullopt;
  }

 private:
  variant<StringMap*, tiering::SerializedMap*, detail::ListpackWrap> impl_;
};  // namespace dfly

// Delete if length is zero
void DeleteHw(HMapWrap& hw, const OpArgs& op_args, std::string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  if (auto del_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH); del_it) {
    del_it->post_updater.Run();
    db_slice.Del(op_args.db_cntx, del_it->it);
    if (op_args.shard->journal()) {
      RecordJournal(op_args, "DEL"sv, {key});
    }
  }
}

auto KeyAndArgs(Transaction* t, EngineShard* es) {
  return std::make_pair(t->GetShardArgs(es->shard_id()).Front(), t->GetOpArgs(es));
}

// A wrappable callback returns a OpResult<T> or the future version of it for tiered values.
// Because the top-level value needs to be an OpResult, the variant is wrapped as an OpResult again.
// However, we can take the "result" out of the bare value and keep it only on the top-level.
template <typename T> using CbVariant = std::variant<T, ::util::fb2::Future<OpResult<T>>>;

// Unwrap possibly future result to a regular one
template <typename T> OpResult<T> Unwrap(OpResult<CbVariant<T>> result) {
  if (!result.ok())
    return result.status();

  Overloaded ov{
      [](T res) -> OpResult<T> { return res; },
      [](util::fb2::Future<OpResult<T>> fut) -> OpResult<T> { return fut.Get(); },
  };
  return visit(ov, std::move(result).value());
}

// Execute callback on generic HMapWrap, possibly on offloaded value and waiting for result
template <typename F, typename T = typename std::invoke_result_t<F, HMapWrap>::Type>
OpResult<T> ExecuteRO(Transaction* tx, F&& f) {
  auto shard_cb = [f = std::forward<F>(f)](Transaction* t,
                                           EngineShard* es) -> OpResult<CbVariant<T>> {
    // Fetch value of hash type
    auto [key, op_args] = KeyAndArgs(t, es);
    auto it_res = op_args.GetDbSlice().FindReadOnly(op_args.db_cntx, key, OBJ_HASH);
    RETURN_ON_BAD_STATUS(it_res);
    auto& pv = (*it_res)->second;

    // Enqueue read for future values
    if (pv.IsExternal() && !pv.IsCool()) {
      using D = tiering::SerializedMapDecoder;
      util::fb2::Future<OpResult<T>> fut;
      auto read_cb = [fut, f = std::move(f)](io::Result<D*> res) mutable {
        HMapWrap hw{res.value()->Get()};
        fut.Resolve(f(hw));
      };

      es->tiered_storage()->Read(op_args.db_cntx.db_index, key, pv.GetExternalSlice(), D{},
                                 std::move(read_cb));
      return CbVariant<T>{std::move(fut)};
    }

    HMapWrap hw{pv, op_args.db_cntx};
    auto res = f(hw);

    if (hw.Length() == 0)  // Expirations might have emptied it
      DeleteHw(hw, op_args, key);

    // Move result into variant or keep error status
    RETURN_ON_BAD_STATUS(res);
    return CbVariant<T>{std::move(res).value()};
  };

  return Unwrap(tx->ScheduleSingleHopT(std::move(shard_cb)));
}

// Wrap write handler
template <typename F> auto WrapW(F&& f) {
  using RT = std::invoke_result_t<F, HMapWrap&>;
  return [f = std::forward<F>(f)](Transaction* t, EngineShard* es) -> RT {
    auto [key, op_args] = KeyAndArgs(t, es);

    auto it_res = op_args.GetDbSlice().FindMutable(op_args.db_cntx, key, OBJ_HASH);
    RETURN_ON_BAD_STATUS(it_res);
    auto& pv = it_res->it->second;

    // Remove document before modification
    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv);

    HMapWrap hw{pv, op_args.db_cntx};
    auto res = f(hw);
    hw.Launder(pv);

    // Run post updater
    it_res->post_updater.Run();

    if (hw.Length() == 0)
      DeleteHw(hw, op_args, key);
    else
      op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, &pv);

    return res;
  };
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

  HMapWrap hw{pv, op_args.db_cntx};
  optional<string_view> res;
  if (!add_res.is_new) {
    if (auto it = hw.Find(field); it)
      res = it->second;
  }

  if (OpStatus status = IncrementValue(res, param); status != OpStatus::OK)
    return status;

  if (holds_alternative<double>(*param)) {
    double new_val = get<double>(*param);
    char buf[128];
    char* str = RedisReplyBuilder::FormatDouble(new_val, buf, sizeof(buf));
    hw.AddOrUpdate(field, str);
  } else {  // integer increment
    int64_t new_val = get<int64_t>(*param);
    absl::AlphaNum an(new_val);
    hw.AddOrUpdate(field, an.Piece());
  }

  hw.Launder(pv);
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, &pv);

  return OpStatus::OK;
}

OpResult<StringVec> OpScan(const HMapWrap& hw, uint64_t* cursor, const ScanOpts& scan_op) {
  /* We set the max number of iterations to ten times the specified
   * COUNT, so if the hash table is in a pathological state (very
   * sparsely populated) we avoid to block too much time at the cost
   * of returning no or very few elements. (taken from redis code at db.c line 904 */
  constexpr size_t INTERATION_FACTOR = 10;

  StringVec res;
  // If NOVALUES, we expect 1 element per match (key). Otherwise, 2 elements (key + value).
  uint32_t count = scan_op.limit * (scan_op.novalues ? 1 : 2);

  if (auto lw = hw.Get<detail::ListpackWrap>(); lw) {
    // TODO: Optimize unnecessary value reads from iterator
    for (const auto [key, value] : *lw) {
      if (scan_op.Matches(key)) {
        res.emplace_back(key);
        if (!scan_op.novalues) {
          res.emplace_back(value);
        }
      }
    }
    *cursor = 0;
  } else {
    StringMap* sm = *hw.Get<StringMap*>();

    long max_iterations = count * INTERATION_FACTOR;

    // note about this lambda - don't capture here! it should be convertible to C function!
    auto scanCb = [&](const void* obj) {
      sds val = (sds)obj;
      size_t len = sdslen(val);
      if (scan_op.Matches(string_view(val, len))) {
        res.emplace_back(val, len);
        if (!scan_op.novalues) {
          val = StringMap::GetValue(val);
          res.emplace_back(val, sdslen(val));
        }
      }
    };

    do {
      *cursor = sm->Scan(*cursor, scanCb);
    } while (*cursor && max_iterations-- && res.size() < count);
  }

  return res;
}

OpResult<vector<OptStr>> OpHMGet(const HMapWrap& hw, CmdArgList fields) {
  DCHECK(!fields.empty());

  std::vector<OptStr> result(fields.size());
  if (auto lw = hw.Get<detail::ListpackWrap>(); lw) {
    absl::flat_hash_map<string_view, absl::InlinedVector<size_t, 3>> reverse;
    reverse.reserve(fields.size() + 1);
    for (size_t i = 0; i < fields.size(); ++i) {
      reverse[ArgS(fields, i)].push_back(i);  // map fields to their index.
    }

    for (const auto [key, value] : *lw) {
      if (auto it = reverse.find(key); it != reverse.end()) {
        for (size_t index : it->second) {
          DCHECK_LT(index, result.size());
          result[index].emplace(value);
        }
      }
    }
  } else {
    StringMap* sm = *hw.Get<StringMap*>();
    for (size_t i = 0; i < fields.size(); ++i) {
      if (auto it = sm->Find(fields[i]); it != sm->end()) {
        result[i].emplace(it->second, sdslen(it->second));
      }
    }
  }

  return result;
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
    size_t malloc_reserved = zmalloc_size(lp);
    size_t min_sz = EstimateListpackMinBytes(values);
    if (min_sz > malloc_reserved) {
      lp = (uint8_t*)zrealloc(lp, min_sz);
    }
    detail::ListpackWrap lw{lp};
    for (size_t i = 0; i < values.size(); i += 2) {
      created += lw.Insert(values[i], values[i + 1], op_sp.skip_if_exists);
    }
    pv.SetRObjPtr(lw.GetPointer());
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

  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, &pv);

  if (auto* ts = op_args.shard->tiered_storage(); ts) {
    StashPrimeValue(op_args.db_cntx.db_index, key, false, &pv, ts);
  }

  return created;
}

void HGetGeneric(CmdArgList args, uint8_t getall_mask, CommandContext* cmd_cntx) {
  auto cb = [getall_mask](const HMapWrap& hw) -> OpResult<vector<string>> {
    vector<string> res;
    bool keyval = (getall_mask == (FIELDS | VALUES));
    res.reserve(hw.Length() * (keyval ? 2 : 1));

    for (const auto& [key, value] : hw.Range()) {
      if (getall_mask & FIELDS)
        res.emplace_back(key);
      if (getall_mask & VALUES)
        res.emplace_back(value);
    }

    return res;
  };

  OpResult<vector<string>> result = ExecuteRO(cmd_cntx->tx(), cb);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  switch (result.status()) {
    case OpStatus::OK:
    case OpStatus::KEY_NOTFOUND: {
      bool is_map = (getall_mask == (VALUES | FIELDS));
      return rb->SendBulkStrArr(*result, is_map ? CollectionType::MAP : CollectionType::ARRAY);
    }
    default:
      return cmd_cntx->SendError(result.status());
  };
}

OpResult<vector<long>> OpHExpire(const OpArgs& op_args, string_view key, uint32_t ttl_sec,
                                 ExpireFlags flags, CmdArgList values) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue* pv = &((*op_res).it->second);
  auto res = HSetFamily::SetFieldsExpireTime(op_args, ttl_sec, flags, key, values, pv);

  // If it is a hash which became empty after expiring fields, we must delete the key safely.
  // We use DelMutable which consumes the iterator/updater to prevent the crash.
  if (pv->Encoding() == kEncodingStrMap2) {
    auto* sm = static_cast<StringMap*>(pv->RObjPtr());
    if (sm->UpperBoundSize() == 0) {
      db_slice.DelMutable(op_args.db_cntx, std::move(*op_res));
    }
  }

  return res;
}

// HSETEX key [NX] [KEEPTTL] tll_sec field value field value ...
void HSetEx(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser{args};

  string_view key = parser.Next();
  OpSetParams op_sp;

  const auto option_already_set = [&cmd_cntx] {
    return cmd_cntx->SendError(WrongNumArgsError(cmd_cntx->cid()->name()), kSyntaxErrType);
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
  auto* rb = cmd_cntx->rb();
  if (parser.HasError()) {
    return cmd_cntx->SendError(parser.TakeError().MakeReply());
  }

  constexpr uint32_t kMaxTtl = (1UL << 26);
  if (op_sp.ttl == 0 || op_sp.ttl > kMaxTtl) {
    return cmd_cntx->SendError(kInvalidIntErr);
  }

  CmdArgList fields = parser.Tail();

  if (fields.size() % 2 != 0) {
    return cmd_cntx->SendError(facade::WrongNumArgsError(cmd_cntx->cid()->name()), kSyntaxErrType);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, fields, op_sp);
  };

  OpResult<uint32_t> result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  if (result) {
    rb->SendLong(*result);
  } else {
    cmd_cntx->SendError(result.status());
  }
}

struct HSetReplies {
  void Send(OpResult<uint32_t> result) const {
    switch (result.status()) {
      case OpStatus::OK:
      case OpStatus::KEY_NOTFOUND:
        return cmd_cntx->SendLong(result.value_or(0));
      default:
        return cmd_cntx->SendError(result.status());
    };
  }

  CommandContext* cmd_cntx;
};

void CmdHDel(CmdArgList args, CommandContext* cmd_cntx) {
  auto cb = [&](HMapWrap& hw) -> OpResult<uint32_t> {
    unsigned deleted = 0;
    for (string_view s : args.subspan(1))
      deleted += hw.Erase(s);
    return deleted;
  };
  HSetReplies{cmd_cntx}.Send(cmd_cntx->tx()->ScheduleSingleHopT(WrapW(cb)));
}

void CmdHExpire(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser{args};
  using MinMaxTtl = FInt<0, (1 << 26)>;
  auto [key, ttl_sec] = parser.Next<string_view, MinMaxTtl>();

  ExpireFlags flags = parser
                          .TryMapNext("NX", ExpireFlags::EXPIRE_NX, "XX", ExpireFlags::EXPIRE_XX,
                                      "GT", ExpireFlags::EXPIRE_GT, "LT", ExpireFlags::EXPIRE_LT)
                          .value_or(ExpireFlags::EXPIRE_ALWAYS);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (parser.HasError()) {
    return cmd_cntx->SendError(parser.TakeError().MakeReply());
  }
  if (!parser.Check("FIELDS"sv)) {
    return cmd_cntx->SendError("Mandatory argument FIELDS is missing or not at the right position",
                               kSyntaxErrType);
  }

  uint32_t numFields = parser.Next<uint32_t>();

  CmdArgList fields = parser.Tail();
  if (fields.size() != numFields) {
    return rb->SendError("The `numfields` parameter must match the number of arguments",
                         kSyntaxErrType);
  }

  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpHExpire(t->GetOpArgs(shard), key, ttl_sec, flags, fields);
  };
  OpResult<vector<long>> result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::OK:
      return rb->SendLongArr(absl::MakeConstSpan(result.value()));
    case OpStatus::KEY_NOTFOUND:
      return rb->SendLongArr(absl::MakeConstSpan(vector<long>(numFields, -2)));
    default:
      return cmd_cntx->SendError(result.status());
  };
}

void CmdHGet(CmdArgList args, CommandContext* cmd_cntx) {
  auto cb = [field = args[1]](const HMapWrap& hw) -> OpResult<string> {
    if (auto it = hw.Find(field); it)
      return string{it->second};
    return OpStatus::KEY_NOTFOUND;
  };

  OpResult<string> result = ExecuteRO(cmd_cntx->tx(), cb);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  switch (result.status()) {
    case OpStatus::OK:
      return rb->SendBulkString(*result);
    case OpStatus::KEY_NOTFOUND:
      return rb->SendNull();
    default:
      return cmd_cntx->SendError(result.status());
  };
}

void CmdHMGet(CmdArgList args, CommandContext* cmd_cntx) {
  auto fields = args.subspan(1);
  auto cb = [fields](const HMapWrap& hw) { return OpHMGet(hw, fields); };

  OpResult<vector<OptStr>> result = ExecuteRO(cmd_cntx->tx(), cb);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  switch (result.status()) {
    case OpStatus::OK:
    case OpStatus::KEY_NOTFOUND: {
      RedisReplyBuilder::ArrayScope scope{rb, fields.size()};
      for (size_t i = 0; i < fields.size(); i++) {
        if (result.ok() && (*result)[i].has_value())
          rb->SendBulkString(*(*result)[i]);
        else
          rb->SendNull();
      }
    } break;
    default:
      cmd_cntx->SendError(result.status());
  };
}

void CmdHStrLen(CmdArgList args, CommandContext* cmd_cntx) {
  auto cb = [field = ArgS(args, 1)](const HMapWrap& hw) -> OpResult<uint32_t> {
    if (auto it = hw.Find(field); it)
      return it->second.length();
    return OpStatus::KEY_NOTFOUND;
  };
  HSetReplies{cmd_cntx}.Send(ExecuteRO(cmd_cntx->tx(), cb));
}

void CmdHLen(CmdArgList args, CommandContext* cmd_cntx) {
  auto cb = [](const HMapWrap& hw) -> OpResult<uint32_t> { return hw.Length(); };
  HSetReplies{cmd_cntx}.Send(ExecuteRO(cmd_cntx->tx(), cb));
}

void CmdHExists(CmdArgList args, CommandContext* cmd_cntx) {
  auto cb = [field = args[1]](const HMapWrap& hw) -> OpResult<uint32_t> {
    return hw.Find(field) ? 1 : 0;
  };
  HSetReplies{cmd_cntx}.Send(ExecuteRO(cmd_cntx->tx(), cb));
}

void CmdHIncrBy(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);
  string_view incrs = ArgS(args, 2);
  int64_t ival = 0;

  if (!absl::SimpleAtoi(incrs, &ival)) {
    return cmd_cntx->SendError(kInvalidIntErr);
  }

  IncrByParam param{ival};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, field, &param);
  };

  OpStatus status = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OK) {
    cmd_cntx->SendLong(get<int64_t>(param));
  } else {
    switch (status) {
      case OpStatus::INVALID_VALUE:
        cmd_cntx->SendError("hash value is not an integer");
        break;
      case OpStatus::OUT_OF_RANGE:
        cmd_cntx->SendError(kIncrOverflow);
        break;
      default:
        cmd_cntx->SendError(status);
        break;
    }
  }
}

void CmdHIncrByFloat(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view field = ArgS(args, 1);
  string_view incrs = ArgS(args, 2);
  double dval = 0;

  if (!absl::SimpleAtod(incrs, &dval)) {
    return cmd_cntx->SendError(kInvalidFloatErr);
  }

  if (isnan(dval) || isinf(dval)) {
    return cmd_cntx->SendError(kNanOrInfDuringIncr);
  }

  IncrByParam param{dval};

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpIncrBy(t->GetOpArgs(shard), key, field, &param);
  };

  OpStatus status = cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));

  if (status == OpStatus::OK) {
    auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
    rb->SendDouble(get<double>(param));
  } else {
    switch (status) {
      case OpStatus::INVALID_VALUE:
        cmd_cntx->SendError("hash value is not a float");
        break;
      default:
        cmd_cntx->SendError(status);
        break;
    }
  }
}

void CmdHKeys(CmdArgList args, CommandContext* cmd_cntx) {
  HGetGeneric(args, FIELDS, cmd_cntx);
}

void CmdHVals(CmdArgList args, CommandContext* cmd_cntx) {
  HGetGeneric(args, VALUES, cmd_cntx);
}

void CmdHGetAll(CmdArgList args, CommandContext* cmd_cntx) {
  HGetGeneric(args, GetAllMode::FIELDS | GetAllMode::VALUES, cmd_cntx);
}

void CmdHScan(CmdArgList args, CommandContext* cmd_cntx) {
  std::string_view token = ArgS(args, 1);
  uint64_t cursor = 0;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (!absl::SimpleAtoi(token, &cursor)) {
    return rb->SendError("invalid cursor");
  }

  // HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
  if (args.size() > 7) {
    DVLOG(1) << "got " << args.size() << " this is more than it should be";
    return rb->SendError(kSyntaxErr);
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(args.subspan(2), true);
  if (!ops) {
    DVLOG(1) << "HScan invalid args - return " << ops << " to the user";
    return cmd_cntx->SendError(ops.status());
  }

  const ScanOpts& scan_op = ops.value();
  auto cb = [&](const HMapWrap& hw) { return OpScan(hw, &cursor, scan_op); };

  OpResult<StringVec> result = ExecuteRO(cmd_cntx->tx(), cb);
  switch (result.status()) {
    case OpStatus::KEY_NOTFOUND:
      cursor = 0;
      [[fallthrough]];
    case OpStatus::OK: {
      RedisReplyBuilder::ArrayScope scope{rb, 2};
      rb->SendBulkString(absl::StrCat(cursor));
      rb->SendBulkStrArr(*result);
      break;
    }
    default:
      cmd_cntx->SendError(result.status());
  }
}

void CmdHSet(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);

  string_view cmd{cmd_cntx->cid()->name()};
  auto* rb = cmd_cntx->rb();
  if (args.size() % 2 != 1) {
    return rb->SendError(facade::WrongNumArgsError(cmd), kSyntaxErrType);
  }

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args);
  };

  OpResult<uint32_t> result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  if (result && cmd == "HSET") {
    rb->SendLong(*result);
  } else {
    cmd_cntx->SendError(result.status());
  }
}

void CmdHSetNx(CmdArgList args, CommandContext* cmd_cntx) {
  string_view key = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, args.subspan(1), OpSetParams{.skip_if_exists = true});
  };
  HSetReplies{cmd_cntx}.Send(cmd_cntx->tx()->ScheduleSingleHopT(cb));
}

void StrVecEmplaceBack(StringVec& str_vec, const listpackEntry& lp) {
  if (lp.sval) {
    str_vec.emplace_back(reinterpret_cast<char*>(lp.sval), lp.slen);
    return;
  }
  str_vec.emplace_back(absl::StrCat(lp.lval));
}

void CmdHRandField(CmdArgList args, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (args.size() > 3) {
    DVLOG(1) << "Wrong number of command arguments: " << args.size();
    return rb->SendError(kSyntaxErr);
  }

  string_view key = ArgS(args, 0);
  int32_t count;
  bool with_values = false;

  if ((args.size() > 1) && (!SimpleAtoi(ArgS(args, 1), &count))) {
    return rb->SendError("count value is not an integer", kSyntaxErrType);
  }

  if (args.size() == 3) {
    string arg = absl::AsciiStrToUpper(ArgS(args, 2));
    if (arg != "WITHVALUES")
      return rb->SendError(kSyntaxErr);
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
        auto res_it = db_slice.FindMutable(db_context, key, OBJ_HASH);
        if (res_it) {
          db_slice.DelMutable(db_context, std::move(*res_it));
        }
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

  OpResult<StringVec> result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
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
      rb->SendBulkStrArr(*result, CollectionType::ARRAY);
  } else if (result.status() == OpStatus::KEY_NOTFOUND) {
    if (args.size() == 1)
      rb->SendNull();
    else
      rb->SendEmptyArray();
  } else {
    cmd_cntx->SendError(result.status());
  }
}

}  // namespace

using CI = CommandId;

#define HFUNC(x) SetHandler(&Cmd##x)

void HSetFamily::Register(CommandRegistry* registry) {
  registry->StartFamily(acl::HASH);
  *registry << CI{"HDEL", CO::FAST | CO::JOURNALED, -3, 1, 1}.HFUNC(HDel)
            << CI{"HLEN", CO::FAST | CO::READONLY, 2, 1, 1}.HFUNC(HLen)
            << CI{"HEXISTS", CO::FAST | CO::READONLY, 3, 1, 1}.HFUNC(HExists)
            << CI{"HGET", CO::FAST | CO::READONLY, 3, 1, 1}.HFUNC(HGet)
            << CI{"HGETALL", CO::FAST | CO::READONLY, 2, 1, 1}.HFUNC(HGetAll)
            << CI{"HMGET", CO::FAST | CO::READONLY, -3, 1, 1}.HFUNC(HMGet)
            << CI{"HMSET", CO::JOURNALED | CO::FAST | CO::DENYOOM, -4, 1, 1}.HFUNC(HSet)
            << CI{"HINCRBY", CO::JOURNALED | CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(HIncrBy)
            << CI{"HINCRBYFLOAT", CO::JOURNALED | CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(
                   HIncrByFloat)
            << CI{"HKEYS", CO::READONLY, 2, 1, 1}.HFUNC(HKeys)
            << CI{"HEXPIRE", CO::JOURNALED | CO::FAST | CO::DENYOOM, -5, 1, 1}.HFUNC(HExpire)
            << CI{"HRANDFIELD", CO::READONLY, -2, 1, 1}.HFUNC(HRandField)
            << CI{"HSCAN", CO::READONLY, -3, 1, 1}.HFUNC(HScan)
            << CI{"HSET", CO::JOURNALED | CO::FAST | CO::DENYOOM, -4, 1, 1}.HFUNC(HSet)
            << CI{"HSETEX", CO::JOURNALED | CO::FAST | CO::DENYOOM, -5, 1, 1}.SetHandler(HSetEx)
            << CI{"HSETNX", CO::JOURNALED | CO::DENYOOM | CO::FAST, 4, 1, 1}.HFUNC(HSetNx)
            << CI{"HSTRLEN", CO::READONLY | CO::FAST, 3, 1, 1}.HFUNC(HStrLen)
            << CI{"HVALS", CO::READONLY, 2, 1, 1}.HFUNC(HVals);
}

auto HSetFamily::LoadZiplistBlob(std::string_view blob, PrimeValue* pv) -> LoadBlobResult {
  unsigned char* lp = lpNew(blob.size());
  if (!ZiplistPairsConvertAndValidateIntegrity((const uint8_t*)blob.data(), blob.size(), &lp)) {
    LOG(ERROR) << "Hash ziplist integrity check failed.";
    zfree(lp);
    return LoadBlobResult::kCorrupted;
  }

  if (lpLength(lp) == 0) {
    lpFree(lp);
    return LoadBlobResult::kEmpty;
  }

  if (lpBytes(lp) > server.max_listpack_map_bytes) {
    StringMap* sm = ConvertToStrMap(lp);
    lpFree(lp);
    pv->InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
  } else {
    lp = lpShrinkToFit(lp);
    pv->InitRobj(OBJ_HASH, kEncodingListPack, lp);
  }

  return LoadBlobResult::kSuccess;
}

auto HSetFamily::LoadListpackBlob(std::string_view blob, PrimeValue* pv) -> LoadBlobResult {
  if (!lpValidateIntegrity((uint8_t*)blob.data(), blob.size(), 0, nullptr, nullptr)) {
    LOG(ERROR) << "Hash listpack integrity check failed.";
    return LoadBlobResult::kCorrupted;
  }

  unsigned char* lp = lpNew(blob.size());
  std::memcpy(lp, blob.data(), blob.size());

  if (lpLength(lp) == 0) {
    lpFree(lp);
    return LoadBlobResult::kEmpty;
  }

  if (lpBytes(lp) > server.max_listpack_map_bytes) {
    StringMap* sm = ConvertToStrMap(lp);
    lpFree(lp);
    pv->InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
  } else {
    lp = lpShrinkToFit(lp);
    pv->InitRobj(OBJ_HASH, kEncodingListPack, lp);
  }

  return LoadBlobResult::kSuccess;
}

StringMap* HSetFamily::ConvertToStrMap(uint8_t* lp) {
  StringMap* sm = CompactObj::AllocateMR<StringMap>();

  detail::ListpackWrap lw{lp};
  sm->Reserve(lw.size());
  for (const auto [key, value] : lw)
    LOG_IF(ERROR, !sm->AddOrUpdate(key, value)) << "Internal error: duplicate key " << key;
  return sm;
}

// returns -1 if no expiry is associated with the field, -3 if no field is found.
int32_t HSetFamily::FieldExpireTime(const DbContext& db_context, const PrimeValue& pv,
                                    std::string_view field) {
  DCHECK_EQ(OBJ_HASH, pv.ObjType());

  if (pv.Encoding() == kEncodingListPack) {
    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    return lw.Find(field) == lw.end() ? -3 : -1;
  } else {
    StringMap* string_map = (StringMap*)pv.RObjPtr();
    string_map->set_time(MemberTimeSeconds(db_context.time_now_ms));
    auto it = string_map->Find(field);
    if (it == string_map->end())
      return -3;
    return it.HasExpiry() ? it.ExpiryTime() : -1;
  }
}

// returns vector of results for each field in values:
// -2 if the provided key does not exist.
// 0 if the specified NX | XX | GT | LT condition has not been met.
// 1 if the expiration time was set/updated.
// 2 when HEXPIRE/HPEXPIRE is called with 0 seconds and the field is deleted.
static std::vector<long> UpdateTTL(facade::CmdArgList values, uint32_t ttl_sec, ExpireFlags flags,
                                   StringMap* owner) {
  std::vector<long> res;
  res.reserve(values.size());

  for (size_t i = 0; i < values.size(); i++) {
    std::string_view field = facade::ToSV(values[i]);
    auto it = owner->Find(field);
    if (it != owner->end()) {
      switch (flags) {
        case ExpireFlags::EXPIRE_NX:
          if (it.HasExpiry()) {
            res.emplace_back(0);
            continue;
          }
          break;
        case ExpireFlags::EXPIRE_XX:
          if (!it.HasExpiry()) {
            res.emplace_back(0);
            continue;
          }
          break;
        case ExpireFlags::EXPIRE_GT:
          if (it.ExpiryTime() - owner->time_now() >= ttl_sec) {
            res.emplace_back(0);
            continue;
          }
          break;
        case ExpireFlags::EXPIRE_LT:
          if (it.ExpiryTime() - owner->time_now() <= ttl_sec) {
            res.emplace_back(0);
            continue;
          }
          break;
        case ExpireFlags::EXPIRE_ALWAYS:
          break;
      }
      if (ttl_sec == 0) {
        owner->Erase(field);
        res.emplace_back(2);
      } else {
        it.SetExpiryTime(ttl_sec);
        res.emplace_back(1);
      }
    } else {
      res.emplace_back(-2);
    }
  }

  return res;
}

vector<long> HSetFamily::SetFieldsExpireTime(const OpArgs& op_args, uint32_t ttl_sec,
                                             ExpireFlags flags, string_view key, CmdArgList values,
                                             PrimeValue* pv) {
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
  vector<long> res = UpdateTTL(values, ttl_sec, flags, sm);
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);
  return res;
}

}  // namespace dfly
