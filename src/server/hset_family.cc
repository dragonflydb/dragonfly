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

#include "absl/flags/declare.h"
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
#include "server/search/doc_index.h"
#include "server/tiered_storage.h"
#include "server/tiering/decoders.h"
#include "server/transaction.h"
#include "server/tx_base.h"

ABSL_DECLARE_FLAG(size_t, listpack_max_field_len);
ABSL_DECLARE_FLAG(size_t, listpack_max_bytes);

using namespace std;

namespace dfly {

using namespace facade;
using absl::SimpleAtoi;

namespace {

using IncrByParam = std::variant<double, int64_t>;
using OptStr = std::optional<std::string>;
enum GetAllMode : uint8_t { FIELDS = 1, VALUES = 2 };

// TODO: replace all the listpack code with our detail::Listpack wrapper.
bool IsGoodForListpack(const ParsedArgs& args, const uint8_t* lp) {
  DCHECK_GE(args.size(), 2u);

  // For a single field-value pair on an empty or single-entry listpack, approve automatically
  // even with large values. A one-field hash is efficient to look-up or mutate.
  if (args.size() == 2 && args[0].size() < 4096) {
    if (lpLength((uint8_t*)lp) == 0)
      return true;

    // if we override the same field of a singleton hashmap, we allow listpack as well.
    if (lpLength((uint8_t*)lp) == 2) {
      uint8_t* first = lpFirst((uint8_t*)lp);
      unsigned slen = 0;
      long long lval;
      uint8_t* vstr = lpGetValue(first, &slen, &lval);
      if (vstr && args[0].size() == slen && memcmp(vstr, args[0].data(), slen) == 0) {
        return true;
      }
    }
  }

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
struct HMapWrap {
 private:
  template <typename F> decltype(auto) VisitRef(F f) const {
    return std::visit(Overloaded{[&f](auto* s) { return f(*s); }, f}, impl_);
  }

  template <typename F> decltype(auto) VisitMut(F& f) {
    return std::visit(Overloaded{[&f](auto* s) { return f(*s); }, f}, impl_);
  }

 public:
  // Create from non-external prime value
  HMapWrap(const PrimeValue& pv, DbContext db_cntx) {
    DCHECK(!pv.IsExternal() || pv.IsCool());
    if (pv.Encoding() == kEncodingListPack)
      impl_ = detail::ListpackWrap{static_cast<uint8_t*>(pv.RObjPtr())};
    else
      impl_ = GetStringMap(pv, db_cntx);
  }

  explicit HMapWrap(detail::ListpackWrap lw) : impl_{std::move(lw)} {
  }

  size_t Length() const {
    Overloaded ov{
        [](StringMap* s) { return s->UpperBoundSize(); },
        [](const detail::ListpackWrap& lw) { return lw.size(); },
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
                                          StringMap::iterator>;
    auto cb = [f](auto& h) -> std::pair<IT, IT> {
      return {{f, h.begin()}, {std::nullopt, h.end()}};
    };
    return base::it::Range(VisitRef(cb));
  }

  bool Erase(std::string_view key) {
    Overloaded ov{[key](StringMap& s) { return s.Erase(key); },
                  [key](detail::ListpackWrap& lw) { return lw.Delete(key); }};
    return VisitMut(ov);
  }

  void AddOrUpdate(std::string_view key, std::string_view value) {
    Overloaded ov{[&](StringMap& sm) { sm.AddOrUpdate(key, value, UINT32_MAX, true); },
                  [&](detail::ListpackWrap& lw) { lw.Insert(key, value, false); }};
    VisitMut(ov);
  }

  void Launder(PrimeValue& pv) {
    Overloaded ov{
        [](StringMap& s) {},
        [&](detail::ListpackWrap& lw) { pv.SetRObjPtr(lw.GetPointer()); },
    };
    VisitMut(ov);
  }

  void Launder(tiering::ListpackMapDecoder* dec) {
    Overloaded ov{
        [](StringMap& s) {},
        [&](detail::ListpackWrap& lw) { *dec->GetMutable() = lw; },
    };
    VisitMut(ov);
  }

  template <typename T> optional<T> Get() const {
    if (holds_alternative<T>(impl_))
      return get<T>(impl_);
    return nullopt;
  }

 private:
  variant<StringMap*, detail::ListpackWrap> impl_;
};

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
      using D = tiering::ListpackMapDecoder;
      util::fb2::Future<OpResult<T>> fut;
      auto read_cb = [fut, f = std::move(f)](io::Result<D*> res) mutable {
        if (!res) {
          fut.Resolve(OpResult<T>{OpStatus::IO_ERROR});
          return;
        }

        HMapWrap hw{res.value()->Get()};
        fut.Resolve(f(hw));
      };

      es->tiered_storage()->Read(std::make_pair(op_args.db_cntx.db_index, key),
                                 pv.GetExternalSlice(), D{}, std::move(read_cb));
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
template <typename F>
auto ExecuteW(Transaction* tx, F&& f,
              absl::InlinedVector<std::string_view, 4> modified_fields = {}) {
  using T = typename std::invoke_result_t<F, HMapWrap&>::Type;
  auto shard_cb = [f = std::forward<F>(f), fields = std::move(modified_fields)](
                      Transaction* t, EngineShard* es) -> OpResult<CbVariant<T>> {
    // Fetch value of hash type
    auto [key, op_args] = KeyAndArgs(t, es);

    auto it_res = op_args.GetDbSlice().FindMutable(op_args.db_cntx, key, OBJ_HASH);
    RETURN_ON_BAD_STATUS(it_res);
    auto& pv = it_res->it->second;

    // Enqueue read for future values
    if (pv.IsExternal() && !pv.IsCool()) {
      using D = tiering::ListpackMapDecoder;
      util::fb2::Future<OpResult<T>> fut;
      auto read_cb = [fut, f = std::move(f)](io::Result<D*> res) mutable {
        if (!res) {
          fut.Resolve(OpResult<T>{OpStatus::IO_ERROR});
          return;
        }

        HMapWrap hw{*res.value()->GetMutable()};
        fut.Resolve(f(hw));
        hw.Launder(res.value());
      };

      es->tiered_storage()->Read(std::make_pair(op_args.db_cntx.db_index, key),
                                 pv.GetExternalSlice(), D{}, std::move(read_cb), false);
      return CbVariant<T>{std::move(fut)};
    }

    // Remove document before modification, preserving HNSW external vector data.
    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv, fields);

    HMapWrap hw{pv, op_args.db_cntx};
    auto res = f(hw);
    hw.Launder(pv);

    // Run post updater
    it_res->post_updater.Run();

    if (hw.Length() == 0)
      DeleteHw(hw, op_args, key);
    else
      op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, &pv);

    RETURN_ON_BAD_STATUS(res);
    return CbVariant<T>{std::move(res).value()};
  };

  return Unwrap(tx->ScheduleSingleHopT(std::move(shard_cb)));
}

size_t EstimateListpackMinBytes(const ParsedArgs& members) {
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

  if (pv.IsExternal() && !pv.IsCool())
    return OpStatus::CANCELLED;  // Not supported for offloaded values

  if (add_res.is_new) {
    pv.InitRobj(OBJ_HASH, kEncodingListPack, lpNew(0));
  } else {
    std::string_view fields_arr[] = {field};
    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv, fields_arr);

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

OpResult<vector<OptStr>> OpHMGet(const HMapWrap& hw, const ParsedArgs& fields) {
  DCHECK(!fields.empty());

  std::vector<OptStr> result(fields.size());
  if (auto sm = hw.Get<StringMap*>(); sm) {
    for (size_t i = 0; i < fields.size(); ++i) {
      if (auto it = (*sm)->Find(fields[i]); it != (*sm)->end()) {
        result[i].emplace(it->second, sdslen(it->second));
      }
    }
  } else {
    absl::flat_hash_map<string_view, absl::InlinedVector<size_t, 3>> reverse;
    reverse.reserve(fields.size() + 1);
    for (size_t i = 0; i < fields.size(); ++i) {
      reverse[fields[i]].push_back(i);  // map fields to their index.
    }

    for (const auto [key, value] : hw.Range()) {
      if (auto it = reverse.find(key); it != reverse.end()) {
        for (size_t index : it->second) {
          DCHECK_LT(index, result.size());
          result[index].emplace(value);
        }
      }
    }
  }

  return result;
}

struct OpSetParams {
  enum class Mode : uint8_t {
    kNormal,  // overwrite every field
    kNX,      // set each field only if it does not already exist
    kFNX,     // set all fields only if none of them exist
    kFXX,     // set all fields only if all of them exist
  };

  // Selects how OpSet reports its result: Dragonfly returns the number of created fields, Redis
  // returns 1 when the fields were applied. (Tiering support is gated by ttl, not by format.)
  enum class Format : uint8_t { kDragonfly, kRedis };

  uint32_t ttl = UINT32_MAX;
  bool keepttl = false;
  Mode mode = Mode::kNormal;
  Format format = Format::kDragonfly;

  optional<util::fb2::Future<bool>>* backpressure = nullptr;
};

// OpSet's reported result: Dragonfly returns the number of newly created fields, Redis returns 1
// (the fields were applied — any FNX/FXX condition has already been verified by the caller).
uint32_t SetReply(const OpSetParams& op_sp, uint32_t created) {
  return op_sp.format == OpSetParams::Format::kRedis ? 1u : created;
}

OpResult<CbVariant<uint32_t>> OpSet(const OpArgs& op_args, string_view key,
                                    const ParsedArgs& values,
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

  // Only Dragonfly NX skips existing fields; FNX/FXX were already resolved by the caller, so every
  // other mode overwrites.
  const bool skip_existing = op_sp.mode == OpSetParams::Mode::kNX;

  // If the value is external, enqueue read and modify it there
  if (pv.IsExternal() && !pv.IsCool()) {
    if (op_sp.ttl != UINT32_MAX)
      return OpStatus::CANCELLED;  // member TTLs can't be stored in an offloaded hash

    using D = tiering::ListpackMapDecoder;
    util::fb2::Future<OpResult<uint32_t>> fut;
    auto read_cb = [fut, values, op_sp, skip_existing](io::Result<D*> res) mutable {
      if (!res) {
        fut.Resolve({OpStatus::IO_ERROR});
        return;
      }

      auto& lw = *res.value()->GetMutable();
      uint32_t created = 0;
      for (size_t i = 0; i < values.size(); i += 2) {
        created += lw.Insert(values[i], values[i + 1], skip_existing);
      }
      fut.Resolve(SetReply(op_sp, created));
    };

    op_args.shard->tiered_storage()->Read(std::make_pair(op_args.db_cntx.db_index, key),
                                          pv.GetExternalSlice(), D{}, std::move(read_cb), false);
    return CbVariant<uint32_t>{std::move(fut)};
  }

  if (add_res.is_new) {
    if (op_sp.ttl == UINT32_MAX) {
      lp = lpNew(0);
      pv.InitRobj(OBJ_HASH, kEncodingListPack, lp);
    } else {
      pv.InitRobj(OBJ_HASH, kEncodingStrMap2, CompactObj::AllocateMR<StringMap>());
    }
  } else {
    // Collect field names being modified for HNSW field data preservation.
    absl::InlinedVector<std::string_view, 4> field_names;
    for (size_t i = 0; i < values.size(); i += 2)
      field_names.push_back(values[i]);

    op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, pv, field_names);
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
      created += lw.Insert(values[i], values[i + 1], skip_existing);
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
      if (skip_existing)
        added = sm->AddOrSkip(field, value, op_sp.ttl);
      else
        added = sm->AddOrUpdate(field, value, op_sp.ttl, op_sp.keepttl);

      created += unsigned(added);
    }
  }

  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, &pv);

  if (auto* ts = op_args.shard->tiered_storage(); ts) {
    StashPrimeValue(op_args.db_cntx.db_index, key, it->first, &pv, ts, op_sp.backpressure);
  }

  return CbVariant<uint32_t>{SetReply(op_sp, created)};
}

void HGetGeneric(uint8_t getall_mask, CommandContext* cmd_cntx) {
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
                                 ExpireFlags flags, const ParsedArgs& values) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(op_res);

  PrimeValue* pv = &((*op_res).it->second);
  if (pv->IsExternal() && !pv->IsCool())
    return OpStatus::CANCELLED;  // can't mutate offloaded hashes synchronously

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

// Evaluates the FNX/FXX collective condition of HSETEX.
//   fnx=true  (FNX): holds only if NONE of the fields exist.
//   fnx=false (FXX): holds only if ALL of the fields exist.
// A missing key counts as "no fields exist".
OpResult<bool> CheckHSetExCondition(const OpArgs& op_args, string_view key,
                                    const ParsedArgs& fields, bool fnx) {
  auto& db_slice = op_args.GetDbSlice();
  auto res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_HASH);
  if (!res) {
    if (res.status() == OpStatus::KEY_NOTFOUND)
      return fnx;  // none exist: FNX holds, FXX fails
    return res.status();
  }

  const PrimeValue& pv = (*res)->second;
  if (pv.IsExternal() && !pv.IsCool())
    return OpStatus::CANCELLED;  // can't inspect offloaded hashes synchronously

  HMapWrap hw{pv, op_args.db_cntx};
  bool holds = true;
  for (size_t i = 0; i < fields.size(); i += 2) {
    // FNX requires every field to be absent; FXX requires every field to be present.
    if (fnx == bool(hw.Find(fields[i]))) {
      holds = false;
      break;
    }
  }
  if (hw.Length() == 0)  // Find() may have lazily expired fields and emptied the hash.
    DeleteHw(hw, op_args, key);
  return holds;
}

struct HSetExParams {
  OpSetParams op_sp;
  ParsedArgs fields;  // field/value pairs; valid only when the parser has no error.
};

// Parses HSETEX arguments after the key, reporting any error into `parser` (surfaced by the caller
// via RETURN_ON_PARSE_ERROR). `cmd_name` is only used to format error messages.
//
// Dragonfly format: HSETEX key [NX | FNX | FXX] [KEEPTTL] ttl_sec field value [field value ...]
// Redis format:     HSETEX key [FNX | FXX] [EX sec | PX ms | EXAT ts-sec | PXAT ts-ms | KEEPTTL]
//                          FIELDS numfields field value [field value ...]
//
// The syntaxes are told apart by the token following the leading flags: the Redis format has its
// mandatory FIELDS keyword, the Dragonfly format a bare numeric ttl_sec. NX (per-field skip) and
// the collective FNX/FXX condition are mutually exclusive; FNX/FXX behave identically in both
// syntaxes (set all-or-nothing). Only the reported value differs (see OpSetParams::Format).
HSetExParams ParseHSetEx(CmdArgParser* parser, string_view cmd_name) {
  using Mode = OpSetParams::Mode;
  using Format = OpSetParams::Format;
  constexpr int kMaxTtl = 1 << 26;

  HSetExParams res;
  OpSetParams& op_sp = res.op_sp;

  bool has_exp = false;

  // EX/PX are relative (now = 0), EXAT/PXAT absolute (now = current time); ms = true for PX/PXAT.
  // The value must land in (now, now + kMaxTtl] so the resulting ttl_sec stays in [1, kMaxTtl].
  const int64_t now_ms = GetCurrentTimeMs();
  const auto expiry = [&](int64_t now, bool ms) {
    return [&, now, ms](CmdArgParser* p) {
      has_exp = true;
      int64_t span = ms ? int64_t(kMaxTtl) * 1000 : kMaxTtl;
      int64_t v = p->Next<int64_t>();
      if (v <= now || v > now + span)
        p->ReportCustom(
            InvalidExpireTime(cmd_name));  // no-op if Next already reported a non-integer
      else
        op_sp.ttl = ms ? (v - now + 999) / 1000 : v - now;
    };
  };

  // A single Map makes the set modes (NX/FNX/FXX) mutually exclusive; OneOf also rejects a repeated
  // flag. Parsing stops at the first non-flag token (ttl_sec or FIELDS).
  parser->Apply(
      OneOf(Map(&op_sp.mode, "NX", Mode::kNX, "FNX", Mode::kFNX, "FXX", Mode::kFXX)),
      OneOf(Exist("KEEPTTL", &op_sp.keepttl)),
      OneOf(Tag("EX", expiry(0, false)), Tag("PX", expiry(0, true)),
            Tag("EXAT", expiry(now_ms / 1000, false)), Tag("PXAT", expiry(now_ms, true))));

  // FIELDS marks the Redis format, a bare ttl_sec the Dragonfly format. The parser short-circuits
  // once errored, so the steps below need no per-step checks.
  if (parser->Check("FIELDS")) {
    op_sp.format = Format::kRedis;
    uint32_t numfields = parser->Next<uint32_t>();
    if (op_sp.mode == Mode::kNX || (op_sp.keepttl && has_exp))
      parser->Report(CmdArgParser::CUSTOM_ERROR);  // NX is Dragonfly-only; one expiry option max

    res.fields = parser->UnparsedArgs();
    if (numfields == 0 || res.fields.size() != size_t(numfields) * 2)
      parser->ReportCustom("The `numfields` parameter must match the number of arguments");
  } else if (has_exp) {
    // EX/PX/EXAT/PXAT belong to the Redis form; without FIELDS the command is malformed.
    parser->Report(CmdArgParser::CUSTOM_ERROR);
  } else {
    op_sp.format = Format::kDragonfly;
    op_sp.ttl = parser->Next<FInt<1, kMaxTtl>>();

    res.fields = parser->UnparsedArgs();
    if (res.fields.empty() || res.fields.size() % 2 != 0)
      parser->ReportCustom(WrongNumArgsError(cmd_name));
  }
  return res;
}

void HSetEx(CmdArgList args, CommandContext* cmd_cntx) {
  CmdArgParser parser{cmd_cntx->tail_args()};
  string_view key = parser.Next();
  HSetExParams parsed = ParseHSetEx(&parser, cmd_cntx->cid()->name());
  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  // Evaluate the FNX/FXX condition (if any), then let OpSet set the fields and report the
  // format-appropriate value (created count for Dragonfly, 1 for Redis).
  auto cb = [&](Transaction* t, EngineShard* shard) -> OpResult<CbVariant<uint32_t>> {
    using Mode = OpSetParams::Mode;
    auto op_args = t->GetOpArgs(shard);
    const OpSetParams& op_sp = parsed.op_sp;
    if (op_sp.mode == Mode::kFNX || op_sp.mode == Mode::kFXX) {
      OpResult<bool> cond =
          CheckHSetExCondition(op_args, key, parsed.fields, op_sp.mode == Mode::kFNX);
      RETURN_ON_BAD_STATUS(cond);
      if (!*cond)
        return CbVariant<uint32_t>{uint32_t(0)};  // condition not met -> nothing set
    }
    return OpSet(op_args, key, parsed.fields, op_sp);
  };

  OpResult<uint32_t> result = Unwrap(cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb)));
  if (result)
    cmd_cntx->rb()->SendLong(*result);
  else
    cmd_cntx->SendError(result.status());
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

void CmdHDel(CmdArgParser parser, CommandContext* cmd_cntx) {
  parser.Next();  // skip key
  // Collect field names for HNSW data preservation.
  ParsedArgs fields_span = parser.UnparsedArgs();
  absl::InlinedVector<std::string_view, 4> field_names;
  for (auto f : fields_span)
    field_names.push_back(f);

  auto cb = [&](HMapWrap& hw) -> OpResult<uint32_t> {
    unsigned deleted = 0;
    for (string_view s : fields_span)
      deleted += hw.Erase(s);
    return deleted;
  };
  HSetReplies{cmd_cntx}.Send(ExecuteW(cmd_cntx->tx(), std::move(cb), std::move(field_names)));
}

void CmdHExpire(CmdArgParser parser, CommandContext* cmd_cntx) {
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

  ParsedArgs fields = parser.UnparsedArgs();
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

enum class FieldExpireOutput : uint8_t {
  kTtlSeconds,    // remaining time-to-live in seconds (HTTL)
  kPExpireTimeMs  // absolute expiration as a Unix timestamp in milliseconds (HPEXPIRETIME)
};

template <FieldExpireOutput kOut>
OpResult<vector<int64_t>> OpHExpireTime(Transaction* t, EngineShard* shard, string_view key,
                                        const ParsedArgs& fields) {
  auto& db_slice = t->GetDbSlice(shard->shard_id());
  const DbContext& db_cntx = t->GetDbContext();
  auto it_res = db_slice.FindReadOnly(db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(it_res);

  const PrimeValue& pv = (*it_res)->second;
  if (pv.IsExternal() && !pv.IsCool())
    return OpStatus::CANCELLED;  // can't inspect offloaded hashes synchronously

  vector<int64_t> res;
  res.reserve(fields.size());

  for (auto field : fields) {
    int32_t exp_time = HSetFamily::FieldExpireTime(db_cntx, pv, field);
    if (exp_time <= 0) {
      // -3 from FieldExpireTime means field not found -> -2; -1 means no expiry -> stays -1.
      res.push_back(exp_time == -3 ? -2 : exp_time);
    } else if constexpr (kOut == FieldExpireOutput::kTtlSeconds) {
      res.push_back(int64_t(exp_time) - MemberTimeSeconds(db_cntx.time_now_ms));
    } else {
      // FieldExpireTime is relative to kMemberExpiryBase; convert to an absolute Unix ms timestamp.
      res.push_back((static_cast<int64_t>(exp_time) + static_cast<int64_t>(kMemberExpiryBase)) *
                    1000);
    }
  }

  // FieldExpireTime triggers lazy field expiry; drop the key if all fields are now gone.
  HSetFamily::DeleteIfEmpty(db_slice, db_cntx, key, pv);

  return res;
}

// Shared handler for HTTL and HPEXPIRETIME; the per-field value format differs via kOut. A missing
// key replies with -2 for every requested field.
template <FieldExpireOutput kOut>
void HExpireTimeGeneric(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  parser.ExpectTag("FIELDS", "Mandatory argument FIELDS is missing or not at the right position");
  uint32_t numFields =
      parser.Next<FInt<1u, UINT32_MAX>>("Number of fields must be a positive integer");
  ParsedArgs fields = parser.UnparsedArgs();

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  if (fields.size() != numFields) {
    return rb->SendError("The `numfields` parameter must match the number of arguments",
                         kSyntaxErrType);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpHExpireTime<kOut>(t, shard, key, fields);
  };
  OpResult<vector<int64_t>> result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::OK:
      return rb->SendLongArr(absl::MakeConstSpan(result.value()));
    case OpStatus::KEY_NOTFOUND:
      return rb->SendLongArr(absl::MakeConstSpan(vector<int64_t>(numFields, -2)));
    default:
      return cmd_cntx->SendError(result.status());
  };
}

void CmdHTtl(CmdArgParser parser, CommandContext* cmd_cntx) {
  HExpireTimeGeneric<FieldExpireOutput::kTtlSeconds>(std::move(parser), cmd_cntx);
}

void CmdHPExpireTime(CmdArgParser parser, CommandContext* cmd_cntx) {
  HExpireTimeGeneric<FieldExpireOutput::kPExpireTimeMs>(std::move(parser), cmd_cntx);
}

// Removes the TTL from the listed existing fields by re-inserting them without an expiry.
// No-op for listpack-encoded hashes (which never store per-field TTLs).
void PersistFields(const OpArgs& op_args, string_view key, const ParsedArgs& fields,
                   PrimeValue* pv) {
  if (pv->Encoding() != kEncodingStrMap2)
    return;

  absl::InlinedVector<string_view, 4> field_names(fields.begin(), fields.end());
  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, *pv, field_names);

  StringMap* sm = container_utils::GetStringMap(*pv, op_args.db_cntx);
  for (string_view field : field_names) {
    auto it = sm->Find(field);
    if (it != sm->end() && it.HasExpiry())
      sm->AddOrUpdate(field, string_view{it->second, sdslen(it->second)}, UINT32_MAX, false);
  }

  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);
}

OpResult<vector<OptStr>> OpHGetEx(const OpArgs& op_args, string_view key, const ParsedArgs& fields,
                                  const DbSlice::ExpireParams& exp_params) {
  auto& db_slice = op_args.GetDbSlice();
  auto op_res = db_slice.FindMutable(op_args.db_cntx, key, OBJ_HASH);
  RETURN_ON_BAD_STATUS(op_res);
  PrimeValue* pv = &((*op_res).it->second);

  if (pv->IsExternal() && !pv->IsCool())
    return OpStatus::CANCELLED;  // offloaded hashes can't be read/mutated synchronously

  // Capture the current field values before mutating TTLs: a past/zero expiry deletes the field,
  // but its value must still be returned (Redis semantics).
  vector<OptStr> values;
  {
    HMapWrap hw{*pv, op_args.db_cntx};
    values = std::move(*OpHMGet(hw, fields));
  }

  if (exp_params.persist) {
    PersistFields(op_args, key, fields, pv);
  } else if (exp_params.IsDefined()) {
    // A non-positive relative TTL means the expiry is already due: ttl_sec 0 deletes the field
    // (its value was captured above).
    int64_t rel_msec = exp_params.Calculate(op_args.db_cntx.time_now_ms, false).first;
    uint32_t ttl_sec = rel_msec <= 0 ? 0 : static_cast<uint32_t>((rel_msec + 999) / 1000);
    HSetFamily::SetFieldsExpireTime(op_args, ttl_sec, ExpireFlags::EXPIRE_ALWAYS, key, fields, pv);
  }

  // Lazy field expiry during the read, or a 0-ttl deletion above, may have emptied the hash.
  if (pv->Encoding() == kEncodingStrMap2) {
    auto* sm = static_cast<StringMap*>(pv->RObjPtr());
    if (sm->UpperBoundSize() == 0)
      db_slice.DelMutable(op_args.db_cntx, std::move(*op_res));
  }

  return values;
}

void CmdHGetEx(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  string_view cmd_name = cmd_cntx->cid()->name();
  // The transaction clock the op applies the TTL against, so a relative EX/PX resolves against the
  // same time the op uses rather than a separate wall-clock read.
  const uint64_t now_ms = cmd_cntx->tx()->GetDbContext().time_now_ms;

  // At most one of EX/PX/EXAT/PXAT/PERSIST is accepted before FIELDS; OneOf rejects a second option
  // as a syntax error (Redis instead reports a misplaced-FIELDS error here).
  DbSlice::ExpireParams exp_params;
  auto read_expiry = [&](ExpT type) {
    return [&, type](CmdArgParser* p) {
      // HGETEX accepts 0 (expire now -> delete the field) but rejects negatives, unlike SET/GETEX.
      // A non-integer leaves the parser's own error in place (ReportCustom won't overwrite it).
      int64_t value = p->Next<int64_t>();
      if (value < 0)
        return p->ReportCustom("invalid expire time, must be >= 0");

      // Reject overflow and values past the hash-field TTL cap (1<<26 s, as for HEXPIRE/HSETEX).
      exp_params = DbSlice::ExpireParams{type, value, now_ms};
      constexpr int64_t kMaxTtlMs = (int64_t{1} << 26) * 1000;
      auto [rel_msec, abs_msec] = exp_params.Calculate(now_ms, false);
      if (abs_msec < 0 || rel_msec > kMaxTtlMs)
        return p->ReportCustom(InvalidExpireTime(cmd_name));
    };
  };
  parser.Apply(OneOf(Tag("EX", read_expiry(ExpT::EX)), Tag("PX", read_expiry(ExpT::PX)),
                     Tag("EXAT", read_expiry(ExpT::EXAT)), Tag("PXAT", read_expiry(ExpT::PXAT)),
                     Exist("PERSIST", &exp_params.persist)));
  parser.ExpectTag("FIELDS", "Mandatory argument FIELDS is missing or not at the right position");
  uint32_t numFields =
      parser.Next<FInt<1u, UINT32_MAX>>("Number of fields must be a positive integer");
  ParsedArgs fields = parser.UnparsedArgs();

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  if (fields.size() != numFields)
    return rb->SendError("The `numfields` parameter must match the number of arguments",
                         kSyntaxErrType);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpHGetEx(t->GetOpArgs(shard), key, fields, exp_params);
  };
  OpResult<vector<OptStr>> result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));

  switch (result.status()) {
    case OpStatus::OK:
    case OpStatus::KEY_NOTFOUND: {
      RedisReplyBuilder::ArrayScope scope{rb, numFields};
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

void CmdHGet(CmdArgParser parser, CommandContext* cmd_cntx) {
  parser.Next();  // skip key
  auto cb = [field = parser.Next()](const HMapWrap& hw) -> OpResult<string> {
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

void CmdHMGet(CmdArgParser parser, CommandContext* cmd_cntx) {
  parser.Next();  // skip key
  ParsedArgs fields = parser.UnparsedArgs();
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

void CmdHStrLen(CmdArgParser parser, CommandContext* cmd_cntx) {
  parser.Next();  // skip key
  auto cb = [field = parser.Next()](const HMapWrap& hw) -> OpResult<uint32_t> {
    if (auto it = hw.Find(field); it)
      return it->second.length();
    return OpStatus::KEY_NOTFOUND;
  };
  HSetReplies{cmd_cntx}.Send(ExecuteRO(cmd_cntx->tx(), cb));
}

void CmdHLen(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto cb = [](const HMapWrap& hw) -> OpResult<uint32_t> { return hw.Length(); };
  HSetReplies{cmd_cntx}.Send(ExecuteRO(cmd_cntx->tx(), cb));
}

void CmdHExists(CmdArgParser parser, CommandContext* cmd_cntx) {
  parser.Next();  // skip key
  auto cb = [field = parser.Next()](const HMapWrap& hw) -> OpResult<uint32_t> {
    return hw.Find(field) ? 1 : 0;
  };
  HSetReplies{cmd_cntx}.Send(ExecuteRO(cmd_cntx->tx(), cb));
}

void CmdHIncrBy(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  string_view field = parser.Next();
  string_view incrs = parser.Next();
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

void CmdHIncrByFloat(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();
  string_view field = parser.Next();
  string_view incrs = parser.Next();
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

void CmdHKeys(CmdArgParser parser, CommandContext* cmd_cntx) {
  HGetGeneric(FIELDS, cmd_cntx);
}

void CmdHVals(CmdArgParser parser, CommandContext* cmd_cntx) {
  HGetGeneric(VALUES, cmd_cntx);
}

void CmdHGetAll(CmdArgParser parser, CommandContext* cmd_cntx) {
  HGetGeneric(GetAllMode::FIELDS | GetAllMode::VALUES, cmd_cntx);
}

void CmdHScan(CmdArgParser parser, CommandContext* cmd_cntx) {
  parser.Next();  // skip key
  std::string_view token = parser.Next();
  uint64_t cursor = 0;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (!absl::SimpleAtoi(token, &cursor)) {
    return rb->SendError("invalid cursor");
  }

  // HSCAN key cursor [MATCH pattern] [COUNT count] [NOVALUES]
  if (cmd_cntx->tail_args().size() > 7) {
    DVLOG(1) << "got " << cmd_cntx->tail_args().size() << " this is more than it should be";
    return rb->SendError(kSyntaxErr);
  }

  OpResult<ScanOpts> ops = ScanOpts::TryFrom(cmd_cntx->tail_args().Tail(2), true);
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

void CmdHSet(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view cmd{cmd_cntx->cid()->name()};
  auto* rb = cmd_cntx->rb();
  // tail_args = key + field/value pairs; a valid command has an odd count.
  if (cmd_cntx->tail_args().size() % 2 != 1) {
    return rb->SendError(facade::WrongNumArgsError(cmd), kSyntaxErrType);
  }

  string_view key = parser.Next();

  optional<util::fb2::Future<bool>> tiered_backpressure;
  OpSetParams params{.backpressure = &tiered_backpressure};

  ParsedArgs values = parser.UnparsedArgs();
  auto cb = [&, values](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, values, params);
  };

  auto delayed_result = cmd_cntx->tx()->ScheduleSingleHopT(std::move(cb));
  OpResult<uint32_t> result = Unwrap(std::move(delayed_result));

  if (tiered_backpressure)
    tiered_backpressure->GetFor(10ms);

  if (result && cmd == "HSET") {
    rb->SendLong(*result);
  } else {
    cmd_cntx->SendError(result.status());
  }
}

void CmdHSetNx(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();

  ParsedArgs values = parser.UnparsedArgs();
  auto cb = [&, values](Transaction* t, EngineShard* shard) {
    return OpSet(t->GetOpArgs(shard), key, values, OpSetParams{.mode = OpSetParams::Mode::kNX});
  };
  HSetReplies{cmd_cntx}.Send(Unwrap(cmd_cntx->tx()->ScheduleSingleHopT(cb)));
}

void StrVecEmplaceBack(StringVec& str_vec, const listpackEntry& lp) {
  if (lp.sval) {
    str_vec.emplace_back(reinterpret_cast<char*>(lp.sval), lp.slen);
    return;
  }
  str_vec.emplace_back(absl::StrCat(lp.lval));
}

void CmdHRandField(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  const size_t argc = cmd_cntx->tail_args().size();
  if (argc > 3) {
    DVLOG(1) << "Wrong number of command arguments: " << argc;
    return rb->SendError(kSyntaxErr);
  }

  string_view key = parser.Next();
  int32_t count;
  bool with_values = false;

  if ((argc > 1) && (!SimpleAtoi(parser.Next(), &count))) {
    return rb->SendError("count value is not an integer", kSyntaxErrType);
  }

  if (argc == 3) {
    string arg = absl::AsciiStrToUpper(parser.Next());
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

      if (argc == 1) {
        auto opt_pair = string_map->RandomPair();
        if (opt_pair.has_value()) {
          auto [key, value] = *opt_pair;
          str_vec.emplace_back(key, sdslen(key));
        }
      } else {
        size_t real_size = string_map->SizeSlow();
        size_t actual_count =
            (count >= 0) ? std::min(size_t(count), real_size) : size_t(-int64_t(count));
        std::vector<sds> keys, vals;
        if (real_size > 0 && actual_count > 0) {
          if (count >= 0) {
            string_map->RandomPairsUnique(actual_count, keys, vals, with_values);
          } else {
            string_map->RandomPairs(actual_count, keys, vals, with_values);
          }
        }
        for (size_t i = 0; i < keys.size(); ++i) {
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
      if (argc == 1) {
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
    if (result->size() == 1 && argc == 1)
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
    if (argc == 1)
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
            << CI{"HPEXPIRETIME", CO::READONLY | CO::FAST, -4, 1, 1}.HFUNC(HPExpireTime)
            << CI{"HGETEX", CO::JOURNALED | CO::FAST | CO::DENYOOM, -4, 1, 1}.HFUNC(HGetEx)
            << CI{"HTTL", CO::READONLY | CO::FAST, -4, 1, 1}.HFUNC(HTtl)
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

auto HSetFamily::LoadListpackBlob(std::string_view blob, bool deep, PrimeValue* pv)
    -> LoadBlobResult {
  if (!lpValidateIntegrity((uint8_t*)blob.data(), blob.size(), deep ? 1 : 0, nullptr, nullptr)) {
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
    pv.SetMemberTime(MemberTimeSeconds(db_context.time_now_ms));
    StringMap* string_map = (StringMap*)pv.RObjPtr();
    auto it = string_map->Find(field);
    if (it == string_map->end())
      return -3;
    return it.HasExpiry() ? it.ExpiryTime() : -1;
  }
}

bool HSetFamily::DeleteIfEmpty(DbSlice& db_slice, const DbContext& db_cntx, std::string_view key,
                               const PrimeValue& pv) {
  if (pv.Encoding() != kEncodingStrMap2)
    return false;

  if (auto* sm = static_cast<StringMap*>(pv.RObjPtr()); !sm->Empty())
    return false;

  if (auto res = db_slice.FindMutable(db_cntx, key, OBJ_HASH); res) {
    db_slice.DelMutable(db_cntx, std::move(*res));
    if (db_slice.shard_owner()->journal()) {
      RecordDelete(db_cntx.db_index, key);
    }
    return true;
  }
  return false;
}

// returns vector of results for each field in values:
// -2 if the provided key does not exist.
// 0 if the specified NX | XX | GT | LT condition has not been met.
// 1 if the expiration time was set/updated.
// 2 when HEXPIRE/HPEXPIRE is called with 0 seconds and the field is deleted.
static std::vector<long> UpdateTTL(ParsedArgs values, uint32_t ttl_sec, ExpireFlags flags,
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
                                             ExpireFlags flags, string_view key,
                                             const ParsedArgs& fields, PrimeValue* pv) {
  DCHECK_EQ(OBJ_HASH, pv->ObjType());
  // values contains field names — collect them for HNSW field data preservation.
  absl::InlinedVector<std::string_view, 4> field_names(fields.begin(), fields.end());
  op_args.shard->search_indices()->RemoveDoc(key, op_args.db_cntx, *pv, field_names);

  if (pv->Encoding() == kEncodingListPack) {
    // a valid result can never be a listpack, since it doesnt keep ttl
    uint8_t* lp = (uint8_t*)pv->RObjPtr();
    StringMap* sm = HSetFamily::ConvertToStrMap(lp);
    pv->InitRobj(OBJ_HASH, kEncodingStrMap2, sm);
  }

  // This needs to be explicitly fetched again since the pv might have changed.
  StringMap* sm = container_utils::GetStringMap(*pv, op_args.db_cntx);
  vector<long> res = UpdateTTL(fields, ttl_sec, flags, sm);
  op_args.shard->search_indices()->AddDoc(key, op_args.db_cntx, pv);
  return res;
}

}  // namespace dfly
