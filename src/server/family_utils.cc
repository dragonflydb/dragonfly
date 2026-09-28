#include "server/family_utils.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>
#include <xxhash.h>

#include "base/logging.h"
#include "core/detail/listpack_wrap.h"
#include "core/oah_set.h"
#include "core/string_map.h"
#include "server/db_slice.h"
#include "server/tx_base.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/sds.h"
#include "redis/stream.h"
#include "redis/util.h"
#include "redis/ziplist.h"
#include "redis/zmalloc.h"
}

namespace dfly {

using namespace std;

namespace {

struct ZiplistCbArgs {
  long count = 0;
  absl::flat_hash_set<string_view> fields;
  unsigned char** lp;
};

int ZiplistPairsEntryConvertAndValidate(unsigned char* p, unsigned int head_count, void* userdata) {
  unsigned char* str;
  unsigned int slen;
  long long vll;

  ZiplistCbArgs* data = (ZiplistCbArgs*)userdata;

  if (data->fields.empty()) {
    data->fields.reserve(head_count / 2);
  }

  if (!ziplistGet(p, &str, &slen, &vll))
    return 0;

  if (((data->count) & 1) == 0) {
    sds field = str ? sdsnewlen(str, slen) : sdsfromlonglong(vll);
    auto [_, inserted] = data->fields.emplace(field, sdslen(field));
    if (!inserted) {
      sdsfree(field);
      return 0;
    }
  }

  if (str) {
    *(data->lp) = lpAppend(*(data->lp), (unsigned char*)str, slen);
  } else {
    *(data->lp) = lpAppendInteger(*(data->lp), vll);
  }

  (data->count)++;
  return 1;
}

}  // namespace

string XXH3_Digest(std::string_view s) {
  uint64_t hash = XXH3_64bits(s.data(), s.size());
  return absl::StrCat(absl::Hex(hash, absl::kZeroPad16));
}

int32_t FieldExpireTime(const DbContext& db_cntx, const PrimeValue& pv, string_view field) {
  DCHECK(pv.ObjType() == OBJ_SET || pv.ObjType() == OBJ_HASH);
  if (pv.Encoding() == kEncodingIntSet) {
    long long llval;
    return string2ll(field.data(), field.size(), &llval) ? -1 : -3;
  }
  if (pv.Encoding() == kEncodingListPack) {
    detail::ListpackWrap lw{static_cast<uint8_t*>(pv.RObjPtr())};
    return lw.Find(field) == lw.end() ? -3 : -1;
  }

  pv.SetMemberTime(MemberTimeSeconds(db_cntx.time_now_ms));
  auto get_expiry = [field](auto* owner) -> int32_t {
    auto it = owner->Find(field);
    if (it == owner->end())
      return -3;
    return it.HasExpiry() ? it.ExpiryTime() : -1;
  };
  return pv.ObjType() == OBJ_SET ? VisitSet(pv.RObjPtr(), get_expiry)
                                 : get_expiry(static_cast<StringMap*>(pv.RObjPtr()));
}

bool DeleteCollectionIfEmpty(DbSlice& db_slice, const DbContext& db_cntx, string_view key,
                             const PrimeValue& pv) {
  const auto obj_type = pv.ObjType();
  DCHECK(obj_type == OBJ_SET || obj_type == OBJ_HASH);
  if (pv.Encoding() != kEncodingStrMap2)
    return false;

  bool empty = obj_type == OBJ_SET ? VisitSet(pv.RObjPtr(), [](auto* s) { return s->Empty(); })
                                   : static_cast<StringMap*>(pv.RObjPtr())->Empty();
  if (!empty)
    return false;

  if (auto res = db_slice.FindMutable(db_cntx, key, obj_type); res) {
    db_slice.DelMutable(db_cntx, std::move(*res));
    if (db_slice.shard_owner()->journal()) {
      RecordDelete(db_cntx.db_index, key);
    }
    return true;
  }
  return false;
}

sds WrapSds(std::string_view s) {
  static thread_local sds tmp_sds = sdsempty();
  return tmp_sds = sdscpylen(tmp_sds, s.data(), s.length());
}

NonUniquePicksGenerator::NonUniquePicksGenerator(RandomPick max_range) : max_range_(max_range) {
  CHECK_GT(max_range, RandomPick(0));
}

RandomPick NonUniquePicksGenerator::Generate() {
  return absl::Uniform(bitgen_, 0u, max_range_);
}

UniquePicksGenerator::UniquePicksGenerator(std::uint32_t picks_count, RandomPick max_range)
    : remaining_picks_count_(picks_count), picked_indexes_(picks_count) {
  CHECK_GE(max_range, picks_count);
  current_random_limit_ = max_range - picks_count;
}

RandomPick UniquePicksGenerator::Generate() {
  DCHECK_GT(remaining_picks_count_, 0u);

  remaining_picks_count_--;

  const RandomPick max_index = current_random_limit_++;
  const RandomPick random_index = absl::Uniform(bitgen_, 0u, max_index + 1u);

  const bool random_index_is_picked = picked_indexes_.emplace(random_index).second;
  if (random_index_is_picked) {
    return random_index;
  }

  picked_indexes_.insert(max_index);
  return max_index;
}

streamConsumer* StreamCreateConsumer(streamCG* cg, string_view name, uint64_t now_ms, int flags) {
  DCHECK(cg);
  DCHECK(!name.empty());
  if (cg == NULL)
    return NULL;

  streamConsumer* consumer = (streamConsumer*)zmalloc(sizeof(*consumer));

  int success =
      raxTryInsert(cg->consumers, (unsigned char*)name.data(), name.size(), consumer, NULL);
  if (!success) {
    zfree(consumer);
    return NULL;
  }
  consumer->name = sdsnewlen(name.data(), name.size());
  consumer->pel = raxNew();
  consumer->seen_time = now_ms;
  consumer->active_time = -1;

  return consumer;
}

int ZiplistPairsConvertAndValidateIntegrity(const uint8_t* zl, size_t size, unsigned char** lp) {
  ZiplistCbArgs data;
  data.lp = lp;

  int ret = ziplistValidateIntegrity(const_cast<uint8_t*>(zl), size, 1,
                                     ZiplistPairsEntryConvertAndValidate, &data);

  if (data.count & 1)
    ret = 0;

  for (auto field : data.fields) {
    sdsfree((sds)field.data());
  }
  return ret;
}

}  // namespace dfly
