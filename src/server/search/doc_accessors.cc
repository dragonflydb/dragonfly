// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_accessors.h"

#include <jsoncons/json.hpp>

#include "core/json_object.h"
#include "core/search/search.h"
#include "core/string_map.h"
#include "server/container_utils.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
};

namespace dfly {

using namespace std;

namespace {

string_view SdsToSafeSv(sds str) {
  return str != nullptr ? string_view{str, sdslen(str)} : ""sv;
}

}  // namespace

string_view ListPackAccessor::Get(string_view active_field) const {
  return container_utils::LpFind(lp_, active_field, intbuf_[0].data()).value_or(""sv);
}

SearchDocData ListPackAccessor::Serialize() const {
  SearchDocData out{};

  uint8_t* fptr = lpFirst(lp_);
  DCHECK_NE(fptr, nullptr);

  while (fptr) {
    string_view k = container_utils::LpGetView(fptr, intbuf_[0].data());
    fptr = lpNext(lp_, fptr);
    string_view v = container_utils::LpGetView(fptr, intbuf_[1].data());
    fptr = lpNext(lp_, fptr);

    out[k] = v;
  }

  return out;
}

string_view StringMapAccessor::Get(string_view active_field) const {
  return SdsToSafeSv(hset_->Find(active_field));
}

SearchDocData StringMapAccessor::Serialize() const {
  SearchDocData out{};
  for (const auto& [k, v] : *hset_)
    out[SdsToSafeSv(k)] = SdsToSafeSv(v);
  return out;
}

string_view JsonAccessor::Get(string_view active_field) const {
  buf_ = json_->get_value_or<string>(active_field, string{});
  return buf_;
}

SearchDocData JsonAccessor::Serialize() const {
  SearchDocData out{};
  for (const auto& member : json_->object_range()) {
    out[member.key()] = member.value().as_string();
  }
  return out;
}

unique_ptr<BaseAccessor> GetAccessor(const DbContext& db_cntx, const PrimeValue& pv) {
  DCHECK(pv.ObjType() == OBJ_HASH || pv.ObjType() == OBJ_JSON);

  if (pv.ObjType() == OBJ_JSON) {
    DCHECK(pv.GetJson());
    return make_unique<JsonAccessor>(pv.GetJson());
  }

  if (pv.Encoding() == kEncodingListPack) {
    auto ptr = reinterpret_cast<ListPackAccessor::LpPtr>(pv.RObjPtr());
    return make_unique<ListPackAccessor>(ptr);
  } else {
    auto* sm = container_utils::GetStringMap(pv, db_cntx);
    return make_unique<StringMapAccessor>(sm);
  }
}

}  // namespace dfly
