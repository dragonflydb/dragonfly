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

bool ListPackAccessor::Check(FieldConsumer f, string_view active_field) const {
  std::array<uint8_t, LP_INTBUF_SIZE> intbuf;

  if (!active_field.empty()) {
    return f(container_utils::LpFind(lp_, active_field, intbuf.data()).value_or(""));
  }

  uint8_t* fptr = lpFirst(lp_);
  DCHECK_NE(fptr, nullptr);

  while (fptr) {
    fptr = lpNext(lp_, fptr);  // skip key
    string_view v = container_utils::LpGetView(fptr, intbuf.data());
    fptr = lpNext(lp_, fptr);

    if (f(v))
      return true;
  }

  return false;
}

SearchDocData ListPackAccessor::Serialize() const {
  std::array<uint8_t, LP_INTBUF_SIZE> intbuf[2];
  SearchDocData out{};

  uint8_t* fptr = lpFirst(lp_);
  DCHECK_NE(fptr, nullptr);

  while (fptr) {
    string_view k = container_utils::LpGetView(fptr, intbuf[0].data());
    fptr = lpNext(lp_, fptr);
    string_view v = container_utils::LpGetView(fptr, intbuf[1].data());
    fptr = lpNext(lp_, fptr);

    out[k] = v;
  }

  return out;
}

bool StringMapAccessor::Check(FieldConsumer f, string_view active_field) const {
  if (!active_field.empty()) {
    return f(SdsToSafeSv(hset_->Find(active_field)));
  }

  for (const auto& [k, v] : *hset_) {
    if (f(SdsToSafeSv(v)))
      return true;
  }

  return false;
}

SearchDocData StringMapAccessor::Serialize() const {
  SearchDocData out{};
  for (const auto& [k, v] : *hset_)
    out[SdsToSafeSv(k)] = SdsToSafeSv(v);
  return out;
}

bool JsonAccessor::Check(FieldConsumer f, string_view active_field) const {
  if (!active_field.empty()) {
    return f(json_->get_value_or<string>(active_field, string{}));
  }
  for (const auto& member : json_->object_range()) {
    if (f(member.value().as_string()))
      return true;
  }
  return false;
}

SearchDocData JsonAccessor::Serialize() const {
  SearchDocData out{};
  for (const auto& member : json_->object_range()) {
    out[member.key()] = member.value().as_string();
  }
  return out;
}

unique_ptr<BaseAccessor> GetAccessor(const OpArgs& op_args, const PrimeValue& pv) {
  DCHECK(pv.ObjType() == OBJ_HASH || pv.ObjType() == OBJ_JSON);

  if (pv.ObjType() == OBJ_JSON) {
    DCHECK(pv.GetJson());
    return make_unique<JsonAccessor>(pv.GetJson());
  }

  if (pv.Encoding() == kEncodingListPack) {
    auto ptr = reinterpret_cast<ListPackAccessor::LpPtr>(pv.RObjPtr());
    return make_unique<ListPackAccessor>(ptr);
  } else {
    auto* sm = container_utils::GetStringMap(pv, op_args.db_cntx);
    return make_unique<StringMapAccessor>(sm);
  }
}

}  // namespace dfly
