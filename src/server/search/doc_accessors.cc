// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/doc_accessors.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>

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

string FtVectorToString(const search::FtVector& vec) {
  return absl::StrCat("[", absl::StrJoin(vec, ","), "]");
}

}  // namespace

string_view ListPackAccessor::GetString(string_view active_field) const {
  return container_utils::LpFind(lp_, active_field, intbuf_[0].data()).value_or(""sv);
}

search::FtVector ListPackAccessor::GetVector(string_view active_field) const {
  return BytesToFtVector(GetString(active_field));
}

SearchDocData ListPackAccessor::Serialize(search::Schema schema) const {
  SearchDocData out{};

  uint8_t* fptr = lpFirst(lp_);
  DCHECK_NE(fptr, nullptr);

  while (fptr) {
    string_view k = container_utils::LpGetView(fptr, intbuf_[0].data());
    fptr = lpNext(lp_, fptr);
    string_view v = container_utils::LpGetView(fptr, intbuf_[1].data());
    fptr = lpNext(lp_, fptr);

    if (schema.fields.at(k).type == search::SchemaField::VECTOR)
      out[k] = FtVectorToString(GetVector(k));
    else
      out[k] = v;
  }

  return out;
}

string_view StringMapAccessor::GetString(string_view active_field) const {
  return SdsToSafeSv(hset_->Find(active_field));
}

search::FtVector StringMapAccessor::GetVector(string_view active_field) const {
  return BytesToFtVector(GetString(active_field));
}

SearchDocData StringMapAccessor::Serialize(search::Schema schema) const {
  SearchDocData out{};
  for (const auto& [kptr, vptr] : *hset_) {
    string_view k = SdsToSafeSv(kptr);
    string_view v = SdsToSafeSv(vptr);

    if (schema.fields.at(k).type == search::SchemaField::VECTOR)
      out[k] = FtVectorToString(GetVector(k));
    else
      out[k] = v;
  }

  return out;
}

struct JsonAccessor::JsonPathContainer : public jsoncons::jsonpath::jsonpath_expression<JsonType> {
};

string_view JsonAccessor::GetString(string_view active_field) const {
  auto res = GetPath(active_field)->evaluate(json_);
  DCHECK(res.is_array());
  if (res.empty())
    return "";
  buf_ = res[0].as_string();
  return buf_;
}

search::FtVector JsonAccessor::GetVector(string_view active_field) const {
  auto res = GetPath(active_field)->evaluate(json_);
  DCHECK(res.is_array());
  if (res.empty())
    return {};

  search::FtVector out;
  for (auto v : res[0].array_range())
    out.push_back(v.as<float>());
  return out;
}

JsonAccessor::JsonPathContainer* JsonAccessor::GetPath(std::string_view field) const {
  if (auto it = path_cache_.find(field); it != path_cache_.end()) {
    return it->second.get();
  }

  error_code ec;
  auto path_expr = jsoncons::jsonpath::make_expression<JsonType>(field, ec);
  DCHECK(!ec) << "missing validation on ft.create step";

  JsonPathContainer path_container{move(path_expr)};
  auto ptr = make_unique<JsonPathContainer>(move(path_container));

  JsonPathContainer* path = ptr.get();
  path_cache_[field] = move(ptr);
  return path;
}

SearchDocData JsonAccessor::Serialize(search::Schema schema) const {
  SearchDocData out{};
  for (const auto& member : json_.object_range()) {
    out[member.key()] = member.value().as_string();
  }
  return out;
}

void JsonAccessor::RemoveFieldFromCache(string_view field) {
  path_cache_.erase(field);
}

thread_local absl::flat_hash_map<std::string, std::unique_ptr<JsonAccessor::JsonPathContainer>>
    JsonAccessor::path_cache_;

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
