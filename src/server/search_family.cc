// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search_family.h"

#include <jsoncons/json.hpp>
#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/json_object.h"
#include "core/search/search.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/transaction.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/object.h"
};

namespace dfly {

using namespace std;
using namespace facade;

namespace {

string_view SdsToSafeSv(sds str) {
  return str != nullptr ? string_view{str, sdslen(str)} : ""sv;
}

using DocumentData = absl::flat_hash_map<std::string, std::string>;
using SerializedDocument = pair<std::string /*key*/, DocumentData>;
using Query = search::AstExpr;

// Base class for document accessors
struct BaseAccessor : public search::DocumentAccessor {
  using FieldConsumer = search::DocumentAccessor::FieldConsumer;

  virtual DocumentData Serialize() const = 0;
};

// Accessor for hashes stored with listpack
struct ListPackAccessor : public BaseAccessor {
  using LpPtr = uint8_t*;

  ListPackAccessor(LpPtr ptr) : lp_{ptr} {
  }

  bool Check(FieldConsumer f, string_view active_field) const override {
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

  DocumentData Serialize() const override {
    std::array<uint8_t, LP_INTBUF_SIZE> intbuf[2];
    DocumentData out{};

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

 private:
  LpPtr lp_;
};

// Accessor for hashes stored with StringMap
struct StringMapAccessor : public BaseAccessor {
  StringMapAccessor(StringMap* hset) : hset_{hset} {
  }

  bool Check(FieldConsumer f, string_view active_field) const override {
    if (!active_field.empty()) {
      return f(SdsToSafeSv(hset_->Find(active_field)));
    }

    for (const auto& [k, v] : *hset_) {
      if (f(SdsToSafeSv(v)))
        return true;
    }

    return false;
  }

  DocumentData Serialize() const override {
    DocumentData out{};
    for (const auto& [k, v] : *hset_)
      out[SdsToSafeSv(k)] = SdsToSafeSv(v);
    return out;
  }

 private:
  StringMap* hset_;
};

// Accessor for json values
struct JsonAccessor : public BaseAccessor {
  JsonAccessor(JsonType* json) : json_{json} {
  }

  bool Check(FieldConsumer f, string_view active_field) const override {
    if (!active_field.empty()) {
      return f(json_->get_value_or<string>(active_field, string{}));
    }
    for (const auto& member : json_->object_range()) {
      if (f(member.value().as_string()))
        return true;
    }
    return false;
  }

  DocumentData Serialize() const override {
    DocumentData out{};
    for (const auto& member : json_->object_range()) {
      out[member.key()] = member.value().as_string();
    }
    return out;
  }

 private:
  JsonType* json_;
};

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

// Perform brute force search for all hashes in shard with specific prefix
// that match the query
void OpSearch(const OpArgs& op_args, const SearchFamily::IndexData& index, const Query& query,
              vector<SerializedDocument>* shard_out) {
  auto& db_slice = op_args.shard->db_slice();
  DCHECK(db_slice.IsDbValid(op_args.db_cntx.db_index));
  auto [prime_table, _] = db_slice.GetTables(op_args.db_cntx.db_index);

  string scratch;
  auto cb = [&](PrimeTable::iterator it) {
    // Check entry is hash
    const PrimeValue& pv = it->second;
    if (pv.ObjType() != index.GetObjCode())
      return;

    // Check key starts with prefix
    string_view key = it->first.GetSlice(&scratch);
    if (key.rfind(index.prefix, 0) != 0)
      return;

    // Check entry matches filter
    auto accessor = GetAccessor(op_args, pv);
    if (query->Check(search::SearchInput{accessor.get()}))
      shard_out->emplace_back(key, accessor->Serialize());
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = prime_table->Traverse(cursor, cb);
  } while (cursor);
}

}  // namespace

void SearchFamily::FtCreate(CmdArgList args, ConnectionContext* cntx) {
  string_view idx = ArgS(args, 0);

  IndexData index{};

  for (size_t i = 1; i < args.size(); i++) {
    ToUpper(&args[i]);

    // [ON HASH | JSON]
    if (ArgS(args, i) == "ON") {
      if (++i >= args.size())
        return (*cntx)->SendError(kSyntaxErr);

      ToUpper(&args[i]);
      string_view type = ArgS(args, i);
      if (type == "HASH")
        index.type = IndexData::HASH;
      else if (type == "JSON")
        index.type = IndexData::JSON;
      else
        return (*cntx)->SendError("Invalid rule type: " + string{type});
      continue;
    }

    // [PREFIX count prefix [prefix ...]]
    if (ArgS(args, i) == "PREFIX") {
      if (i + 2 >= args.size())
        return (*cntx)->SendError(kSyntaxErr);

      if (ArgS(args, ++i) != "1")
        return (*cntx)->SendError("Multiple prefixes are not supported");

      index.prefix = ArgS(args, ++i);
      continue;
    }
  }

  {
    lock_guard lk{indices_mu_};
    indices_[idx] = move(index);
  }
  (*cntx)->SendOk();
}

void SearchFamily::FtSearch(CmdArgList args, ConnectionContext* cntx) {
  string_view index_name = ArgS(args, 0);
  string_view query_str = ArgS(args, 1);

  IndexData index;
  {
    lock_guard lk{indices_mu_};
    auto it = indices_.find(index_name);
    if (it == indices_.end()) {
      (*cntx)->SendError(string{index_name} + ": no such index");
      return;
    }
    index = it->second;
  }

  Query query = search::ParseQuery(query_str);
  if (!query) {
    (*cntx)->SendError("Syntax error");
    return;
  }

  vector<vector<SerializedDocument>> docs(shard_set->size());
  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* shard) {
    OpSearch(t->GetOpArgs(shard), index, query, &docs[shard->shard_id()]);
    return OpStatus::OK;
  });

  size_t total_count = 0;
  for (const auto& shard_docs : docs)
    total_count += shard_docs.size();

  (*cntx)->StartArray(total_count * 2 + 1);
  (*cntx)->SendLong(total_count);
  for (const auto& shard_docs : docs) {
    for (const auto& [key, doc] : shard_docs) {
      (*cntx)->SendBulkString(key);
      (*cntx)->StartCollection(doc.size(), RedisReplyBuilder::MAP);
      for (const auto& [k, v] : doc) {
        (*cntx)->SendBulkString(k);
        (*cntx)->SendBulkString(v);
      }
    }
  }
}

uint8_t SearchFamily::IndexData::GetObjCode() const {
  return type == JSON ? OBJ_JSON : OBJ_HASH;
}

#define HFUNC(x) SetHandler(&SearchFamily::x)

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"FT.CREATE", CO::FAST, -2, 0, 0, 0}.HFUNC(FtCreate)
            << CI{"FT.SEARCH", CO::GLOBAL_TRANS, -3, 0, 0, 0}.HFUNC(FtSearch);
}

Mutex SearchFamily::indices_mu_{};
absl::flat_hash_map<std::string, SearchFamily::IndexData> SearchFamily::indices_{};

}  // namespace dfly
