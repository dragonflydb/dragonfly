// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search_family.h"

#include <variant>
#include <vector>

#include "base/logging.h"
#include "core/search/search.h"
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

struct BaseAccessor : public search::HSetAccessor {
  using FieldConsumer = search::HSetAccessor::FieldConsumer;

  virtual DocumentData Serialize() const = 0;
};

struct ListPackAccessor : public BaseAccessor {
  using LpPtr = uint8_t*;

  ListPackAccessor(LpPtr ptr) : lp_{ptr} {
  }

  bool Check(FieldConsumer f, string_view active_field) const override {
    uint8_t intbuf[LP_INTBUF_SIZE];

    if (!active_field.empty()) {
      return f(container_utils::LpFind(lp_, active_field, intbuf).value_or(""));
    }

    uint8_t* fptr = lpFirst(lp_);
    while (fptr) {
      fptr = lpNext(lp_, fptr);  // skip key
      string_view v = container_utils::LpGetView(fptr, intbuf);
      fptr = lpNext(lp_, fptr);

      if (f(v))
        return true;
    }

    return false;
  }

  DocumentData Serialize() const override {
    uint8_t intbuf[2][LP_INTBUF_SIZE];
    DocumentData out{};

    uint8_t* fptr = lpFirst(lp_);
    while (fptr) {
      string_view k = container_utils::LpGetView(fptr, intbuf[0]);
      fptr = lpNext(lp_, fptr);  // skip key
      string_view v = container_utils::LpGetView(fptr, intbuf[1]);
      fptr = lpNext(lp_, fptr);

      out[k] = v;
    }

    return out;
  }

 private:
  LpPtr lp_;
};

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

unique_ptr<BaseAccessor> GetAccessor(const OpArgs& op_args, const PrimeValue& pv) {
  if (pv.Encoding() == kEncodingListPack) {
    auto ptr = reinterpret_cast<ListPackAccessor::LpPtr>(pv.RObjPtr());
    return make_unique<ListPackAccessor>(ptr);
  } else {
    DCHECK_EQ(pv.Encoding(), kEncodingStrMap2);
    auto* sm = container_utils::GetStringMap(pv, op_args.db_cntx);
    return make_unique<StringMapAccessor>(sm);
  }
}

// Perform brute force search for all hashes in shard with specific prefix
// that match the query
void OpSearch(const OpArgs& op_args, string_view prefix, const Query& query,
              vector<SerializedDocument>* shard_out) {
  auto& db_slice = op_args.shard->db_slice();
  DCHECK(db_slice.IsDbValid(op_args.db_cntx.db_index));
  auto [prime_table, _] = db_slice.GetTables(op_args.db_cntx.db_index);

  string scratch;
  auto cb = [&](PrimeTable::iterator it) {
    // Check entry is hash
    const PrimeValue& pv = it->second;
    if (pv.ObjType() != OBJ_HASH)
      return;

    // Check key starts with prefix
    string_view key = it->first.GetSlice(&scratch);
    if (key.rfind(prefix, 0) != 0)
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
  string prefix;

  if (args.size() > 1 && ArgS(args, 1) == "ON") {
    CHECK_EQ(ArgS(args, 2), "HASH");
    CHECK_EQ(ArgS(args, 3), "PREFIX");
    CHECK_EQ(ArgS(args, 4), "1");
    prefix = ArgS(args, 5);
  }
  {
    lock_guard lk{indices_mu_};
    indices_[idx] = prefix;
  }
  (*cntx)->SendOk();
}

void SearchFamily::FtSearch(CmdArgList args, ConnectionContext* cntx) {
  string_view index = ArgS(args, 0);
  string_view query_str = ArgS(args, 1);

  string prefix;
  {
    lock_guard lk{indices_mu_};
    auto it = indices_.find(index);
    if (it == indices_.end()) {
      (*cntx)->SendError("Search index not found");
      return;
    }
    prefix = it->second;
  }

  Query query = search::ParseQuery(query_str);
  if (!query) {
    (*cntx)->SendError("Invalid query");
    return;
  }

  vector<vector<SerializedDocument>> docs(shard_set->size());
  cntx->transaction->ScheduleSingleHop([&](Transaction* t, EngineShard* shard) {
    OpSearch(t->GetOpArgs(shard), prefix, query, &docs[shard->shard_id()]);
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

#define HFUNC(x) SetHandler(&SearchFamily::x)

void SearchFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"FT.CREATE", CO::FAST, -2, 0, 0, 0}.HFUNC(FtCreate)
            << CI{"FT.SEARCH", CO::GLOBAL_TRANS, -3, 0, 0, 0}.HFUNC(FtSearch);
}

Mutex SearchFamily::indices_mu_{};
absl::flat_hash_map<std::string, std::string> SearchFamily::indices_{};

}  // namespace dfly
