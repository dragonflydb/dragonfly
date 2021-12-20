// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/db_slice.h"

#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "util/fiber_sched_algo.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace boost;
using namespace std;
using namespace util;

DbSlice::DbSlice(uint32_t index, EngineShard* owner) : shard_id_(index), owner_(owner) {
  db_arr_.emplace_back();
  CreateDbRedis(0);
}

DbSlice::~DbSlice() {
  for (auto& db : db_arr_) {
    if (!db.main_table)
      continue;
    db.main_table.reset();
  }
}

void DbSlice::Reserve(DbIndex db_ind, size_t key_size) {
  ActivateDb(db_ind);

  auto& db = db_arr_[db_ind];
  DCHECK(db.main_table);

  db.main_table->reserve(key_size);
}

auto DbSlice::Find(DbIndex db_index, std::string_view key) const -> OpResult<MainIterator> {
  auto [it, expire_it] = FindExt(db_index, key);

  if (it == MainIterator{})
    return OpStatus::KEY_NOTFOUND;

  return it;
}

pair<MainIterator, ExpireIterator> DbSlice::FindExt(DbIndex db_ind, std::string_view key) const {
  DCHECK_LT(db_ind, db_arr_.size());
  DCHECK(db_arr_[db_ind].main_table);

  auto& db = db_arr_[db_ind];
  MainIterator it = db.main_table->find(key);

  if (it == MainIterator{}) {
    return make_pair(it, ExpireIterator{});
  }

  ExpireIterator expire_it;
  if (it->second.HasExpire()) {  // check expiry state
    expire_it = db.expire_table->find(it->first);

    CHECK(expire_it != ExpireIterator{});
    if (expire_it->second <= now_ms_) {
      db.expire_table->erase(expire_it);

      db.stats.obj_memory_usage -= (it->first.capacity() + it->second.str.capacity());
      db.main_table->erase(it);
      return make_pair(MainIterator{}, ExpireIterator{});
    }
  }

  return make_pair(it, expire_it);
}

auto DbSlice::AddOrFind(DbIndex db_index, std::string_view key) -> pair<MainIterator, bool> {
  DCHECK_LT(db_index, db_arr_.size());
  DCHECK(db_arr_[db_index].main_table);

  auto& db = db_arr_[db_index];

  pair<MainIterator, bool> res = db.main_table->emplace(key, MainValue{});
  if (res.second) {  // new entry
    db.stats.obj_memory_usage += res.first->first.capacity();

    return make_pair(res.first, true);
  }

  return res;
}

void DbSlice::ActivateDb(DbIndex db_ind) {
  if (db_arr_.size() <= db_ind)
    db_arr_.resize(db_ind + 1);
  CreateDbRedis(db_ind);
}

void DbSlice::CreateDbRedis(unsigned index) {
  auto& db = db_arr_[index];
  if (!db.main_table) {
    db.main_table.reset(new MainTable);
    db.expire_table.reset(new ExpireTable);
  }
}

// Returns true if a state has changed, false otherwise.
bool DbSlice::Expire(DbIndex db_ind, MainIterator it, uint64_t at) {
  auto& db = db_arr_[db_ind];
  if (at == 0 && it->second.HasExpire()) {
    CHECK_EQ(1u, db.expire_table->erase(it->first));
    it->second.SetExpire(false);

    return true;
  }

  if (!it->second.HasExpire() && at) {
    CHECK(db.expire_table->emplace(it->first, at).second);
    it->second.SetExpire(true);

    return true;
  }

  return false;
}

void DbSlice::AddNew(DbIndex db_ind, std::string_view key, MainValue obj, uint64_t expire_at_ms) {
  CHECK(AddIfNotExist(db_ind, key, std::move(obj), expire_at_ms));
}

bool DbSlice::AddIfNotExist(DbIndex db_ind, std::string_view key, MainValue obj,
                        uint64_t expire_at_ms) {
  auto& db = db_arr_[db_ind];

  auto [new_entry, success] = db.main_table->emplace(key, obj);
  if (!success)
    return false;  // in this case obj won't be moved and will be destroyed during unwinding.

  db.stats.obj_memory_usage += (new_entry->first.capacity() + new_entry->second.str.capacity());

  if (expire_at_ms) {
    new_entry->second.SetExpire(true);
    CHECK(db.expire_table->emplace(new_entry->first, expire_at_ms).second);
  }

  return true;
}

size_t DbSlice::DbSize(DbIndex db_ind) const {
  DCHECK_LT(db_ind, db_array_size());

  if (IsDbValid(db_ind)) {
    return db_arr_[db_ind].main_table->size();
  }
  return 0;
}

}  // namespace dfly
