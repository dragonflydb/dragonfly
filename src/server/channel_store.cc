#include "server/channel_store.h"

// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <shared_mutex>

extern "C" {
#include "redis/util.h"
}

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

namespace dfly {
using namespace std;

ChannelStore::Subscriber::Subscriber(ConnectionContext* cntx, uint32_t tid)
    : conn_cntx(cntx), borrow_token(cntx->conn_state.subscribe_info->borrow_token), thread_id(tid) {
}

ChannelStore::Subscriber::Subscriber(uint32_t tid)
    : conn_cntx(nullptr), borrow_token(0), thread_id(tid) {
}

bool ChannelStore::Subscriber::ByThread(const Subscriber& lhs, const Subscriber& rhs) {
  if (lhs.thread_id == rhs.thread_id)
    return (lhs.conn_cntx != nullptr) < (rhs.conn_cntx != nullptr);
  return lhs.thread_id < rhs.thread_id;
}

ChannelStore::UpdatablePointer::UpdatablePointer(const UpdatablePointer& other) {
  ptr.store(other.ptr.load(memory_order_relaxed), memory_order_relaxed);
}

ChannelStore::SubscribeMap* ChannelStore::UpdatablePointer::Get() const {
  return ptr.load(memory_order_relaxed);
}

void ChannelStore::UpdatablePointer::Set(ChannelStore::SubscribeMap* sm) {
  ptr.store(sm, memory_order_relaxed);
}

ChannelStore::SubscribeMap* ChannelStore::UpdatablePointer::operator->() {
  return Get();
}

const ChannelStore::SubscribeMap& ChannelStore::UpdatablePointer::operator*() const {
  return *Get();
}

void ChannelStore::ChannelMap::Add(string_view key, ConnectionContext* me, uint32_t thread_id) {
  auto it = find(key);
  if (it == end())
    it = emplace(key, new SubscribeMap{}).first;
  it->second->emplace(me, thread_id);
}

void ChannelStore::ChannelMap::Remove(string_view key, ConnectionContext* me) {
  if (auto it = find(key); it != end()) {
    it->second->erase(me);
    if (it->second->empty())
      erase(it);
  }
}

void ChannelStore::ChannelMap::DeleteAll() {
  for (auto [k, ptr] : *this)
    delete ptr.Get();
}

ChannelStore::ChannelStore(ChannelStore::ControlBlock* cb)
    : channels_{new ChannelMap{}}, patterns_{new ChannelMap{}}, control_block_{cb} {
  cb->most_recent = this;
}

ChannelStore::ChannelStore(ChannelMap* channels, ChannelMap* patterns, ControlBlock* cb)
    : channels_{channels}, patterns_{patterns}, control_block_{cb} {
}

void ChannelStore::ControlBlock::Destroy() {
  update_mu.lock();
  update_mu.unlock();
  for (auto* chan_map : {most_recent->channels_, most_recent->patterns_}) {
    chan_map->DeleteAll();
    delete chan_map;
  }
  delete most_recent;
}

unsigned ChannelStore::SlotSize(bool pattern, string_view key) {
  ChannelMap* map = pattern ? patterns_ : channels_;
  auto it = map->find(key);
  return it == map->end() ? 0 : it->second->size();
}

vector<ChannelStore::Subscriber> ChannelStore::FetchSubscribers(string_view channel) const {
  vector<Subscriber> res;

  if (auto it = channels_->find(channel); it != channels_->end()) {
    Fill(*it->second, string{}, &res);
  }

  for (const auto& [pat, subs] : *patterns_) {
    if (stringmatchlen(pat.data(), pat.size(), channel.data(), channel.size(), 0) == 1) {
      Fill(*subs, pat, &res);
    }
  }

  sort(res.begin(), res.end(), Subscriber::ByThread);
  return res;
}

void ChannelStore::Fill(const SubscribeMap& src, const string& pattern, vector<Subscriber>* out) {
  out->reserve(out->size() + src.size());
  for (const auto [cntx, thread_id] : src) {
    CHECK(cntx->conn_state.subscribe_info);

    Subscriber s(cntx, thread_id);
    s.pattern = pattern;
    s.borrow_token.Inc();

    out->push_back(std::move(s));
  }
}

std::vector<string> ChannelStore::ListChannels(const string_view pattern) const {
  vector<string> res;
  for (const auto& [channel, _] : *channels_) {
    if (pattern.empty() ||
        stringmatchlen(pattern.data(), pattern.size(), channel.data(), channel.size(), 0) == 1) {
      res.push_back(channel);
    }
  }
  return res;
}

size_t ChannelStore::PatternCount() const {
  return patterns_->size();
}

ChannelStoreUpdater::ChannelStoreUpdater(ChannelStore* store, bool pattern, ConnectionContext* cntx,
                                         uint32_t thread_id)
    : store_{store}, pattern_{pattern}, cntx_{cntx}, thread_id_{thread_id} {
}

void ChannelStoreUpdater::Add(string_view key) {
  ops_.emplace_back(key, true);
  // If we insert the first entry, we need to add a new map slot
  needs_copy_ = store_->SlotSize(pattern_, key) == 0;
}

void ChannelStoreUpdater::Remove(string_view key) {
  ops_.emplace_back(key, false);
  // If we remove the last entry, we need to clear the map slot
  needs_copy_ = store_->SlotSize(pattern_, key) == 1;
}

void ChannelStoreUpdater::Apply() {
  using ChannelMap = ChannelStore::ChannelMap;
  using SubscribeMap = ChannelStore::SubscribeMap;

  ChannelStore::ControlBlock* cb = store_->control_block_;
  cb->update_mu.lock();
  store_ = cb->most_recent;

  ChannelMap* target = pattern_ ? store_->patterns_ : store_->channels_;

  if (needs_copy_)
    target = new ChannelMap{*target};

  for (auto [key, add] : ops_) {
    auto it = target->find(key);
    DCHECK(it != target->end() || add);

    if (add && it == target->end()) {
      DCHECK(needs_copy_);
      target->emplace(key, new SubscribeMap{{cntx_, thread_id_}});
      continue;
    }

    if (!add && it->second->size() == 1) {
      DCHECK(it->second->begin()->first == cntx_);
      freelist_.push_back(it->second.Get());
      target->erase(it);
      continue;
    }

    DCHECK(it->second->size() > 0);
    auto* replacement = new SubscribeMap{*it->second};
    if (add)
      replacement->emplace(cntx_, thread_id_);
    else
      replacement->erase(cntx_);

    freelist_.push_back(it->second.Get());
    it->second.Set(replacement);
  }

  auto* replacement = store_;
  if (needs_copy_) {
    replacement = pattern_ ? new ChannelStore{store_->channels_, target, cb}
                           : new ChannelStore{target, store_->patterns_, cb};
  }

  shard_set->pool()->Await([replacement](unsigned idx, util::ProactorBase*) {
    ServerState::tlocal()->UpdateChannelStore(replacement);
  });

  cb->most_recent = replacement;
  cb->update_mu.unlock();

  if (needs_copy_)
    delete store_;

  for (auto ptr : freelist_)
    delete ptr;
}

}  // namespace dfly
