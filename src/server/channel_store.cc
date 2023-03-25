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

namespace {

bool Matches(string_view pattern, string_view channel) {
  return stringmatchlen(pattern.data(), pattern.size(), channel.data(), channel.size(), 0) == 1;
}

}  // namespace

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

ChannelStore::ChannelStore() : channels_{new ChannelMap{}}, patterns_{new ChannelMap{}} {
  control_block.most_recent = this;
}

ChannelStore::ChannelStore(ChannelMap* channels, ChannelMap* patterns)
    : channels_{channels}, patterns_{patterns} {
}

void ChannelStore::Destroy() {
  control_block.update_mu.lock();
  control_block.update_mu.unlock();

  auto* store = control_block.most_recent.load(memory_order_relaxed);
  for (auto* chan_map : {store->channels_, store->patterns_}) {
    chan_map->DeleteAll();
    delete chan_map;
  }
  delete control_block.most_recent;
}

ChannelStore::ControlBlock ChannelStore::control_block;

vector<ChannelStore::Subscriber> ChannelStore::FetchSubscribers(string_view channel) const {
  vector<Subscriber> res;

  if (auto it = channels_->find(channel); it != channels_->end())
    Fill(*it->second, string{}, &res);

  for (const auto& [pat, subs] : *patterns_) {
    if (Matches(pat, channel))
      Fill(*subs, pat, &res);
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
    if (pattern.empty() || Matches(pattern, channel))
      res.push_back(channel);
  }
  return res;
}

size_t ChannelStore::PatternCount() const {
  return patterns_->size();
}

ChannelStoreUpdater::ChannelStoreUpdater(bool pattern, bool to_add, ConnectionContext* cntx,
                                         uint32_t thread_id)
    : pattern_{pattern}, to_add_{to_add}, cntx_{cntx}, thread_id_{thread_id} {
}

void ChannelStoreUpdater::Record(string_view key) {
  ops_.emplace_back(key);
}

pair<ChannelStore::ChannelMap*, bool> ChannelStoreUpdater::GetTargetMap(ChannelStore* store) {
  auto* target = pattern_ ? store->patterns_ : store->channels_;

  for (auto key : ops_) {
    auto it = target->find(key);
    DCHECK(it != target->end() || to_add_);
    // We need to make a copy, if we are going to add or delete new map slot.
    if ((to_add_ && it == target->end()) || (!to_add_ && it->second->size() == 1))
      return {new ChannelStore::ChannelMap{*target}, true};
  }

  return {target, false};
}

void ChannelStoreUpdater::Modify(ChannelMap* target, string_view key) {
  using SubscribeMap = ChannelStore::SubscribeMap;

  auto it = target->find(key);

  // New key, add new slot.
  if (to_add_ && it == target->end()) {
    target->emplace(key, new SubscribeMap{{cntx_, thread_id_}});
    return;
  }

  // Last entry for key, remove slot.
  if (!to_add_ && it->second->size() == 1) {
    DCHECK(it->second->begin()->first == cntx_);
    freelist_.push_back(it->second.Get());
    target->erase(it);
    return;
  }

  // RCU update existing SubscribeMap entry.
  DCHECK(it->second->size() > 0);
  auto* replacement = new SubscribeMap{*it->second};
  if (to_add_)
    replacement->emplace(cntx_, thread_id_);
  else
    replacement->erase(cntx_);

  // The pointer can still be in use, so delay freeing it
  // until the dispatch and update the slot atomically.
  freelist_.push_back(it->second.Get());
  it->second.Set(replacement);
}

void ChannelStoreUpdater::Apply() {
  // Wait for other updates to finish, lock the control block and update store pointer.
  auto& cb = ChannelStore::control_block;
  cb.update_mu.lock();
  auto* store = cb.most_recent.load(memory_order_relaxed);

  // Get target map (copied if needed) and apply operations.
  auto [target, copied] = GetTargetMap(store);
  for (auto key : ops_)
    Modify(target, key);

  // Prepare replacement.
  auto* replacement = store;
  if (copied) {
    auto* new_chans = pattern_ ? store->channels_ : target;
    auto* new_patterns = pattern_ ? target : store->patterns_;
    replacement = new ChannelStore{new_chans, new_patterns};
  }

  // Update control block and unlock it.
  cb.most_recent.store(replacement, memory_order_relaxed);
  cb.update_mu.unlock();

  // Update thread local references. Readers fetch subscribers via FetchSubscribers,
  // which runs without preemption, and store references to them in self container Subscriber
  // structs. This means that any point on the other thread is safe to update the channel store.
  // Regardless of whether we need to replace, we dispatch to make sure all
  // queued SubscribeMaps in the freelist are no longer in use.
  shard_set->pool()->Await([](unsigned idx, util::ProactorBase*) {
    ServerState::tlocal()->UpdateChannelStore(
        ChannelStore::control_block.most_recent.load(memory_order_relaxed));
  });

  // Delete previous map and channel store.
  if (copied) {
    delete (pattern_ ? store->patterns_ : store->channels_);
    delete store;
  }

  for (auto ptr : freelist_)
    delete ptr;
}

}  // namespace dfly
