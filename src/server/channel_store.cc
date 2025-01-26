#include "server/channel_store.h"

// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <shared_mutex>

extern "C" {
#include "redis/util.h"
}

#include <absl/container/fixed_array.h>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

namespace dfly {
using namespace std;

namespace {

bool Matches(string_view pattern, string_view channel) {
  return stringmatchlen(pattern.data(), pattern.size(), channel.data(), channel.size(), 0) == 1;
}

// Build functor for sending messages to connection
auto BuildSender(string_view channel, facade::ArgRange messages) {
  absl::FixedArray<string_view, 1> views(messages.Size());
  size_t messages_size = accumulate(messages.begin(), messages.end(), 0,
                                    [](int sum, string_view str) { return sum + str.size(); });
  auto buf = shared_ptr<char[]>{new char[channel.size() + messages_size]};
  {
    memcpy(buf.get(), channel.data(), channel.size());
    char* ptr = buf.get() + channel.size();

    size_t i = 0;
    for (string_view message : messages) {
      memcpy(ptr, message.data(), message.size());
      views[i++] = {ptr, message.size()};
      ptr += message.size();
    }
  }

  return [channel, buf = std::move(buf), views = std::move(views)](facade::Connection* conn,
                                                                   string pattern) {
    string_view channel_view{buf.get(), channel.size()};
    for (std::string_view message_view : views)
      conn->SendPubMessageAsync({std::move(pattern), buf, channel_view, message_view});
  };
}

}  // namespace

bool ChannelStore::Subscriber::ByThread(const Subscriber& lhs, const Subscriber& rhs) {
  return ByThreadId(lhs, rhs.Thread());
}

bool ChannelStore::Subscriber::ByThreadId(const Subscriber& lhs, const unsigned thread) {
  return lhs.Thread() < thread;
}

ChannelStore::UpdatablePointer::UpdatablePointer(const UpdatablePointer& other) {
  ptr.store(other.ptr.load(memory_order_relaxed), memory_order_relaxed);
}

ChannelStore::SubscribeMap* ChannelStore::UpdatablePointer::Get() const {
  return ptr.load(memory_order_acquire);  // sync pointed memory
}

void ChannelStore::UpdatablePointer::Set(ChannelStore::SubscribeMap* sm) {
  ptr.store(sm, memory_order_release);  // sync pointed memory
}

ChannelStore::SubscribeMap* ChannelStore::UpdatablePointer::operator->() const {
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

unsigned ChannelStore::SendMessages(std::string_view channel, facade::ArgRange messages) const {
  vector<Subscriber> subscribers = FetchSubscribers(channel);
  if (subscribers.empty())
    return 0;

  // Make sure none of the threads publish buffer limits is reached. We don't reserve memory ahead
  // and don't prevent the buffer from possibly filling, but the approach is good enough for
  // limiting fast producers. Most importantly, we can use DispatchBrief below as we block here
  int32_t last_thread = -1;
  for (auto& sub : subscribers) {
    int sub_thread = sub.Thread();
    DCHECK_LE(last_thread, sub_thread);
    if (last_thread == sub_thread)  // skip same thread
      continue;

    if (sub.IsExpired())
      continue;

    // Make sure the connection thread has enough memory budget to accept the message.
    // This is a heuristic and not entirely hermetic since the connection memory might
    // get filled again.
    facade::Connection::EnsureMemoryBudget(sub.Thread());
    last_thread = sub_thread;
  }

  auto subscribers_ptr = make_shared<decltype(subscribers)>(std::move(subscribers));
  auto cb = [subscribers_ptr, send = BuildSender(channel, messages)](unsigned idx, auto*) {
    auto it = lower_bound(subscribers_ptr->begin(), subscribers_ptr->end(), idx,
                          ChannelStore::Subscriber::ByThreadId);
    while (it != subscribers_ptr->end() && it->Thread() == idx) {
      // if ptr->cntx() is null, a connection might have closed or be in the process of closing
      if (auto* ptr = it->Get(); ptr && ptr->cntx() != nullptr)
        send(ptr, it->pattern);
      it++;
    }
  };
  shard_set->pool()->DispatchBrief(std::move(cb));

  return subscribers_ptr->size();
}

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
    // `cntx` is expected to be valid as it unregisters itself from the channel_store before
    // closing.
    CHECK(cntx->conn_state.subscribe_info);
    Subscriber sub{cntx->conn()->Borrow(), pattern};
    out->push_back(std::move(sub));
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
  shard_set->pool()->AwaitBrief([](unsigned idx, util::ProactorBase*) {
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
