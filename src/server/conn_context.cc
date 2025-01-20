// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "core/heap_size.h"
#include "facade/acl_commands_def.h"
#include "server/acl/acl_commands_def.h"
#include "server/channel_store.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "src/facade/dragonfly_connection.h"

namespace dfly {

using namespace std;
using namespace facade;

static void SendSubscriptionChangedResponse(string_view action, std::optional<string_view> topic,
                                            unsigned count, RedisReplyBuilder* rb) {
  rb->StartCollection(3, RedisReplyBuilder::CollectionType::PUSH);
  rb->SendBulkString(action);
  if (topic.has_value())
    rb->SendBulkString(topic.value());
  else
    rb->SendNull();
  rb->SendLong(count);
}

StoredCmd::StoredCmd(const CommandId* cid, ArgSlice args, facade::ReplyMode mode)
    : cid_{cid}, buffer_{}, sizes_(args.size()), reply_mode_{mode} {
  size_t total_size = 0;
  for (auto args : args) {
    total_size += args.size();
  }

  buffer_.resize(total_size);
  char* next = buffer_.data();
  for (unsigned i = 0; i < args.size(); i++) {
    if (args[i].size() > 0)
      memcpy(next, args[i].data(), args[i].size());
    next += args[i].size();
    sizes_[i] = args[i].size();
  }
}

StoredCmd::StoredCmd(string&& buffer, const CommandId* cid, ArgSlice args, facade::ReplyMode mode)
    : cid_{cid}, buffer_{std::move(buffer)}, sizes_(args.size()), reply_mode_{mode} {
  for (unsigned i = 0; i < args.size(); i++) {
    // Assume tightly packed list.
    DCHECK(i + 1 == args.size() || args[i].data() + args[i].size() == args[i + 1].data());
    sizes_[i] = args[i].size();
  }
}

void StoredCmd::Fill(absl::Span<std::string_view> args) {
  DCHECK_GE(args.size(), sizes_.size());

  unsigned offset = 0;
  for (unsigned i = 0; i < sizes_.size(); i++) {
    args[i] = MutableSlice{buffer_.data() + offset, sizes_[i]};
    offset += sizes_[i];
  }
}

size_t StoredCmd::NumArgs() const {
  return sizes_.size();
}

std::string StoredCmd::FirstArg() const {
  if (sizes_.size() == 0) {
    return {};
  }
  return buffer_.substr(0, sizes_[0]);
}

facade::ReplyMode StoredCmd::ReplyMode() const {
  return reply_mode_;
}

template <typename C> size_t IsStoredInlined(const C& c) {
  const char* start = reinterpret_cast<const char*>(&c);
  const char* end = start + sizeof(C);
  const char* data = reinterpret_cast<const char*>(c.data());
  return data >= start && data <= end;
}

size_t StoredCmd::UsedMemory() const {
  size_t buffer_size = IsStoredInlined(buffer_) ? 0 : buffer_.size();
  size_t sz_size = IsStoredInlined(sizes_) ? 0 : sizes_.size() * sizeof(uint32_t);
  return buffer_size + sz_size;
}

const CommandId* StoredCmd::Cid() const {
  return cid_;
}

ConnectionContext::ConnectionContext(facade::Connection* owner, acl::UserCredentials cred)
    : facade::ConnectionContext(owner) {
  if (owner) {
    skip_acl_validation = owner->IsPrivileged();
  }

  keys = std::move(cred.keys);
  pub_sub = std::move(cred.pub_sub);
  if (cred.acl_commands.empty()) {
    acl_commands = std::vector<uint64_t>(acl::NumberOfFamilies(), acl::NONE_COMMANDS);
  } else {
    acl_commands = std::move(cred.acl_commands);
  }
}

ConnectionContext::ConnectionContext(const ConnectionContext* owner, Transaction* tx)
    : facade::ConnectionContext(nullptr), transaction{tx} {
  if (owner) {
    acl_commands = owner->acl_commands;
    keys = owner->keys;
    pub_sub = owner->pub_sub;
    skip_acl_validation = owner->skip_acl_validation;
    ns = owner->ns;
  } else {
    acl_commands = std::vector<uint64_t>(acl::NumberOfFamilies(), acl::NONE_COMMANDS);
  }
  if (tx) {  // If we have a carrier transaction, this context is used for squashing
    DCHECK(owner);
    conn_state.db_index = owner->conn_state.db_index;
    conn_state.squashing_info = {owner};
  }
}

void ConnectionContext::ChangeMonitor(bool start) {
  // This will either remove or register a new connection
  // at the "top level" thread --> ServerState context
  // note that we are registering/removing this connection to the thread at which at run
  // then notify all other threads that there is a change in the number of monitors
  auto& my_monitors = ServerState::tlocal()->Monitors();
  if (start) {
    my_monitors.Add(conn());
  } else {
    VLOG(1) << "connection " << conn()->GetClientId() << " no longer needs to be monitored";
    my_monitors.Remove(conn());
  }
  // Tell other threads that about the change in the number of connection that we monitor
  shard_set->pool()->AwaitBrief(
      [start](unsigned, auto*) { ServerState::tlocal()->Monitors().NotifyChangeCount(start); });
  EnableMonitoring(start);
}

void ConnectionContext::ChangeSubscription(bool to_add, bool to_reply, CmdArgList args,
                                           facade::RedisReplyBuilder* rb) {
  vector<unsigned> result = ChangeSubscriptions(args, false, to_add, to_reply);

  if (to_reply) {
    for (size_t i = 0; i < result.size(); ++i) {
      const char* action[2] = {"unsubscribe", "subscribe"};
      rb->StartCollection(3, RedisReplyBuilder::CollectionType::PUSH);
      rb->SendBulkString(action[to_add]);
      rb->SendBulkString(ArgS(args, i));
      rb->SendLong(result[i]);
    }
  }
}

void ConnectionContext::ChangePSubscription(bool to_add, bool to_reply, CmdArgList args,
                                            facade::RedisReplyBuilder* rb) {
  vector<unsigned> result = ChangeSubscriptions(args, true, to_add, to_reply);

  if (to_reply) {
    const char* action[2] = {"punsubscribe", "psubscribe"};
    if (result.size() == 0) {
      return SendSubscriptionChangedResponse(action[to_add], std::nullopt, 0, rb);
    }

    for (size_t i = 0; i < result.size(); ++i) {
      SendSubscriptionChangedResponse(action[to_add], ArgS(args, i), result[i], rb);
    }
  }
}

void ConnectionContext::UnsubscribeAll(bool to_reply, facade::RedisReplyBuilder* rb) {
  if (to_reply && (!conn_state.subscribe_info || conn_state.subscribe_info->channels.empty())) {
    return SendSubscriptionChangedResponse("unsubscribe", std::nullopt, 0, rb);
  }
  StringVec channels(conn_state.subscribe_info->channels.begin(),
                     conn_state.subscribe_info->channels.end());
  CmdArgVec arg_vec(channels.begin(), channels.end());
  ChangeSubscription(false, to_reply, CmdArgList{arg_vec}, rb);
}

void ConnectionContext::PUnsubscribeAll(bool to_reply, facade::RedisReplyBuilder* rb) {
  if (to_reply && (!conn_state.subscribe_info || conn_state.subscribe_info->patterns.empty())) {
    return SendSubscriptionChangedResponse("punsubscribe", std::nullopt, 0, rb);
  }

  StringVec patterns(conn_state.subscribe_info->patterns.begin(),
                     conn_state.subscribe_info->patterns.end());
  CmdArgVec arg_vec(patterns.begin(), patterns.end());
  ChangePSubscription(false, to_reply, CmdArgList{arg_vec}, rb);
}

size_t ConnectionState::ExecInfo::UsedMemory() const {
  return dfly::HeapSize(body) + dfly::HeapSize(watched_keys);
}

size_t ConnectionState::ScriptInfo::UsedMemory() const {
  return dfly::HeapSize(lock_tags) + async_cmds_heap_mem;
}

size_t ConnectionState::SubscribeInfo::UsedMemory() const {
  return dfly::HeapSize(channels) + dfly::HeapSize(patterns);
}

size_t ConnectionState::UsedMemory() const {
  return dfly::HeapSize(exec_info) + dfly::HeapSize(script_info) + dfly::HeapSize(subscribe_info);
}

size_t ConnectionContext::UsedMemory() const {
  return facade::ConnectionContext::UsedMemory() + dfly::HeapSize(conn_state);
}

vector<unsigned> ConnectionContext::ChangeSubscriptions(CmdArgList channels, bool pattern,
                                                        bool to_add, bool to_reply) {
  vector<unsigned> result(to_reply ? channels.size() : 0, 0);

  if (!to_add && !conn_state.subscribe_info)
    return result;

  if (!conn_state.subscribe_info) {
    DCHECK(to_add);

    conn_state.subscribe_info.reset(new ConnectionState::SubscribeInfo);
    subscriptions++;
  }

  auto& sinfo = *conn_state.subscribe_info.get();
  auto& local_store = pattern ? sinfo.patterns : sinfo.channels;

  int32_t tid = util::ProactorBase::me()->GetPoolIndex();
  DCHECK_GE(tid, 0);

  ChannelStoreUpdater csu{pattern, to_add, this, uint32_t(tid)};

  // Gather all the channels we need to subscribe to / remove.
  size_t i = 0;
  for (string_view channel : channels) {
    if (to_add && local_store.emplace(channel).second)
      csu.Record(channel);
    else if (!to_add && local_store.erase(channel) > 0)
      csu.Record(channel);

    if (to_reply)
      result[i++] = sinfo.SubscriptionCount();
  }

  csu.Apply();

  // Important to reset conn_state.subscribe_info only after all references to it were
  // removed.
  if (!to_add && conn_state.subscribe_info->IsEmpty()) {
    conn_state.subscribe_info.reset();
    DCHECK_GE(subscriptions, 1u);
    subscriptions--;
  }

  return result;
}

void ConnectionState::ExecInfo::Clear() {
  DCHECK(!preborrowed_interpreter);  // Must have been released properly
  state = EXEC_INACTIVE;
  body.clear();
  is_write = false;
  ClearWatched();
}

void ConnectionState::ExecInfo::ClearWatched() {
  watched_keys.clear();
  watched_dirty.store(false, memory_order_relaxed);
  watched_existed = 0;
}

bool ConnectionState::ClientTracking::ShouldTrackKeys() const {
  if (!IsTrackingOn()) {
    return false;
  }

  if (noloop_ == true) {
    // Once we implement REDIRECT this should return true since noloop
    // without it only affects the current connection
    return false;
  }

  if (option_ == NONE) {
    return true;
  }

  const bool match = (seq_num_ == (1 + caching_seq_num_));
  return option_ == OPTIN ? match : !match;
}

}  // namespace dfly
