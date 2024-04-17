// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "core/heap_size.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "src/facade/dragonfly_connection.h"

namespace dfly {

using namespace std;
using namespace facade;

StoredCmd::StoredCmd(const CommandId* cid, CmdArgList args, facade::ReplyMode mode)
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

StoredCmd::StoredCmd(string&& buffer, const CommandId* cid, CmdArgList args, facade::ReplyMode mode)
    : cid_{cid}, buffer_{std::move(buffer)}, sizes_(args.size()), reply_mode_{mode} {
  for (unsigned i = 0; i < args.size(); i++) {
    // Assume tightly packed list.
    DCHECK(i + 1 == args.size() || args[i].data() + args[i].size() == args[i + 1].data());
    sizes_[i] = args[i].size();
  }
}

void StoredCmd::Fill(CmdArgList args) {
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

ConnectionContext::ConnectionContext(::io::Sink* stream, facade::Connection* owner)
    : facade::ConnectionContext(stream, owner) {
  if (owner) {
    skip_acl_validation = owner->IsPrivileged();
  }
  acl_commands = std::vector<uint64_t>(acl::NumberOfFamilies(), acl::ALL_COMMANDS);
}

ConnectionContext::ConnectionContext(const ConnectionContext* owner, Transaction* tx,
                                     facade::CapturingReplyBuilder* crb)
    : facade::ConnectionContext(nullptr, nullptr), transaction{tx} {
  acl_commands = std::vector<uint64_t>(acl::NumberOfFamilies(), acl::ALL_COMMANDS);
  if (tx) {  // If we have a carrier transaction, this context is used for squashing
    DCHECK(owner);
    conn_state.db_index = owner->conn_state.db_index;
    conn_state.squashing_info = {owner};
  }
  auto* prev_reply_builder = Inject(crb);
  CHECK_EQ(prev_reply_builder, nullptr);
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
  shard_set->pool()->Await(
      [start](auto*) { ServerState::tlocal()->Monitors().NotifyChangeCount(start); });
  EnableMonitoring(start);
}

vector<unsigned> ChangeSubscriptions(bool pattern, CmdArgList args, bool to_add, bool to_reply,
                                     ConnectionContext* conn) {
  vector<unsigned> result(to_reply ? args.size() : 0, 0);

  auto& conn_state = conn->conn_state;
  if (!to_add && !conn_state.subscribe_info)
    return result;

  if (!conn_state.subscribe_info) {
    DCHECK(to_add);

    conn_state.subscribe_info.reset(new ConnectionState::SubscribeInfo);
    conn->subscriptions++;
  }

  auto& sinfo = *conn->conn_state.subscribe_info.get();
  auto& local_store = pattern ? sinfo.patterns : sinfo.channels;

  int32_t tid = util::ProactorBase::me()->GetPoolIndex();
  DCHECK_GE(tid, 0);

  ChannelStoreUpdater csu{pattern, to_add, conn, uint32_t(tid)};

  // Gather all the channels we need to subscribe to / remove.
  for (size_t i = 0; i < args.size(); ++i) {
    string_view channel = ArgS(args, i);
    if (to_add && local_store.emplace(channel).second)
      csu.Record(channel);
    else if (!to_add && local_store.erase(channel) > 0)
      csu.Record(channel);

    if (to_reply)
      result[i] = sinfo.SubscriptionCount();
  }

  csu.Apply();

  // Important to reset conn_state.subscribe_info only after all references to it were
  // removed.
  if (!to_add && conn_state.subscribe_info->IsEmpty()) {
    conn_state.subscribe_info.reset();
    DCHECK_GE(conn->subscriptions, 1u);
    conn->subscriptions--;
  }

  return result;
}

void ConnectionContext::ChangeSubscription(bool to_add, bool to_reply, CmdArgList args) {
  vector<unsigned> result = ChangeSubscriptions(false, args, to_add, to_reply, this);

  if (to_reply) {
    for (size_t i = 0; i < result.size(); ++i) {
      const char* action[2] = {"unsubscribe", "subscribe"};
      auto rb = static_cast<RedisReplyBuilder*>(reply_builder());
      rb->StartCollection(3, RedisReplyBuilder::CollectionType::PUSH);
      rb->SendBulkString(action[to_add]);
      rb->SendBulkString(ArgS(args, i));
      rb->SendLong(result[i]);
    }
  }
}

void ConnectionContext::ChangePSubscription(bool to_add, bool to_reply, CmdArgList args) {
  vector<unsigned> result = ChangeSubscriptions(true, args, to_add, to_reply, this);

  if (to_reply) {
    const char* action[2] = {"punsubscribe", "psubscribe"};
    if (result.size() == 0) {
      return SendSubscriptionChangedResponse(action[to_add], std::nullopt, 0);
    }

    for (size_t i = 0; i < result.size(); ++i) {
      SendSubscriptionChangedResponse(action[to_add], ArgS(args, i), result[i]);
    }
  }
}

void ConnectionContext::UnsubscribeAll(bool to_reply) {
  if (to_reply && (!conn_state.subscribe_info || conn_state.subscribe_info->channels.empty())) {
    return SendSubscriptionChangedResponse("unsubscribe", std::nullopt, 0);
  }
  StringVec channels(conn_state.subscribe_info->channels.begin(),
                     conn_state.subscribe_info->channels.end());
  CmdArgVec arg_vec(channels.begin(), channels.end());
  ChangeSubscription(false, to_reply, CmdArgList{arg_vec});
}

void ConnectionContext::PUnsubscribeAll(bool to_reply) {
  if (to_reply && (!conn_state.subscribe_info || conn_state.subscribe_info->patterns.empty())) {
    return SendSubscriptionChangedResponse("punsubscribe", std::nullopt, 0);
  }

  StringVec patterns(conn_state.subscribe_info->patterns.begin(),
                     conn_state.subscribe_info->patterns.end());
  CmdArgVec arg_vec(patterns.begin(), patterns.end());
  ChangePSubscription(false, to_reply, CmdArgList{arg_vec});
}

void ConnectionContext::SendSubscriptionChangedResponse(string_view action,
                                                        std::optional<string_view> topic,
                                                        unsigned count) {
  auto rb = static_cast<RedisReplyBuilder*>(reply_builder());
  rb->StartCollection(3, RedisReplyBuilder::CollectionType::PUSH);
  rb->SendBulkString(action);
  if (topic.has_value())
    rb->SendBulkString(topic.value());
  else
    rb->SendNull();
  rb->SendLong(count);
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

}  // namespace dfly
