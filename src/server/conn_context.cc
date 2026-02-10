// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "common/heap_size.h"
#include "facade/acl_commands_def.h"
#include "facade/reply_builder.h"
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
using cmn::HeapSize;

namespace {
void SendSubscriptionChangedResponse(string_view action, std::optional<string_view> topic,
                                     unsigned count, RedisReplyBuilder* rb) {
  rb->StartCollection(3, CollectionType::PUSH);
  rb->SendBulkString(action);
  if (topic.has_value())
    rb->SendBulkString(topic.value());
  else
    rb->SendNull();
  rb->SendLong(count);
}

vector<string> FormatExecSlowlog(const ConnectionState& state) {
  const auto& info = state.exec_info;
  return {absl::StrCat("num_cmds: ", info.body.size()), absl::StrCat("is_write: ", info.is_write)};
}

vector<string> FormatEvalSlowlog(const ConnectionState& state) {
  const auto& sinfo = *state.script_info;
  return {
      sinfo.stats.sha,
      absl::StrCat("num_cmds: ", sinfo.stats.num_commands),
      absl::StrCat("is_write: ", !sinfo.read_only),
      absl::StrCat("lock_tags: ", sinfo.lock_tags.size()),
  };
}

}  // namespace

StoredCmd::StoredCmd(const CommandId* cid, facade::ArgSlice args, facade::ReplyMode mode)
    : cid_{cid}, args_{args}, reply_mode_{mode} {
  backed_ = std::make_unique<cmn::BackedArguments>(args.begin(), args.end(), args.size());
  args_ = facade::ParsedArgs{*backed_};
}

CmdArgList StoredCmd::Slice(CmdArgVec* scratch) const {
  return args_.ToSlice(scratch);
}

std::string StoredCmd::FirstArg() const {
  if (NumArgs() == 0) {
    return {};
  }
  return string{args_.Front()};
}

ConnectionContext::ConnectionContext(facade::Connection* owner, acl::UserCredentials cred)
    : facade::ConnectionContext(owner) {
  if (owner) {
    skip_acl_validation = owner->IsPrivileged();
    has_main_or_memcache_listener = owner->IsMainOrMemcache();
  }

  keys = std::move(cred.keys);
  pub_sub = std::move(cred.pub_sub);
  if (cred.acl_commands.empty()) {
    acl_commands = std::vector<uint64_t>(acl::NumberOfFamilies(), acl::NONE_COMMANDS);
  } else {
    acl_commands = std::move(cred.acl_commands);
  }
  acl_db_idx = cred.db;
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

void ConnectionContext::ChangeSubscription(bool to_add, bool to_reply, bool sharded,
                                           CmdArgList args, facade::RedisReplyBuilder* rb) {
  vector<unsigned> result = ChangeSubscriptions(args, false, to_add, to_reply);

  if (to_reply) {
    const string_view actionRegular[2] = {"unsubscribe", "subscribe"};
    const string_view actionSharded[2] = {"sunsubscribe", "ssubscribe"};
    const absl::Span<const string_view> action = sharded ? actionSharded : actionRegular;
    SinkReplyBuilder::ReplyScope scope{rb};
    for (size_t i = 0; i < result.size(); ++i) {
      SendSubscriptionChangedResponse(action[to_add], ArgS(args, i), result[i], rb);
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

    SinkReplyBuilder::ReplyScope scope{rb};
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
  ChangeSubscription(false, to_reply, false, CmdArgList{arg_vec}, rb);
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
  return HeapSize(body) + HeapSize(watched_keys);
}

void ConnectionState::ExecInfo::AddStoredCmd(const CommandId* cid, ArgSlice args) {
  body.emplace_back(cid, args);
  stored_cmd_bytes += body.back().UsedMemory();
  is_write |= cid->IsJournaled();
}

size_t ConnectionState::ExecInfo::ClearStoredCmds() {
  const size_t used = GetStoredCmdBytes();
  vector<StoredCmd>{}.swap(body);
  stored_cmd_bytes = 0;
  return used;
}

size_t ConnectionState::ScriptInfo::UsedMemory() const {
  return HeapSize(lock_tags) + async_cmds_heap_mem;
}

size_t ConnectionState::SubscribeInfo::UsedMemory() const {
  return HeapSize(channels) + HeapSize(patterns);
}

size_t ConnectionState::UsedMemory() const {
  return HeapSize(exec_info) + HeapSize(script_info) + HeapSize(subscribe_info);
}

size_t ConnectionContext::UsedMemory() const {
  return facade::ConnectionContext::UsedMemory() + HeapSize(conn_state) +
         HeapSize(authed_username) + HeapSize(acl_commands) + HeapSize(keys.key_globs) +
         HeapSize(pub_sub.globs);
}

void ConnectionContext::Unsubscribe(std::string_view channel) {
  auto* sinfo = conn_state.subscribe_info.get();
  DCHECK(sinfo);
  auto erased = sinfo->channels.erase(channel);
  DCHECK(erased);
  if (sinfo->IsEmpty()) {
    conn_state.subscribe_info.reset();
    DCHECK_GE(subscriptions, 1u);
    --subscriptions;
  }
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
  const size_t cleared_size = ClearStoredCmds();
  ServerState::tlocal()->stats.stored_cmd_bytes -= cleared_size;
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

void CommandContext::ReuseInternal() {
  cid_ = nullptr;
  tx_ = nullptr;
  arg_slice_backing.clear();
  start_time_ns = 0;
}

void CommandContext::RecordLatency(facade::ArgSlice tail_args) const {
  DCHECK_GT(start_time_ns, 0u);
  int64_t after = absl::GetCurrentTimeNanos();

  ServerState* ss = ServerState::SafeTLocal();  // Might have migrated thread, read after invocation
  int64_t execution_time_usec = (after - start_time_ns) / 1000;

  cid_->RecordLatency(ss->thread_index(), execution_time_usec);
  DCHECK(conn_cntx_ != nullptr);

  // TODO: we should probably discard more than only blocking commands here
  const auto* conn = server_conn_cntx()->conn();
  if (conn == nullptr || (cid_->opt_mask() & CO::BLOCKING))
    return;

  if (!ss->ShouldLogSlowCmd(execution_time_usec))  // It was not a slow command
    return;

  vector<string> aux_params;
  CmdArgVec aux_slice;

  // Rewrite arguments for exec/eval with stats
  if (auto mck = cid_->MultiControlKind(); mck) {
    auto* cntx = static_cast<dfly::ConnectionContext*>(conn_cntx());
    switch (*mck) {
      case CO::MultiControlKind::EXEC:
        aux_params = FormatExecSlowlog(cntx->conn_state);
        break;
      case CO::MultiControlKind::EVAL:
        aux_params = FormatEvalSlowlog(cntx->conn_state);
        break;
    };
    aux_slice = {aux_params.begin(), aux_params.end()};
    if (tail_args.size() > 0) {
      tail_args.remove_prefix(1);  // remove script/sha from eval/evalsha
      aux_slice.insert(aux_slice.end(), tail_args.begin(), tail_args.end());
    }
    tail_args = aux_slice;
  }
  ServerState::SafeTLocal()->GetSlowLog().Add(cid_->name(), tail_args, conn->GetName(),
                                              conn->RemoteEndpointStr(), execution_time_usec,
                                              absl::GetCurrentTimeNanos() / 1000);
}

}  // namespace dfly
