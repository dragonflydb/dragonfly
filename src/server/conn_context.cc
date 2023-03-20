// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "src/facade/dragonfly_connection.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;

StoredCmd::StoredCmd(const CommandId* d, CmdArgList args) : descr(d) {
  size_t total_size = 0;
  for (auto args : args) {
    total_size += args.size();
  }

  backing_storage_.reset(new char[total_size]);
  arg_vec_.resize(args.size());
  char* next = backing_storage_.get();
  for (size_t i = 0; i < args.size(); ++i) {
    auto src = args[i];
    memcpy(next, src.data(), src.size());
    arg_vec_[i] = MutableSlice{next, src.size()};
    next += src.size();
  }
  arg_list_ = {arg_vec_.data(), arg_vec_.size()};
}

void ConnectionContext::ChangeMonitor(bool start) {
  // This will either remove or register a new connection
  // at the "top level" thread --> ServerState context
  // note that we are registering/removing this connection to the thread at which at run
  // then notify all other threads that there is a change in the number of monitors
  auto& my_monitors = ServerState::tlocal()->Monitors();
  if (start) {
    my_monitors.Add(owner());
  } else {
    VLOG(1) << "connection " << owner()->GetClientInfo()
            << " no longer needs to be monitored - removing 0x" << std::hex << this;
    my_monitors.Remove(owner());
  }
  // Tell other threads that about the change in the number of connection that we monitor
  shard_set->pool()->Await(
      [start](auto*) { ServerState::tlocal()->Monitors().NotifyChangeCount(start); });
  EnableMonitoring(start);
}

vector<unsigned> ChangeSubscriptions(bool pattern, CmdArgList args, bool to_add, bool to_reply,
                                     ConnectionContext* conn, ChannelStore* store) {
  vector<unsigned> result(to_reply ? args.size() : 0, 0);

  auto& conn_state = conn->conn_state;
  if (!to_add && !conn_state.subscribe_info)
    return result;

  if (!conn_state.subscribe_info) {
    DCHECK(to_add);

    conn_state.subscribe_info.reset(new ConnectionState::SubscribeInfo);
    conn->force_dispatch = true;  // to be able to read input and still write the output.
  }

  auto& sinfo = *conn->conn_state.subscribe_info.get();
  auto& local_store = pattern ? sinfo.patterns : sinfo.channels;

  auto sadd = pattern ? &ChannelStore::AddPatternSub : &ChannelStore::AddSub;
  auto sremove = pattern ? &ChannelStore::RemovePatternSub : &ChannelStore::RemoveSub;

  int32_t tid = util::ProactorBase::GetIndex();
  DCHECK_GE(tid, 0);

  // Gather all the channels we need to subscribe to / remove.
  for (size_t i = 0; i < args.size(); ++i) {
    string_view channel = ArgS(args, i);
    if (to_add) {
      if (local_store.emplace(channel).second)
        (store->*sadd)(channel, conn, tid);
    } else {
      if (local_store.erase(channel) > 0)
        (store->*sremove)(channel, conn);
    }

    if (to_reply)
      result[i] = sinfo.SubscriptionCount();
  }

  // Important to reset conn_state.subscribe_info only after all references to it were
  // removed.
  if (!to_add && conn_state.subscribe_info->IsEmpty()) {
    conn_state.subscribe_info.reset();
    conn->force_dispatch = false;
  }

  return result;
}

void ConnectionContext::ChangeSubscription(ChannelStore* store, bool to_add, bool to_reply,
                                           CmdArgList args) {
  vector<unsigned> result = ChangeSubscriptions(false, args, to_add, to_reply, this, store);

  if (to_reply) {
    for (size_t i = 0; i < result.size(); ++i) {
      owner()->SendMsgVecAsync({to_add, make_shared<string>(ArgS(args, i)), result[i]});
    }
  }
}

void ConnectionContext::ChangePSubscription(ChannelStore* store, bool to_add, bool to_reply,
                                            CmdArgList args) {
  vector<unsigned> result = ChangeSubscriptions(true, args, to_add, to_reply, this, store);

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

void ConnectionContext::UnsubscribeAll(ChannelStore* store, bool to_reply) {
  if (to_reply && (!conn_state.subscribe_info || conn_state.subscribe_info->channels.empty())) {
    return SendSubscriptionChangedResponse("unsubscribe", std::nullopt, 0);
  }
  StringVec channels(conn_state.subscribe_info->channels.begin(),
                     conn_state.subscribe_info->channels.end());
  CmdArgVec arg_vec(channels.begin(), channels.end());
  ChangeSubscription(store, false, to_reply, CmdArgList{arg_vec});
}

void ConnectionContext::PUnsubscribeAll(ChannelStore* store, bool to_reply) {
  if (to_reply && (!conn_state.subscribe_info || conn_state.subscribe_info->patterns.empty())) {
    return SendSubscriptionChangedResponse("punsubscribe", std::nullopt, 0);
  }

  StringVec patterns(conn_state.subscribe_info->patterns.begin(),
                     conn_state.subscribe_info->patterns.end());
  CmdArgVec arg_vec(patterns.begin(), patterns.end());
  ChangePSubscription(store, false, to_reply, CmdArgList{arg_vec});
}

void ConnectionContext::SendSubscriptionChangedResponse(string_view action,
                                                        std::optional<string_view> topic,
                                                        unsigned count) {
  (*this)->StartArray(3);
  (*this)->SendBulkString(action);
  if (topic.has_value())
    (*this)->SendBulkString(topic.value());
  else
    (*this)->SendNull();
  (*this)->SendLong(count);
}

void ConnectionState::ExecInfo::Clear() {
  state = EXEC_INACTIVE;
  body.clear();
  ClearWatched();
}

void ConnectionState::ExecInfo::ClearWatched() {
  watched_keys.clear();
  watched_dirty.store(false, memory_order_relaxed);
  watched_existed = 0;
}

}  // namespace dfly
