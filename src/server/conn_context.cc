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

void ConnectionContext::ChangeSubscription(ChannelStore* store, bool to_add, bool to_reply,
                                           CmdArgList args) {
  vector<unsigned> result(to_reply ? args.size() : 0, 0);

  if (to_add || conn_state.subscribe_info) {
    std::vector<pair<ShardId, string_view>> channels;
    channels.reserve(args.size());

    if (!conn_state.subscribe_info) {
      DCHECK(to_add);

      conn_state.subscribe_info.reset(new ConnectionState::SubscribeInfo);
      // to be able to read input and still write the output.
      this->force_dispatch = true;
    }

    // Gather all the channels we need to subscribe to / remove.
    for (size_t i = 0; i < args.size(); ++i) {
      bool res = false;
      string_view channel = ArgS(args, i);
      if (to_add) {
        res = conn_state.subscribe_info->channels.emplace(channel).second;
      } else {
        res = conn_state.subscribe_info->channels.erase(channel) > 0;
      }

      if (to_reply)
        result[i] = conn_state.subscribe_info->SubscriptionCount();

      if (res) {
        ShardId sid = Shard(channel, shard_set->size());
        channels.emplace_back(sid, channel);
      }
    }

    int32_t tid = util::ProactorBase::GetIndex();
    DCHECK_GE(tid, 0);
    for (const auto& chan : channels) {
      if (to_add)
        store->AddSubscription(chan.second, this, tid);
      else
        store->RemoveSubscription(chan.second, this);
    }

    // It's important to reset
    if (!to_add && conn_state.subscribe_info->IsEmpty()) {
      conn_state.subscribe_info.reset();
      force_dispatch = false;
    }
  }

  if (to_reply) {
    using PubMessage = facade::Connection::PubMessage;
    for (size_t i = 0; i < result.size(); ++i) {
      PubMessage msg;
      msg.type = to_add ? PubMessage::kSubscribe : PubMessage::kUnsubscribe;
      msg.channel = make_shared<string>(ArgS(args, i));
      msg.channel_cnt = result[i];
      owner()->SendMsgVecAsync(move(msg));
    }
  }
}

void ConnectionContext::ChangePSub(ChannelStore* store, bool to_add, bool to_reply,
                                   CmdArgList args) {
  vector<unsigned> result(to_reply ? args.size() : 0, 0);

  if (to_add || conn_state.subscribe_info) {
    std::vector<string_view> patterns;
    patterns.reserve(args.size());

    if (!conn_state.subscribe_info) {
      DCHECK(to_add);

      conn_state.subscribe_info.reset(new ConnectionState::SubscribeInfo);
      this->force_dispatch = true;
    }

    // Gather all the patterns we need to subscribe to / remove.
    for (size_t i = 0; i < args.size(); ++i) {
      bool res = false;
      string_view pattern = ArgS(args, i);
      if (to_add) {
        res = conn_state.subscribe_info->patterns.emplace(pattern).second;
      } else {
        res = conn_state.subscribe_info->patterns.erase(pattern) > 0;
      }

      if (to_reply)
        result[i] = conn_state.subscribe_info->SubscriptionCount();

      if (res) {
        patterns.emplace_back(pattern);
      }
    }

    int32_t tid = util::ProactorBase::GetIndex();
    DCHECK_GE(tid, 0);

    for (string_view pattern : patterns) {
      if (to_add)
        store->AddGlobPattern(pattern, this, tid);
      else
        store->RemoveGlobPattern(pattern, this);
    }

    // Important to reset conn_state.subscribe_info only after all references to it were
    // removed from channel slices.
    if (!to_add && conn_state.subscribe_info->IsEmpty()) {
      conn_state.subscribe_info.reset();
      force_dispatch = false;
    }
  }

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
  ChangePSub(store, false, to_reply, CmdArgList{arg_vec});
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
