// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/conn_context.h"

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "src/facade/dragonfly_connection.h"
#include "util/proactor_base.h"
namespace dfly {

using namespace std;

void ConnectionContext::SendMonitorMsg(std::string_view msg,
                                       util::fibers_ext::BlockingCounter borrows) {
  CHECK(owner());

  owner()->SendMonitorMsg(msg, borrows);
}

void ConnectionContext::ChangeMonitor(bool start) {
  // This will either remove or register a new connection
  // at the "top level" thread --> ServerState context
  if (start) {
    conn_state.monitor.reset(new ConnectionState::Monitor);
    MonitorsRepo::MonitorInfo new_entry(this);
    shard_set->pool()->Await(
        [&new_entry](auto*) { ServerState::tlocal()->Monitors().Add(new_entry); });
  } else {
    VLOG(1) << "connection " << owner()->GetClientInfo()
            << " no longer needs to be monitored - removing 0x" << std::hex << (const void*)this;
    shard_set->pool()->Await(
        [self = this](auto*) { ServerState::tlocal()->Monitors().Remove(self); });
    conn_state.monitor.reset();
  }
  EnableMonitoring(start);
}

void ConnectionContext::ChangeSubscription(bool to_add, bool to_reply, CmdArgList args) {
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

    sort(channels.begin(), channels.end());

    // prepare the array in order to distribute the updates to the shards.
    vector<unsigned> shard_idx(shard_set->size() + 1, 0);
    for (const auto& k_v : channels) {
      shard_idx[k_v.first]++;
    }
    unsigned prev = shard_idx[0];
    shard_idx[0] = 0;

    // compute cumulative sum, or in other words a beginning index in channels for each shard.
    for (size_t i = 1; i < shard_idx.size(); ++i) {
      unsigned cur = shard_idx[i];
      shard_idx[i] = shard_idx[i - 1] + prev;
      prev = cur;
    }

    int32_t tid = util::ProactorBase::GetIndex();
    DCHECK_GE(tid, 0);

    // Update the subscribers on publisher's side.
    auto cb = [&](EngineShard* shard) {
      ChannelSlice& cs = shard->channel_slice();
      unsigned start = shard_idx[shard->shard_id()];
      unsigned end = shard_idx[shard->shard_id() + 1];

      DCHECK_LT(start, end);
      for (unsigned i = start; i < end; ++i) {
        if (to_add) {
          cs.AddSubscription(channels[i].second, this, tid);
        } else {
          cs.RemoveSubscription(channels[i].second, this);
        }
      }
    };

    // Update subscription
    shard_set->RunBriefInParallel(move(cb),
                                  [&](ShardId sid) { return shard_idx[sid + 1] > shard_idx[sid]; });

    // It's important to reset
    if (!to_add && conn_state.subscribe_info->IsEmpty()) {
      conn_state.subscribe_info.reset();
      force_dispatch = false;
    }
  }

  if (to_reply) {
    const char* action[2] = {"unsubscribe", "subscribe"};

    for (size_t i = 0; i < result.size(); ++i) {
      (*this)->StartArray(3);
      (*this)->SendBulkString(action[to_add]);
      (*this)->SendBulkString(ArgS(args, i));  // channel

      // number of subscribed channels for this connection *right after*
      // we subscribe.
      (*this)->SendLong(result[i]);
    }
  }
}

void ConnectionContext::ChangePSub(bool to_add, bool to_reply, CmdArgList args) {
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

    // Update the subscribers on channel-slice side.
    auto cb = [&](EngineShard* shard) {
      ChannelSlice& cs = shard->channel_slice();
      for (string_view pattern : patterns) {
        if (to_add) {
          cs.AddGlobPattern(pattern, this, tid);
        } else {
          cs.RemoveGlobPattern(pattern, this);
        }
      }
    };

    // Update pattern subscription. Run on all shards.
    shard_set->RunBriefInParallel(move(cb));

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
  ChangePSub(false, to_reply, CmdArgList{arg_vec});
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
