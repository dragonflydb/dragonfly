// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/parsed_command.h"

#include "base/logging.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"
#include "facade/reply_capture.h"

namespace facade {

using namespace std;

namespace {

inline void SendSimpleDirect(std::string_view str, SinkReplyBuilder* rb) {
  if (!str.empty())  // empty string means no-reply
    rb->SendSimpleString(str);
}

}  // namespace

string MCRender::RenderNotFound() const {
  if (flags_.no_reply)
    return {};
  return flags_.meta ? "NF" : "NOT_FOUND";
}

string MCRender::RenderGetEnd() const {
  if (flags_.no_reply || flags_.meta)
    return {};
  return "END";
}

std::string MCRender::RenderStored() const {
  if (flags_.no_reply)
    return {};
  return flags_.meta ? "HD" : "STORED";
}

std::string MCRender::RenderNotStored() const {
  if (flags_.no_reply)
    return {};
  return flags_.meta ? "NS" : "NOT_STORED";
}

string MCRender::RenderMiss() const {
  if (flags_.no_reply || !flags_.meta)
    return {};
  return "EN";
}

string MCRender::RenderDeleted() const {
  if (flags_.no_reply)
    return {};
  return flags_.meta ? "HD" : "DELETED";
}

void ParsedCommand::ResetForReuse() {
  allow_async_execution_ = false;
  is_deferred_reply_ = false;
  reply_payload_ = std::monostate{};

  state_.store(0, std::memory_order_relaxed);
  offsets_.clear();
  if (HeapMemory() > 1024) {
    storage_.clear();  // also deallocates the heap.
    offsets_.shrink_to_fit();
  }
}

void ParsedCommand::SendError(std::string_view str, std::string_view type) {
  if (!is_deferred_reply_) {
    rb_->SendError(str, type);
  } else {
    reply_payload_ = payload::make_error(str, type);
    NotifyReplied();
  }
}

void ParsedCommand::SendError(facade::OpStatus status) {
  if (!is_deferred_reply_) {
    if (status == OpStatus::OK)
      rb_->SendSimpleString("OK");
    else
      rb_->SendError(StatusToMsg(status));
  } else {
    if (status == OpStatus::OK)
      reply_payload_ = payload::SimpleString{"OK"};
    else
      reply_payload_ = payload::make_error(StatusToMsg(status));
    NotifyReplied();
  }
}

void ParsedCommand::SendError(const facade::ErrorReply& error) {
  if (error.status)
    return SendError(*error.status);
  SendError(error.ToSv(), error.kind);
}

void ParsedCommand::SendStored(bool ok) {
  if (!is_deferred_reply_) {
    SendDirect(payload::StoredReply{ok});
  } else {
    reply_payload_ = payload::StoredReply{ok};
    NotifyReplied();
  }
}

void ParsedCommand::SendSimpleString(std::string_view str) {
  if (!is_deferred_reply_) {
    SendSimpleDirect(str, rb_);
  } else {
    reply_payload_ = payload::make_simple_or_noreply(str);
    NotifyReplied();
  }
}

void ParsedCommand::SendMiss(unsigned index) {
  if (is_deferred_reply_) {
    if (holds_alternative<std::monostate>(reply_payload_)) {
      // Lazily allocate the array on first use. Already initialized to not found,
      // so we do not need to do anything else here.
      auto arr = std::make_unique<facade::payload::SingleGetReply[]>(size());
      reply_payload_ = std::move(arr);
      // We do not notify here as there will be more entries to fill.
    }
  } else {
    // Direct reply for either MC or Redis.
    if (mc_cmd_ != nullptr)
      rb_->SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderMiss());
    else
      static_cast<RedisReplyBuilder*>(rb_)->SendNull();
  }
}

void ParsedCommand::SendValue(unsigned index, std::string_view value, uint64_t mc_ver,
                              uint32_t mc_flag) {
  if (is_deferred_reply_) {
    if (holds_alternative<std::monostate>(reply_payload_)) {
      // Lazily allocate the array on first use.
      auto arr = std::make_unique<facade::payload::SingleGetReply[]>(size());
      reply_payload_ = std::move(arr);
    }

    // Fill in the entry.
    auto& arr = get<std::unique_ptr<facade::payload::SingleGetReply[]>>(reply_payload_);
    arr[index].value = std::string(value);
    arr[index].mc_ver = mc_ver;
    arr[index].mc_flag = mc_flag;
    arr[index].is_found = true;
    // We do not notify here as there will be more entries to fill.
  } else {
    if (mc_cmd_) {
      auto* rb = static_cast<MCReplyBuilder*>(rb_);
      // Send the value for MC. Note thet we use already parsed key at index from the command
      // arguments.
      rb->SendValue(mc_cmd_->cmd_flags, at(index), value, mc_ver, mc_flag,
                    mc_cmd_->replies_cas_token());
    } else {
      auto* rrb = static_cast<RedisReplyBuilder*>(rb_);
      // TODO: we would like to call the following code lazily to start the array only once.
      // but it currently does not work with RESP as our ParsedCommand does not really
      // contain arguments for RESP flow (DispatchCommand does not accept ParsedCommand yet).
      // if (index == 0)
      //  rrb->StartArray(size());
      rrb->SendBulkString(value);
    }
  }
}

void ParsedCommand::SendGetEnd() {
  if (is_deferred_reply_) {
    // Finalize the reply payload and notify.
    NotifyReplied();
  } else if (mc_cmd_ != nullptr) {  // Only for MC
    // Direct reply for MC with the END marker.
    SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderGetEnd());
  }
}

bool ParsedCommand::SendPayload() {
  if (is_deferred_reply_) {
    if (const auto* stored = get_if<payload::StoredReply>(&reply_payload_); stored) {
      SendDirect(*stored);
    } else if (const auto* get_reply = get_if<payload::MGetReply>(&reply_payload_); get_reply) {
      SendDirect(*get_reply);
    } else {
      CapturingReplyBuilder::Apply(std::move(reply_payload_), rb_);
    }
    reply_payload_ = {};
    return true;
  }
  return false;
}

bool ParsedCommand::CheckDoneAndMarkHead() {
  uint8_t state = state_.load(std::memory_order_acquire);

  while ((state & ASYNC_REPLY_DONE) == 0) {
    // If we marked it as head already, return false.
    if (state & HEAD_REPLY) {
      return false;
    }

    // Mark it as head. If succeeded (i.e ASYNC_REPLY_DONE is still not set), return false
    if (state_.compare_exchange_weak(state, state | HEAD_REPLY, std::memory_order_acq_rel)) {
      return false;
    }
    // Otherwise, retry with updated state.
  }

  // ASYNC_REPLY_DONE is set, return true.
  return true;
}

void ParsedCommand::NotifyReplied() {
  // A synchronization point. We set ASYNC_REPLY_DONE to mark it's safe now to read the payload.
  uint8_t prev_state = state_.fetch_or(ASYNC_REPLY_DONE, std::memory_order_acq_rel);

  DVLOG(1) << "ParsedCommand::NotifyReplied with state " << unsigned(prev_state);

  if (prev_state & DELETE_INTENT) {
    delete this;
    return;
  }
  // If it was marked as head already, notify the connection that the head is done.
  if (prev_state & HEAD_REPLY) {
    // TODO: this might crash as we currently do not wait for async commands on connection close.
    DCHECK(conn_cntx_);
    conn_cntx_->conn()->Notify();
  }
}

void ParsedCommand::SendDirect(const payload::StoredReply& sr) {
  if (sr.ok) {
    if (mc_cmd_)
      rb_->SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderStored());
    else
      rb_->SendOk();
  } else {
    if (mc_cmd_)
      rb_->SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderNotStored());
    else
      static_cast<RedisReplyBuilder*>(rb_)->SendNull();
  }
}

void ParsedCommand::SendDirect(const payload::MGetReply& pl) {
  RedisReplyBuilder* rrb = mc_cmd_ ? nullptr : static_cast<RedisReplyBuilder*>(rb_);
  if (rrb) {
    rrb->StartArray(size());
  }

  for (size_t i = 0; i < size(); ++i) {
    const auto& entry = pl[i];
    if (entry.is_found) {
      if (mc_cmd_) {
        auto* rb = static_cast<MCReplyBuilder*>(rb_);
        rb->SendValue(mc_cmd_->cmd_flags, at(i), entry.value, entry.mc_ver, entry.mc_flag,
                      mc_cmd_->replies_cas_token());
      } else {
        rrb->SendBulkString(entry.value);
      }
    } else {  // not found
      if (mc_cmd_) {
        rb_->SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderNotFound());
      } else {
        rrb->SendNull();
      }
    }
  }
  if (mc_cmd_) {
    rb_->SendSimpleString(MCRender{mc_cmd_->cmd_flags}.RenderGetEnd());
  }
}

}  // namespace facade
