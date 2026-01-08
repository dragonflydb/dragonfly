// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/parsed_command.h"

#include "base/logging.h"
#include "core/overloaded.h"
#include "facade/conn_context.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"
#include "facade/reply_capture.h"

namespace facade {

using namespace std;

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

std::string MCRender::RenderStored(bool ok) const {
  if (flags_.no_reply)
    return {};
  if (ok)
    return flags_.meta ? "HD" : "STORED";
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
  is_deferred_reply_ = false;
  reply_ = std::monostate{};

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
    reply_ = payload::make_error(str, type);
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
      reply_ = payload::SimpleString{"OK"};
    else
      reply_ = payload::make_error(StatusToMsg(status));
  }
}

void ParsedCommand::SendError(const facade::ErrorReply& error) {
  if (error.status)
    return SendError(*error.status);
  SendError(error.ToSv(), error.kind);
}

void ParsedCommand::SendSimpleString(std::string_view str) {
  if (!is_deferred_reply_) {
    rb_->SendSimpleString(str);
  } else {
    reply_ = payload::make_simple_or_noreply(str);
  }
}

void ParsedCommand::SendNull() {
  if (is_deferred_reply_) {
    reply_ = payload::Null{};
  } else {
    DCHECK(mc_cmd_ == nullptr);  // RESP only
    static_cast<RedisReplyBuilder*>(rb_)->SendNull();
  }
}

void ParsedCommand::Resolve(ErrorReply&& error) {
  DCHECK(is_deferred_reply_);
  SendError(error);
}

bool ParsedCommand::IsReady() const {
  dfly::Overloaded ov{[](const payload::Payload& pl) { return pl.index() > 0; },
                      [](const AsyncTask& task) { return task.blocker->IsCompleted(); }};
  return visit(ov, reply_);
}

bool ParsedCommand::OnCompletion(util::fb2::detail::Waiter* waiter) {
  dfly::Overloaded ov{[](const payload::Payload& pl) {
                        DCHECK(pl.index() > 0);
                        return true;
                      },
                      [waiter](const AsyncTask& task) {
                        task.blocker->OnCompletion(waiter);
                        return false;
                      }};
  return visit(ov, reply_);
}

void ParsedCommand::Reply() {
  dfly::Overloaded ov{
      [this](payload::Payload&& pl) { CapturingReplyBuilder::Apply(std::move(pl), rb_); },
      [this](AsyncTask&& task) {
        DCHECK(task.blocker->IsCompleted());
        task.replier(rb_);
      }};
  visit(ov, exchange(reply_, monostate{}));
}

}  // namespace facade
