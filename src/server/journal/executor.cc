// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/executor.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <memory>

#include "base/logging.h"
#include "facade/reply_capture.h"
#include "facade/service_interface.h"
#include "server/main_service.h"

using namespace std;

namespace dfly {

namespace {
// Build a CmdData from parts passed to absl::StrCat.
template <typename... Ts> void BuildFromParts(cmn::BackedArguments* dest, Ts... parts) {
  vector<string> raw_parts{absl::StrCat(std::forward<Ts>(parts))...};

  dest->Assign(raw_parts.begin(), raw_parts.end(), raw_parts.size());
}

}  // namespace

JournalExecutor::JournalExecutor(Service* service)
    : service_{service},
      reply_builder_{new facade::CapturingReplyBuilder{facade::ReplyMode::NONE}},
      conn_context_{nullptr, nullptr} {
  conn_context_.is_replicating = true;
  conn_context_.journal_emulated = true;
  conn_context_.skip_acl_validation = true;
  conn_context_.ns = &namespaces->GetDefaultNamespace();
}

JournalExecutor::~JournalExecutor() {
}

facade::DispatchResult JournalExecutor::Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd) {
  SelectDb(dbid);
  CommandContext cntx_cmd;
  cntx_cmd.Init(reply_builder_.get(), &conn_context_);

  // TODO: we should improve interfaces in callers (replica and rdb_load) so that we pass
  // CommandContext directly and avoid this swap.
  cntx_cmd.SwapArgs(cmd);
  return Execute(&cntx_cmd);
}

void JournalExecutor::FlushAll() {
  CommandContext cmd;
  cmd.Init(reply_builder_.get(), &conn_context_);
  BuildFromParts(&cmd, "FLUSHALL");
  std::ignore = Execute(&cmd);
}

void JournalExecutor::FlushSlots(const cluster::SlotRange& slot_range) {
  CommandContext cmd;
  cmd.Init(reply_builder_.get(), &conn_context_);
  BuildFromParts(&cmd, "DFLYCLUSTER", "FLUSHSLOTS", slot_range.start, slot_range.end);
  std::ignore = Execute(&cmd);
}

facade::DispatchResult JournalExecutor::Execute(CommandContext* cmd_cntx) {
  return service_->DispatchCommand(facade::ParsedArgs{*cmd_cntx}, cmd_cntx);
}

void JournalExecutor::SelectDb(DbIndex dbid) {
  if (ensured_dbs_.size() <= dbid)
    ensured_dbs_.resize(dbid + 1);

  if (!ensured_dbs_[dbid]) {
    CommandContext cmd;

    cmd.Init(reply_builder_.get(), &conn_context_);
    BuildFromParts(&cmd, "SELECT", dbid);
    std::ignore = Execute(&cmd);
    ensured_dbs_[dbid] = true;

    // TODO: This is a temporary fix for #4146.
    // For some reason without this the replication breaks in regtests.
    auto cb = [](EngineShard* shard) { return OpStatus::OK; };
    shard_set->RunBriefInParallel(std::move(cb));
  } else {
    conn_context_.conn_state.db_index = dbid;
  }
}

}  // namespace dfly
