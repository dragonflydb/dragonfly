// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/executor.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <memory>

#include "base/logging.h"
#include "server/main_service.h"

using namespace std;

namespace dfly {

namespace {
// Build a CmdData from parts passed to absl::StrCat.
template <typename... Ts> journal::ParsedEntry::CmdData BuildFromParts(Ts... parts) {
  vector<string> raw_parts{absl::StrCat(forward<Ts>(parts))...};

  auto cmd_str = accumulate(raw_parts.begin(), raw_parts.end(), std::string{});
  auto buf = make_unique<char[]>(cmd_str.size());
  memcpy(buf.get(), cmd_str.data(), cmd_str.size());

  CmdArgVec slice_parts{};
  size_t start = 0;
  for (auto part : raw_parts) {
    slice_parts.push_back(MutableSlice{buf.get() + start, part.size()});
    start += part.size();
  }

  return {std::move(buf), std::move(slice_parts)};
}
}  // namespace

JournalExecutor::JournalExecutor(Service* service)
    : service_{service}, conn_context_{&null_sink_, nullptr} {
  conn_context_.is_replicating = true;
  conn_context_.journal_emulated = true;
}

void JournalExecutor::Execute(DbIndex dbid, std::vector<journal::ParsedEntry::CmdData>& cmds) {
  SelectDb(dbid);
  for (auto& cmd : cmds) {
    VLOG(0) << "EXEC";
    for (auto part : cmd.cmd_args) {
      VLOG(0) << facade::ToSV(part);
    }
    Execute(cmd);
  }
}

void JournalExecutor::Execute(DbIndex dbid, journal::ParsedEntry::CmdData& cmd) {
  SelectDb(dbid);
  Execute(cmd);
}

void JournalExecutor::FlushAll() {
  auto cmd = BuildFromParts("FLUSHALL");
  Execute(cmd);
}

void JournalExecutor::Execute(journal::ParsedEntry::CmdData& cmd) {
  auto span = CmdArgList{cmd.cmd_args.data(), cmd.cmd_args.size()};
  service_->DispatchCommand(span, &conn_context_);
}

void JournalExecutor::SelectDb(DbIndex dbid) {
  conn_context_.conn_state.db_index = dbid;

  if (ensured_dbs_.size() <= dbid)
    ensured_dbs_.resize(dbid + 1);

  if (!ensured_dbs_[dbid]) {
    auto cmd = BuildFromParts("SELECT", dbid);
    Execute(cmd);
    ensured_dbs_[dbid] = true;
  }
}

}  // namespace dfly
