// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_registry.h"

#include "absl/strings/str_cat.h"
#include "base/bits.h"
#include "base/logging.h"
#include "facade/error.h"
#include "server/conn_context.h"

namespace dfly {
using namespace facade;
using namespace std;

using absl::StrAppend;
using absl::StrCat;

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, int8_t step)
    : name_(name), opt_mask_(mask), arity_(arity), first_key_(first_key), last_key_(last_key),
      step_key_(step) {
  if (mask & CO::ADMIN) {
    opt_mask_ |= CO::NOSCRIPT;
  }
}

uint32_t CommandId::OptCount(uint32_t mask) {
  return absl::popcount(mask);
}

CommandRegistry::CommandRegistry() {
  CommandId cd("COMMAND", CO::RANDOM | CO::LOADING | CO::NOSCRIPT, -1, 0, 0, 0);

  cd.SetHandler([this](const auto& args, auto* cntx) { return Command(args, cntx); });

  const char* nm = cd.name();
  cmd_map_.emplace(nm, std::move(cd));
}

void CommandRegistry::Command(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() > 1) {
    ToUpper(&args[1]);
    string_view subcmd = ArgS(args, 1);
    if (subcmd == "COUNT") {
      return (*cntx)->SendLong(cmd_map_.size());
    } else {
      return (*cntx)->SendError(kSyntaxErr, kSyntaxErrType);
    }
  }
  size_t len = cmd_map_.size();

  (*cntx)->StartArray(len);

  for (const auto& val : cmd_map_) {
    const CommandId& cd = val.second;
    (*cntx)->StartArray(6);
    (*cntx)->SendSimpleString(cd.name());
    (*cntx)->SendLong(cd.arity());
    (*cntx)->StartArray(CommandId::OptCount(cd.opt_mask()));

    for (uint32_t i = 0; i < 32; ++i) {
      unsigned obit = (1u << i);
      if (cd.opt_mask() & obit) {
        const char* name = CO::OptName(CO::CommandOpt{obit});
        (*cntx)->SendSimpleString(name);
      }
    }

    (*cntx)->SendLong(cd.first_key_pos());
    (*cntx)->SendLong(cd.last_key_pos());
    (*cntx)->SendLong(cd.key_arg_step());
  }
}

CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
  string_view k = cmd.name();
  CHECK(cmd_map_.emplace(k, std::move(cmd)).second) << k;

  return *this;
}

KeyIndex DetermineKeys(const CommandId* cid, const CmdArgList& args) {
  DCHECK_EQ(0u, cid->opt_mask() & CO::GLOBAL_TRANS);

  KeyIndex key_index;

  if (cid->first_key_pos() > 0) {
    key_index.start = cid->first_key_pos();
    int last = cid->last_key_pos();
    key_index.end = last > 0 ? last + 1 : (int(args.size()) + 1 + last);
    key_index.step = cid->key_arg_step();

    return key_index;
  }

  string_view name{cid->name()};
  if (name == "EVAL" || name == "EVALSHA") {
    DCHECK_GE(args.size(), 3u);
    uint32_t num_keys;

    CHECK(absl::SimpleAtoi(ArgS(args, 2), &num_keys));
    key_index.start = 3;
    key_index.end = 3 + num_keys;
    key_index.step = 1;

    return key_index;
  }

  LOG(FATAL) << "TBD: Not supported";

  return key_index;
}

namespace CO {

const char* OptName(CO::CommandOpt fl) {
  using namespace CO;

  switch (fl) {
    case WRITE:
      return "write";
    case READONLY:
      return "readonly";
    case DENYOOM:
      return "denyoom";
    case FAST:
      return "fast";
    case LOADING:
      return "loading";
    case RANDOM:
      return "random";
    case ADMIN:
      return "admin";
    case NOSCRIPT:
      return "noscript";
    case BLOCKING:
      return "blocking";
    case GLOBAL_TRANS:
      return "global-trans";
  }
  return "unknown";
}

}  // namespace CO

}  // namespace dfly
