// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/debugcmd.h"

#include <absl/strings/str_cat.h>

#include <boost/fiber/operations.hpp>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/string_family.h"
#include "util/uring/uring_fiber_algo.h"

namespace dfly {

using namespace std;
using namespace util;
namespace this_fiber = ::boost::this_fiber;
using boost::fibers::fiber;

struct PopulateBatch {
  DbIndex dbid;
  uint64_t index[32];
  uint64_t sz = 0;

  PopulateBatch(DbIndex id) : dbid(id) {
  }
};

void DoPopulateBatch(std::string_view prefix, size_t val_size, const SetCmd::SetParams& params,
                     const PopulateBatch& ps) {
  SetCmd sg(&EngineShard::tlocal()->db_slice());

  for (unsigned i = 0; i < ps.sz; ++i) {
    string key = absl::StrCat(prefix, ":", ps.index[i]);
    string val = absl::StrCat("value:", ps.index[i]);

    if (val.size() < val_size) {
      val.resize(val_size, 'x');
    }
    sg.Set(params, key, val);
  }
}

DebugCmd::DebugCmd(EngineShardSet* ess, ConnectionContext* cntx) : ess_(ess), cntx_(cntx) {
}

void DebugCmd::Run(CmdArgList args) {
  std::string_view subcmd = ArgS(args, 1);
  if (subcmd == "HELP") {
    std::string_view help_arr[] = {
        "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "POPULATE <count> [<prefix>] [<size>]",
        "    Create <count> string keys named key:<num>. If <prefix> is specified then",
        "    it is used instead of the 'key' prefix.",
        "HELP",
        "    Prints this help.",
    };
    return cntx_->SendSimpleStrArr(help_arr, ABSL_ARRAYSIZE(help_arr));
  }

  VLOG(1) << "subcmd " << subcmd;

  if (subcmd == "POPULATE") {
    return Populate(args);
  }

  string reply = absl::StrCat("Unknown subcommand or wrong number of arguments for '", subcmd,
                              "'. Try DEBUG HELP.");
  return cntx_->SendError(reply);
}

void DebugCmd::Populate(CmdArgList args) {
  if (args.size() < 3 || args.size() > 5) {
    return cntx_->SendError(
        "Unknown subcommand or wrong number of arguments for 'populate'. Try DEBUG HELP.");
  }

  uint64_t total_count = 0;
  if (!absl::SimpleAtoi(ArgS(args, 2), &total_count))
    return cntx_->SendError(kUintErr);
  std::string_view prefix{"key"};

  if (args.size() > 3) {
    prefix = ArgS(args, 3);
  }
  uint32_t val_size = 0;
  if (args.size() > 4) {
    std::string_view str = ArgS(args, 4);
    if (!absl::SimpleAtoi(str, &val_size))
      return cntx_->SendError(kUintErr);
  }

  size_t runners_count = ess_->pool()->size();
  vector<pair<uint64_t, uint64_t>> ranges(runners_count - 1);
  uint64_t batch_size = total_count / runners_count;
  size_t from = 0;
  for (size_t i = 0; i < ranges.size(); ++i) {
    ranges[i].first = from;
    ranges[i].second = batch_size;
    from += batch_size;
  }
  ranges.emplace_back(from, total_count - from);

  vector<fiber> fb_arr(ranges.size());
  for (size_t i = 0; i < ranges.size(); ++i) {
    auto range = ranges[i];

    // whatever we do, we should not capture i by reference.
    fb_arr[i] = ess_->pool()->at(i)->LaunchFiber([=] {
      this->PopulateRangeFiber(range.first, range.second, prefix, val_size);
    });
  }
  for (auto& fb : fb_arr)
    fb.join();

  cntx_->SendOk();
}

void DebugCmd::PopulateRangeFiber(uint64_t from, uint64_t len, std::string_view prefix,
                                  unsigned value_len) {
  this_fiber::properties<FiberProps>().set_name("populate_range");
  VLOG(1) << "PopulateRange: " << from << "-" << (from + len - 1);

  string key = absl::StrCat(prefix, ":");
  size_t prefsize = key.size();
  DbIndex db_indx = cntx_->db_index();
  std::vector<PopulateBatch> ps(ess_->size(), PopulateBatch{db_indx});
  SetCmd::SetParams params{db_indx};

  for (uint64_t i = from; i < from + len; ++i) {
    absl::StrAppend(&key, i);
    ShardId sid = Shard(key, ess_->size());
    key.resize(prefsize);

    auto& pops = ps[sid];
    pops.index[pops.sz++] = i;
    if (pops.sz == 32) {
      ess_->Add(sid, [=, p = pops] {
        DoPopulateBatch(prefix, value_len, params, p);
        if (i % 50 == 0) {
          this_fiber::yield();
        }
      });

      // we capture pops by value so we can override it here.
      pops.sz = 0;
    }
  }

  ess_->RunBriefInParallel([&](EngineShard* shard) {
    DoPopulateBatch(prefix, value_len, params, ps[shard->shard_id()]);
  });
}

}  // namespace dfly
