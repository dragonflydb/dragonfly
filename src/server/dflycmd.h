// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>

#include "server/conn_context.h"

namespace util {
class ListenerInterface;
}  // namespace util

namespace dfly {

class EngineShardSet;

namespace journal {
class Journal;
}  // namespace journal

class DflyCmd {
 public:
  DflyCmd(util::ListenerInterface* listener, journal::Journal* journal);

  void Run(CmdArgList args, ConnectionContext* cntx);

 private:
  void HandleJournal(CmdArgList args, ConnectionContext* cntx);

  util::ListenerInterface* listener_;
  journal::Journal* journal_;
  ::boost::fibers::mutex mu_;
  TxId journal_txid_ = 0;
};

}  // namespace dfly
