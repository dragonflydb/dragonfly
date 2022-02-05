// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <array>
#include <boost/fiber/mutex.hpp>

#include "server/conn_context.h"

namespace dfly {

class EngineShardSet;

// This class has a state through the lifetime of a server because it manipulates scripts
class ScriptMgr {
 public:
  ScriptMgr(EngineShardSet* ess);

  void Run(CmdArgList args, ConnectionContext* cntx);

  bool InsertFunction(std::string_view sha,  std::string_view body);
  const char* Find(std::string_view sha) const;

 private:
  EngineShardSet* ess_;
  using ScriptKey = std::array<char, 40>;
  absl::flat_hash_map<ScriptKey, std::unique_ptr<char[]>> db_;
  mutable ::boost::fibers::mutex mu_;
};

}  // namespace dfly
