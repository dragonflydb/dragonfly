// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "server/common.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "util/fibers/future.h"
#include "util/proactor_pool.h"

namespace dfly {

class ConnectionContext;
class CommandRegistry;
using facade::OpResult;
using facade::OpStatus;

// Stores a string, the pending result of a tiered read or nothing
struct StringValue {
  StringValue() : v_{} {
  }
  StringValue(std::string s) : v_{std::move(s)} {
  }
  StringValue(util::fb2::Future<std::string> f) : v_{std::move(f)} {
  }

  // Get and consume value. If backed by a future, blocks until resolved.
  std::string Get() &&;

  // If no value is stored
  bool IsEmpty() const;

  // Read string from prime value - either from memory or issue tiered storage read
  static StringValue Read(DbIndex dbid, std::string_view key, const PrimeValue& pv,
                          EngineShard* es);

  bool IsFuturized() const {
    return std::holds_alternative<util::fb2::Future<std::string>>(v_);
  }

 private:
  std::variant<std::monostate, std::string, util::fb2::Future<std::string>> v_;
};

// Helper for performing SET operations with various options
class SetCmd {
 public:
  explicit SetCmd(OpArgs op_args, bool manual_journal)
      : op_args_(op_args), manual_journal_{manual_journal} {
  }

  enum SetFlags {
    SET_ALWAYS = 0,
    SET_IF_NOTEXIST = 1 << 0,     /* NX: Set if key not exists. */
    SET_IF_EXISTS = 1 << 1,       /* XX: Set if key exists. */
    SET_KEEP_EXPIRE = 1 << 2,     /* KEEPTTL: Set and keep the ttl */
    SET_GET = 1 << 3,             /* GET: Set if want to get key before set */
    SET_EXPIRE_AFTER_MS = 1 << 4, /* EX,PX,EXAT,PXAT: Expire after ms. */
    SET_STICK = 1 << 5,           /* Set STICK flag */
  };

  struct SetParams {
    uint16_t flags = SET_ALWAYS;
    uint16_t memcache_flags = 0;
    uint64_t expire_after_ms = 0;     // Relative value based on now. 0 means no expiration.
    StringValue* prev_val = nullptr;  // If set, previous value is stored at pointer

    constexpr bool IsConditionalSet() const {
      return flags & SET_IF_NOTEXIST || flags & SET_IF_EXISTS;
    }
  };

  OpStatus Set(const SetParams& params, std::string_view key, std::string_view value);

 private:
  OpStatus SetExisting(const SetParams& params, DbSlice::Iterator it, DbSlice::ExpIterator e_it,
                       std::string_view key, std::string_view value);

  void AddNew(const SetParams& params, DbSlice::Iterator it, DbSlice::ExpIterator e_it,
              std::string_view key, std::string_view value);

  // Called at the end of AddNew of SetExisting
  void PostEdit(const SetParams& params, std::string_view key, std::string_view value,
                PrimeValue* pv);

  void RecordJournal(const SetParams& params, std::string_view key, std::string_view value);

  OpStatus CachePrevIfNeeded(const SetParams& params, DbSlice::Iterator it);

  const OpArgs op_args_;
  bool manual_journal_;
};

class StringFamily {
 public:
  static void Register(CommandRegistry* registry);

 private:
  static void Append(CmdArgList args, ConnectionContext* cntx);
  static void Decr(CmdArgList args, ConnectionContext* cntx);
  static void DecrBy(CmdArgList args, ConnectionContext* cntx);
  static void Get(CmdArgList args, ConnectionContext* cntx);
  static void GetDel(CmdArgList args, ConnectionContext* cntx);
  static void GetRange(CmdArgList args, ConnectionContext* cntx);
  static void GetSet(CmdArgList args, ConnectionContext* cntx);
  static void GetEx(CmdArgList args, ConnectionContext* cntx);
  static void Incr(CmdArgList args, ConnectionContext* cntx);
  static void IncrBy(CmdArgList args, ConnectionContext* cntx);
  static void IncrByFloat(CmdArgList args, ConnectionContext* cntx);
  static void MGet(CmdArgList args, ConnectionContext* cntx);
  static void MSet(CmdArgList args, ConnectionContext* cntx);
  static void MSetNx(CmdArgList args, ConnectionContext* cntx);

  static void Set(CmdArgList args, ConnectionContext* cntx);
  static void SetEx(CmdArgList args, ConnectionContext* cntx);
  static void SetNx(CmdArgList args, ConnectionContext* cntx);
  static void SetRange(CmdArgList args, ConnectionContext* cntx);
  static void StrLen(CmdArgList args, ConnectionContext* cntx);
  static void Prepend(CmdArgList args, ConnectionContext* cntx);
  static void PSetEx(CmdArgList args, ConnectionContext* cntx);

  static void ClThrottle(CmdArgList args, ConnectionContext* cntx);

  // These functions are used internally, they do not implement any specific command
  static void IncrByGeneric(std::string_view key, int64_t val, ConnectionContext* cntx);
  static void ExtendGeneric(CmdArgList args, bool prepend, ConnectionContext* cntx);
  static void SetExGeneric(bool seconds, CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
