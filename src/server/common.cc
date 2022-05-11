// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/common.h"

#include <absl/strings/str_cat.h>

extern "C" {
#include "redis/object.h"
}

#include "base/logging.h"
#include "server/error.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;

thread_local ServerState ServerState::state_;

atomic_uint64_t used_mem_peak(0);
atomic_uint64_t used_mem_current(0);
unsigned kernel_version = 0;
size_t max_memory_limit = 0;

ServerState::ServerState() {
}

ServerState::~ServerState() {
}

void ServerState::Init() {
  gstate_ = GlobalState::IDLE;
}

void ServerState::Shutdown() {
  gstate_ = GlobalState::SHUTTING_DOWN;
  interpreter_.reset();
}

Interpreter& ServerState::GetInterpreter() {
  if (!interpreter_) {
    interpreter_.emplace();
  }

  return interpreter_.value();
}

const char* GlobalState::Name(S s) {
  switch (s) {
    case GlobalState::IDLE:
      return "IDLE";
    case GlobalState::LOADING:
      return "LOADING";
    case GlobalState::SAVING:
      return "SAVING";
    case GlobalState::SHUTTING_DOWN:
      return "SHUTTING DOWN";
  }
  ABSL_INTERNAL_UNREACHABLE;
}

const char* ObjTypeName(int type) {
  switch (type) {
    case OBJ_STRING:
      return "string";
    case OBJ_LIST:
      return "list";
    case OBJ_SET:
      return "set";
    case OBJ_ZSET:
      return "zset";
    case OBJ_HASH:
      return "hash";
    case OBJ_STREAM:
      return "stream";
    default:
      LOG(ERROR) << "Unsupported type " << type;
  }
  return "invalid";
};

bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes) {
  if (str.empty())
    return false;

  const char* cstr = str.data();
  bool neg = (*cstr == '-');
  if (neg) {
    cstr++;
  }
  char* end;
  double d = strtod(cstr, &end);

  // If this didn't consume the entire string, fail.
  if (end + 1 < str.end())
    return false;

  int64 scale = 1;
  switch (*end) {
    // NB: an int64 can only go up to <8 EB.
    case 'E':
      scale <<= 10;  // Fall through...
      ABSL_FALLTHROUGH_INTENDED;
    case 'P':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'T':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'G':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'M':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'K':
    case 'k':
      scale <<= 10;
      ABSL_FALLTHROUGH_INTENDED;
    case 'B':
    case '\0':
      break;  // To here.
    default:
      return false;
  }
  d *= scale;
  if (d > kint64max || d < 0)
    return false;
  *num_bytes = static_cast<int64>(d + 0.5);
  if (neg) {
    *num_bytes = -*num_bytes;
  }
  return true;
}

#define ADD(x) (x) += o.x

TieredStats& TieredStats::operator+=(const TieredStats& o) {
  static_assert(sizeof(TieredStats) == 32);

  ADD(external_reads);
  ADD(external_writes);
  ADD(storage_capacity);
  ADD(storage_reserved);
  return *this;
}

}  // namespace dfly
