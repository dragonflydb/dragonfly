// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/types.h"

#include "server/cluster/cluster_defs.h"

namespace dfly::journal {

using namespace std;

void AppendPrefix(string_view cmd, string* dest) {
  absl::StrAppend(dest, ", cmd='");
  absl::StrAppend(dest, cmd);
  absl::StrAppend(dest, "', args=[");
}

void AppendSuffix(string* dest) {
  if (dest->back() == ',')
    dest->pop_back();
  absl::StrAppend(dest, "]");
}

template <typename C> string Concat(const C& list) {
  string res;
  for (auto arg : list) {
    absl::StrAppend(&res, "'");
    absl::StrAppend(&res, facade::ToSV(arg));
    absl::StrAppend(&res, "',");
  }
  return res;
}

string Entry::ToString() const {
  string rv = absl::StrCat("{op=", opcode, ", dbid=", dbid);

  if (HasPayload()) {
    AppendPrefix(payload.cmd, &rv);
    rv += visit([](const auto& list) { return Concat(list); }, payload.args);
    AppendSuffix(&rv);
  } else {
    absl::StrAppend(&rv, ", empty");
  }

  rv += "}";
  return rv;
}

string ParsedEntry::ToString() const {
  string rv = absl::StrCat("{op=", opcode, ", dbid=", dbid, ", cmd='");
  for (auto& arg : cmd.cmd_args) {
    absl::StrAppend(&rv, facade::ToSV(arg));
    absl::StrAppend(&rv, " ");
  }
  rv.pop_back();
  rv += "'}";
  return rv;
}

}  // namespace dfly::journal
