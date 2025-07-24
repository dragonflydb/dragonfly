// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/types.h"

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

string Entry::ToString() const {
  string rv = absl::StrCat("{op=", opcode, ", dbid=", dbid);

  if (HasPayload()) {
    AppendPrefix(payload.cmd, &rv);
    for (string_view arg : base::it::Wrap(facade::kToSV, payload.args))
      absl::StrAppend(&rv, "'", facade::ToSV(arg), "',");
    AppendSuffix(&rv);
  } else {
    absl::StrAppend(&rv, ", empty");
  }

  rv += "}";
  return rv;
}

string ParsedEntry::ToString() const {
  string rv = absl::StrCat("{op=", opcode, ", dbid=", dbid, ", cmd='");
  absl::StrAppend(&rv, cmd.command, " ");

  size_t offset = 0;
  for (uint32_t sz : cmd.arg_sizes) {
    absl::StrAppend(&rv, cmd.arg_buf.substr(offset, sz), " ");
    offset += sz;
  }

  rv.pop_back();
  rv += "'}";
  return rv;
}

}  // namespace dfly::journal
