// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/escaping.h>
#include <mimalloc.h>

#include <cstdint>
#include <fstream>
#include <iostream>

#include "base/flags.h"
#include "base/init.h"
#include "facade/resp_parser.h"
#include "redis/zmalloc.h"

using namespace facade;
using namespace std;

ABSL_FLAG(string, input, "", "If not empty - reads data from the file instead of stdin. ");

// Validates RESP3 server responses by using RESPParser.
// Server traffic can be recorded using:
// tcpflow  -i any port 6379 -o /tmp/tcp_flow
int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);
  init_zmalloc_threadlocal(mi_heap_get_backing());

  RESPParser parser;
  char buf[1024];
  istream* input_stream = &cin;
  if (!absl::GetFlag(FLAGS_input).empty()) {
    input_stream = new ifstream(absl::GetFlag(FLAGS_input), ios::binary);
    if (!input_stream->good()) {
      cerr << "Failed to open input file: " << absl::GetFlag(FLAGS_input) << "\n";
      return -1;
    }
  }
  size_t offset = 0;
  do {
    input_stream->read(buf, sizeof(buf));
    size_t read = input_stream->gcount();
    if (read == 0) {
      break;
    }
    DVLOG(1) << "Read " << read << " bytes from input stream, offset: " << offset;
    offset += read;

    auto resp_obj = parser.Feed(buf, read);
    if (!resp_obj) {
      cerr << "Parse error near offset " << offset - parser.PendingSize()
           << " when parsing: " << absl::CHexEscape(string_view{buf, read}) << "\n";
      return -1;
    }

    while (!resp_obj->Empty()) {
      resp_obj = parser.Feed(nullptr, 0);
      if (!resp_obj) {
        cerr << "Parse error near offset " << offset - parser.PendingSize() << "\n";
        return -1;
      }
    }
  } while (!input_stream->eof());

  if (parser.PendingSize() != 0) {
    cerr << "Unexpected EOF with " << parser.PendingSize() << " unparsed bytes\n";
    return -1;
  }

  if (input_stream != &cin) {
    delete input_stream;
  }
  cout << "LGTM\n";
  return 0;
}
