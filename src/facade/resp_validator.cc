// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/escaping.h>

#include <fstream>
#include <iostream>

#include "base/flags.h"
#include "base/init.h"
#include "facade/resp_parser.h"

using namespace facade;
using namespace std;

ABSL_FLAG(string, input, "", "If not empty - reads data from the file instead of stdin. ");

// Validates RESP3 server responses by using RESPParser.
// Server traffic can be recorded using:
// tcpflow  -i any port 6379 -o /tmp/tcp_flow
int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  RESPParser parser;
  bool input_pending = false;
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

    size_t consumed = 0;
    auto reply = parser.Feed(buf, read, &consumed);
    while (true) {
      if (!reply) {
        cerr << "Parse error at offset " << offset
             << " when parsing: " << absl::CHexEscape({buf, read}) << "\n";
        return -1;
      }
      offset += consumed;

      input_pending = reply->Empty();
      if (input_pending || !parser.HasBufferedInput()) {
        break;
      }
      reply = parser.Feed(nullptr, 0, &consumed);
    }
  } while (!input_stream->eof());

  if (input_stream != &cin) {
    delete input_stream;
  }
  if (input_pending) {
    cerr << "Unexpected end of input at offset " << offset << "\n";
    return -1;
  }
  cout << "LGTM\n";
  return 0;
}
