// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/escaping.h>

#include <cstdint>
#include <fstream>

#include "base/flags.h"
#include "base/init.h"
#include "facade/redis_parser.h"
#include "io/io.h"

using namespace facade;
using namespace std;

ABSL_FLAG(string, input, "", "If not empty - reads data from the file instead of stdin. ");

// Validates RESP3 server responses by using RespParser.
// Server traffic can be recorded using:
// tcpflow  -i any port 6379 -o /tmp/tcp_flow
int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  RedisParser parser(RedisParser::Mode::CLIENT);
  RedisParser::Result parse_result = RedisParser::OK;
  char buf[1024];
  istream* input_stream = &cin;
  if (!absl::GetFlag(FLAGS_input).empty()) {
    input_stream = new ifstream(absl::GetFlag(FLAGS_input), ios::binary);
    if (!input_stream->good()) {
      cerr << "Failed to open input file: " << absl::GetFlag(FLAGS_input) << "\n";
      return -1;
    }
  }
  size_t len = 0, offset = 0;
  do {
    input_stream->read(buf + len, sizeof(buf) - len);
    size_t read = input_stream->gcount();
    if (read == 0) {
      if (parse_result != RedisParser::OK) {
        cerr << "unexpected: " << parse_result << "\n";
      }
      break;
    }
    DVLOG(1) << "Read " << read << " bytes from input stream, offset: " << offset;
    len += read;

    RespExpr::Vec args;
    uint32_t consumed = 0;
    char* next = buf;
    while (len) {
      string_view sv{next, len};
      parse_result = parser.Parse(io::Buffer(sv), &consumed, &args);
      if (parse_result != RedisParser::OK && parse_result != RedisParser::INPUT_PENDING) {
        cerr << "Parse error: " << int(parse_result) << " at offset " << offset
             << " when parsing: " << absl::CHexEscape({reinterpret_cast<const char*>(next), len})
             << "\n";
        return -1;
      }

      if (consumed == 0) {  // not enough data to parse.
        DVLOG(1) << "No data consumed, waiting for more input.";
        memcpy(buf, next, len);  // move the remaining data to the start of the buffer.
        break;
      }
      len -= consumed;
      next += consumed;
      offset += consumed;
    }
  } while (!input_stream->eof());

  if (input_stream != &cin) {
    delete input_stream;
  }
  cout << "LGTM\n";
  return 0;
}
