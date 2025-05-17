// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/escaping.h>

#include <cstdint>

#include "base/init.h"
#include "facade/redis_parser.h"
#include "io/io.h"

using namespace facade;
using namespace std;

// Validates RESP3 server responses by using RespParser.
// Server traffic can be recorded using:
// tcpflow  -i any port 6379 -o /tmp/tcp_flow
int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  RedisParser parser(RedisParser::Mode::CLIENT);
  RedisParser::Result parse_result = RedisParser::OK;
  char buf[1024];
  do {
    cin.read(buf, sizeof(buf));
    size_t len = cin.gcount();
    if (len == 0) {
      if (parse_result != RedisParser::OK) {
        cerr << "unexpected: " << parse_result << endl;
      }
      break;
    }
    RespExpr::Vec args;
    uint32_t consumed = 0;
    char* next = buf;
    while (len) {
      parse_result = parser.Parse(io::Buffer({next, len}), &consumed, &args);
      if (parse_result != RedisParser::OK && parse_result != RedisParser::INPUT_PENDING) {
        cerr << "Parse error: " << int(parse_result)
             << " when parsing: " << absl::CHexEscape({reinterpret_cast<const char*>(next), len})
             << endl;
        return -1;
      }
      len -= consumed;
      next += consumed;
    }
  } while (!cin.eof());

  cout << "LGTM\n";
  return 0;
}
