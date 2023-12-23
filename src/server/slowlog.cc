// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/slowlog.h"

#include "base/logging.h"

namespace dfly {

using namespace std;

void SlowLogShard::ChangeLength(const size_t new_length) {
  log_entries_.set_capacity(new_length);
}

void SlowLogShard::Reset() {
  log_entries_.clear();
}

void SlowLogShard::Add(const string_view command_name, CmdArgList args,
                       const string_view client_name, const string_view client_ip,
                       uint64_t exec_time_usec, uint64_t unix_ts_usec) {
  DCHECK_GT(log_entries_.capacity(), 0u);

  vector<pair<string, uint32_t>> slowlog_args;
  size_t slowlog_effective_length = args.size();
  if (args.size() > kMaximumSlowlogArgCount) {
    // we store one argument fewer because the last argument is "wasted"
    // for telling how many further arguments there are
    slowlog_effective_length = kMaximumSlowlogArgCount - 1;
  }
  slowlog_args.reserve(slowlog_effective_length);
  slowlog_args.emplace_back(command_name, 0);

  for (size_t i = 0; i < slowlog_effective_length; ++i) {
    string_view arg = facade::ArgS(args, i);
    size_t extra_bytes = 0;
    // If any of the arguments is deemed too long, it will be truncated
    // and the truncated string will be suffixed by the number of truncated bytes in
    // this format: "... (n more bytes)"
    size_t extra_bytes_suffix_length = 0;
    if (arg.size() > kMaximumSlowlogArgLength) {
      extra_bytes = arg.size() - kMaximumSlowlogArgLength;
    }
    slowlog_args.emplace_back(arg.substr(0, kMaximumSlowlogArgLength - extra_bytes_suffix_length),
                              extra_bytes);
  }

  log_entries_.push_back(SlowLogEntry{slowlog_entry_id_++, unix_ts_usec, exec_time_usec,
                                      /* +1 for the command */ args.size() + 1,
                                      std::move(slowlog_args), string(client_ip),
                                      string(client_name)});
}

}  // namespace dfly
