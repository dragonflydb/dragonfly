// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_manifest.h"

#include <absl/crc/crc32c.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <fcntl.h>
// For IORING_FSYNC_DATASYNC.
#include <linux/io_uring.h>
#include <stdio.h>
#include <unistd.h>

#include "base/logging.h"
#include "io/file_util.h"
#include "server/error.h"
#include "server/journal/aof.h"
#include "server/journal/aof_segment_writer.h"
#include "util/fibers/uring_file.h"

namespace dfly {

using namespace std;
using nonstd::make_unexpected;

// Example:
//   dfaof-manifest 1
//   state active
//   checkpoint 3
//   base dfs owned dump-base-3.dfs     (or "base dfs user <path>", or "base none")
//   cut_time_ms 0
//   shard_count 2
//   cut 0 5 1234                       (shard id, cut_seq, cut_lsn)
//   cut 1 7 1300
//   crc32c 1a2b3c4d                    (over every byte before this line)
//
// The path is last on its line, so it may contain spaces.

namespace {

constexpr string_view kMagic = "dfaof-manifest";

string_view StateName(AofManifest::State state) {
  return state == AofManifest::State::kActive ? "active" : "bootstrapping";
}

// The value of a "<key> <value>" line, or nullopt if the line has another key.
optional<string_view> Value(string_view line, string_view key) {
  if (!absl::ConsumePrefix(&line, key) || !absl::ConsumePrefix(&line, " "))
    return nullopt;
  return line;
}

template <typename T> bool NumValue(string_view line, string_view key, T* out) {
  optional<string_view> val = Value(line, key);
  return val && absl::SimpleAtoi(*val, out);
}

bool ParseState(string_view val, AofManifest::State* state) {
  if (val == "active")
    *state = AofManifest::State::kActive;
  else if (val == "bootstrapping")
    *state = AofManifest::State::kBootstrapping;
  else
    return false;
  return true;
}

bool ParseBase(string_view val, AofManifest* m) {
  if (val == "none")
    return true;
  vector<string_view> parts = absl::StrSplit(val, absl::MaxSplits(' ', 2));
  if (parts.size() != 3 || parts[2].empty())
    return false;
  // TODO: only DFS bases for now; the format token leaves room for RDB.
  if (parts[0] != "dfs")
    return false;
  if (parts[1] != "owned" && parts[1] != "user")
    return false;
  m->base_owned = parts[1] == "owned";
  m->base_path = parts[2];
  return true;
}

bool ParseCut(string_view val, uint32_t shard_id, AofManifest::Cut* cut) {
  vector<string_view> parts = absl::StrSplit(val, ' ');
  uint32_t sid = 0;
  return parts.size() == 3 && absl::SimpleAtoi(parts[0], &sid) && sid == shard_id &&
         absl::SimpleAtoi(parts[1], &cut->seq) && absl::SimpleAtoi(parts[2], &cut->lsn);
}

error_code Corrupt(string_view what) {
  error_code ec = make_error_code(AofError::kBadManifest);
  LOG(ERROR) << ec.message() << ": " << what;
  return ec;
}

error_code LastErrno() {
  return {errno, system_category()};
}

}  // namespace

string EncodeAofManifest(const AofManifest& m) {
  DCHECK_EQ(m.base_path.find('\n'), string::npos);
  string out = absl::StrCat(kMagic, " ", kAofManifestVersion, "\n");
  absl::StrAppend(&out, "state ", StateName(m.state), "\n");
  absl::StrAppend(&out, "checkpoint ", m.checkpoint_id, "\n");
  if (m.base_path.empty()) {
    absl::StrAppend(&out, "base none\n");
  } else {
    absl::StrAppend(&out, "base dfs ", m.base_owned ? "owned" : "user", " ", m.base_path, "\n");
  }
  absl::StrAppend(&out, "cut_time_ms ", m.cut_time_ms, "\n");
  absl::StrAppend(&out, "shard_count ", m.cuts.size(), "\n");
  for (size_t i = 0; i < m.cuts.size(); ++i)
    absl::StrAppend(&out, "cut ", i, " ", m.cuts[i].seq, " ", m.cuts[i].lsn, "\n");
  uint32_t crc = static_cast<uint32_t>(absl::ComputeCrc32c(out));
  absl::StrAppend(&out, "crc32c ", absl::Hex(crc, absl::kZeroPad8), "\n");
  return out;
}

io::Result<AofManifest> DecodeAofManifest(string_view text) {
  // Every line ends with a newline, so the last element is empty.
  vector<string_view> lines = absl::StrSplit(text, '\n');
  if (lines.size() < 2 || !lines.back().empty())
    return make_unexpected(Corrupt("truncated"));
  lines.pop_back();

  // The crc line is last and covers every byte before it.
  string_view crc_line = lines.back();
  lines.pop_back();
  optional<string_view> crc_hex = Value(crc_line, "crc32c");
  uint32_t crc;
  if (!crc_hex || !absl::SimpleHexAtoi(*crc_hex, &crc))
    return make_unexpected(Corrupt("bad crc32c line"));
  string_view body = text.substr(0, text.size() - crc_line.size() - 1);
  if (crc != static_cast<uint32_t>(absl::ComputeCrc32c(body)))
    return make_unexpected(Corrupt("crc32c mismatch"));

  // Fixed order: the header lines, then one cut line per shard.
  constexpr size_t kHeaderLines = 6;
  if (lines.size() < kHeaderLines)
    return make_unexpected(Corrupt("truncated"));
  uint32_t version;
  if (!NumValue(lines[0], kMagic, &version))
    return make_unexpected(Corrupt("not a manifest"));
  if (version != kAofManifestVersion)
    return make_unexpected(make_error_code(AofError::kManifestVersion));

  AofManifest m;
  optional<string_view> state = Value(lines[1], "state");
  if (!state || !ParseState(*state, &m.state))
    return make_unexpected(Corrupt("bad state"));
  if (!NumValue(lines[2], "checkpoint", &m.checkpoint_id))
    return make_unexpected(Corrupt("bad checkpoint"));
  optional<string_view> base = Value(lines[3], "base");
  if (!base || !ParseBase(*base, &m))
    return make_unexpected(Corrupt("bad base"));
  if (!NumValue(lines[4], "cut_time_ms", &m.cut_time_ms))
    return make_unexpected(Corrupt("bad cut_time_ms"));
  uint32_t shard_count;
  if (!NumValue(lines[5], "shard_count", &shard_count) || shard_count == 0 ||
      lines.size() != kHeaderLines + shard_count)
    return make_unexpected(Corrupt("bad shard_count"));
  m.cuts.resize(shard_count);
  for (uint32_t i = 0; i < shard_count; ++i) {
    optional<string_view> cut = Value(lines[kHeaderLines + i], "cut");
    if (!cut || !ParseCut(*cut, i, &m.cuts[i]))
      return make_unexpected(Corrupt(absl::StrCat("bad cut of shard ", i)));
  }
  return m;
}

error_code WriteAofManifest(string_view dir, const AofManifest& manifest) {
  string path = absl::StrCat(dir, "/", kAofManifestName);
  string tmp_path = absl::StrCat(path, ".tmp");
  string text = EncodeAofManifest(manifest);

  auto res = util::fb2::OpenLinux(tmp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644 /* rw-r--r-- */);
  if (!res)
    return res.error();
  unique_ptr<util::fb2::LinuxFile> file = std::move(*res);
  error_code ec = file->Write(io::Buffer(text), 0, 0);
  if (!ec)
    ec = file->FSync(IORING_FSYNC_DATASYNC);
  error_code close_ec = file->Close();
  if (!ec)
    ec = close_ec;
  // The old manifest stays in place until the rename.
  if (!ec && rename(tmp_path.c_str(), path.c_str()) != 0)
    ec = LastErrno();
  if (ec) {
    unlink(tmp_path.c_str());
    return ec;
  }
  return AofSyncDir(string(dir));
}

io::Result<AofManifest> ReadAofManifest(string_view dir) {
  io::Result<string> text = io::ReadFileToString(absl::StrCat(dir, "/", kAofManifestName));
  if (!text)
    return make_unexpected(text.error());
  return DecodeAofManifest(*text);
}

}  // namespace dfly
