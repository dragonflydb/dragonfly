// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_shard.h"

#include <fcntl.h>

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>

#include <filesystem>

#include "base/logging.h"
#include "util/fibers/fibers_ext.h"

namespace dfly {
namespace journal {
using namespace std;
using namespace util;
namespace fibers = boost::fibers;
namespace fs = std::filesystem;

namespace {

string ShardName(std::string_view base, unsigned index) {
  return absl::StrCat(base, "-", absl::Dec(index, absl::kZeroPad4), ".log");
}

}  // namespace

#define CHECK_EC(x)                                                                 \
  do {                                                                              \
    auto __ec$ = (x);                                                               \
    CHECK(!__ec$) << "Error: " << __ec$ << " " << __ec$.message() << " for " << #x; \
  } while (false)



JournalShard::JournalShard() {
}

JournalShard::~JournalShard() {
  CHECK(!shard_file_);
}

std::error_code JournalShard::Open(const std::string_view dir, unsigned index) {
  CHECK(!shard_file_);

  fs::path dir_path;

  if (dir.empty()) {
  } else {
    dir_path = dir;
    error_code ec;

    fs::file_status dir_status = fs::status(dir_path, ec);
    if (ec) {
      if (ec == errc::no_such_file_or_directory) {
        fs::create_directory(dir_path, ec);
        dir_status = fs::status(dir_path, ec);
      }
      if (ec)
        return ec;
    }
    // LOG(INFO) << int(dir_status.type());
  }
  dir_path.append(ShardName("journal", index));
  shard_path_ = dir_path;

  // For file integrity guidelines see:
  // https://lwn.net/Articles/457667/
  // https://www.evanjones.ca/durability-filesystem.html
  // NOTE: O_DSYNC is omitted.
  constexpr auto kJournalFlags = O_CLOEXEC | O_CREAT | O_TRUNC | O_RDWR;
  io::Result<std::unique_ptr<uring::LinuxFile>> res =
      uring::OpenLinux(shard_path_, kJournalFlags, 0666);
  if (!res) {
    return res.error();
  }
  DVLOG(1) << "Opened journal " << shard_path_;

  shard_file_ = std::move(res).value();
  shard_index_ = index;
  file_offset_ = 0;
  status_ec_.clear();

  return error_code{};
}

error_code JournalShard::Close() {
  VLOG(1) << "JournalShard::Close";

  CHECK(shard_file_);
  lameduck_ = true;

  auto ec = shard_file_->Close();

  DVLOG(1) << "Closing " << shard_path_;
  LOG_IF(ERROR, ec) << "Error closing journal file " << ec;
  shard_file_.reset();

  return ec;
}

void JournalShard::AddLogRecord(TxId txid, unsigned opcode) {
  string line = absl::StrCat(lsn_, " ", txid, " ", opcode, "\n");
  error_code ec = shard_file_->Write(io::Buffer(line), file_offset_, 0);
  CHECK_EC(ec);
  file_offset_ += line.size();
  ++lsn_;
}

}  // namespace journal
}  // namespace dfly