// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/journal_slice.h"

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>
#include <fcntl.h>

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

struct JournalSlice::RingItem {
  LSN lsn;
  TxId txid;
  Op opcode;
};

JournalSlice::JournalSlice() {
}

JournalSlice::~JournalSlice() {
  CHECK(!shard_file_);
}

void JournalSlice::Init(unsigned index) {
  if (ring_buffer_)  // calling this function multiple times is allowed and it's a no-op.
    return;

  slice_index_ = index;
  ring_buffer_.emplace(128);  // TODO: to make it configurable
}

std::error_code JournalSlice::Open(std::string_view dir) {
  CHECK(!shard_file_);
  DCHECK_NE(slice_index_, UINT32_MAX);

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

  dir_path.append(ShardName("journal", slice_index_));
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
  file_offset_ = 0;
  status_ec_.clear();

  return error_code{};
}

error_code JournalSlice::Close() {
  VLOG(1) << "JournalSlice::Close";

  CHECK(shard_file_);
  lameduck_ = true;

  auto ec = shard_file_->Close();

  DVLOG(1) << "Closing " << shard_path_;
  LOG_IF(ERROR, ec) << "Error closing journal file " << ec;
  shard_file_.reset();

  return ec;
}

void JournalSlice::AddLogRecord(const Entry& entry, bool await) {
  DCHECK(ring_buffer_);
  iterating_cb_arr_ = true;
  for (const auto& k_v : change_cb_arr_) {
    k_v.second(entry, await);
  }
  iterating_cb_arr_ = false;

  RingItem item;
  item.lsn = lsn_;
  item.opcode = entry.opcode;
  item.txid = entry.txid;
  VLOG(1) << "Writing item " << item.lsn;
  ring_buffer_->EmplaceOrOverride(move(item));

  if (shard_file_) {
    string line = absl::StrCat(lsn_, " ", entry.txid, " ", entry.opcode, "\n");
    error_code ec = shard_file_->Write(io::Buffer(line), file_offset_, 0);
    CHECK_EC(ec);
    file_offset_ += line.size();
  }

  ++lsn_;
}

uint32_t JournalSlice::RegisterOnChange(ChangeCallback cb) {
  uint32_t id = next_cb_id_++;
  change_cb_arr_.emplace_back(id, std::move(cb));
  return id;
}

void JournalSlice::UnregisterOnChange(uint32_t id) {
  CHECK(!iterating_cb_arr_);

  auto it = find_if(change_cb_arr_.begin(), change_cb_arr_.end(),
                    [id](const auto& e) { return e.first == id; });
  CHECK(it != change_cb_arr_.end());
  change_cb_arr_.erase(it);
}

}  // namespace journal
}  // namespace dfly
