// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiered_storage.h"

extern "C" {
#include "redis/object.h"
}

#include "base/logging.h"
#include "server/db_slice.h"
#include "util/proactor_base.h"

namespace dfly {
using namespace std;

struct IndexKey {
  DbIndex db_indx;
  PrimeKey key;

  IndexKey() {}

  // We define here a weird copy constructor because map uses pair<const PrimeKey,..>
  // and "const" prevents moving IndexKey.
  IndexKey(const IndexKey& o) : db_indx(o.db_indx), key(o.key.AsRef()) {
  }

  IndexKey(IndexKey&&) = default;

  IndexKey(DbIndex i, PrimeKey k) : db_indx(i), key(std::move(k)) {}

  bool operator==(const IndexKey& ik) const {
    return ik.db_indx == db_indx && ik.key == key;
  }

  // IndexKey& operator=(IndexKey&&) {}
  // IndexKey& operator=(const IndexKey&) =delete;
};

struct EntryHash {
  size_t operator()(const IndexKey& ik) const {
    return ik.key.HashCode() ^ (size_t(ik.db_indx) << 16);
  }
};

struct TieredStorage::ActiveIoRequest {
  char* block_ptr;

  // entry -> offset
  absl::flat_hash_map<IndexKey, size_t, EntryHash> entries;

  ActiveIoRequest(size_t sz) {
    DCHECK_EQ(0u, sz % 4096);
    block_ptr = (char*)aligned_malloc(sz, 4096);
    DCHECK_EQ(0, intptr_t(block_ptr) % 4096);
  }

  ~ActiveIoRequest() {
    free(block_ptr);
  }
};

void TieredStorage::FinishIoRequest(int io_res, ActiveIoRequest* req) {
  bool success = true;
  if (io_res < 0) {
    LOG(ERROR) << "Error writing into ssd file: " << util::detail::SafeErrorMessage(-io_res);
    success = false;
  }

  for (const auto& k_v : req->entries) {
    const IndexKey& ikey = k_v.first;
    PrimeTable* pt = db_slice_.GetTables(ikey.db_indx).first;
    PrimeIterator it = pt->Find(ikey.key);
    CHECK(!it.is_done()) << "TBD";
    CHECK(it->second.HasIoPending());

    it->second.SetIoPending(false);
    if (success) {
      size_t item_size = it->second.Size();
      it->second.SetExternal(k_v.second, item_size);
    }
  }

  delete req;
}

TieredStorage::TieredStorage(DbSlice* db_slice) : db_slice_(*db_slice) {
}

TieredStorage::~TieredStorage() {
  for (auto* db : db_arr_)
    delete db;
}

error_code TieredStorage::Open(const string& path) {
  error_code ec = io_mgr_.Open(path);
  if (!ec) {
    if (io_mgr_.Size()) {  // Add initial storage.
      alloc_.AddStorage(0, io_mgr_.Size());
    }
  }
  return ec;
}

void TieredStorage::Shutdown() {
  io_mgr_.Shutdown();
}

void TieredStorage::UnloadItem(DbIndex db_index, PrimeIterator it) {
  CHECK_EQ(OBJ_STRING, it->second.ObjType());

  size_t blob_len = it->second.Size();
  pending_unload_bytes_ += blob_len;
  if (db_index >= db_arr_.size()) {
    db_arr_.resize(db_index + 1);
  }

  if (db_arr_[db_index] == nullptr) {
    db_arr_[db_index] = new PerDb;
  }

  PerDb* db = db_arr_[db_index];
  db->pending_upload[it.bucket_cursor().value()] += blob_len;

  size_t grow_size = 0;
  if (!io_mgr_.grow_pending() && pending_unload_bytes_ > 4080) {
    grow_size = SerializePendingItems();
  }

  if (grow_size == 0 && alloc_.allocated_bytes() > size_t(alloc_.capacity() * 0.85)) {
    grow_size = 1ULL << 28;
  }

  if (grow_size && !io_mgr_.grow_pending()) {
    size_t start = io_mgr_.Size();

    auto cb = [start, grow_size, this](int io_res) {
      if (io_res == 0) {
        alloc_.AddStorage(start, grow_size);
      } else {
        LOG_FIRST_N(ERROR, 10) << "Error enlarging storage " << io_res;
      }
    };

    io_mgr_.GrowAsync(grow_size, move(cb));
  }
}

size_t TieredStorage::SerializePendingItems() {
  DCHECK(!io_mgr_.grow_pending());

  vector<pair<size_t, uint64_t>> sorted_cursors;
  constexpr size_t kArrLen = 64;

  PrimeTable::iterator iters[kArrLen];
  unsigned count = 0;

  auto is_good = [](const PrimeValue& pv) {
    return pv.ObjType() == OBJ_STRING && !pv.IsExternal() && pv.Size() >= 64 && !pv.HasIoPending();
  };

  auto tr_cb = [&](PrimeTable::iterator it) {
    if (is_good(it->second)) {
      CHECK_LT(count, kArrLen);
      iters[count++] = it;
    }
  };

  size_t open_block_size = 0;
  size_t file_offset = 0;
  size_t block_offset = 0;
  ActiveIoRequest* active_req = nullptr;

  for (size_t i = 0; i < db_arr_.size(); ++i) {
    PerDb* db = db_arr_[i];
    if (db == nullptr || db->pending_upload.empty())
      continue;

    sorted_cursors.resize(db->pending_upload.size());
    size_t index = 0;
    for (const auto& k_v : db->pending_upload) {
      sorted_cursors[index++] = {k_v.second, k_v.first};
    }
    sort(sorted_cursors.begin(), sorted_cursors.end(), std::greater<>());
    DbIndex db_ind = i;

    for (const auto& pair : sorted_cursors) {
      uint64_t cursor_val = pair.second;
      PrimeTable::cursor curs(cursor_val);
      db_slice_.GetTables(db_ind).first->Traverse(curs, tr_cb);

      for (unsigned j = 0; j < count; ++j) {
        PrimeIterator it = iters[j];
        size_t item_size = it->second.Size();
        DCHECK_GT(item_size, 0u);

        if (item_size + block_offset > open_block_size) {
          if (open_block_size > 0) {  // need to close
            // save the block asynchronously.
            ++submitted_io_writes_;
            submitted_io_write_size_ += open_block_size;

            string_view sv{active_req->block_ptr, open_block_size};
            auto cb = [this, active_req](int res) { FinishIoRequest(res, active_req); };

            io_mgr_.WriteAsync(file_offset, sv, move(cb));
            open_block_size = 0;
          }

          DCHECK_EQ(0u, open_block_size);
          int64_t res = alloc_.Malloc(item_size);
          if (res < 0) {
            return -res;
          }

          file_offset = res;
          open_block_size = ExternalAllocator::GoodSize(item_size);
          block_offset = 0;
          active_req = new ActiveIoRequest(open_block_size);
        }

        DCHECK_LE(item_size + block_offset, open_block_size);

        it->second.GetString(active_req->block_ptr + block_offset);

        DCHECK(!it->second.HasIoPending());
        it->second.SetIoPending(true);

        IndexKey key(db_ind, it->first.AsRef());
        active_req->entries.try_emplace(move(key), file_offset + block_offset);
        block_offset += item_size;  // saved into opened block.
        pending_unload_bytes_ -= item_size;
      }
      count = 0;
      db->pending_upload.erase(cursor_val);
    }  // sorted_cursors
  }    // db_arr

  if (open_block_size > 0) {
    auto cb = [this, active_req](int res) { FinishIoRequest(res, active_req); };

    string_view sv{active_req->block_ptr, open_block_size};
    io_mgr_.WriteAsync(file_offset, sv, move(cb));
  }

  return 0;
}

}  // namespace dfly
