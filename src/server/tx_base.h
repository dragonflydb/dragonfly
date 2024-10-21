// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <optional>

#include "base/iterator.h"
#include "src/facade/facade_types.h"

namespace dfly {

class EngineShard;
class Transaction;
class Namespace;
class DbSlice;

using DbIndex = uint16_t;
using ShardId = uint16_t;
using LockFp = uint64_t;  // a key fingerprint used by the LockTable.

using facade::ArgSlice;

constexpr DbIndex kInvalidDbId = DbIndex(-1);
constexpr ShardId kInvalidSid = ShardId(-1);
constexpr DbIndex kMaxDbId = 1024;  // Reasonable starting point.

struct KeyLockArgs {
  DbIndex db_index = 0;
  absl::Span<const LockFp> fps;
};

// Describes key indices.
struct KeyIndex {
  KeyIndex(unsigned start = 0, unsigned end = 0, unsigned step = 1,
           std::optional<unsigned> bonus = std::nullopt)
      : start(start), end(end), step(step), bonus(bonus) {
  }

  using iterator_category = std::forward_iterator_tag;
  using value_type = unsigned;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type;
  using reference = value_type;

  unsigned operator*() const;
  KeyIndex& operator++();
  bool operator!=(const KeyIndex& ki) const;

  unsigned NumArgs() const {
    return (end - start) + unsigned(bonus.has_value());
  }

  auto Range() const {
    return base::it::Range(*this, KeyIndex{end, end, step, std::nullopt});
  }

  auto Range(facade::ArgRange args) const {
    return base::it::Transform([args](unsigned idx) { return args[idx]; }, Range());
  }

 public:
  unsigned start, end, step;      // [start, end) with step
  std::optional<unsigned> bonus;  // destination key, for example for commands that end with STORE
};

struct DbContext {
  Namespace* ns = nullptr;
  DbIndex db_index = 0;
  uint64_t time_now_ms = 0;

  // Convenience method.
  DbSlice& GetDbSlice(ShardId shard_id) const;
};

struct OpArgs {
  EngineShard* shard = nullptr;
  const Transaction* tx = nullptr;
  DbContext db_cntx;

  OpArgs() = default;

  OpArgs(EngineShard* s, const Transaction* tx, const DbContext& cntx)
      : shard(s), tx(tx), db_cntx(cntx) {
  }

  // Convenience method.
  DbSlice& GetDbSlice() const;
};

// A strong type for a lock tag. Helps to disambiguate between keys and the parts of the
// keys that are used for locking.
class LockTag {
  std::string_view str_;

 public:
  using is_stackonly = void;  // marks that this object does not use heap.

  LockTag() = default;
  explicit LockTag(std::string_view key);

  explicit operator std::string_view() const {
    return str_;
  }

  LockFp Fingerprint() const;

  // To make it hashable.
  template <typename H> friend H AbslHashValue(H h, const LockTag& tag) {
    return H::combine(std::move(h), tag.str_);
  }

  bool operator==(const LockTag& o) const {
    return str_ == o.str_;
  }
};

// Checks whether the touched key is valid for a blocking transaction watching it.
using KeyReadyChecker =
    std::function<bool(EngineShard*, const DbContext& context, Transaction* tx, std::string_view)>;

// References arguments in another array.
using IndexSlice = std::pair<uint32_t, uint32_t>;  // [begin, end)

// ShardArgs - hold a span to full arguments and a span of sub-ranges
// referencing those arguments.
class ShardArgs {
  using ArgsIndexPair = std::pair<facade::CmdArgList, absl::Span<const IndexSlice>>;
  ArgsIndexPair slice_;

 public:
  class Iterator {
    facade::CmdArgList arglist_;
    absl::Span<const IndexSlice>::const_iterator index_it_;
    uint32_t delta_ = 0;

   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::string_view;
    using difference_type = ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    // First version, corresponds to spans over arguments.
    Iterator(facade::CmdArgList list, absl::Span<const IndexSlice>::const_iterator it)
        : arglist_(list), index_it_(it) {
    }

    bool operator==(const Iterator& o) const {
      return index_it_ == o.index_it_ && delta_ == o.delta_ && arglist_.data() == o.arglist_.data();
    }

    bool operator!=(const Iterator& o) const {
      return !(*this == o);
    }

    std::string_view operator*() const {
      return facade::ArgS(arglist_, index());
    }

    Iterator& operator++() {
      ++delta_;
      if (index() >= index_it_->second) {
        ++index_it_;
        ++delta_ = 0;
      }
      return *this;
    }

    Iterator operator++(int) {
      Iterator copy = *this;
      operator++();
      return copy;
    }

    size_t index() const {
      return index_it_->first + delta_;
    }
  };

  using const_iterator = Iterator;

  ShardArgs(facade::CmdArgList fa, absl::Span<const IndexSlice> s) : slice_(ArgsIndexPair(fa, s)) {
  }

  ShardArgs() : slice_(ArgsIndexPair{}) {
  }

  size_t Size() const;

  Iterator cbegin() const {
    return Iterator{slice_.first, slice_.second.begin()};
  }

  Iterator cend() const {
    return Iterator{slice_.first, slice_.second.end()};
  }

  Iterator begin() const {
    return cbegin();
  }

  Iterator end() const {
    return cend();
  }

  bool Empty() const {
    return slice_.second.empty();
  }

  std::string_view Front() const {
    return *cbegin();
  }
};

// Record non auto journal command with own txid and dbid.
void RecordJournal(const OpArgs& op_args, std::string_view cmd, const ShardArgs& args,
                   uint32_t shard_cnt = 1);
void RecordJournal(const OpArgs& op_args, std::string_view cmd, ArgSlice args,
                   uint32_t shard_cnt = 1);

// Record expiry in journal with independent transaction. Must be called from shard thread holding
// key.
void RecordExpiry(DbIndex dbid, std::string_view key);

// Trigger journal write to sink, no journal record will be added to journal.
// Must be called from shard thread of journal to sink.
void TriggerJournalWriteToSink();

// std::ostream& operator<<(std::ostream& os, ArgSlice list);

}  // namespace dfly
