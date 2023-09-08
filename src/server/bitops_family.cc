// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bitops_family.h"

#include <bitset>

extern "C" {
#include "redis/object.h"
}

#include "base/logging.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "util/varz.h"

namespace dfly {
using namespace facade;

namespace {

using ShardStringResults = std::vector<OpResult<std::string>>;
const int32_t OFFSET_FACTOR = 8;  // number of bits in byte
const char* OR_OP_NAME = "OR";
const char* XOR_OP_NAME = "XOR";
const char* AND_OP_NAME = "AND";
const char* NOT_OP_NAME = "NOT";

using BitsStrVec = std::vector<std::string>;

// The following is the list of the functions that would handle the
// commands that handle the bit operations
void BitPos(CmdArgList args, ConnectionContext* cntx);
void BitCount(CmdArgList args, ConnectionContext* cntx);
void BitField(CmdArgList args, ConnectionContext* cntx);
void BitFieldRo(CmdArgList args, ConnectionContext* cntx);
void BitOp(CmdArgList args, ConnectionContext* cntx);
void GetBit(CmdArgList args, ConnectionContext* cntx);
void SetBit(CmdArgList args, ConnectionContext* cntx);

OpResult<std::string> ReadValue(const DbContext& context, std::string_view key, EngineShard* shard);
OpResult<bool> ReadValueBitsetAt(const OpArgs& op_args, std::string_view key, uint32_t offset);
OpResult<std::size_t> CountBitsForValue(const OpArgs& op_args, std::string_view key, int64_t start,
                                        int64_t end, bool bit_value);
OpResult<int64_t> FindFirstBitWithValue(const OpArgs& op_args, std::string_view key, bool value,
                                        int64_t start, int64_t end, bool as_bit);
std::string GetString(const PrimeValue& pv, EngineShard* shard);
bool SetBitValue(uint32_t offset, bool bit_value, std::string* entry);
std::size_t CountBitSetByByteIndices(std::string_view at, std::size_t start, std::size_t end);
std::size_t CountBitSet(std::string_view str, int64_t start, int64_t end, bool bits);
std::size_t CountBitSetByBitIndices(std::string_view at, std::size_t start, std::size_t end);
OpResult<std::string> RunBitOpOnShard(std::string_view op, const OpArgs& op_args, ArgSlice keys);
std::string RunBitOperationOnValues(std::string_view op, const BitsStrVec& values);

// ------------------------------------------------------------------------- //

// Converts `args[i] to uppercase, then sets `*as_bit` to true if `args[i]` equals "BIT", false if
// `args[i]` equals "BYTE", or returns false if `args[i]` has some other invalid value.
bool ToUpperAndGetAsBit(CmdArgList args, size_t i, bool* as_bit) {
  CHECK_NOTNULL(as_bit);
  ToUpper(&args[i]);
  std::string_view arg = ArgS(args, i);
  if (arg == "BIT") {
    *as_bit = true;
    return true;
  } else if (arg == "BYTE") {
    *as_bit = false;
    return true;
  } else {
    return false;
  }
}

// This function can be used for any case where we allowing out of bound
// access where the default in this case would be 0 -such as bitop
uint8_t GetByteAt(std::string_view s, std::size_t at) {
  return at >= s.size() ? 0 : s[at];
}

// For XOR, OR, AND operations on a collection of bytes
template <typename BitOp, typename SkipOp>
std::string BitOpString(BitOp operation_f, SkipOp skip_f, const BitsStrVec& values,
                        std::string new_value) {
  // at this point, values are not empty
  std::size_t max_size = new_value.size();

  if (values.size() > 1) {
    for (std::size_t i = 0; i < max_size; i++) {
      std::uint8_t new_entry = operation_f(GetByteAt(values[0], i), GetByteAt(values[1], i));
      for (std::size_t j = 2; j < values.size(); ++j) {
        new_entry = operation_f(new_entry, GetByteAt(values[j], i));
        if (skip_f(new_entry)) {
          break;
        }
      }
      new_value[i] = new_entry;
    }
    return new_value;
  } else {
    return values[0];
  }
}

// Helper functions to support operations
// so we would not need to check which
// operations to run in the look (unlike
// https://github.com/redis/redis/blob/c2b0c13d5c0fab49131f6f5e844f80bfa43f6219/src/bitops.c#L607)
constexpr bool SkipAnd(uint8_t byte) {
  return byte == 0x0;
}

constexpr bool SkipOr(uint8_t byte) {
  return byte == 0xff;
}

constexpr bool SkipXor(uint8_t) {
  return false;
}

constexpr uint8_t AndOp(uint8_t left, uint8_t right) {
  return left & right;
}

constexpr uint8_t OrOp(uint8_t left, uint8_t right) {
  return left | right;
}

constexpr uint8_t XorOp(uint8_t left, uint8_t right) {
  return left ^ right;
}

std::string BitOpNotString(std::string from) {
  std::transform(from.begin(), from.end(), from.begin(), [](auto c) { return ~c; });
  return from;
}

//  Bits manipulation functions
constexpr int32_t GetBitIndex(uint32_t offset) noexcept {
  return offset % OFFSET_FACTOR;
}

constexpr int32_t GetNormalizedBitIndex(uint32_t offset) noexcept {
  return (OFFSET_FACTOR - 1) - GetBitIndex(offset);
}

constexpr int32_t GetByteIndex(uint32_t offset) noexcept {
  return offset / OFFSET_FACTOR;
}

uint8_t GetByteValue(std::string_view str, uint32_t offset) {
  return static_cast<uint8_t>(str[GetByteIndex(offset)]);
}

constexpr bool CheckBitStatus(uint8_t byte, uint32_t offset) {
  return byte & (0x1 << offset);
}

constexpr std::uint8_t CountBitsRange(std::uint8_t byte, std::uint8_t from, uint8_t to) {
  int count = 0;
  for (int i = from; i < to; i++) {
    count += CheckBitStatus(byte, GetNormalizedBitIndex(i));
  }
  return count;
}

// Count the number of bits that are on, on bytes boundaries: i.e. Start and end are the indices for
// bytes locations inside str CountBitSetByByteIndices
std::size_t CountBitSetByByteIndices(std::string_view at, std::size_t start, std::size_t end) {
  if (start >= end) {
    return 0;
  }
  end = std::min(end, at.size());  // don't overflow
  std::uint32_t count =
      std::accumulate(std::next(at.begin(), start), std::next(at.begin(), end), 0,
                      [](auto counter, uint8_t ch) { return counter + absl::popcount(ch); });
  return count;
}

// Count the number of bits that are on, on bits boundaries: i.e. Start and end are the indices for
// bits locations inside str
std::size_t CountBitSetByBitIndices(std::string_view at, std::size_t start, std::size_t end) {
  auto first_byte_index = GetByteIndex(start);
  auto last_byte_index = GetByteIndex(end);
  if (start % OFFSET_FACTOR == 0 && end % OFFSET_FACTOR == 0) {
    return CountBitSetByByteIndices(at, first_byte_index, last_byte_index);
  }
  const auto last_bit_first_byte =
      first_byte_index != last_byte_index ? OFFSET_FACTOR : GetBitIndex(end);
  const auto first_byte = GetByteValue(at, start);
  std::uint32_t count = CountBitsRange(first_byte, GetBitIndex(start), last_bit_first_byte);
  if (first_byte_index < last_byte_index) {
    first_byte_index++;
    const auto last_byte = GetByteValue(at, end);
    count += CountBitsRange(last_byte, 0, GetBitIndex(end));
    count += CountBitSetByByteIndices(at, first_byte_index, last_byte_index);
  }
  return count;
}

// Returns normalized offset of `offset` in `size`. `size` is assumed to be a size of a container,
// and as such the returned value is always in the range [0, size]. If `offset` is negative, it is
// treated as an offset from the end and is normalized to be a positive offset from the start.
int64_t NormalizedOffset(int64_t size, int64_t offset) {
  if (offset < 0) {
    offset = size + offset;
  }
  return std::min(std::max(offset, int64_t{0}), size);
}

// General purpose function to count the number of bits that are on.
// The parameters for start, end and bits are defaulted to the start of the string,
// end of the string and bits are false.
// Note that when bits is false, it means that we are looking on byte boundaries.
std::size_t CountBitSet(std::string_view str, int64_t start, int64_t end, bool bits) {
  const int64_t size = bits ? str.size() * OFFSET_FACTOR : str.size();

  if (start > 0 && end > 0 && end < start) {
    return 0;  // for illegal range with positive we just return 0
  }

  if (start < 0 && end < 0 && start > end) {
    return 0;  // for illegal range with negative we just return 0
  }

  start = NormalizedOffset(size, start);
  if (end > 0 && end < start) {
    return 0;
  }
  end = NormalizedOffset(size, end);
  if (start > end) {
    std::swap(start, end);  // we're going backward
  }
  if (end > size) {
    end = size;  // don't overflow
  }
  ++end;
  return bits ? CountBitSetByBitIndices(str, start, end)
              : CountBitSetByByteIndices(str, start, end);
}

// return true if bit is on
bool GetBitValue(const std::string& entry, uint32_t offset) {
  const auto byte_val{GetByteValue(entry, offset)};
  const auto index{GetNormalizedBitIndex(offset)};
  return CheckBitStatus(byte_val, index);
}

bool GetBitValueSafe(const std::string& entry, uint32_t offset) {
  return ((entry.size() * OFFSET_FACTOR) > offset) ? GetBitValue(entry, offset) : false;
}

constexpr uint8_t TurnBitOn(uint8_t on, uint32_t offset) {
  return on |= 1 << offset;
}

constexpr uint8_t TunBitOff(uint8_t on, uint32_t offset) {
  return on &= ~(1 << offset);
}

bool SetBitValue(uint32_t offset, bool bit_value, std::string* entry) {
  // we need to return the old value after setting the value for offset
  const auto old_value{GetBitValue(*entry, offset)};  // save this as the return value
  auto byte{GetByteValue(*entry, offset)};
  std::bitset<8> bits{byte};
  const auto bit_index{GetNormalizedBitIndex(offset)};
  byte = bit_value ? TurnBitOn(byte, bit_index) : TunBitOff(byte, bit_index);
  (*entry)[GetByteIndex(offset)] = byte;
  return old_value;
}

// ------------------------------------------------------------------------- //

class ElementAccess {
  bool added_ = false;
  PrimeIterator element_iter_;
  std::string_view key_;
  DbContext context_;
  EngineShard* shard_ = nullptr;

 public:
  ElementAccess(std::string_view key, const OpArgs& args) : key_{key}, context_{args.db_cntx} {
  }

  OpStatus Find(EngineShard* shard);

  bool IsNewEntry() const {
    CHECK_NOTNULL(shard_);
    return added_;
  }

  constexpr DbIndex Index() const {
    return context_.db_index;
  }

  std::string Value() const;

  void Commit(std::string_view new_value) const;
};

OpStatus ElementAccess::Find(EngineShard* shard) {
  try {
    std::pair<PrimeIterator, bool> add_res = shard->db_slice().AddOrFind(context_, key_);
    if (!add_res.second) {
      if (add_res.first->second.ObjType() != OBJ_STRING) {
        return OpStatus::WRONG_TYPE;
      }
    }
    element_iter_ = add_res.first;
    added_ = add_res.second;
    shard_ = shard;
    return OpStatus::OK;
  } catch (const std::bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }
}

std::string ElementAccess::Value() const {
  CHECK_NOTNULL(shard_);
  if (!added_) {  // Exist entry - return it
    return GetString(element_iter_->second, shard_);
  } else {  // we only have reference to the new entry but no value
    return std::string{};
  }
}

void ElementAccess::Commit(std::string_view new_value) const {
  if (shard_) {
    auto& db_slice = shard_->db_slice();
    db_slice.PreUpdate(Index(), element_iter_);
    element_iter_->second.SetString(new_value);
    db_slice.PostUpdate(Index(), element_iter_, key_, !added_);
  }
}

// =============================================
// Set a new value to a given bit

OpResult<bool> BitNewValue(const OpArgs& args, std::string_view key, uint32_t offset,
                           bool bit_value) {
  EngineShard* shard = args.shard;
  ElementAccess element_access{key, args};
  auto& db_slice = shard->db_slice();
  DCHECK(db_slice.IsDbValid(element_access.Index()));
  bool old_value = false;

  auto find_res = element_access.Find(shard);

  if (find_res != OpStatus::OK) {
    return find_res;
  }

  if (element_access.IsNewEntry()) {
    std::string new_entry(GetByteIndex(offset) + 1, 0);
    old_value = SetBitValue(offset, bit_value, &new_entry);
    element_access.Commit(new_entry);
  } else {
    bool reset = false;
    std::string existing_entry{element_access.Value()};
    if ((existing_entry.size() * OFFSET_FACTOR) <= offset) {
      existing_entry.resize(GetByteIndex(offset) + 1, 0);
      reset = true;
    }
    old_value = SetBitValue(offset, bit_value, &existing_entry);
    if (reset || old_value != bit_value) {  // we made a "real" change to the entry, save it
      element_access.Commit(existing_entry);
    }
  }
  return old_value;
}

// ---------------------------------------------------------

std::string RunBitOperationOnValues(std::string_view op, const BitsStrVec& values) {
  // This function accept an operation (either OR, XOR, NOT or OR), and run bit operation
  // on all the values we got from the database. Note that in case that one of the values
  // is shorter than the other it would return a 0 and the operation would continue
  // until we ran the longest value. The function will return the resulting new value
  std::size_t max_len = 0;
  std::size_t max_len_index = 0;

  const auto BitOperation = [&]() {
    if (op == OR_OP_NAME) {
      std::string default_str{values[max_len_index]};
      return BitOpString(OrOp, SkipOr, std::move(values), std::move(default_str));
    } else if (op == XOR_OP_NAME) {
      return BitOpString(XorOp, SkipXor, std::move(values), std::string(max_len, 0));
    } else if (op == AND_OP_NAME) {
      return BitOpString(AndOp, SkipAnd, std::move(values), std::string(max_len, 0));
    } else if (op == NOT_OP_NAME) {
      return BitOpNotString(values[0]);
    } else {
      LOG(FATAL) << "Operation not supported '" << op << "'";
      return std::string{};  // otherwise we will have warning of not returning value
    }
  };

  if (values.empty()) {  // this is ok in case we don't have the src keys
    return std::string{};
  }
  // The new result is the max length input
  max_len = values[0].size();
  for (std::size_t i = 1; i < values.size(); ++i) {
    if (values[i].size() > max_len) {
      max_len = values[i].size();
      max_len_index = i;
    }
  }
  return BitOperation();
}

OpResult<std::string> CombineResultOp(ShardStringResults result, std::string_view op) {
  // take valid result for each shard
  BitsStrVec values;
  for (auto&& res : result) {
    if (res) {
      auto v = res.value();
      values.emplace_back(std::move(v));
    } else {
      if (res.status() != OpStatus::KEY_NOTFOUND) {
        // something went wrong, just bale out
        return res;
      }
    }
  }

  // and combine them to single result
  return RunBitOperationOnValues(op, values);
}

// For bitop not - we cannot accumulate
OpResult<std::string> RunBitOpNot(const OpArgs& op_args, ArgSlice keys) {
  DCHECK(keys.size() == 1);

  EngineShard* es = op_args.shard;
  // if we found the value, just return, if not found then skip, otherwise report an error
  auto key = keys.front();
  OpResult<PrimeIterator> find_res = es->db_slice().Find(op_args.db_cntx, key, OBJ_STRING);
  if (find_res) {
    return GetString(find_res.value()->second, es);
  } else {
    return find_res.status();
  }
}

// Read only operation where we are running the bit operation on all the
// values that belong to same shard.
OpResult<std::string> RunBitOpOnShard(std::string_view op, const OpArgs& op_args, ArgSlice keys) {
  DCHECK(!keys.empty());
  if (op == NOT_OP_NAME) {
    return RunBitOpNot(op_args, keys);
  }
  EngineShard* es = op_args.shard;
  BitsStrVec values;
  values.reserve(keys.size());

  // collect all the value for this shard
  for (auto& key : keys) {
    OpResult<PrimeIterator> find_res = es->db_slice().Find(op_args.db_cntx, key, OBJ_STRING);
    if (find_res) {
      values.emplace_back(GetString(find_res.value()->second, es));
    } else {
      if (find_res.status() == OpStatus::KEY_NOTFOUND) {
        continue;  // this is allowed, just return empty string per Redis
      } else {
        return find_res.status();
      }
    }
  }
  // Run the operation on all the values that we found
  std::string op_result = RunBitOperationOnValues(op, values);
  return op_result;
}

template <typename T> void HandleOpValueResult(const OpResult<T>& result, ConnectionContext* cntx) {
  static_assert(std::is_integral<T>::value,
                "we are only handling types that are integral types in the return types from "
                "here");
  if (result) {
    (*cntx)->SendLong(result.value());
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        (*cntx)->SendError(kWrongTypeErr);
        break;
      case OpStatus::OUT_OF_MEMORY:
        (*cntx)->SendError(kOutOfMemory);
        break;
      default:
        (*cntx)->SendLong(0);  // in case we don't have the value we should just send 0
        break;
    }
  }
}

// ------------------------------------------------------------------------- //
//  Impl for the command functions
void BitPos(CmdArgList args, ConnectionContext* cntx) {
  // Support for the command BITPOS
  // See details at https://redis.io/commands/bitpos/

  if (args.size() < 1 || args.size() > 5) {
    return (*cntx)->SendError(kSyntaxErr);
  }

  std::string_view key = ArgS(args, 0);

  int32_t value{0};
  int64_t start = 0;
  int64_t end = std::numeric_limits<int64_t>::max();
  bool as_bit = false;

  if (!absl::SimpleAtoi(ArgS(args, 1), &value)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  if (args.size() >= 3) {
    if (!absl::SimpleAtoi(ArgS(args, 2), &start)) {
      return (*cntx)->SendError(kInvalidIntErr);
    }
    if (args.size() >= 4) {
      if (!absl::SimpleAtoi(ArgS(args, 3), &end)) {
        return (*cntx)->SendError(kInvalidIntErr);
      }

      if (args.size() >= 5) {
        if (!ToUpperAndGetAsBit(args, 4, &as_bit)) {
          return (*cntx)->SendError(kSyntaxErr);
        }
      }
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return FindFirstBitWithValue(t->GetOpArgs(shard), key, value, start, end, as_bit);
  };
  Transaction* trans = cntx->transaction;
  OpResult<int64_t> res = trans->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cntx);
}

void BitCount(CmdArgList args, ConnectionContext* cntx) {
  // Support for the command BITCOUNT
  // See details at https://redis.io/commands/bitcount/
  // Please note that if the key don't exists, it would return 0

  if (args.size() == 2 || args.size() > 4) {
    return (*cntx)->SendError(kSyntaxErr);
  }
  // return (*cntx)->SendLong(0);
  std::string_view key = ArgS(args, 0);
  bool as_bit = false;
  int64_t start = 0;
  int64_t end = std::numeric_limits<int64_t>::max();
  if (args.size() >= 3) {
    if (absl::SimpleAtoi(ArgS(args, 1), &start) == 0 ||
        absl::SimpleAtoi(ArgS(args, 2), &end) == 0) {
      return (*cntx)->SendError(kInvalidIntErr);
    }
    if (args.size() == 4) {
      if (!ToUpperAndGetAsBit(args, 3, &as_bit)) {
        return (*cntx)->SendError(kSyntaxErr);
      }
    }
  }
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return CountBitsForValue(t->GetOpArgs(shard), key, start, end, as_bit);
  };
  Transaction* trans = cntx->transaction;
  OpResult<std::size_t> res = trans->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cntx);
}

void BitField(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void BitFieldRo(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void BitOp(CmdArgList args, ConnectionContext* cntx) {
  static const std::array<std::string_view, 4> BITOP_OP_NAMES{OR_OP_NAME, XOR_OP_NAME, AND_OP_NAME,
                                                              NOT_OP_NAME};
  ToUpper(&args[0]);
  std::string_view op = ArgS(args, 0);
  std::string_view dest_key = ArgS(args, 1);
  bool illegal = std::none_of(BITOP_OP_NAMES.begin(), BITOP_OP_NAMES.end(),
                              [&op](auto val) { return op == val; });

  if (illegal || (op == NOT_OP_NAME && args.size() > 3)) {
    return (*cntx)->SendError(kSyntaxErr);  // too many arguments
  }

  // Multi shard access - read only
  ShardStringResults result_set(shard_set->size(), OpStatus::KEY_NOTFOUND);
  ShardId dest_shard = Shard(dest_key, result_set.size());

  auto shard_bitop = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->GetShardArgs(shard->shard_id());
    DCHECK(!largs.empty());

    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(largs.front(), dest_key);
      largs.remove_prefix(1);
      if (largs.empty()) {  // no more keys to check
        return OpStatus::OK;
      }
    }
    OpArgs op_args = t->GetOpArgs(shard);
    result_set[shard->shard_id()] = RunBitOpOnShard(op, op_args, largs);
    return OpStatus::OK;
  };

  cntx->transaction->Schedule();
  cntx->transaction->Execute(std::move(shard_bitop), false);  // we still have more work to do
  // All result from each shard
  const auto joined_results = CombineResultOp(result_set, op);
  // Second phase - save to targe key if successful
  if (!joined_results) {
    cntx->transaction->Conclude();
    (*cntx)->SendError(joined_results.status());
    return;
  } else {
    auto op_result = joined_results.value();
    auto store_cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == dest_shard) {
        ElementAccess operation{dest_key, t->GetOpArgs(shard)};
        auto find_res = operation.Find(shard);

        if (find_res == OpStatus::OK) {
          operation.Commit(op_result);
        }

        if (shard->journal()) {
          RecordJournal(t->GetOpArgs(shard), "SET", {dest_key, op_result});
        }
      }
      return OpStatus::OK;
    };

    cntx->transaction->Execute(std::move(store_cb), true);
    (*cntx)->SendLong(op_result.size());
  }
}

void GetBit(CmdArgList args, ConnectionContext* cntx) {
  // Support for the command "GETBIT key offset"
  // see https://redis.io/commands/getbit/

  uint32_t offset{0};
  std::string_view key = ArgS(args, 0);

  if (!absl::SimpleAtoi(ArgS(args, 1), &offset)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return ReadValueBitsetAt(t->GetOpArgs(shard), key, offset);
  };
  Transaction* trans = cntx->transaction;
  OpResult<bool> res = trans->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cntx);
}

void SetBit(CmdArgList args, ConnectionContext* cntx) {
  // Support for the command "SETBIT key offset new_value"
  // see https://redis.io/commands/setbit/

  uint32_t offset{0};
  int32_t value{0};
  std::string_view key = ArgS(args, 0);

  if (!absl::SimpleAtoi(ArgS(args, 1), &offset) || !absl::SimpleAtoi(ArgS(args, 2), &value)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return BitNewValue(t->GetOpArgs(shard), key, offset, value != 0);
  };

  Transaction* trans = cntx->transaction;
  OpResult<bool> res = trans->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cntx);
}

// ------------------------------------------------------------------------- //
// This are the "callbacks" that we're using from above
std::string GetString(const PrimeValue& pv, EngineShard* shard) {
  std::string res;
  if (pv.IsExternal()) {
    auto* tiered = shard->tiered_storage();
    auto [offset, size] = pv.GetExternalSlice();
    res.resize(size);

    std::error_code ec = tiered->Read(offset, size, res.data());
    CHECK(!ec) << "TBD: " << ec;
  } else {
    pv.GetString(&res);
  }

  return res;
}

OpResult<bool> ReadValueBitsetAt(const OpArgs& op_args, std::string_view key, uint32_t offset) {
  OpResult<std::string> result = ReadValue(op_args.db_cntx, key, op_args.shard);
  if (result) {
    return GetBitValueSafe(result.value(), offset);
  } else {
    return result.status();
  }
}

OpResult<std::string> ReadValue(const DbContext& context, std::string_view key,
                                EngineShard* shard) {
  OpResult<PrimeIterator> it_res = shard->db_slice().Find(context, key, OBJ_STRING);
  if (!it_res.ok()) {
    return it_res.status();
  }

  const PrimeValue& pv = it_res.value()->second;

  return GetString(pv, shard);
}

OpResult<std::size_t> CountBitsForValue(const OpArgs& op_args, std::string_view key, int64_t start,
                                        int64_t end, bool bit_value) {
  OpResult<std::string> result = ReadValue(op_args.db_cntx, key, op_args.shard);

  if (result) {  // if this is not found, just return 0 - per Redis
    if (result.value().empty()) {
      return 0;
    }
    if (end == std::numeric_limits<int64_t>::max()) {
      end = result.value().size();
    }
    return CountBitSet(result.value(), start, end, bit_value);
  } else {
    return result.status();
  }
}

// Returns the bit position (where MSB is 0, LSB is 7) of the leftmost bit that
// equals `value` in `byte`. Returns 8 if not found.
std::size_t GetFirstBitWithValueInByte(uint8_t byte, bool value) {
  if (value) {
    return absl::countl_zero(byte);
  } else {
    return absl::countl_one(byte);
  }
}

int64_t FindFirstBitWithValueAsBit(std::string_view value_str, bool bit_value, int64_t start,
                                   int64_t end) {
  for (int64_t i = start; i <= end; ++i) {
    if (static_cast<size_t>(GetByteIndex(i)) >= value_str.size()) {
      break;
    }
    const uint8_t current_byte = GetByteValue(value_str, i);
    bool current_bit = CheckBitStatus(current_byte, GetNormalizedBitIndex(i));
    if (current_bit != bit_value) {
      continue;
    }

    return i;
  }

  return -1;
}

int64_t FindFirstBitWithValueAsByte(std::string_view value_str, bool bit_value, int64_t start,
                                    int64_t end) {
  for (int64_t i = start; i <= end; ++i) {
    if (static_cast<size_t>(i) >= value_str.size()) {
      break;
    }
    const uint8_t current_byte = value_str[i];
    const uint8_t kNotFoundByte = bit_value ? 0 : std::numeric_limits<uint8_t>::max();
    if (current_byte == kNotFoundByte) {
      continue;
    }

    return i * OFFSET_FACTOR + GetFirstBitWithValueInByte(current_byte, bit_value);
  }

  return -1;
}

OpResult<int64_t> FindFirstBitWithValue(const OpArgs& op_args, std::string_view key, bool bit_value,
                                        int64_t start, int64_t end, bool as_bit) {
  OpResult<std::string> value = ReadValue(op_args.db_cntx, key, op_args.shard);

  std::string_view value_str;
  if (value) {  // non-existent keys are treated as empty strings, per Redis
    value_str = value.value();
  }

  int64_t size = value_str.size();
  if (as_bit) {
    size *= OFFSET_FACTOR;
  }

  int64_t normalized_start = NormalizedOffset(size, start);
  int64_t normalized_end = NormalizedOffset(size, end);
  if (normalized_start > normalized_end) {
    return -1;  // Return -1 for negative ranges, per Redis
  }

  int64_t position;
  if (as_bit) {
    position = FindFirstBitWithValueAsBit(value_str, bit_value, normalized_start, normalized_end);
  } else {
    position = FindFirstBitWithValueAsByte(value_str, bit_value, normalized_start, normalized_end);
  }

  if (position == -1 && !bit_value && static_cast<size_t>(start) < value_str.size() &&
      end == std::numeric_limits<int64_t>::max()) {
    // Returning bit-size of the value, compatible with Redis (but is a weird API).
    return value_str.size() * OFFSET_FACTOR;
  } else {
    return position;
  }
}

}  // namespace

namespace acl {
constexpr uint32_t kBitPos = READ | BITMAP | SLOW;
constexpr uint32_t kBitCount = READ | BITMAP | SLOW;
constexpr uint32_t kBitField = WRITE | BITMAP | SLOW;
constexpr uint32_t kBitFieldRo = READ | BITMAP | FAST;
constexpr uint32_t kBitOp = WRITE | BITMAP | SLOW;
constexpr uint32_t kGetBit = READ | BITMAP | FAST;
constexpr uint32_t kSetBit = WRITE | BITMAP | SLOW;
}  // namespace acl

void BitOpsFamily::Register(CommandRegistry* registry, acl::CommandTableBuilder builder) {
  using CI = CommandId;

  *registry
      << CI{"BITPOS", CO::CommandOpt::READONLY, -3, 1, 1, 1, acl::kBitPos}.SetHandler(&BitPos)
      << CI{"BITCOUNT", CO::READONLY, -2, 1, 1, 1, acl::kBitCount}.SetHandler(&BitCount)
      << CI{"BITFIELD", CO::WRITE, -3, 1, 1, 1, acl::kBitField}.SetHandler(&BitField)
      << CI{"BITFIELD_RO", CO::READONLY, -5, 1, 1, 1, acl::kBitFieldRo}.SetHandler(&BitFieldRo)
      << CI{"BITOP", CO::WRITE | CO::NO_AUTOJOURNAL, -4, 2, -1, 1, acl::kBitOp}.SetHandler(&BitOp)
      << CI{"GETBIT", CO::READONLY | CO::FAST, 3, 1, 1, 1, acl::kGetBit}.SetHandler(&GetBit)
      << CI{"SETBIT", CO::WRITE | CO::DENYOOM, 4, 1, 1, 1, acl::kSetBit}.SetHandler(&SetBit);

  builder | "BITPOS" | "BITCOUNT" | "BITFIELD" | "BITFIELD_RO" | "BITOP" | "GETBIT" | "SETBIT";
}

}  // namespace dfly
