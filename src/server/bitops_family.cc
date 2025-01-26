// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bitops_family.h"

#include <bitset>

#include "absl/strings/match.h"
#include "base/expected.hpp"
#include "base/logging.h"
#include "facade/cmd_arg_parser.h"
#include "facade/op_status.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "src/core/overloaded.h"
#include "util/varz.h"

namespace dfly {
using namespace facade;
using namespace std;

namespace {

using ShardStringResults = vector<OpResult<string>>;
const int32_t OFFSET_FACTOR = 8;  // number of bits in byte
const char* OR_OP_NAME = "OR";
const char* XOR_OP_NAME = "XOR";
const char* AND_OP_NAME = "AND";
const char* NOT_OP_NAME = "NOT";

using BitsStrVec = vector<string>;

// The following is the list of the functions that would handle the
// commands that handle the bit operations
void BitPos(CmdArgList args, const CommandContext& cmd_cntx);
void BitCount(CmdArgList args, const CommandContext& cmd_cntx);
void BitField(CmdArgList args, const CommandContext& cmd_cntx);
void BitFieldRo(CmdArgList args, const CommandContext& cmd_cntx);
void BitOp(CmdArgList args, const CommandContext& cmd_cntx);
void GetBit(CmdArgList args, const CommandContext& cmd_cntx);
void SetBit(CmdArgList args, const CommandContext& cmd_cntx);

OpResult<string> ReadValue(const DbContext& context, string_view key, EngineShard* shard);
OpResult<bool> ReadValueBitsetAt(const OpArgs& op_args, string_view key, uint32_t offset);
OpResult<std::size_t> CountBitsForValue(const OpArgs& op_args, string_view key, int64_t start,
                                        int64_t end, bool bit_value);
OpResult<int64_t> FindFirstBitWithValue(const OpArgs& op_args, string_view key, bool value,
                                        int64_t start, int64_t end, bool as_bit);
string GetString(const PrimeValue& pv);
bool SetBitValue(uint32_t offset, bool bit_value, string* entry);
std::size_t CountBitSetByByteIndices(string_view at, std::size_t start, std::size_t end);
std::size_t CountBitSet(string_view str, int64_t start, int64_t end, bool bits);
std::size_t CountBitSetByBitIndices(string_view at, std::size_t start, std::size_t end);
string RunBitOperationOnValues(string_view op, const BitsStrVec& values);

// ------------------------------------------------------------------------- //

// This function can be used for any case where we allowing out of bound
// access where the default in this case would be 0 -such as bitop
uint8_t GetByteAt(string_view s, std::size_t at) {
  return at >= s.size() ? 0 : s[at];
}

// For XOR, OR, AND operations on a collection of bytes
template <typename BitOp, typename SkipOp>
string BitOpString(BitOp operation_f, SkipOp skip_f, const BitsStrVec& values, string new_value) {
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

string BitOpNotString(string from) {
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

uint8_t GetByteValue(string_view str, uint32_t offset) {
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
std::size_t CountBitSetByByteIndices(string_view at, std::size_t start, std::size_t end) {
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
std::size_t CountBitSetByBitIndices(string_view at, std::size_t start, std::size_t end) {
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
std::size_t CountBitSet(string_view str, int64_t start, int64_t end, bool bits) {
  const int64_t strlen = bits ? str.size() * OFFSET_FACTOR : str.size();

  if (start < 0)
    start = strlen + start;
  if (end < 0)
    end = strlen + end;

  end = min(end, strlen);

  if (strlen == 0 || start > end)
    return 0;

  start = max(start, int64_t(0));
  end = max(end, int64_t(0));

  ++end;
  return bits ? CountBitSetByBitIndices(str, start, end)
              : CountBitSetByByteIndices(str, start, end);
}

// return true if bit is on
bool GetBitValue(const string& entry, uint32_t offset) {
  const auto byte_val{GetByteValue(entry, offset)};
  const auto index{GetNormalizedBitIndex(offset)};
  return CheckBitStatus(byte_val, index);
}

bool GetBitValueSafe(const string& entry, uint32_t offset) {
  return ((entry.size() * OFFSET_FACTOR) > offset) ? GetBitValue(entry, offset) : false;
}

constexpr uint8_t TurnBitOn(uint8_t on, uint32_t offset) {
  return on |= 1 << offset;
}

constexpr uint8_t TurnBitOff(uint8_t on, uint32_t offset) {
  return on &= ~(1 << offset);
}

bool SetBitValue(uint32_t offset, bool bit_value, string* entry) {
  // we need to return the old value after setting the value for offset
  const auto old_value{GetBitValue(*entry, offset)};  // save this as the return value
  auto byte{GetByteValue(*entry, offset)};
  const auto bit_index{GetNormalizedBitIndex(offset)};
  byte = bit_value ? TurnBitOn(byte, bit_index) : TurnBitOff(byte, bit_index);
  (*entry)[GetByteIndex(offset)] = byte;
  return old_value;
}

// ------------------------------------------------------------------------- //

class ElementAccess {
  bool added_ = false;
  DbSlice::Iterator element_iter_;
  string_view key_;
  DbContext context_;
  EngineShard* shard_ = nullptr;
  mutable DbSlice::AutoUpdater post_updater_;

  void SetFields(EngineShard* shard, DbSlice::AddOrFindResult res);

 public:
  ElementAccess(string_view key, const OpArgs& args) : key_{key}, context_{args.db_cntx} {
  }

  OpStatus Find(EngineShard* shard);
  // Still finds the element even if it's WRONG_TYPE. This is used for blind updates.
  // See BITOP operation.
  OpStatus FindAllowWrongType(EngineShard* shard);

  bool IsNewEntry() const {
    CHECK_NOTNULL(shard_);
    return added_;
  }

  constexpr DbIndex Index() const {
    return context_.db_index;
  }

  string Value() const;

  void Commit(string_view new_value) const;

  // return nullopt when key exists but it's not encoded as string
  // return true if key exists and false if it doesn't
  std::optional<bool> Exists(EngineShard* shard);
};

std::optional<bool> ElementAccess::Exists(EngineShard* shard) {
  auto res = context_.GetDbSlice(shard->shard_id()).FindReadOnly(context_, key_, OBJ_STRING);
  if (res.status() == OpStatus::WRONG_TYPE) {
    return {};
  }
  return res.status() != OpStatus::KEY_NOTFOUND;
}

void ElementAccess::SetFields(EngineShard* shard, DbSlice::AddOrFindResult res) {
  element_iter_ = res.it;
  added_ = res.is_new;
  shard_ = shard;
  post_updater_ = std::move(res.post_updater);
}

OpStatus ElementAccess::Find(EngineShard* shard) {
  auto op_res = context_.GetDbSlice(shard->shard_id()).AddOrFind(context_, key_);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;

  if (!add_res.is_new && add_res.it->second.ObjType() != OBJ_STRING) {
    return OpStatus::WRONG_TYPE;
  }

  SetFields(shard, std::move(add_res));
  return OpStatus::OK;
}

OpStatus ElementAccess::FindAllowWrongType(EngineShard* shard) {
  auto op_res = context_.GetDbSlice(shard->shard_id()).AddOrFind(context_, key_);
  RETURN_ON_BAD_STATUS(op_res);
  auto& add_res = *op_res;

  SetFields(shard, std::move(add_res));
  return OpStatus::OK;
}

string ElementAccess::Value() const {
  CHECK_NOTNULL(shard_);
  if (!added_) {  // Exist entry - return it
    return GetString(element_iter_->second);
  } else {  // we only have reference to the new entry but no value
    return string{};
  }
}

void ElementAccess::Commit(string_view new_value) const {
  if (shard_) {
    if (new_value.empty()) {
      if (!IsNewEntry()) {
        post_updater_.Run();
      } else {
        // No need to run, it was a new entry and it got removed
        post_updater_.Cancel();
      }
      context_.GetDbSlice(shard_->shard_id()).Del(context_, element_iter_);
    } else {
      element_iter_->second.SetString(new_value);
      post_updater_.Run();
    }
  }
}

// =============================================
// Set a new value to a given bit

OpResult<bool> BitNewValue(const OpArgs& args, string_view key, uint32_t offset, bool bit_value) {
  EngineShard* shard = args.shard;
  ElementAccess element_access{key, args};
  auto& db_slice = args.GetDbSlice();
  DCHECK(db_slice.IsDbValid(element_access.Index()));
  bool old_value = false;

  auto find_res = element_access.Find(shard);

  if (find_res != OpStatus::OK) {
    return find_res;
  }

  if (element_access.IsNewEntry()) {
    string new_entry(GetByteIndex(offset) + 1, 0);
    old_value = SetBitValue(offset, bit_value, &new_entry);
    element_access.Commit(new_entry);
  } else {
    bool reset = false;
    string existing_entry{element_access.Value()};
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

string RunBitOperationOnValues(string_view op, const BitsStrVec& values) {
  // This function accept an operation (either OR, XOR, NOT or OR), and run bit operation
  // on all the values we got from the database. Note that in case that one of the values
  // is shorter than the other it would return a 0 and the operation would continue
  // until we ran the longest value. The function will return the resulting new value
  std::size_t max_len = 0;
  std::size_t max_len_index = 0;

  const auto BitOperation = [&]() {
    if (op == OR_OP_NAME) {
      string default_str{values[max_len_index]};
      return BitOpString(OrOp, SkipOr, std::move(values), std::move(default_str));
    } else if (op == XOR_OP_NAME) {
      return BitOpString(XorOp, SkipXor, std::move(values), string(max_len, 0));
    } else if (op == AND_OP_NAME) {
      return BitOpString(AndOp, SkipAnd, std::move(values), string(max_len, 0));
    } else if (op == NOT_OP_NAME) {
      return BitOpNotString(values[0]);
    } else {
      LOG(FATAL) << "Operation not supported '" << op << "'";
      return string{};  // otherwise we will have warning of not returning value
    }
  };

  if (values.empty()) {  // this is ok in case we don't have the src keys
    return string{};
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

OpResult<string> CombineResultOp(ShardStringResults result, string_view op) {
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
OpResult<string> RunBitOpNot(const OpArgs& op_args, string_view key) {
  // if we found the value, just return, if not found then skip, otherwise report an error
  DbSlice& db_slice = op_args.GetDbSlice();
  auto find_res = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_STRING);
  if (find_res) {
    return GetString(find_res.value()->second);
  } else {
    return find_res.status();
  }
}

// Read only operation where we are running the bit operation on all the
// values that belong to same shard.
OpResult<string> RunBitOpOnShard(string_view op, const OpArgs& op_args, ShardArgs::Iterator start,
                                 ShardArgs::Iterator end) {
  DCHECK(start != end);
  if (op == NOT_OP_NAME) {
    return RunBitOpNot(op_args, *start);
  }

  DbSlice& db_slice = op_args.GetDbSlice();
  BitsStrVec values;

  // collect all the value for this shard
  for (; start != end; ++start) {
    auto find_res = db_slice.FindReadOnly(op_args.db_cntx, *start, OBJ_STRING);
    if (find_res) {
      values.emplace_back(GetString(find_res.value()->second));
    } else {
      if (find_res.status() == OpStatus::KEY_NOTFOUND) {
        continue;  // this is allowed, just return empty string per Redis
      } else {
        return find_res.status();
      }
    }
  }
  // Run the operation on all the values that we found
  string op_result = RunBitOperationOnValues(op, values);
  return op_result;
}

template <typename T>
void HandleOpValueResult(const OpResult<T>& result, SinkReplyBuilder* builder) {
  static_assert(std::is_integral<T>::value,
                "we are only handling types that are integral types in the return types from "
                "here");
  if (result) {
    builder->SendLong(result.value());
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        builder->SendError(kWrongTypeErr);
        break;
      case OpStatus::OUT_OF_MEMORY:
        builder->SendError(kOutOfMemory);
        break;
      default:
        builder->SendLong(0);  // in case we don't have the value we should just send 0
        break;
    }
  }
}

// ------------------------------------------------------------------------- //
//  Impl for the command functions
void BitPos(CmdArgList args, const CommandContext& cmd_cntx) {
  // Support for the command BITPOS
  // See details at https://redis.io/commands/bitpos/
  auto* builder = cmd_cntx.rb;
  if (args.size() < 1 || args.size() > 5) {
    return builder->SendError(kSyntaxErr);
  }

  string_view key = ArgS(args, 0);

  int32_t value{0};
  int64_t start = 0;
  int64_t end = std::numeric_limits<int64_t>::max();
  bool as_bit = false;

  if (!absl::SimpleAtoi(ArgS(args, 1), &value)) {
    return builder->SendError(kInvalidIntErr);
  } else if (value != 0 && value != 1) {
    return builder->SendError("The bit argument must be 1 or 0");
  }

  if (args.size() >= 3) {
    if (!absl::SimpleAtoi(ArgS(args, 2), &start)) {
      return builder->SendError(kInvalidIntErr);
    }

    if (args.size() >= 4) {
      if (!absl::SimpleAtoi(ArgS(args, 3), &end)) {
        return builder->SendError(kInvalidIntErr);
      }

      if (args.size() >= 5) {
        string arg = absl::AsciiStrToUpper(ArgS(args, 4));
        if (arg == "BIT") {
          as_bit = true;
        } else if (arg == "BYTE") {
          as_bit = false;
        } else {
          return builder->SendError(kSyntaxErr);
        }
      }
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return FindFirstBitWithValue(t->GetOpArgs(shard), key, value, start, end, as_bit);
  };
  OpResult<int64_t> res = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, builder);
}

void BitCount(CmdArgList args, const CommandContext& cmd_cntx) {
  // Support for the command BITCOUNT
  // See details at https://redis.io/commands/bitcount/
  // Please note that if the key don't exists, it would return 0

  CmdArgParser parser(args);
  auto key = parser.Next<string_view>();

  auto [start, end] = parser.HasNext()
                          ? parser.Next<int64_t, int64_t>()
                          : std::pair<int64_t, int64_t>{0, std::numeric_limits<int64_t>::max()};

  bool as_bit = parser.HasNext() ? parser.MapNext("BYTE", false, "BIT", true) : false;
  auto* builder = cmd_cntx.rb;
  if (!parser.Finalize()) {
    return builder->SendError(parser.Error()->MakeReply());
  }
  auto cb = [&, &start = start, &end = end](Transaction* t, EngineShard* shard) {
    return CountBitsForValue(t->GetOpArgs(shard), key, start, end, as_bit);
  };
  OpResult<std::size_t> res = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, builder);
}

// GCC yields a wrong warning about uninitialized optional use
#ifndef __clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

enum class EncodingType { UINT, INT, NILL };

struct CommonAttributes {
  EncodingType type;
  size_t encoding_bit_size;
  size_t offset;
};

// We either return the result of the subcommand (int64_t) or nullopt
// to represent overflow/underflow failures
using ResultType = std::optional<int64_t>;

struct Overflow {
  enum Policy { WRAP, SAT, FAIL };

  // Used to check for unsigned overflow/underflow.
  // If incr is non zero, we check for overflows in the expression incr + *value
  // If incr is zero, we check for overflows in the expression *value
  // If the overflow fails because of Policy::FAIL, it returns false. Otherwise, true.
  // The result of handling the overflow is stored in the pointer value
  bool UIntOverflow(int64_t incr, size_t total_bits, int64_t* value) const;

  // Used to check for signed overflow/underflow.
  // If incr is non zero, we check for overflows in the expression incr + *value
  // If incr is zero, we check for overflows in the expression *value
  // If the overflow fails because of Policy::FAIL, it returns false. Otherwise, true.
  // The result of handling the overflow is stored in the pointer value
  bool IntOverflow(size_t total_bits, int64_t incr, bool add, int64_t* value) const;

  Policy type = WRAP;
};

bool Overflow::UIntOverflow(int64_t incr, size_t total_bits, int64_t* value) const {
  // total up to 63 bits -- we do not support 64 bit unsigned
  const uint64_t max = (1UL << total_bits) - 1;

  uint64_t incr_value = incr;
  if (incr_value + *value > max) {
    switch (type) {
      case Overflow::WRAP:
        // safe to do, won't overflow, both incr and value are <= than 2^63 - 1
        *value = (incr_value + *value) % max;
        break;
      case Overflow::SAT:
        *value = max;
        break;
      case Overflow::FAIL:
        *value = 0;
        return false;
    }
    return true;
  }

  *value = incr_value + *value;
  return true;
}

bool Overflow::IntOverflow(size_t total_bits, int64_t incr, bool add, int64_t* value) const {
  // This is exactly how redis handles signed overflow and we use the exact same chore
  const int64_t int_max = std::numeric_limits<int64_t>::max();
  const int64_t max = (total_bits == 64) ? int_max : ((1L << (total_bits - 1)) - 1);
  const int64_t min = (-max) - 1;
  auto switch_overflow = [&](int64_t wrap_case, int64_t sat_case, int64_t i) {
    switch (type) {
      case Overflow::WRAP: {
        uint64_t msb = 1UL << (total_bits - 1);
        uint64_t a = *value, b = incr;
        // Perform addition as unsigned so that's defined
        uint64_t c = a + b;
        if (total_bits < 64) {
          uint64_t mask = static_cast<uint64_t>(-1) << total_bits;
          if (c & msb) {
            c |= mask;
          } else {
            c &= ~mask;
          }
        }
        *value = c;
        break;
      }
      case Overflow::SAT:
        *value = sat_case;
        break;
      case Overflow::FAIL:
        *value = 0;
        return false;
    }
    return true;
  };

  // maxincr/minincr can overflow but it won't be an issue because we only use them
  // after checking 'value' range, so when they are used no overflow
  // happens. 'uint64_t' cast is there just to prevent undefined behavior on
  // overflow */
  int64_t maxincr = static_cast<uint64_t>(max) - *value;
  int64_t minincr = min - *value;

  // overflow
  if (*value > max || (total_bits != 64 && incr > maxincr) ||
      (*value >= 0 && incr > 0 && incr > maxincr)) {
    return switch_overflow(min, max, 1);
  }

  // underflow
  if (*value < min || (total_bits != 64 && incr < minincr) ||
      (*value < 0 && incr < 0 && incr < minincr)) {
    return switch_overflow(max, min, -1);
  }

  *value = *value + incr;

  return true;
}

class Get {
 public:
  explicit Get(CommonAttributes attr) : attr_(attr) {
  }

  // Apply the GET subcommand to the bitfield bytes.
  // Return either the subcommand result (int64_t) or empty optional if failed because of
  // Policy:FAIL
  ResultType ApplyTo(Overflow ov, const string* bitfield);

 private:
  CommonAttributes attr_;
};

ResultType Get::ApplyTo(Overflow ov, const string* bitfield) {
  const auto& bytes = *bitfield;
  const int32_t total_bytes = static_cast<int32_t>(bytes.size());
  const size_t offset = attr_.offset;
  auto last_byte_offset = GetByteIndex(attr_.offset + attr_.encoding_bit_size - 1);

  uint32_t lsb = attr_.offset + attr_.encoding_bit_size - 1;
  if (last_byte_offset > total_bytes) {
    return {};
  }

  const bool is_negative =
      CheckBitStatus(GetByteValue(bytes, offset), GetNormalizedBitIndex(offset));

  int64_t result = 0;
  for (size_t i = 0; i < attr_.encoding_bit_size; ++i) {
    uint8_t byte{GetByteValue(bytes, lsb)};
    int32_t index = GetNormalizedBitIndex(lsb);
    int64_t old_bit = CheckBitStatus(byte, index);
    result |= old_bit << i;
    --lsb;
  }

  if (is_negative && attr_.type == EncodingType::INT && result > 0) {
    result |= -1L ^ ((1L << attr_.encoding_bit_size) - 1);
  }

  return result;
}

class Set {
 public:
  explicit Set(CommonAttributes attr, int64_t value) : attr_(attr), set_value_(value) {
  }

  // Apply the SET subcommand to the bitfield value.
  // Return either the subcommand result (int64_t) or empty optional if failed because of
  // Policy:FAIL Updates the bitfield to contain the new value
  ResultType ApplyTo(Overflow ov, string* bitfield);

 private:
  // Helper function that delegates overflow checking to the Overflow object
  bool HandleOverflow(Overflow ov);

  CommonAttributes attr_;
  int64_t set_value_;
};

ResultType Set::ApplyTo(Overflow ov, string* bitfield) {
  string& bytes = *bitfield;
  const int32_t total_bytes = static_cast<int32_t>(bytes.size());
  auto last_byte_offset = GetByteIndex(attr_.offset + attr_.encoding_bit_size - 1) + 1;
  if (last_byte_offset > total_bytes) {
    bytes.resize(last_byte_offset, 0);
  }

  if (!HandleOverflow(ov)) {
    return {};
  }

  uint32_t lsb = attr_.offset + attr_.encoding_bit_size - 1;
  int64_t old_value = 0;

  for (size_t i = 0; i < attr_.encoding_bit_size; ++i) {
    bool bit_value = (set_value_ >> i) & 0x01;
    uint8_t byte{GetByteValue(bytes, lsb)};
    int32_t index = GetNormalizedBitIndex(lsb);
    int64_t old_bit = CheckBitStatus(byte, index);
    byte = bit_value ? TurnBitOn(byte, index) : TurnBitOff(byte, index);
    bytes[GetByteIndex(lsb)] = byte;
    old_value |= old_bit << i;
    --lsb;
  }

  return old_value;
}

bool Set::HandleOverflow(Overflow ov) {
  size_t total_bits = attr_.encoding_bit_size;
  if (attr_.type == EncodingType::UINT) {
    return ov.UIntOverflow(0, attr_.encoding_bit_size, &set_value_);
  }

  return ov.IntOverflow(total_bits, 0, false, &set_value_);
}

class IncrBy {
 public:
  explicit IncrBy(CommonAttributes attr, int64_t val) : attr_(attr), incr_value_(val) {
  }

  // Apply the INCRBY subcommand to the bitfield value.
  // Return either the subcommand result (int64_t) or empty optional if failed because of
  // Policy:FAIL Updates the bitfield to contain the new incremented value
  ResultType ApplyTo(Overflow ov, string* bitfield);

 private:
  // Helper function that delegates overflow checking to the Overflow object
  bool HandleOverflow(Overflow ov, int64_t* previous);

  CommonAttributes attr_;
  int64_t incr_value_;
};

ResultType IncrBy::ApplyTo(Overflow ov, string* bitfield) {
  string& bytes = *bitfield;
  Get get(attr_);
  auto res = get.ApplyTo(ov, &bytes);

  if (!res) {
    Set set(attr_, incr_value_);
    return set.ApplyTo(ov, &bytes);
  }

  if (!HandleOverflow(ov, &*res)) {
    return {};
  }

  Set set(attr_, *res);
  set.ApplyTo(ov, &bytes);
  return *res;
}

bool IncrBy::HandleOverflow(Overflow ov, int64_t* previous) {
  if (attr_.type == EncodingType::UINT) {
    return ov.UIntOverflow(incr_value_, attr_.encoding_bit_size, previous);
  }

  const size_t total_bits = attr_.encoding_bit_size;
  return ov.IntOverflow(total_bits, incr_value_, true, previous);
}

// Subcommand types for each of the subcommands of the BITFIELD command
using Command = std::variant<Get, Set, Overflow, IncrBy>;

using Result = std::optional<ResultType>;

// Visitor for all the subcommand variants. Calls ApplyTo, to execute the subcommand
class CommandApplyVisitor {
 public:
  explicit CommandApplyVisitor(string bitfield) : bitfield_(std::move(bitfield)) {
  }

  Result operator()(Get get) {
    return get.ApplyTo(overflow_, &bitfield_);
  }

  template <typename T> Result operator()(T update) {
    should_commit_ = true;
    return update.ApplyTo(overflow_, &bitfield_);
  }

  Result operator()(Overflow overflow) {
    overflow_ = overflow;
    return {};
  }

  string_view Bitfield() const {
    return bitfield_;
  }

  bool ShouldCommit() const {
    return should_commit_;
  }

 private:
  // Most recent overflow object encountered. We cache it to make the overflow
  // policy changes stick among different subcommands
  Overflow overflow_;
  // This will be commited if it was updated
  string bitfield_;
  // If either of the subcommands SET|INCRBY is used we should persist the changes.
  // Otherwise, we only used a read only subcommand (GET)
  bool should_commit_ = false;
};

// A lit of subcommands used in BITFIELD command
using CommandList = vector<Command>;

// Helper class used in the shard cb that abstracts away the iteration and execution of subcommands
class StateExecutor {
 public:
  StateExecutor(ElementAccess access, EngineShard* shard)
      : access_{std::move(access)}, shard_(shard) {
  }

  //  Iterates over all of the parsed subcommands and executes them one by one. At the end,
  //  if an update subcommand SET|INCRBY was used, commit back the changes via the ElementAccess
  //  object
  OpResult<vector<ResultType>> Execute(const CommandList& commands);

 private:
  ElementAccess access_;
  EngineShard* shard_;
};

OpResult<vector<ResultType>> StateExecutor::Execute(const CommandList& commands) {
  auto res = access_.Exists(shard_);
  if (!res) {
    return {OpStatus::WRONG_TYPE};
  }
  string value;
  if (*res) {
    access_.Find(shard_);
    value = access_.Value();
  }

  vector<ResultType> results;
  CommandApplyVisitor visitor(std::move(value));
  for (auto& command : commands) {
    auto res = std::visit(visitor, command);
    if (res) {
      results.push_back(*res);
    }
  }

  if (visitor.ShouldCommit()) {
    access_.Find(shard_);
    access_.Commit(visitor.Bitfield());
  }

  return results;
}

nonstd::expected<CommonAttributes, string> ParseCommonAttr(CmdArgParser* parser) {
  CommonAttributes parsed;
  using nonstd::make_unexpected;

  auto [encoding, offset_str] = parser->Next<string_view, string_view>();

  if (encoding.empty()) {
    return make_unexpected(kSyntaxErr);
  }
  if (encoding[0] == 'U' || encoding[0] == 'u') {
    parsed.type = EncodingType::UINT;
  } else if (encoding[0] == 'I' || encoding[0] == 'i') {
    parsed.type = EncodingType::INT;
  } else {
    return make_unexpected(kSyntaxErr);
  }

  string_view bits = encoding.substr(1);

  if (!absl::SimpleAtoi(bits, &parsed.encoding_bit_size)) {
    return make_unexpected(kSyntaxErr);
  }

  if (parsed.encoding_bit_size <= 0 || parsed.encoding_bit_size > 64) {
    return make_unexpected(
        "invalid bitfield type. use something like i16 u8. note that u64 is not supported but i64 "
        "is.");
  }

  if (parsed.encoding_bit_size == 64 && parsed.type == EncodingType::UINT) {
    return make_unexpected(
        "invalid bitfield type. use something like i16 u8. note that u64 is not supported but i64 "
        "is.");
  }

  bool is_proxy = false;
  if (absl::StartsWith(offset_str, "#")) {
    offset_str = offset_str.substr(1);
    is_proxy = true;
  }
  if (!absl::SimpleAtoi(offset_str, &parsed.offset)) {
    return make_unexpected(kSyntaxErr);
  }
  if (is_proxy) {
    parsed.offset = parsed.offset * parsed.encoding_bit_size;
  }
  return parsed;
}

// Parses a list of arguments (without key) to a CommandList.
// Returns the CommandList if the parsing completed succefully or string
// to indicate an error
nonstd::expected<CommandList, string> ParseToCommandList(CmdArgList args, bool read_only) {
  enum class Cmds { OVERFLOW_OPT, GET_OPT, SET_OPT, INCRBY_OPT };
  CommandList result;

  using nonstd::make_unexpected;

  CmdArgParser parser(args);
  while (parser.HasNext()) {
    auto cmd = parser.MapNext("OVERFLOW", Cmds::OVERFLOW_OPT, "GET", Cmds::GET_OPT, "SET",
                              Cmds::SET_OPT, "INCRBY", Cmds::INCRBY_OPT);
    if (parser.Error()) {
      return make_unexpected(kSyntaxErr);
    }

    if (cmd == Cmds::OVERFLOW_OPT) {
      if (read_only) {
        make_unexpected("BITFIELD_RO only supports the GET subcommand");
      }
      using pol = Overflow::Policy;
      auto res = parser.MapNext("SAT", pol::SAT, "WRAP", pol::WRAP, "FAIL", pol::FAIL);
      if (!parser.HasError()) {
        result.push_back(Overflow{res});
        continue;
      }
      parser.Error();
      return make_unexpected(kSyntaxErr);
    }

    auto maybe_attr = ParseCommonAttr(&parser);
    if (!maybe_attr.has_value()) {
      parser.Error();
      return make_unexpected(std::move(maybe_attr.error()));
    }

    auto attr = maybe_attr.value();
    if (cmd == Cmds::GET_OPT) {
      result.push_back(Command(Get(attr)));
      continue;
    }

    if (read_only) {
      return make_unexpected("BITFIELD_RO only supports the GET subcommand");
    }

    int64_t value = parser.Next<int64_t>();
    if (parser.Error()) {
      return make_unexpected(kSyntaxErr);
    }
    if (cmd == Cmds::SET_OPT) {
      result.push_back(Command(Set(attr, value)));
      continue;
    }

    if (cmd == Cmds::INCRBY_OPT) {
      result.push_back(Command(IncrBy(attr, value)));
      continue;
    }
    parser.Error();
    return make_unexpected(kSyntaxErr);
  }

  return result;
}

void SendResults(const vector<ResultType>& results, SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  const size_t total = results.size();
  if (total == 0) {
    rb->SendNullArray();
    return;
  }

  rb->StartArray(total);
  for (const auto& elem : results) {
    if (elem) {
      rb->SendLong(*elem);
      continue;
    }

    rb->SendNull();
  }
}

void BitFieldGeneric(CmdArgList args, bool read_only, Transaction* tx, SinkReplyBuilder* builder) {
  if (args.size() == 1) {
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    rb->SendNullArray();
    return;
  }
  auto key = ArgS(args, 0);
  auto maybe_ops_list = ParseToCommandList(args.subspan(1), read_only);

  if (!maybe_ops_list.has_value()) {
    builder->SendError(maybe_ops_list.error());
    return;
  }
  CommandList cmd_list = std::move(maybe_ops_list.value());

  auto cb = [&cmd_list, &key](Transaction* t, EngineShard* shard) -> OpResult<vector<ResultType>> {
    StateExecutor executor(ElementAccess(key, t->GetOpArgs(shard)), shard);
    return executor.Execute(cmd_list);
  };

  OpResult<vector<ResultType>> res = tx->ScheduleSingleHopT(std::move(cb));

  if (res == OpStatus::WRONG_TYPE) {
    builder->SendError(kWrongTypeErr);
    return;
  }

  SendResults(*res, builder);
}

void BitField(CmdArgList args, const CommandContext& cmd_cntx) {
  BitFieldGeneric(args, false, cmd_cntx.tx, cmd_cntx.rb);
}

void BitFieldRo(CmdArgList args, const CommandContext& cmd_cntx) {
  BitFieldGeneric(args, true, cmd_cntx.tx, cmd_cntx.rb);
}

#ifndef __clang__
#pragma GCC diagnostic pop
#endif

void BitOp(CmdArgList args, const CommandContext& cmd_cntx) {
  static const std::array<string_view, 4> BITOP_OP_NAMES{OR_OP_NAME, XOR_OP_NAME, AND_OP_NAME,
                                                         NOT_OP_NAME};
  string op = absl::AsciiStrToUpper(ArgS(args, 0));
  string_view dest_key = ArgS(args, 1);
  bool illegal = std::none_of(BITOP_OP_NAMES.begin(), BITOP_OP_NAMES.end(),
                              [&op](auto val) { return op == val; });

  auto* builder = cmd_cntx.rb;
  if (illegal || (op == NOT_OP_NAME && args.size() > 3)) {
    return builder->SendError(kSyntaxErr);  // too many arguments
  }

  // Multi shard access - read only
  ShardStringResults result_set(shard_set->size(), OpStatus::KEY_NOTFOUND);
  ShardId dest_shard = Shard(dest_key, result_set.size());

  auto shard_bitop = [&](Transaction* t, EngineShard* shard) {
    ShardArgs largs = t->GetShardArgs(shard->shard_id());
    DCHECK(!largs.Empty());
    ShardArgs::Iterator start = largs.begin(), end = largs.end();
    if (shard->shard_id() == dest_shard) {
      CHECK_EQ(*start, dest_key);
      ++start;
      if (start == end) {  // no more keys to check
        return OpStatus::OK;
      }
    }
    OpArgs op_args = t->GetOpArgs(shard);
    result_set[shard->shard_id()] = RunBitOpOnShard(op, op_args, start, end);
    return OpStatus::OK;
  };

  cmd_cntx.tx->Execute(std::move(shard_bitop), false);  // we still have more work to do
  // All result from each shard
  const auto joined_results = CombineResultOp(result_set, op);
  // Second phase - save to target key if successful
  if (!joined_results) {
    cmd_cntx.tx->Conclude();
    builder->SendError(joined_results.status());
    return;
  } else {
    auto op_result = joined_results.value();
    auto store_cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == dest_shard) {
        ElementAccess operation{dest_key, t->GetOpArgs(shard)};
        auto find_res = operation.FindAllowWrongType(shard);

        // BITOP command acts as a blind update. If the key existed and its type
        // was not a string we still want to Commit with the new value.
        if (find_res == OpStatus::OK || find_res == OpStatus::WRONG_TYPE) {
          operation.Commit(op_result);

          if (shard->journal()) {
            if (op_result.empty()) {
              // We need to delete it if the key exists. If it doesn't, we just
              // skip it and do not send it to the replica at all.
              if (!operation.IsNewEntry()) {
                RecordJournal(t->GetOpArgs(shard), "DEL", {dest_key});
              }
            } else {
              RecordJournal(t->GetOpArgs(shard), "SET", {dest_key, op_result});
            }
          }
        }
      }
      return OpStatus::OK;
    };

    cmd_cntx.tx->Execute(std::move(store_cb), true);
    builder->SendLong(op_result.size());
  }
}

void GetBit(CmdArgList args, const CommandContext& cmd_cntx) {
  // Support for the command "GETBIT key offset"
  // see https://redis.io/commands/getbit/

  uint32_t offset{0};
  string_view key = ArgS(args, 0);

  if (!absl::SimpleAtoi(ArgS(args, 1), &offset)) {
    return cmd_cntx.rb->SendError(kInvalidIntErr);
  }
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return ReadValueBitsetAt(t->GetOpArgs(shard), key, offset);
  };
  OpResult<bool> res = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cmd_cntx.rb);
}

void SetBit(CmdArgList args, const CommandContext& cmd_cntx) {
  // Support for the command "SETBIT key offset new_value"
  // see https://redis.io/commands/setbit/

  CmdArgParser parser(args);
  auto [key, offset, value] = parser.Next<string_view, uint32_t, FInt<0, 1>>();

  if (auto err = parser.Error(); err) {
    return cmd_cntx.rb->SendError(err->MakeReply());
  }

  auto cb = [&, &key = key, &offset = offset, &value = value](Transaction* t, EngineShard* shard) {
    return BitNewValue(t->GetOpArgs(shard), key, offset, value != 0);
  };

  OpResult<bool> res = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  HandleOpValueResult(res, cmd_cntx.rb);
}

// ------------------------------------------------------------------------- //
// This are the "callbacks" that we're using from above
string GetString(const PrimeValue& pv) {
  string res;
  pv.GetString(&res);
  return res;
}

OpResult<bool> ReadValueBitsetAt(const OpArgs& op_args, string_view key, uint32_t offset) {
  OpResult<string> result = ReadValue(op_args.db_cntx, key, op_args.shard);
  if (result) {
    return GetBitValueSafe(result.value(), offset);
  } else {
    return result.status();
  }
}

OpResult<string> ReadValue(const DbContext& context, string_view key, EngineShard* shard) {
  DbSlice& db_slice = context.GetDbSlice(shard->shard_id());
  auto it_res = db_slice.FindReadOnly(context, key, OBJ_STRING);
  if (!it_res.ok()) {
    return it_res.status();
  }

  const PrimeValue& pv = it_res.value()->second;

  return GetString(pv);
}

OpResult<std::size_t> CountBitsForValue(const OpArgs& op_args, string_view key, int64_t start,
                                        int64_t end, bool bit_value) {
  OpResult<string> result = ReadValue(op_args.db_cntx, key, op_args.shard);

  if (result) {  // if this is not found, just return 0 - per Redis
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

int64_t FindFirstBitWithValueAsBit(string_view value_str, bool bit_value, int64_t start,
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

int64_t FindFirstBitWithValueAsByte(string_view value_str, bool bit_value, int64_t start,
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

OpResult<int64_t> FindFirstBitWithValue(const OpArgs& op_args, string_view key, bool bit_value,
                                        int64_t start, int64_t end, bool as_bit) {
  OpResult<string> value = ReadValue(op_args.db_cntx, key, op_args.shard);

  // non-existent keys are handled exactly as in Redis's implementation,
  // even though it contradicts its docs:
  //     If a clear bit isn't found in the specified range, the function returns -1
  //     as the user specified a clear range and there are no 0 bits in that range
  if (!value) {
    return bit_value ? -1 : 0;
  }

  string_view value_str = value.value();
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

void BitOpsFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;
  registry->StartFamily();
  *registry << CI{"BITPOS", CO::CommandOpt::READONLY, -3, 1, 1, acl::kBitPos}.SetHandler(&BitPos)
            << CI{"BITCOUNT", CO::READONLY, -2, 1, 1, acl::kBitCount}.SetHandler(&BitCount)
            << CI{"BITFIELD", CO::WRITE, -3, 1, 1, acl::kBitField}.SetHandler(&BitField)
            << CI{"BITFIELD_RO", CO::READONLY, -5, 1, 1, acl::kBitFieldRo}.SetHandler(&BitFieldRo)
            << CI{"BITOP", CO::WRITE | CO::NO_AUTOJOURNAL, -4, 2, -1, acl::kBitOp}.SetHandler(
                   &BitOp)
            << CI{"GETBIT", CO::READONLY | CO::FAST, 3, 1, 1, acl::kGetBit}.SetHandler(&GetBit)
            << CI{"SETBIT", CO::WRITE | CO::DENYOOM, 4, 1, 1, acl::kSetBit}.SetHandler(&SetBit);
}

}  // namespace dfly
