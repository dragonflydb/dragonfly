// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/bitops_family.h"

#include <bitset>

extern "C" {
#include "redis/object.h"
}

#include <new>

#include "base/logging.h"
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
const int32_t OFFSET_FACTOR = 8;  // number of bits in byte

// The following is the list of the functions that would handle the
// commands that handle the bit operations
void BitPos(CmdArgList args, ConnectionContext* cntx);
void BitCount(CmdArgList args, ConnectionContext* cntx);
void BitField(CmdArgList args, ConnectionContext* cntx);
void BitFieldRo(CmdArgList args, ConnectionContext* cntx);
void BitOp(CmdArgList args, ConnectionContext* cntx);
void GetBit(CmdArgList args, ConnectionContext* cntx);
void SetBit(CmdArgList args, ConnectionContext* cntx);

OpResult<std::string> ReadValue(const OpArgs& op_args, std::string_view key);
OpResult<bool> ReadValueBitsetAt(const OpArgs& op_args, std::string_view key, uint32_t offset);
OpResult<std::size_t> CountBitsForValue(const OpArgs& op_args, std::string_view key, int64_t start,
                                        int64_t end, bool bit_value);
std::string GetString(EngineShard* shard, const PrimeValue& pv);
bool SetBitValue(uint32_t offset, bool bit_value, std::string* entry);
std::size_t CountBitSetByByteIndices(std::string_view at, std::size_t start, std::size_t end);
std::size_t CountBitSet(std::string_view str, int64_t start, int64_t end, bool bits);
std::size_t CountBitSetByBitIndices(std::string_view at, std::size_t start, std::size_t end);

// ------------------------------------------------------------------------- //
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

// General purpose function to count the number of bits that are on.
// The parameters for start, end and bits are defaulted to the start of the string,
// end of the string and bits are false.
// Note that when bits is false, it means that we are looking on byte boundaries.
std::size_t CountBitSet(std::string_view str, int64_t start, int64_t end, bool bits) {
  const int32_t size = bits ? str.size() * OFFSET_FACTOR : str.size();

  auto NormalizedOffset = [size](int32_t orig) {
    if (orig < 0) {
      orig = size + orig;
    }
    return orig;
  };

  if (start > 0 && end > 0 && end < start) {
    return 0;  // for illegal range with positive we just return 0
  }

  if (start < 0 && end < 0 && start > end) {
    return 0;  // for illegal range with negative we just return 0
  }

  start = NormalizedOffset(start);
  if (end > 0 && end < start) {
    return 0;
  }
  end = NormalizedOffset(end);
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
// Helper functions to access the data or change it

class OverrideValue {
  EngineShard* shard_ = nullptr;
  DbIndex index_ = 0;

 public:
  explicit OverrideValue(const OpArgs& args) : shard_{args.shard}, index_{args.db_ind} {
  }

  OpResult<bool> Set(std::string_view key, uint32_t offset, bool bit_value);
};

OpResult<bool> OverrideValue::Set(std::string_view key, uint32_t offset, bool bit_value) {
  auto& db_slice = shard_->db_slice();
  DbIndex index = index_;

  DCHECK(db_slice.IsDbValid(index_));

  std::pair<PrimeIterator, bool> add_res;
  try {
    add_res = db_slice.AddOrFind(index_, key);
  } catch (const std::bad_alloc&) {
    return OpStatus::OUT_OF_MEMORY;
  }
  bool old_value = false;
  PrimeIterator& it = add_res.first;
  bool added = add_res.second;
  auto UpdateBitMapValue = [&](std::string_view value) {
    db_slice.PreUpdate(index, it);
    it->second.SetString(value);
    db_slice.PostUpdate(index, it, key, !added);
  };

  if (added) {  // this is a new entry in the "table"
    std::string new_entry(GetByteIndex(offset) + 1, 0);
    old_value = SetBitValue(offset, bit_value, &new_entry);
    UpdateBitMapValue(new_entry);
  } else {
    if (it->second.ObjType() != OBJ_STRING) {
      return OpStatus::WRONG_TYPE;
    }
    bool reset = false;
    std::string existing_entry{GetString(shard_, it->second)};
    if ((existing_entry.size() * OFFSET_FACTOR) <= offset) {  // need to resize first
      existing_entry.resize(GetByteIndex(offset) + 1, 0);
      reset = true;
    }
    old_value = SetBitValue(offset, bit_value, &existing_entry);
    if (reset || old_value != bit_value) {  // we made a "real" change to the entry, save it
      UpdateBitMapValue(existing_entry);
    }
  }
  return old_value;
}

// ------------------------------------------------------------------------- //
//  Impl for the command functions
void BitPos(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void BitCount(CmdArgList args, ConnectionContext* cntx) {
  // Support for the command BITCOUNT
  // See details at https://redis.io/commands/bitcount/
  // Please note that if the key don't exists, it would return 0

  if (args.size() == 3 || args.size() > 5) {
    return (*cntx)->SendError(kSyntaxErr);
  }
  // return (*cntx)->SendLong(0);
  std::string_view key = ArgS(args, 1);
  bool as_bit = false;
  int64_t start = 0;
  int64_t end = std::numeric_limits<int64_t>::max();
  if (args.size() >= 4) {
    if (absl::SimpleAtoi(ArgS(args, 2), &start) == 0 ||
        absl::SimpleAtoi(ArgS(args, 3), &end) == 0) {
      return (*cntx)->SendError(kInvalidIntErr);
    }
    if (args.size() == 5) {
      ToUpper(&args[4]);
      as_bit = ArgS(args, 4) == "BIT";
    }
  }
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return CountBitsForValue(t->GetOpArgs(shard), key, start, end, as_bit);
  };
  Transaction* trans = cntx->transaction;
  OpResult<std::size_t> result = trans->ScheduleSingleHopT(std::move(cb));
  if (result) {
    (*cntx)->SendLong(result.value());
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        (*cntx)->SendError(kWrongTypeErr);
        break;
      default:
        (*cntx)->SendLong(0);
        break;
    }
  }
}

void BitField(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void BitFieldRo(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendLong(0);
}

void BitOp(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendOk();
}

void GetBit(CmdArgList args, ConnectionContext* cntx) {
  // Support for the command "GETBIT key offset"
  // see https://redis.io/commands/getbit/

  uint32_t offset{0};
  std::string_view key = ArgS(args, 1);

  if (!absl::SimpleAtoi(ArgS(args, 2), &offset)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return ReadValueBitsetAt(t->GetOpArgs(shard), key, offset);
  };
  Transaction* trans = cntx->transaction;
  OpResult<bool> result = trans->ScheduleSingleHopT(std::move(cb));

  if (result) {
    DVLOG(2) << "GET" << trans->DebugId() << "': key: '" << key << ", value '" << result.value()
             << "'\n";
    // we have the value, now we need to get the bit at the location
    long val = result.value() ? 1 : 0;
    (*cntx)->SendLong(val);
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        (*cntx)->SendError(kWrongTypeErr);
        break;
      default:
        DVLOG(2) << "GET " << key << " nil";
        (*cntx)->SendLong(0);  // in case we don't have the value we should just send 0
    }
  }
}

void SetBit(CmdArgList args, ConnectionContext* cntx) {
  // Support for the command "SETBIT key offset new_value"
  // see https://redis.io/commands/setbit/

  uint32_t offset{0};
  int32_t value{0};
  std::string_view key = ArgS(args, 1);

  if (!absl::SimpleAtoi(ArgS(args, 2), &offset) || !absl::SimpleAtoi(ArgS(args, 3), &value)) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    OverrideValue set_operation{t->GetOpArgs(shard)};

    return set_operation.Set(key, offset, value != 0);
  };

  Transaction* trans = cntx->transaction;
  OpResult<bool> result = trans->ScheduleSingleHopT(std::move(cb));
  if (result) {
    long res = result.value() ? 1 : 0;
    (*cntx)->SendLong(res);
  } else {
    switch (result.status()) {
      case OpStatus::WRONG_TYPE:
        (*cntx)->SendError(kWrongTypeErr);
        break;
      case OpStatus::OUT_OF_MEMORY:
        (*cntx)->SendError(kOutOfMemory);
        break;
      default:
        DVLOG(2) << "SETBIT " << key << " nil" << result.status();
        (*cntx)->SendLong(0);  // in case we don't have the value we should just send 0
        break;
    }
  }
}

// ------------------------------------------------------------------------- //
// This are the "callbacks" that we're using from above
std::string GetString(EngineShard* shard, const PrimeValue& pv) {
  std::string res;
  if (pv.IsExternal()) {
    auto* tiered = shard->tiered_storage();
    auto [offset, size] = pv.GetExternalPtr();
    res.resize(size);

    std::error_code ec = tiered->Read(offset, size, res.data());
    CHECK(!ec) << "TBD: " << ec;
  } else {
    pv.GetString(&res);
  }

  return res;
}

OpResult<bool> ReadValueBitsetAt(const OpArgs& op_args, std::string_view key, uint32_t offset) {
  OpResult<std::string> result = ReadValue(op_args, key);
  if (result) {
    return GetBitValueSafe(result.value(), offset);
  } else {
    return result.status();
  }
}

OpResult<std::string> ReadValue(const OpArgs& op_args, std::string_view key) {
  OpResult<PrimeIterator> it_res = op_args.shard->db_slice().Find(op_args.db_ind, key, OBJ_STRING);
  if (!it_res.ok()) {
    return it_res.status();
  }

  const PrimeValue& pv = it_res.value()->second;

  return GetString(op_args.shard, pv);
}

OpResult<std::size_t> CountBitsForValue(const OpArgs& op_args, std::string_view key, int64_t start,
                                        int64_t end, bool bit_value) {
  OpResult<std::string> result = ReadValue(op_args, key);

  if (result) {
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

}  // namespace

void BitOpsFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;

  *registry << CI{"BITPOS", CO::CommandOpt::READONLY, -3, 1, 1, 1}.SetHandler(&BitPos)
            << CI{"BITCOUNT", CO::READONLY, -2, 1, 1, 1}.SetHandler(&BitCount)
            << CI{"BITFIELD", CO::WRITE, -3, 1, 1, 1}.SetHandler(&BitField)
            << CI{"BITFIELD_RO", CO::READONLY, -5, 1, 1, 1}.SetHandler(&BitFieldRo)
            << CI{"BITOP", CO::WRITE, -4, 1, 1, 1}.SetHandler(&BitOp)
            << CI{"GETBIT", CO::READONLY | CO::FAST | CO::FAST, 3, 1, 1, 1}.SetHandler(&GetBit)
            << CI{"SETBIT", CO::WRITE, 4, 1, 1, 1}.SetHandler(&SetBit);
}

}  // namespace dfly
