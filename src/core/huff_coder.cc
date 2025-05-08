// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/huff_coder.h"

#include "base/logging.h"

extern "C" {
#include "huff/huf.h"
}

using namespace std;

namespace dfly {

constexpr size_t kWspSize = HUF_CTABLE_WORKSPACE_SIZE;

bool HuffmanEncoder::Load(std::string_view binary_data, std::string* error_msg) {
  CHECK(!huf_ctable_);

  huf_ctable_.reset(new HUF_CElt[HUF_CTABLE_SIZE_ST(255)]);
  table_max_symbol_ = 255;

  unsigned has_zero_weights = 0;
  size_t read_size = HUF_readCTable(huf_ctable_.get(), &table_max_symbol_, binary_data.data(),
                                    binary_data.size(), &has_zero_weights);

  if (HUF_isError(read_size)) {
    huf_ctable_.reset();
    *error_msg = HUF_getErrorName(read_size);
    return false;
  }
  if (read_size != binary_data.size()) {
    *error_msg = "Corrupted data";
    huf_ctable_.reset();
    return false;
  }
  HUF_CTableHeader header = HUF_readCTableHeader(huf_ctable_.get());
  num_bits_ = header.tableLog;
  table_max_symbol_ = header.maxSymbolValue;

  return true;
}

bool HuffmanEncoder::Build(const unsigned hist[], unsigned max_symbol, std::string* error_msg) {
  CHECK(!huf_ctable_);
  huf_ctable_.reset(new HUF_CElt[HUF_CTABLE_SIZE_ST(max_symbol)]);

  unique_ptr<uint32_t[]> wrkspace(new uint32_t[HUF_CTABLE_WORKSPACE_SIZE_U32]);

  size_t num_bits =
      HUF_buildCTable_wksp(huf_ctable_.get(), hist, max_symbol, 0, wrkspace.get(), kWspSize);
  if (HUF_isError(num_bits)) {
    *error_msg = HUF_getErrorName(num_bits);
    huf_ctable_.reset();
    return false;
  }
  num_bits_ = static_cast<uint8_t>(num_bits);
  table_max_symbol_ = max_symbol;
  return true;
}

void HuffmanEncoder::Reset() {
  huf_ctable_.reset();
  table_max_symbol_ = 0;
}

bool HuffmanEncoder::Encode(std::string_view data, uint8_t* dest, uint32_t* dest_size,
                            std::string* error_msg) const {
  DCHECK(huf_ctable_);

  size_t res =
      HUF_compress1X_usingCTable(dest, *dest_size, data.data(), data.size(), huf_ctable_.get(), 0);

  if (HUF_isError(res)) {
    *error_msg = HUF_getErrorName(res);
    return false;
  }
  *dest_size = static_cast<uint32_t>(res);
  return true;
}

unsigned HuffmanEncoder::GetNBits(uint8_t symbol) const {
  DCHECK(huf_ctable_);
  return HUF_getNbBitsFromCTable(huf_ctable_.get(), symbol);
}

size_t HuffmanEncoder::EstimateCompressedSize(const unsigned hist[], unsigned max_symbol) const {
  DCHECK(huf_ctable_);
  size_t res = HUF_estimateCompressedSize(huf_ctable_.get(), hist, max_symbol);
  return res;
}

string HuffmanEncoder::Export() const {
  DCHECK(huf_ctable_);

  // Reverse engineered: (maxSymbolValue + 1) / 2 + 1.
  constexpr unsigned kMaxTableSize = 130;
  string res;
  res.resize(kMaxTableSize);

  unique_ptr<uint32_t[]> wrkspace(new uint32_t[HUF_CTABLE_WORKSPACE_SIZE_U32]);

  // Seems we can reuse the same workspace, its capacity is enough.
  size_t size = HUF_writeCTable_wksp(res.data(), res.size(), huf_ctable_.get(), table_max_symbol_,
                                     num_bits_, wrkspace.get(), kWspSize);
  CHECK(!HUF_isError(size));
  res.resize(size);
  return res;
}

// Copied from HUF_tightCompressBound.
size_t HuffmanEncoder::CompressedBound(size_t src_size) const {
  return ((src_size * num_bits_) >> 3) + 8;
}

bool HuffmanDecoder::Load(std::string_view binary_data, std::string* error_msg) {
  DCHECK(!huf_dtable_);
  huf_dtable_.reset(new HUF_DTable[HUF_DTABLE_SIZE(HUF_TABLELOG_MAX)]);
  huf_dtable_[0] = (HUF_TABLELOG_MAX - 1) * 0x01000001;  // some sort of magic number

  constexpr size_t kWspSize = HUF_DECOMPRESS_WORKSPACE_SIZE;
  unique_ptr<uint8_t[]> wrksp(new uint8_t[kWspSize]);

  size_t res = HUF_readDTableX1_wksp(huf_dtable_.get(), binary_data.data(), binary_data.size(),
                                     wrksp.get(), kWspSize, 0);
  if (HUF_isError(res)) {
    *error_msg = HUF_getErrorName(res);
    huf_dtable_.reset();
    return false;
  }
  if (res != binary_data.size()) {
    *error_msg = "Corrupted data";
    huf_dtable_.reset();
    return false;
  }
  return true;
}

bool HuffmanDecoder::Decode(std::string_view src, size_t dest_size, char* dest) const {
  DCHECK(huf_dtable_);
  size_t res =
      HUF_decompress1X_usingDTable(dest, dest_size, src.data(), src.size(), huf_dtable_.get(), 1);

  if (HUF_isError(res)) {
    LOG(FATAL) << "Failed to decompress: " << HUF_getErrorName(res);
    return false;
  }
  return true;
}

}  // namespace dfly
