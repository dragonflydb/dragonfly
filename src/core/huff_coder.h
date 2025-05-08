// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <memory>
#include <string_view>

namespace dfly {

class HuffmanEncoder {
 public:
  bool Build(const unsigned hist[], unsigned max_symbol, std::string* error_msg);

  bool Encode(std::string_view data, uint8_t* dest, uint32_t* dest_size,
              std::string* error_msg) const;

  size_t EstimateCompressedSize(const unsigned hist[], unsigned max_symbol) const;

  void Reset();

  // Load using the serialized data produced by Export().
  bool Load(std::string_view binary_data, std::string* error_msg);

  // Exports a binary representation of the table, that can be loaded using Load().
  std::string Export() const;

  uint8_t num_bits() const {
    return num_bits_;
  }

  bool valid() const {
    return bool(huf_ctable_);
  }

  unsigned max_symbol() const {
    return table_max_symbol_;
  }

  unsigned GetNBits(uint8_t symbol) const;

  // Estimation of the size of the destination buffer needed to store the compressed data.
  // destination of this size must be passed to Encode().
  size_t CompressedBound(size_t src_size) const;

 private:
  using HUF_CElt = size_t;
  std::unique_ptr<HUF_CElt[]> huf_ctable_;
  unsigned table_max_symbol_ = 0;
  uint8_t num_bits_ = 0;
};

class HuffmanDecoder {
 public:
  bool Load(std::string_view binary_data, std::string* error_msg);
  bool valid() const {
    return bool(huf_dtable_);
  }

  // decoded_size should be the *precise* size of the decoded data, otherwise the function will
  // fail. dest should point to a buffer of at least decoded_size bytes.
  // Returns true if decompression was successful, false if the data is corrupted.
  bool Decode(std::string_view src, size_t decoded_size, char* dest) const;

 private:
  using HUF_DTable = uint32_t;
  std::unique_ptr<HUF_DTable[]> huf_dtable_;
};

}  // namespace dfly
