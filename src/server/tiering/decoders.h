// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "core/compact_object.h"
#include "core/string_or_view.h"

namespace dfly::tiering {

// Decodes serialized value and provides it to callbacks.
// Acts as generic interface to callback driver (OpManager)
struct Decoder {
  virtual ~Decoder() = default;

  // Poor man's type-erasure copy
  virtual std::unique_ptr<Decoder> Clone() const = 0;

  // Initialize decoder from slice
  virtual void Initialize(std::string_view slice) = 0;

  // Store value in compact object
  virtual void Upload(CompactObj* obj) = 0;

  bool modified = false;           // Must be set if modified (not equal to original slice)
  size_t estimated_mem_usage = 0;  // Estimated usage if uploaded
};

// Basic "bare" decoder that just stores the provided slice
struct BareDecoder : public Decoder {
  std::unique_ptr<Decoder> Clone() const override;
  void Initialize(std::string_view slice) override;
  void Upload(CompactObj* obj) override;

  std::string_view slice;
};

struct StringDecoder : public Decoder {
  explicit StringDecoder(const CompactObj& obj);

  std::unique_ptr<Decoder> Clone() const override;
  void Initialize(std::string_view slice) override;
  void Upload(CompactObj* obj) override;

  std::string_view Read() const;
  std::string* Write();

 private:
  explicit StringDecoder(CompactObj::StrEncoding encoding);

  std::string_view slice_;
  CompactObj::StrEncoding encoding_;
  dfly::StringOrView value_;
};

}  // namespace dfly::tiering
