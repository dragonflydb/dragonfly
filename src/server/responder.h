#pragma once

#include <atomic>
#include <memory>

#include "absl/types/span.h"
#include "base/logging.h"

namespace dfly {

class ConnectionContext;
class Transaction;

class Responder {
 public:
  virtual ~Responder() = default;

  virtual bool Wait(Transaction* tx);  // Block until result portion, returns true when done.

  virtual void Respond(ConnectionContext* cntx) = 0;  // Send result portion.
};

constexpr int kResponderSizeLimit = 64;

template <typename RT, typename... Args> RT* MakeResponder(absl::Span<char> buf, Args&&... args) {
  static_assert(sizeof(RT) < kResponderSizeLimit);

  size_t space = buf.size();
  void* ptr = buf.data();

  void* aligned = std::align(alignof(RT), sizeof(RT), ptr, space);
  DCHECK(aligned != nullptr);

  return new (aligned) RT{std::forward<Args>(args)...};
}

}  // namespace dfly
