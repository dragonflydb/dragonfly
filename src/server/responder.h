#pragma once

#include <atomic>

#include "base/logging.h"
#include "server/common.h"
#include "util/fibers/event_count.h"

namespace dfly {

class ConnectionContext;

class Responder {
 public:
  virtual ~Responder() = default;

  virtual bool Wait() = 0;

  virtual void Respond(ConnectionContext* cntx) = 0;
};

constexpr int kResponderSizeLimit = 64;

template <typename RT, typename... Args> RT* MakeResponder(absl::Span<char> buf, Args&&... args) {
  static_assert(sizeof(RT{}) < kResponderSizeLimit);

  size_t space = buf.size();
  void* ptr = buf.data();

  void* aligned = std::align(alignof(RT), sizeof(RT), ptr, space);
  DCHECK(aligned != nullptr);

  return new (aligned) RT{std::forward<Args>(args)...};
}

template <typename T> class SimpleResponder : public Responder {
 public:
  virtual ~SimpleResponder() override = default;

  void operator<<(T&& value);

  virtual bool Wait() override;  // Return for result portion, returns true when done.

  virtual void Respond(ConnectionContext* cntx) override;  // Send result portion.

 private:
  T value_;

  std::atomic_bool ready_;
  ::util::fibers_ext::EventCount ec_;
};

}  // namespace dfly
