#pragma once

#include <atomic>

#include "base/logging.h"
#include "server/common.h"
#include "server/responder.h"
#include "util/fibers/event_count.h"

namespace dfly {

template <typename T> class SimpleResponder : public Responder {
 public:
  virtual ~SimpleResponder() override = default;

  void operator<<(T&& value);

  virtual void Respond(ConnectionContext* cntx) override;

 private:
  T value_;
};

class AtomicCounterResponder : public Responder {
 public:
  virtual ~AtomicCounterResponder() override = default;

  void operator+=(long sum);

  virtual void Respond(ConnectionContext* cntx) override;

 private:
  std::atomic_long cnt_{0};
};

}  // namespace dfly
