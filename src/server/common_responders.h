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

  virtual bool Wait() override;

  virtual void Respond(ConnectionContext* cntx) override;

 private:
  T value_;

  std::atomic_bool ready_{false};
  ::util::fibers_ext::EventCount ec_{};
};

class AtomicCounterResponder : public Responder {
 public:
  virtual ~AtomicCounterResponder() override = default;

  AtomicCounterResponder(Transaction* tx);

  void operator+=(long sum);

  virtual bool Wait() override;

  virtual void Respond(ConnectionContext* cntx) override;

 private:
  std::atomic_long cnt_;
  std::atomic_long left_;
  ::util::fibers_ext::EventCount ec_;
};

}  // namespace dfly
