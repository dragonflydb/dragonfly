#include "server/common_responders.h"

#include "server/conn_context.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;

template <typename T> void SimpleResponder<T>::operator<<(T&& value) {
  DCHECK(!ready_.load(memory_order_relaxed));
  value_ = move(value);
  ready_.store(true, memory_order_relaxed);
  ec_.notifyAll();
}

template <typename T> bool SimpleResponder<T>::Wait() {
  ec_.await([this] { return ready_.load(memory_order_relaxed); });
  return true;
}

template <> void SimpleResponder<long>::Respond(ConnectionContext* cntx) {
  (*cntx)->SendLong(value_);
}

template class SimpleResponder<long>;

AtomicCounterResponder::AtomicCounterResponder(Transaction* tx)
    : cnt_{0}, left_{tx->GetUniqueShardCnt()}, ec_{} {
}

void AtomicCounterResponder::operator+=(long diff) {
  cnt_.fetch_add(diff, memory_order_relaxed);
  if (left_.fetch_sub(1, memory_order_release))
    ec_.notifyAll();
}

bool AtomicCounterResponder::Wait() {
  return ec_.await([this] { return left_.load(memory_order_relaxed) <= 0; });
}

void AtomicCounterResponder::Respond(ConnectionContext* cntx) {
  (*cntx)->SendLong(cnt_.load(memory_order_relaxed));
}

}  // namespace dfly
