#include "server/responder.h"

#include "server/conn_context.h"

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

}  // namespace dfly
