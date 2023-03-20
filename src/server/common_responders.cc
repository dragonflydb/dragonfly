#include "server/common_responders.h"

#include "server/conn_context.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;

template <typename T> void SimpleResponder<T>::operator<<(T value) {
  value_ = move(value);
}

template <> void SimpleResponder<long>::Respond(ConnectionContext* cntx) {
  (*cntx)->SendLong(value_);
}

template class SimpleResponder<long>;

void AtomicCounterResponder::operator+=(long diff) {
  cnt_.fetch_add(diff, memory_order_relaxed);
}

void AtomicCounterResponder::Respond(ConnectionContext* cntx) {
  (*cntx)->SendLong(cnt_.load(memory_order_relaxed));
}

}  // namespace dfly
