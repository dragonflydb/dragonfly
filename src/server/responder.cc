#include "server/responder.h"

#include "server/transaction.h"

namespace dfly {

bool Responder::Wait(Transaction* tx) {
  tx->Wait();
  return true;
}

}  // namespace dfly
