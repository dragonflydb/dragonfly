#include "server/hiredis_wrap.h"

#include <hiredis/read.h>

extern "C" {
#include <hiredis/hiredis.h>
}

namespace hiredis {

redisReader* redisReaderCreate() {
  return ::redisReaderCreate();
}

int redisReaderGetReply(redisReader* r, void** reply) {
  return ::redisReaderGetReply(r, reply);
}

void redisReaderFree(redisReader* r) {
  return ::redisReaderFree(r);
}

/* Function to free the reply objects hiredis returns by default. */
void freeReplyObject(void* reply) {
  return ::freeReplyObject(reply);
}

int redisReaderFeed(redisReader* r, const char* buf, size_t len) {
  return ::redisReaderFeed(r, buf, len);
}

}  // namespace hiredis
