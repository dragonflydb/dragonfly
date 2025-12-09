#pragma once

#include <stddef.h>
#include <string.h>
#include <sys/types.h>

struct redisReader;
struct redisReply;

namespace hiredis {

redisReader* redisReaderCreate();
int redisReaderFeed(redisReader* r, const char* buf, size_t len);
int redisReaderGetReply(redisReader* r, void** reply);
void redisReaderFree(redisReader* r);
/* Function to free the reply objects hiredis returns by default. */
void freeReplyObject(void* reply);

}  // namespace hiredis
