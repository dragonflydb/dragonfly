1. To move lua_project to dragonfly from helio (DONE)
2. To limit lua stack to something reasonable like 4096.
3. To inject our own allocator to lua to track its memory.


## Object lifecycle and thread-safety.

Currently our transactional and locking model is based on an assumption that any READ or WRITE
access to objects must be performed in a shard where they belong.

However, this assumption can be relaxed to get significant gains for read-only queries.

### Explanation
Our transactional framework prevents from READ-locked objects to be mutated. It does not prevent from their PrimaryTable to grow or change, of course. These objects can move to different entries inside the table. However, our CompactObject maintains the following property - its reference CompactObject.AsRef() is valid no matter where the master object moves and it's valid and safe for reading even from other threads. The exception regarding thread safety is SmallString which uses translation table for its pointers.

If we change the SmallString translation table to be global and thread-safe (it should not have lots of write contention anyway) we may access primetable keys and values from another thread and write them directly to sockets.

Use-case: large strings that need to be copied. Sets that need to be serialized for SMEMBERS/HGETALL commands etc. Additional complexity - we will need to lock those variables even for single hop transactions and unlock them afterwards. The unlocking hop does not need to increase user-visible latency since it can be done after we send reply to the socket.