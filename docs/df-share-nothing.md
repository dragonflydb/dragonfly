# Dragonfly Architecture

Dragonfly is a modern replacement for memory stores like Redis and Memcached. It scales vertically on a single instance to support millions of requests per second. It is more memory efficient, has been designed with reliability in mind, and includes a better caching design.

## Threading model

Dragonfly uses a single process with a multiple-thread architecture. Each Dragonfly thread is indirectly assigned several responsibilities via fibers.

One such responsibility is handling incoming connections. Once a socket listener accepts a client connection, the connection spends its entire lifetime bound to a single thread inside a fiber. Dragonfly is written to be 100% non-blocking; it uses fibers to provide asynchronicity in each thread. One of the essential properties of asynchronicity is that a thread cannot be blocked as long as it has pending CPU tasks. Dragonfly preserves this property by wrapping each unit of execution context in a fiber; we wrap units of execution that can potentially be blocked on I/O. For example, a connection loop runs within a fiber; a function that writes a snapshot runs inside a fiber, and so on.

As a side comment - asynchronicity and parallelism are different terms. Nodejs, for example, provides asynchronous execution but is single-threaded. Similarly, each Dragonfly thread is asynchronous on its own; therefore, Dragonfly is responsive to incoming events even when it handles long-running commands like saving to disk or running Lua scripts.


### Thread actors in DF

The DF in-memory database is sharded into `N` parts, where `N` is less or equal to the number of threads in the system. Each database shard is owned and accessed by a single thread.
The same thread can handle TCP connections and simultaneously host a database shard.
See the diagram below.


<br>
<img src="http://static.dragonflydb.io/repo-assets/thread-per-core.svg" border="0"/>

Here, our DF process spawns 4 threads, where threads 1 through 3 handle I/O (i.e., manage client connections) and threads 2 through 4 manage DB shards. Thread 2, for example, divides its CPU time between handling incoming requests and processing DB operations on the shard it owns.

So when we say that thread 1 is an I/O thread, we mean that Dragonfly can pin fibers that manage client connections to thread 1. In general, any thread can have many responsibilities that require CPU time; database management and connection handling are only two of those responsibilities.


## Fibers

I suggest reading my [intro post](https://www.romange.com/2018/12/15/introduction-to-fibers-in-c-/) about `Boost.Fibers` to learn more about fibers.

By the way, I want to compliment `Boost.Fibers` library–it has been exceptionally well designed:
it's unintrusive, lightweight, and efficient. Moreover, its default scheduler can be overridden. In the case of `helio`, the I/O library that powers Dragonfly, we overrode the `Boost.Fibers` scheduler to support shared-nothing architecture and integrate it with the I/O polling loop.

Importantly, fibers require bottom-up support in the application layer to preserve their asynchronicity. For example, in the snippet below, a blocking write into `fd` won't magically allow a fiber to preempt and switch to another fiber. No, the whole thread will be blocked.


```cpp
...
write(fd, buf, 1000000);

...
pthread_mutex_lock(...);

```

Similarly, with a `pthread_mutex_lock` call, the whole thread might be blocked, wasting precious CPU time.. Therefore, the Dragonfly code uses *fiber-friendly* primitives for I/O, communication, and coordination. These primitives are supplied by the `helio` and `Boost.Fibers` libraries.

## Life of a command request

This section explains how Dragonfly handles a command in the context of shared-nothing architecture. In most architectures used today, multi-threaded servers use mutex locks to protect their data structures, but Dragonfly does not. Why is this?

Inter-thread interactions in Dragonfly occur only via passing messages from thread to thread. For example, consider the following sequence diagram of handling a SET request:


```uml
@startuml

actor       User       as A1
boundary    connection  as B1
entity      "Shard K"   as E1
A1 ->  B1 : SET KEY VAL
B1 -> E1 : SET KEY VAL / k = HASH(KEY) % N
E1 -> B1 : OK
B1 -> A1 : Response

@enduml
```

<img src="https://www.plantuml.com/plantuml/svg/NOn12m8X48Nl_eh7Gb272Az1WGl2Wb6G5NGqLsW9PaBjqBzlL-lId6Q-zxvnFdD4dNCAlzKbA2bk_ABUnJS0U2OAFWzC9Msb29I7N3AWiNSNUvYckbeA9R7SOknX3QjFCFgAYzg9jd3zXx720njqodRp4IqmmrxegLe_7CnNLDDr3Ed9bC87"/>

Here, a connection fiber resides in a thread different from one that handles the `KEY` entity. We use hashing to decide which shard owns which key.

Another way to think of this flow is that a connection fiber serves as a coordinator for issuing transactional commands to other threads. In this simple example, the external "SET" command requires a single message passed from the coordinator to the destination shard thread. When we think of the Dragonfly model in the context of a single command request, I prefer to use the following diagram instead of the [one above](#thread-actors-in-df).

<br>
<img src="http://static.dragonflydb.io/repo-assets/coordinator.svg" border="0"/>

Here, a coordinator (or connection fiber) might even reside on one of the threads that coincidently owns one of the shards. However, it is easier to think of it as a separate entity that never directly accesses any shard data.

The coordinator serves as a virtualization layer that hides all the complexity of talking to multiple shards. It employs start-of-the-art algorithms to provide atomicity (and strict serializability) semantics for multi-key commands like "mset, mget, and blpop." It also offers strict serializability for Lua scripts and multi-command transactions.

Hiding such complexity is valuable to the end customer, but it comes with some CPU and latency costs. We believe the trade-off is worthwhile given the value that Dragonfly provides.

If you want to deep dive into Dragonfly architecture without the complexities of transactional code, it's worth checking [Midi Redis](https://github.com/romange/midi-redis/),
which implements a toy backend supporting `PING`, `SET`, and `GET` [commands](https://github.com/romange/midi-redis/blob/main/server/main_service.cc#L239).

In fact, Dragonfly grew from that project; they share a common commit history.

By the way, to learn how to build even simpler TCP backends than `midi-redis`, `helio` library provides sample backends like these: [echo_server](https://github.com/romange/helio/blob/master/examples/echo_server.cc) and [ping_iouring_server.cc](https://github.com/romange/helio/blob/master/examples/pingserver/ping_iouring_server.cc). These backends reach millions of QPS on multi-core servers much like Dragonfly and midi-redis do.
