/*
 * AFL++ Socket Fuzzing adapter for Dragonfly
 * Based on preeny desock but enhanced for real server applications
 *
 * How it works:
 * - accept() creates a socketpair and returns one end
 * - recv() on that fd reads from stdin (AFL++ input)
 * - send() writes to /dev/null (we ignore responses for fuzzing)
 * - This allows Dragonfly to handle "connections" fed by AFL++
 */

#define _GNU_SOURCE

#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

// Track if we've already accepted a connection
static int accepted_fd = -1;
static int stdin_consumed = 0;
static pthread_mutex_t accept_mutex = PTHREAD_MUTEX_INITIALIZER;

// Original functions
static int (*original_accept)(int, struct sockaddr*, socklen_t*) = NULL;
static int (*original_accept4)(int, struct sockaddr*, socklen_t*, int) = NULL;
static int (*original_bind)(int, const struct sockaddr*, socklen_t) = NULL;
static int (*original_listen)(int, int) = NULL;
static ssize_t (*original_recv)(int, void*, size_t, int) = NULL;
static ssize_t (*original_send)(int, const void*, size_t, int) = NULL;

__attribute__((constructor)) void dragonfly_socketfuzz_init() {
  original_accept = dlsym(RTLD_NEXT, "accept");
  original_accept4 = dlsym(RTLD_NEXT, "accept4");
  original_bind = dlsym(RTLD_NEXT, "bind");
  original_listen = dlsym(RTLD_NEXT, "listen");
  original_recv = dlsym(RTLD_NEXT, "recv");
  original_send = dlsym(RTLD_NEXT, "send");

  fprintf(stderr, "[socketfuzz] Dragonfly socket fuzzing initialized\n");
}

// Thread to feed stdin into socketpair - forward declaration
static void* feed_stdin_to_socket(void* arg);

// Intercept bind - just succeed without real bind
int bind(int sockfd, const struct sockaddr* addr, socklen_t addrlen) {
  if (addr && addr->sa_family == AF_INET) {
    struct sockaddr_in* in_addr = (struct sockaddr_in*)addr;
    fprintf(stderr, "[socketfuzz] Emulating bind on port %d\n", ntohs(in_addr->sin_port));
  }
  return 0;
}

// Intercept listen - just succeed
int listen(int sockfd, int backlog) {
  (void)sockfd;
  (void)backlog;
  fprintf(stderr, "[socketfuzz] Emulating listen\n");
  return 0;
}

// Shared implementation for accept/accept4
static int do_accept(struct sockaddr* addr, socklen_t* addrlen, int flags) {
  pthread_mutex_lock(&accept_mutex);

  // Only accept ONE connection - stdin data represents one client
  if (accepted_fd >= 0) {
    fprintf(stderr, "[socketfuzz] Already accepted, blocking further accepts...\n");
    pthread_mutex_unlock(&accept_mutex);
    // Block forever - we only handle one connection
    pause();
    return -1;
  }

  // Create a socketpair for bidirectional communication
  int sock_flags = SOCK_STREAM;
  if (flags & SOCK_NONBLOCK)
    sock_flags |= SOCK_NONBLOCK;
  if (flags & SOCK_CLOEXEC)
    sock_flags |= SOCK_CLOEXEC;

  int fds[2];
  if (socketpair(AF_UNIX, sock_flags, 0, fds) < 0) {
    fprintf(stderr, "[socketfuzz] socketpair() failed: %s\n", strerror(errno));
    pthread_mutex_unlock(&accept_mutex);
    return -1;
  }

  // fds[0] - we return this to Dragonfly
  // fds[1] - we'll use for reading stdin
  accepted_fd = fds[0];

  fprintf(stderr, "[socketfuzz] accept4() returning fd=%d (paired with %d)\n", fds[0], fds[1]);

  // Fill in fake peer address if requested
  if (addr && addrlen && *addrlen >= sizeof(struct sockaddr_in)) {
    struct sockaddr_in* in_addr = (struct sockaddr_in*)addr;
    memset(in_addr, 0, sizeof(*in_addr));
    in_addr->sin_family = AF_INET;
    in_addr->sin_port = htons(54321);
    inet_pton(AF_INET, "127.0.0.1", &in_addr->sin_addr);
    *addrlen = sizeof(struct sockaddr_in);
  }

  // Start thread to feed stdin into socketpair
  int write_fd = fds[1];
  pthread_t feeder_thread;
  pthread_create(&feeder_thread, NULL, feed_stdin_to_socket, (void*)(long)write_fd);
  pthread_detach(feeder_thread);

  pthread_mutex_unlock(&accept_mutex);
  return fds[0];
}

// Intercept accept
int accept(int sockfd, struct sockaddr* addr, socklen_t* addrlen) {
  (void)sockfd;
  return do_accept(addr, addrlen, 0);
}

// Intercept accept4 (used by Dragonfly)
int accept4(int sockfd, struct sockaddr* addr, socklen_t* addrlen, int flags) {
  (void)sockfd;
  return do_accept(addr, addrlen, flags);
}

// Thread to feed stdin into socketpair
static void* feed_stdin_to_socket(void* arg) {
  int write_fd = (int)(long)arg;
  fprintf(stderr, "[socketfuzz] Feeder thread started, reading stdin → fd=%d\n", write_fd);

  char buffer[4096];
  ssize_t n;

  while ((n = read(STDIN_FILENO, buffer, sizeof(buffer))) > 0) {
    ssize_t written = write(write_fd, buffer, n);
    if (written < 0) {
      fprintf(stderr, "[socketfuzz] write() to socket failed\n");
      break;
    }
    fprintf(stderr, "[socketfuzz] Fed %zd bytes from stdin\n", written);
  }

  fprintf(stderr, "[socketfuzz] stdin EOF, closing write end\n");
  fflush(stderr);
  close(write_fd);
  stdin_consumed = 1;

  // Give server time to process the data
  sleep(1);

  // Exit the entire process - fuzzing test case complete
  fprintf(stderr, "[socketfuzz] Test case complete, exiting...\n");
  fflush(stderr);
  _exit(0);
}

// Intercept recv - use real recv on socketpair
ssize_t recv(int sockfd, void* buf, size_t len, int flags) {
  // recv on our accepted fd works normally (it's a socketpair)
  ssize_t result = original_recv(sockfd, buf, len, flags);
  if (sockfd == accepted_fd && result > 0) {
    fprintf(stderr, "[socketfuzz] recv() returned %zd bytes on fd=%d\n", result, sockfd);
  }
  return result;
}

// Intercept send - write to /dev/null (ignore responses for fuzzing)
ssize_t send(int sockfd, const void* buf, size_t len, int flags) {
  if (sockfd == accepted_fd) {
    fprintf(stderr, "[socketfuzz] send() dropping %zu bytes on fd=%d\n", len, sockfd);
    return len;  // Pretend we sent it
  }
  // Other sockets - use real send
  return original_send(sockfd, buf, len, flags);
}

// Intercept setsockopt - just succeed
int setsockopt(int sockfd, int level, int optname, const void* optval, socklen_t optlen) {
  (void)sockfd;
  (void)level;
  (void)optname;
  (void)optval;
  (void)optlen;
  return 0;
}

// Intercept getpeername - return fake address
int getpeername(int sockfd, struct sockaddr* addr, socklen_t* addrlen) {
  if (sockfd == accepted_fd && addr && addrlen && *addrlen >= sizeof(struct sockaddr_in)) {
    struct sockaddr_in* in_addr = (struct sockaddr_in*)addr;
    memset(in_addr, 0, sizeof(*in_addr));
    in_addr->sin_family = AF_INET;
    in_addr->sin_port = htons(54321);
    inet_pton(AF_INET, "127.0.0.1", &in_addr->sin_addr);
    *addrlen = sizeof(struct sockaddr_in);
    return 0;
  }

  // Call original for other fds
  int (*original_getpeername)(int, struct sockaddr*, socklen_t*) = dlsym(RTLD_NEXT, "getpeername");
  return original_getpeername(sockfd, addr, addrlen);
}

// Intercept getsockname - return fake address
int getsockname(int sockfd, struct sockaddr* addr, socklen_t* addrlen) {
  if (addr && addrlen && *addrlen >= sizeof(struct sockaddr_in)) {
    struct sockaddr_in* in_addr = (struct sockaddr_in*)addr;
    memset(in_addr, 0, sizeof(*in_addr));
    in_addr->sin_family = AF_INET;
    in_addr->sin_port = htons(6379);
    inet_pton(AF_INET, "0.0.0.0", &in_addr->sin_addr);
    *addrlen = sizeof(struct sockaddr_in);
    return 0;
  }

  int (*original_getsockname)(int, struct sockaddr*, socklen_t*) = dlsym(RTLD_NEXT, "getsockname");
  return original_getsockname(sockfd, addr, addrlen);
}
