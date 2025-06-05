// ©2013-2016 Cameron Desrochers.
// Distributed under the simplified BSD license (see the license file that
// should have come with this header).
// Uses Jeff Preshing's semaphore implementation (under the terms of its
// separate zlib license, embedded below).

#pragma once

// Provides portable (VC++2010+, Intel ICC 13, GCC 4.7+, and anything C++11 compliant)
// implementation of low-level memory barriers, plus a few semi-portable utility macros (for
// inlining and alignment). Also has a basic atomic type (limited to hardware-supported atomics with
// no memory ordering guarantees). Uses the AE_* prefix for macros (historical reasons), and the
// "moodycamel" namespace for symbols.

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <ctime>
#include <type_traits>

// Platform detection
#if defined(__INTEL_COMPILER)
#define AE_ICC
#elif defined(_MSC_VER)
#define AE_VCPP
#elif defined(__GNUC__)
#define AE_GCC
#endif

#if defined(_M_IA64) || defined(__ia64__)
#define AE_ARCH_IA64
#elif defined(_WIN64) || defined(__amd64__) || defined(_M_X64) || defined(__x86_64__)
#define AE_ARCH_X64
#elif defined(_M_IX86) || defined(__i386__)
#define AE_ARCH_X86
#elif defined(_M_PPC) || defined(__powerpc__)
#define AE_ARCH_PPC
#else
#define AE_ARCH_UNKNOWN
#endif

// AE_UNUSED
#define AE_UNUSED(x) ((void)x)

// AE_NO_TSAN/AE_TSAN_ANNOTATE_*
// For GCC
#if defined(__SANITIZE_THREAD__)
#define AE_TSAN_IS_ENABLED
#endif
// For clang
#if defined(__has_feature)
#if __has_feature(thread_sanitizer) && !defined(AE_TSAN_IS_ENABLED)
#define AE_TSAN_IS_ENABLED
#endif
#endif

#ifdef AE_TSAN_IS_ENABLED
#if __cplusplus >= 201703L  // inline variables require C++17
namespace moodycamel {
inline int ae_tsan_global;
}
#define AE_TSAN_ANNOTATE_RELEASE() \
  AnnotateHappensBefore(__FILE__, __LINE__, (void*)(&::moodycamel::ae_tsan_global))
#define AE_TSAN_ANNOTATE_ACQUIRE() \
  AnnotateHappensAfter(__FILE__, __LINE__, (void*)(&::moodycamel::ae_tsan_global))
extern "C" void AnnotateHappensBefore(const char*, int, void*);
extern "C" void AnnotateHappensAfter(const char*, int, void*);
#else  // when we can't work with tsan, attempt to disable its warnings
#define AE_NO_TSAN __attribute__((no_sanitize("thread")))
#endif
#endif

#ifndef AE_NO_TSAN
#define AE_NO_TSAN
#endif

#ifndef AE_TSAN_ANNOTATE_RELEASE
#define AE_TSAN_ANNOTATE_RELEASE()
#define AE_TSAN_ANNOTATE_ACQUIRE()
#endif

// AE_FORCEINLINE
#if defined(AE_VCPP) || defined(AE_ICC)
#define AE_FORCEINLINE __forceinline
#elif defined(AE_GCC)
//#define AE_FORCEINLINE __attribute__((always_inline))
#define AE_FORCEINLINE inline
#else
#define AE_FORCEINLINE inline
#endif

// AE_ALIGN
#if defined(AE_VCPP) || defined(AE_ICC)
#define AE_ALIGN(x) __declspec(align(x))
#elif defined(AE_GCC)
#define AE_ALIGN(x) __attribute__((aligned(x)))
#else
// Assume GCC compliant syntax...
#define AE_ALIGN(x) __attribute__((aligned(x)))
#endif

// Portable atomic fences implemented below:

namespace moodycamel {

enum memory_order {
  memory_order_relaxed,
  memory_order_acquire,
  memory_order_release,
  memory_order_acq_rel,
  memory_order_seq_cst,

  // memory_order_sync: Forces a full sync:
  // #LoadLoad, #LoadStore, #StoreStore, and most significantly, #StoreLoad
  memory_order_sync = memory_order_seq_cst
};

}  // end namespace moodycamel

#if (defined(AE_VCPP) && (_MSC_VER < 1700 || defined(__cplusplus_cli))) || \
    (defined(AE_ICC) && __INTEL_COMPILER < 1600)
// VS2010 and ICC13 don't support std::atomic_*_fence, implement our own fences

#include <intrin.h>

#if defined(AE_ARCH_X64) || defined(AE_ARCH_X86)
#define AeFullSync _mm_mfence
#define AeLiteSync _mm_mfence
#elif defined(AE_ARCH_IA64)
#define AeFullSync __mf
#define AeLiteSync __mf
#elif defined(AE_ARCH_PPC)
#include <ppcintrinsics.h>
#define AeFullSync __sync
#define AeLiteSync __lwsync
#endif

#ifdef AE_VCPP
#pragma warning(push)
#pragma warning(disable : 4365)  // Disable erroneous 'conversion from long to unsigned int,
                                 // signed/unsigned mismatch' error when using `assert`
#ifdef __cplusplus_cli
#pragma managed(push, off)
#endif
#endif

namespace moodycamel {

AE_FORCEINLINE void compiler_fence(memory_order order) AE_NO_TSAN {
  switch (order) {
    case memory_order_relaxed:
      break;
    case memory_order_acquire:
      _ReadBarrier();
      break;
    case memory_order_release:
      _WriteBarrier();
      break;
    case memory_order_acq_rel:
      _ReadWriteBarrier();
      break;
    case memory_order_seq_cst:
      _ReadWriteBarrier();
      break;
    default:
      assert(false);
  }
}

// x86/x64 have a strong memory model -- all loads and stores have
// acquire and release semantics automatically (so only need compiler
// barriers for those).
#if defined(AE_ARCH_X86) || defined(AE_ARCH_X64)
AE_FORCEINLINE void fence(memory_order order) AE_NO_TSAN {
  switch (order) {
    case memory_order_relaxed:
      break;
    case memory_order_acquire:
      _ReadBarrier();
      break;
    case memory_order_release:
      _WriteBarrier();
      break;
    case memory_order_acq_rel:
      _ReadWriteBarrier();
      break;
    case memory_order_seq_cst:
      _ReadWriteBarrier();
      AeFullSync();
      _ReadWriteBarrier();
      break;
    default:
      assert(false);
  }
}
#else
AE_FORCEINLINE void fence(memory_order order) AE_NO_TSAN {
  // Non-specialized arch, use heavier memory barriers everywhere just in case :-(
  switch (order) {
    case memory_order_relaxed:
      break;
    case memory_order_acquire:
      _ReadBarrier();
      AeLiteSync();
      _ReadBarrier();
      break;
    case memory_order_release:
      _WriteBarrier();
      AeLiteSync();
      _WriteBarrier();
      break;
    case memory_order_acq_rel:
      _ReadWriteBarrier();
      AeLiteSync();
      _ReadWriteBarrier();
      break;
    case memory_order_seq_cst:
      _ReadWriteBarrier();
      AeFullSync();
      _ReadWriteBarrier();
      break;
    default:
      assert(false);
  }
}
#endif
}  // end namespace moodycamel
#else
// Use standard library of atomics
#include <atomic>

namespace moodycamel {

AE_FORCEINLINE void compiler_fence(memory_order order) AE_NO_TSAN {
  switch (order) {
    case memory_order_relaxed:
      break;
    case memory_order_acquire:
      std::atomic_signal_fence(std::memory_order_acquire);
      break;
    case memory_order_release:
      std::atomic_signal_fence(std::memory_order_release);
      break;
    case memory_order_acq_rel:
      std::atomic_signal_fence(std::memory_order_acq_rel);
      break;
    case memory_order_seq_cst:
      std::atomic_signal_fence(std::memory_order_seq_cst);
      break;
    default:
      assert(false);
  }
}

AE_FORCEINLINE void fence(memory_order order) AE_NO_TSAN {
  switch (order) {
    case memory_order_relaxed:
      break;
    case memory_order_acquire:
      AE_TSAN_ANNOTATE_ACQUIRE();
      std::atomic_thread_fence(std::memory_order_acquire);
      break;
    case memory_order_release:
      AE_TSAN_ANNOTATE_RELEASE();
      std::atomic_thread_fence(std::memory_order_release);
      break;
    case memory_order_acq_rel:
      AE_TSAN_ANNOTATE_ACQUIRE();
      AE_TSAN_ANNOTATE_RELEASE();
      std::atomic_thread_fence(std::memory_order_acq_rel);
      break;
    case memory_order_seq_cst:
      AE_TSAN_ANNOTATE_ACQUIRE();
      AE_TSAN_ANNOTATE_RELEASE();
      std::atomic_thread_fence(std::memory_order_seq_cst);
      break;
    default:
      assert(false);
  }
}

}  // end namespace moodycamel

#endif

#if !defined(AE_VCPP) || (_MSC_VER >= 1700 && !defined(__cplusplus_cli))
#define AE_USE_STD_ATOMIC_FOR_WEAK_ATOMIC
#endif

#ifdef AE_USE_STD_ATOMIC_FOR_WEAK_ATOMIC
#include <atomic>
#endif
#include <utility>

// WARNING: *NOT* A REPLACEMENT FOR std::atomic. READ CAREFULLY:
// Provides basic support for atomic variables -- no memory ordering guarantees are provided.
// The guarantee of atomicity is only made for types that already have atomic load and store
// guarantees at the hardware level -- on most platforms this generally means aligned pointers and
// integers (only).
namespace moodycamel {
template <typename T> class weak_atomic {
 public:
  AE_NO_TSAN weak_atomic() : value() {
  }
#ifdef AE_VCPP
#pragma warning(push)
#pragma warning(disable : 4100)  // Get rid of (erroneous) 'unreferenced formal parameter' warning
#endif
  template <typename U> AE_NO_TSAN weak_atomic(U&& x) : value(std::forward<U>(x)) {
  }
#ifdef __cplusplus_cli
  // Work around bug with universal reference/nullptr combination that only appears when /clr is on
  AE_NO_TSAN weak_atomic(nullptr_t) : value(nullptr) {
  }
#endif
  AE_NO_TSAN weak_atomic(weak_atomic const& other) : value(other.load()) {
  }
  AE_NO_TSAN weak_atomic(weak_atomic&& other) : value(std::move(other.load())) {
  }
#ifdef AE_VCPP
#pragma warning(pop)
#endif

  AE_FORCEINLINE operator T() const AE_NO_TSAN {
    return load();
  }

#ifndef AE_USE_STD_ATOMIC_FOR_WEAK_ATOMIC
  template <typename U> AE_FORCEINLINE weak_atomic const& operator=(U&& x) AE_NO_TSAN {
    value = std::forward<U>(x);
    return *this;
  }
  AE_FORCEINLINE weak_atomic const& operator=(weak_atomic const& other) AE_NO_TSAN {
    value = other.value;
    return *this;
  }

  AE_FORCEINLINE T load() const AE_NO_TSAN {
    return value;
  }

  AE_FORCEINLINE T fetch_add_acquire(T increment) AE_NO_TSAN {
#if defined(AE_ARCH_X64) || defined(AE_ARCH_X86)
    if (sizeof(T) == 4)
      return _InterlockedExchangeAdd((long volatile*)&value, (long)increment);
#if defined(_M_AMD64)
    else if (sizeof(T) == 8)
      return _InterlockedExchangeAdd64((long long volatile*)&value, (long long)increment);
#endif
#else
#error Unsupported platform
#endif
    assert(false && "T must be either a 32 or 64 bit type");
    return value;
  }

  AE_FORCEINLINE T fetch_add_release(T increment) AE_NO_TSAN {
#if defined(AE_ARCH_X64) || defined(AE_ARCH_X86)
    if (sizeof(T) == 4)
      return _InterlockedExchangeAdd((long volatile*)&value, (long)increment);
#if defined(_M_AMD64)
    else if (sizeof(T) == 8)
      return _InterlockedExchangeAdd64((long long volatile*)&value, (long long)increment);
#endif
#else
#error Unsupported platform
#endif
    assert(false && "T must be either a 32 or 64 bit type");
    return value;
  }
#else
  template <typename U> AE_FORCEINLINE weak_atomic const& operator=(U&& x) AE_NO_TSAN {
    value.store(std::forward<U>(x), std::memory_order_relaxed);
    return *this;
  }

  AE_FORCEINLINE weak_atomic const& operator=(weak_atomic const& other) AE_NO_TSAN {
    value.store(other.value.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }

  AE_FORCEINLINE T load() const AE_NO_TSAN {
    return value.load(std::memory_order_relaxed);
  }

  AE_FORCEINLINE T fetch_add_acquire(T increment) AE_NO_TSAN {
    return value.fetch_add(increment, std::memory_order_acquire);
  }

  AE_FORCEINLINE T fetch_add_release(T increment) AE_NO_TSAN {
    return value.fetch_add(increment, std::memory_order_release);
  }
#endif

 private:
#ifndef AE_USE_STD_ATOMIC_FOR_WEAK_ATOMIC
  // No std::atomic support, but still need to circumvent compiler optimizations.
  // `volatile` will make memory access slow, but is guaranteed to be reliable.
  volatile T value;
#else
  std::atomic<T> value;
#endif
};

}  // end namespace moodycamel

// Portable single-producer, single-consumer semaphore below:

#if defined(_WIN32)
// Avoid including windows.h in a header; we only need a handful of
// items, so we'll redeclare them here (this is relatively safe since
// the API generally has to remain stable between Windows versions).
// I know this is an ugly hack but it still beats polluting the global
// namespace with thousands of generic names or adding a .cpp for nothing.
extern "C" {
struct _SECURITY_ATTRIBUTES;
__declspec(dllimport) void* __stdcall CreateSemaphoreW(_SECURITY_ATTRIBUTES* lpSemaphoreAttributes,
                                                       long lInitialCount, long lMaximumCount,
                                                       const wchar_t* lpName);
__declspec(dllimport) int __stdcall CloseHandle(void* hObject);
__declspec(dllimport) unsigned long __stdcall WaitForSingleObject(void* hHandle,
                                                                  unsigned long dwMilliseconds);
__declspec(dllimport) int __stdcall ReleaseSemaphore(void* hSemaphore, long lReleaseCount,
                                                     long* lpPreviousCount);
}
#elif defined(__MACH__)
#include <mach/mach.h>
#elif defined(__unix__)
#include <semaphore.h>
#elif defined(FREERTOS)
#include <FreeRTOS.h>
#include <semphr.h>
#include <task.h>
#endif

namespace moodycamel {
// Code in the spsc_sema namespace below is an adaptation of Jeff Preshing's
// portable + lightweight semaphore implementations, originally from
// https://github.com/preshing/cpp11-on-multicore/blob/master/common/sema.h
// LICENSE:
// Copyright (c) 2015 Jeff Preshing
//
// This software is provided 'as-is', without any express or implied
// warranty. In no event will the authors be held liable for any damages
// arising from the use of this software.
//
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it
// freely, subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not
//    claim that you wrote the original software. If you use this software
//    in a product, an acknowledgement in the product documentation would be
//    appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be
//    misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.
namespace spsc_sema {
#if defined(_WIN32)
class Semaphore {
 private:
  void* m_hSema;

  Semaphore(const Semaphore& other);
  Semaphore& operator=(const Semaphore& other);

 public:
  AE_NO_TSAN Semaphore(int initialCount = 0) : m_hSema() {
    assert(initialCount >= 0);
    const long maxLong = 0x7fffffff;
    m_hSema = CreateSemaphoreW(nullptr, initialCount, maxLong, nullptr);
    assert(m_hSema);
  }

  AE_NO_TSAN ~Semaphore() {
    CloseHandle(m_hSema);
  }

  bool wait() AE_NO_TSAN {
    const unsigned long infinite = 0xffffffff;
    return WaitForSingleObject(m_hSema, infinite) == 0;
  }

  bool try_wait() AE_NO_TSAN {
    return WaitForSingleObject(m_hSema, 0) == 0;
  }

  bool timed_wait(std::uint64_t usecs) AE_NO_TSAN {
    return WaitForSingleObject(m_hSema, (unsigned long)(usecs / 1000)) == 0;
  }

  void signal(int count = 1) AE_NO_TSAN {
    while (!ReleaseSemaphore(m_hSema, count, nullptr))
      ;
  }
};
#elif defined(__MACH__)
//---------------------------------------------------------
// Semaphore (Apple iOS and OSX)
// Can't use POSIX semaphores due to
// http://lists.apple.com/archives/darwin-kernel/2009/Apr/msg00010.html
//---------------------------------------------------------
class Semaphore {
 private:
  semaphore_t m_sema;

  Semaphore(const Semaphore& other);
  Semaphore& operator=(const Semaphore& other);

 public:
  AE_NO_TSAN Semaphore(int initialCount = 0) : m_sema() {
    assert(initialCount >= 0);
    kern_return_t rc = semaphore_create(mach_task_self(), &m_sema, SYNC_POLICY_FIFO, initialCount);
    assert(rc == KERN_SUCCESS);
    AE_UNUSED(rc);
  }

  AE_NO_TSAN ~Semaphore() {
    semaphore_destroy(mach_task_self(), m_sema);
  }

  bool wait() AE_NO_TSAN {
    return semaphore_wait(m_sema) == KERN_SUCCESS;
  }

  bool try_wait() AE_NO_TSAN {
    return timed_wait(0);
  }

  bool timed_wait(std::uint64_t timeout_usecs) AE_NO_TSAN {
    mach_timespec_t ts;
    ts.tv_sec = static_cast<unsigned int>(timeout_usecs / 1000000);
    ts.tv_nsec = static_cast<int>((timeout_usecs % 1000000) * 1000);

    // added in OSX 10.10:
    // https://developer.apple.com/library/prerelease/mac/documentation/General/Reference/APIDiffsMacOSX10_10SeedDiff/modules/Darwin.html
    kern_return_t rc = semaphore_timedwait(m_sema, ts);
    return rc == KERN_SUCCESS;
  }

  void signal() AE_NO_TSAN {
    while (semaphore_signal(m_sema) != KERN_SUCCESS)
      ;
  }

  void signal(int count) AE_NO_TSAN {
    while (count-- > 0) {
      while (semaphore_signal(m_sema) != KERN_SUCCESS)
        ;
    }
  }
};
#elif defined(__unix__)
//---------------------------------------------------------
// Semaphore (POSIX, Linux)
//---------------------------------------------------------
class Semaphore {
 private:
  sem_t m_sema;

  Semaphore(const Semaphore& other);
  Semaphore& operator=(const Semaphore& other);

 public:
  AE_NO_TSAN Semaphore(int initialCount = 0) : m_sema() {
    assert(initialCount >= 0);
    int rc = sem_init(&m_sema, 0, static_cast<unsigned int>(initialCount));
    assert(rc == 0);
    AE_UNUSED(rc);
  }

  AE_NO_TSAN ~Semaphore() {
    sem_destroy(&m_sema);
  }

  bool wait() AE_NO_TSAN {
    // http://stackoverflow.com/questions/2013181/gdb-causes-sem-wait-to-fail-with-eintr-error
    int rc;
    do {
      rc = sem_wait(&m_sema);
    } while (rc == -1 && errno == EINTR);
    return rc == 0;
  }

  bool try_wait() AE_NO_TSAN {
    int rc;
    do {
      rc = sem_trywait(&m_sema);
    } while (rc == -1 && errno == EINTR);
    return rc == 0;
  }

  bool timed_wait(std::uint64_t usecs) AE_NO_TSAN {
    struct timespec ts;
    const int usecs_in_1_sec = 1000000;
    const int nsecs_in_1_sec = 1000000000;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += static_cast<time_t>(usecs / usecs_in_1_sec);
    ts.tv_nsec += static_cast<long>(usecs % usecs_in_1_sec) * 1000;
    // sem_timedwait bombs if you have more than 1e9 in tv_nsec
    // so we have to clean things up before passing it in
    if (ts.tv_nsec >= nsecs_in_1_sec) {
      ts.tv_nsec -= nsecs_in_1_sec;
      ++ts.tv_sec;
    }

    int rc;
    do {
      rc = sem_timedwait(&m_sema, &ts);
    } while (rc == -1 && errno == EINTR);
    return rc == 0;
  }

  void signal() AE_NO_TSAN {
    while (sem_post(&m_sema) == -1)
      ;
  }

  void signal(int count) AE_NO_TSAN {
    while (count-- > 0) {
      while (sem_post(&m_sema) == -1)
        ;
    }
  }
};
#elif defined(FREERTOS)
//---------------------------------------------------------
// Semaphore (FreeRTOS)
//---------------------------------------------------------
class Semaphore {
 private:
  SemaphoreHandle_t m_sema;

  Semaphore(const Semaphore& other);
  Semaphore& operator=(const Semaphore& other);

 public:
  AE_NO_TSAN Semaphore(int initialCount = 0) : m_sema() {
    assert(initialCount >= 0);
    m_sema = xSemaphoreCreateCounting(static_cast<UBaseType_t>(~0ull),
                                      static_cast<UBaseType_t>(initialCount));
    assert(m_sema);
  }

  AE_NO_TSAN ~Semaphore() {
    vSemaphoreDelete(m_sema);
  }

  bool wait() AE_NO_TSAN {
    return xSemaphoreTake(m_sema, portMAX_DELAY) == pdTRUE;
  }

  bool try_wait() AE_NO_TSAN {
    // Note: In an ISR context, if this causes a task to unblock,
    // the caller won't know about it
    if (xPortIsInsideInterrupt())
      return xSemaphoreTakeFromISR(m_sema, NULL) == pdTRUE;
    return xSemaphoreTake(m_sema, 0) == pdTRUE;
  }

  bool timed_wait(std::uint64_t usecs) AE_NO_TSAN {
    std::uint64_t msecs = usecs / 1000;
    TickType_t ticks = static_cast<TickType_t>(msecs / portTICK_PERIOD_MS);
    if (ticks == 0)
      return try_wait();
    return xSemaphoreTake(m_sema, ticks) == pdTRUE;
  }

  void signal() AE_NO_TSAN {
    // Note: In an ISR context, if this causes a task to unblock,
    // the caller won't know about it
    BaseType_t rc;
    if (xPortIsInsideInterrupt())
      rc = xSemaphoreGiveFromISR(m_sema, NULL);
    else
      rc = xSemaphoreGive(m_sema);
    assert(rc == pdTRUE);
    AE_UNUSED(rc);
  }

  void signal(int count) AE_NO_TSAN {
    while (count-- > 0)
      signal();
  }
};
#else
#error Unsupported platform! (No semaphore wrapper available)
#endif

//---------------------------------------------------------
// LightweightSemaphore
//---------------------------------------------------------
class LightweightSemaphore {
 public:
  typedef std::make_signed<std::size_t>::type ssize_t;

 private:
  weak_atomic<ssize_t> m_count;
  Semaphore m_sema;

  bool waitWithPartialSpinning(std::int64_t timeout_usecs = -1) AE_NO_TSAN {
    ssize_t oldCount;
    // Is there a better way to set the initial spin count?
    // If we lower it to 1000, testBenaphore becomes 15x slower on my Core i7-5930K Windows PC,
    // as threads start hitting the kernel semaphore.
    int spin = 1024;
    while (--spin >= 0) {
      if (m_count.load() > 0) {
        m_count.fetch_add_acquire(-1);
        return true;
      }
      compiler_fence(memory_order_acquire);  // Prevent the compiler from collapsing the loop.
    }
    oldCount = m_count.fetch_add_acquire(-1);
    if (oldCount > 0)
      return true;
    if (timeout_usecs < 0) {
      if (m_sema.wait())
        return true;
    }
    if (timeout_usecs > 0 && m_sema.timed_wait(static_cast<uint64_t>(timeout_usecs)))
      return true;
    // At this point, we've timed out waiting for the semaphore, but the
    // count is still decremented indicating we may still be waiting on
    // it. So we have to re-adjust the count, but only if the semaphore
    // wasn't signaled enough times for us too since then. If it was, we
    // need to release the semaphore too.
    while (true) {
      oldCount = m_count.fetch_add_release(1);
      if (oldCount < 0)
        return false;  // successfully restored things to the way they were
      // Oh, the producer thread just signaled the semaphore after all. Try again:
      oldCount = m_count.fetch_add_acquire(-1);
      if (oldCount > 0 && m_sema.try_wait())
        return true;
    }
  }

 public:
  AE_NO_TSAN LightweightSemaphore(ssize_t initialCount = 0) : m_count(initialCount), m_sema() {
    assert(initialCount >= 0);
  }

  bool tryWait() AE_NO_TSAN {
    if (m_count.load() > 0) {
      m_count.fetch_add_acquire(-1);
      return true;
    }
    return false;
  }

  bool wait() AE_NO_TSAN {
    return tryWait() || waitWithPartialSpinning();
  }

  bool wait(std::int64_t timeout_usecs) AE_NO_TSAN {
    return tryWait() || waitWithPartialSpinning(timeout_usecs);
  }

  void signal(ssize_t count = 1) AE_NO_TSAN {
    assert(count >= 0);
    ssize_t oldCount = m_count.fetch_add_release(count);
    assert(oldCount >= -1);
    if (oldCount < 0) {
      m_sema.signal(1);
    }
  }

  std::size_t availableApprox() const AE_NO_TSAN {
    ssize_t count = m_count.load();
    return count > 0 ? static_cast<std::size_t>(count) : 0;
  }
};
}  // end namespace spsc_sema
}  // end namespace moodycamel

#if defined(AE_VCPP) && (_MSC_VER < 1700 || defined(__cplusplus_cli))
#pragma warning(pop)
#ifdef __cplusplus_cli
#pragma managed(pop)
#endif
#endif
