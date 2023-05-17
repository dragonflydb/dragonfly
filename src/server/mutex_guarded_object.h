// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/base/thread_annotations.h>

#include <mutex>

namespace dfly {

template <typename T> class MutexGuardedObject {
 public:
  class Access {
   public:
    T* operator->() {
      return &object_;
    }
    T& operator*() {
      return object_;
    }

   private:
    friend MutexGuardedObject;
    Access(Mutex& mu, T& object) : guard_(mu), object_(object) {
    }

    std::lock_guard<Mutex> guard_;
    T& object_;
  };

  class ConstAccess {
   public:
    const T* operator->() const {
      return &object_;
    }
    const T& operator*() const {
      return object_;
    }

   private:
    friend MutexGuardedObject;
    ConstAccess(Mutex& mu, const T& object) : guard_(mu), object_(object) {
    }

    std::lock_guard<Mutex> guard_;
    const T& object_;
  };

  void Set(T new_object) ABSL_LOCKS_EXCLUDED(mu_) {
    std::lock_guard gu(mu_);
    SetUnderLock(std::move(new_object));
  }

  void SetUnderLock(T new_object) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    object_ = std::move(new_object);
  }

  ABSL_MUST_USE_RESULT Access Get() ABSL_LOCKS_EXCLUDED(mu_) {
    return {mu_, object_};
  }

  ABSL_MUST_USE_RESULT ConstAccess Get() const ABSL_LOCKS_EXCLUDED(mu_) {
    return {mu_, object_};
  }

 private:
  mutable Mutex mu_;
  T object_ ABSL_GUARDED_BY(mu_);
};

}  // namespace dfly
