cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# AFL++ fuzzing support - must be set BEFORE project() command
option(USE_AFL "Enable AFL++ fuzzing" OFF)
if(USE_AFL)
  # Automatically set AFL++ compilers if not already set
  if(NOT CMAKE_C_COMPILER MATCHES "afl-" AND NOT CMAKE_CXX_COMPILER MATCHES "afl-")
    find_program(AFL_CC afl-clang-fast)
    find_program(AFL_CXX afl-clang-fast++)

    if(AFL_CC AND AFL_CXX)
      message(STATUS "AFL++ fuzzing enabled - setting compilers")
      set(CMAKE_C_COMPILER ${AFL_CC})
      set(CMAKE_CXX_COMPILER ${AFL_CXX})
    else()
      message(FATAL_ERROR "USE_AFL=ON but AFL++ compilers not found!\n"
              "Please install AFL++: apt install afl++ or build from source\n"
              "https://github.com/AFLplusplus/AFLplusplus")
    endif()
  endif()
endif()

# Set targets in folders
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
project(DRAGONFLY C CXX)
set(CMAKE_CXX_STANDARD 20)

# Disabled because it has false positives with ref-counted intrusive pointers.
CHECK_CXX_COMPILER_FLAG("-Wuse-after-free" HAS_USE_AFTER_FREE_WARN)
if (HAS_USE_AFTER_FREE_WARN)
    set(CMAKE_CXX_FLAGS "-Wno-use-after-free ${CMAKE_CXX_FLAGS}")
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set(CMAKE_CXX_FLAGS "-Wthread-safety -Werror=thread-safety ${CMAKE_CXX_FLAGS}")
endif()

# We can not use here CHECK_CXX_COMPILER_FLAG because systems that do not support sanitizers
# fail during linking time.
set(CMAKE_REQUIRED_FLAGS "-fsanitize=address")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_ASAN)

set(CMAKE_REQUIRED_FLAGS "-fsanitize=undefined")
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_UBSAN)
set(CMAKE_REQUIRED_FLAGS "")

# We must define all the required variables from the root cmakefile, otherwise
# they just disappear.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/helio/cmake" ${CMAKE_MODULE_PATH})
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(DF_USE_SSL "Provide support for SSL connections" ON)

find_package(OpenSSL)

# AFL++ configuration - must be before sanitizer checks
if(USE_AFL)
  message(STATUS "AFL++ fuzzing mode active")
  message(STATUS "  C compiler: ${CMAKE_C_COMPILER}")
  message(STATUS "  C++ compiler: ${CMAKE_CXX_COMPILER}")

  # Add USE_AFL as compile definition so #ifdef USE_AFL works in code
  add_compile_definitions(USE_AFL)

  # AFL++ requires specific compiler flags for coverage instrumentation
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g")

  # Force disable sanitizers when fuzzing (AFL++ incompatible with ASAN/UBSAN)
  message(STATUS "Disabling sanitizers (incompatible with AFL++ fuzzing)")
  set(WITH_ASAN OFF CACHE BOOL "Disable ASAN for fuzzing" FORCE)
  set(WITH_UBSAN OFF CACHE BOOL "Disable UBSAN for fuzzing" FORCE)

  # Disable AWS and GCP for fuzzing builds (not needed, reduces build time)
  message(STATUS "Disabling AWS and GCP integrations for fuzzing")
  set(WITH_AWS OFF CACHE BOOL "Disable AWS for fuzzing" FORCE)
  set(WITH_GCP OFF CACHE BOOL "Disable GCP for fuzzing" FORCE)
endif()

option(WITH_ASAN "Enable -fsanitize=address" OFF)
if (SUPPORT_ASAN AND WITH_ASAN)
  message(STATUS "address sanitizer enabled")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
endif()

# UndefinedBehaviorSanitizer (UBSan). Check reference:
# WITH_UBSAN        -> set `-fsanitize=undefined` checks only.
# WITH_UBSAN_STRICT -> additional Clang-only checks:
# 1) implicit integer conversions (truncation, sign-change)
# 2) integer overflow (signed and unsigned)
# 3) indirect call type mismatches (function)
# 4) vtable type confusion (vptr)
# 5) out-of-bounds object access (object-size).
option(WITH_UBSAN "Enable -fsanitize=undefined" OFF)

# Extended UBSan checks. Relevant only if WITH_UBSAN is also ON. Clang only -- GCC
# implements none of these.
option(WITH_UBSAN_STRICT "Enable extended UBSan checks (implicit-conversion, integer, function, vptr, object-size). Clang only." ON)

if (SUPPORT_UBSAN AND WITH_UBSAN)
  message(STATUS "UndefinedBehaviorSanitizer enabled (-fsanitize=undefined)")
  # -fsanitize=undefined: the default UBSan umbrella (~18 true-UB checks)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined")
  set(ubsan_compile_flags "-fsanitize=undefined")

  if (WITH_UBSAN_STRICT)
    if (NOT CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
      message(FATAL_ERROR
        "WITH_UBSAN_STRICT requires Clang, but CMAKE_CXX_COMPILER_ID is "
        "'${CMAKE_CXX_COMPILER_ID}'. Build with clang/clang++, or pass "
        "-DWITH_UBSAN_STRICT=OFF to use base -fsanitize=undefined only.")
    endif()
    message(STATUS "UndefinedBehaviorSanitizer STRICT enabled (extended checks)")
    # Instrumenting 'object-size' (below) wont work with with -O0, so the strict build forces -O1. -O1 plus
    # the sanitizer flags make clang tag some link-step arguments as unused, so -Wno-error keeps that from
    # failing a -Werror build.
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O1 -Wno-error=unused-command-line-argument")
    # Exclude third-party code from instrumentation (see tools/sanitizers/ubsan/ubsan-ignorelist.txt).
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize-ignorelist=${CMAKE_SOURCE_DIR}/tools/sanitizers/ubsan/ubsan-ignorelist.txt")
    # lossy/sign-changing implicit integer conversions:
    # implicit-conversion = implicit-unsigned-integer-truncation + implicit-signed-integer-truncation +
    # implicit-integer-sign-change. Not UB, but caught the PR #7562 bug (1ULL<<32 silently truncated to 0).
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=implicit-conversion")
    # nullability: catches a null pointer used where the code explicitly promised it
    # can't be null (a pointer marked with Clang's _Nonnull annotation). Low-noise:
    # only fires on pointers carrying that annotation.
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=nullability")
    # float-divide-by-zero: floating-point division by zero (usually a bug).
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=float-divide-by-zero")
    # integer: flags integer overflow, bad bit-shifts, and divide-by-zero.
    # Noisy: unsigned "wraparound" is legal and used on purpose (hashing, size math).
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=integer")
    # function: indirect call through a function pointer of the WRONG type (needs RTTI,
    #   which Dragonfly already builds with). Catches ABI / type-confusion bugs.
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=function")
    # vptr: use of an object whose vptr indicates the wrong dynamic type, or whose
    #   lifetime has not begun / has ended (needs RTTI, link with clang++).
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=vptr")
    # object-size: catches reads/writes past the end of an object whose size the
    # compiler knows (e.g. a fixed-size buffer). Needs -O1+; does nothing at -O0.
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=object-size")
    # Extend the summary with the strict checks (for the recap line below).
    set(ubsan_compile_flags "${ubsan_compile_flags},implicit-conversion,nullability,float-divide-by-zero,integer,function,vptr,object-size")
  endif()

  # One-line recap of every UBSan flag added, printed to the configure log.
  message(STATUS "UBSan compile flags: ${ubsan_compile_flags}")
endif()

# Split debug info into .dwo files so the linker handles much less data.
# Both GCC and Clang support this. GDB resolves .dwo references transparently.
CHECK_CXX_COMPILER_FLAG("-gsplit-dwarf" HAS_SPLIT_DWARF_CXX)
CHECK_C_COMPILER_FLAG("-gsplit-dwarf" HAS_SPLIT_DWARF_C)
if (HAS_SPLIT_DWARF_CXX)
  add_compile_options($<$<AND:$<CONFIG:Debug>,$<COMPILE_LANGUAGE:CXX>>:-gsplit-dwarf>)
endif()
if (HAS_SPLIT_DWARF_C)
  add_compile_options($<$<AND:$<CONFIG:Debug>,$<COMPILE_LANGUAGE:C>>:-gsplit-dwarf>)
endif()

set(LEGACY_GLOG OFF CACHE BOOL "Use legacy glog instead of absl")

include(third_party)
include(internal)

include_directories(src)
include_directories(helio)

add_subdirectory(helio)
add_subdirectory(src)
