cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

include(CheckCXXCompilerFlag)
include(CheckCCompilerFlag)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# AFL++ fuzzing support - must be set BEFORE project() command
option(USE_AFL "Enable AFL++ fuzzing" OFF)
option(DF_MINIMAL_DEBUG_INFO "Use minimal debug information for CI builds" OFF)
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

# Use ccache as the compiler launcher when available (local and CI).
# Opt out with -DENABLE_CCACHE=OFF (or CCACHE_DISABLE=1 / ccache config).
option(ENABLE_CCACHE "Use ccache as the compiler launcher when available" ON)
set(_clear_launchers TRUE)
if(ENABLE_CCACHE)
  find_program(CCACHE_PROGRAM ccache)
  if(CCACHE_PROGRAM)
    message(STATUS "ccache is enabled: ${CCACHE_PROGRAM}")
    # Wrap ccache in `env` to configure it without a config file; compress to save space.
    set(_ccache_launcher env CCACHE_COMPRESS=true CCACHE_COMPRESSLEVEL=5)
    # Local only: share one cache across worktrees. Skipped in CI, where paths are
    # stable and CCACHE_BASEDIR can mangle dependency files.
    if(NOT DEFINED ENV{CI})
      list(APPEND _ccache_launcher CCACHE_BASEDIR=$ENV{HOME} CCACHE_NOHASHDIR=true)
    endif()
    set(CMAKE_C_COMPILER_LAUNCHER   ${_ccache_launcher} "${CCACHE_PROGRAM}")
    set(CMAKE_CXX_COMPILER_LAUNCHER ${_ccache_launcher} "${CCACHE_PROGRAM}")
    set(_clear_launchers FALSE)
  else()
    message(STATUS "ccache is enabled but not found, disabling compiler launcher")
  endif()
else()
  message(STATUS "ccache is disabled")
endif()

# When ccache isn't used (disabled or missing), clear any launcher a wrapper such as
# blaze.sh injected via -D, so the build reliably falls back to native compilation.
if(_clear_launchers)
  unset(CMAKE_C_COMPILER_LAUNCHER CACHE)
  unset(CMAKE_CXX_COMPILER_LAUNCHER CACHE)
  set(CMAKE_C_COMPILER_LAUNCHER "")
  set(CMAKE_CXX_COMPILER_LAUNCHER "")
endif()

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
check_cxx_source_compiles("int main() { return 0; }" SUPPORT_USAN)
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
  set(WITH_USAN OFF CACHE BOOL "Disable UBSAN for fuzzing" FORCE)

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

option(WITH_USAN "Enable -fsanitize=undefined" OFF)
if (SUPPORT_USAN AND WITH_USAN)
  message(STATUS "ub sanitizer enabled")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=undefined")
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
set(WITH_AWS OFF CACHE BOOL "Build with helio native AWS support")

include(third_party)
include(internal)

if(DF_MINIMAL_DEBUG_INFO AND CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_options(-g1)
endif()

# Optional Tracy profiler client. Compile-time gated so it is zero overhead by default (WITH_TRACY
# OFF). When ON we (1) force-enable the Tracy client options, (2) globally define the macros our
# instrumentation guards on, and (3) fetch + build the client. On-demand mode means an enabled
# binary only starts capturing once a Tracy viewer/capture connects (the runtime on/off switch).
option(WITH_TRACY "Enable the Tracy profiler client (on-demand)" OFF)
option(WITH_TRACY_FORENSIC "Enable high-volume per-command Tracy zones" OFF)
if (WITH_TRACY)
  message(STATUS "Tracy profiler enabled (on-demand). Connect the Tracy viewer to start capture.")
  set(TRACY_ENABLE ON CACHE BOOL "" FORCE)
  set(TRACY_ON_DEMAND ON CACHE BOOL "" FORCE)
  set(TRACY_FIBERS ON CACHE BOOL "" FORCE)

  # Force the client macros on for EVERY translation unit (dragonfly + the helio submodule), so the
  # guards activate regardless of how INTERFACE compile-defs propagate across helio's custom
  # cxx_link / submodule target boundary:
  #  - TRACY_ENABLE: activates the app zones in facade/tracy_support.h (and the client itself).
  #  - TRACY_FIBERS: activates the per-fiber instrumentation in
  #    helio/util/fibers/detail/fiber_interface.cc, whose guard is
  #    `#if defined(TRACY_ENABLE) && defined(TRACY_FIBERS)`. Without this the fiber lanes stay off.
  # NOTE: WITH_TRACY is a CMake option only - no source #ifdefs on it - so we do NOT define it as a
  # preprocessor macro. Symbol resolution still comes from linking Tracy::TracyClient to dfly_facade
  # and fibers2 (see the respective CMakeLists).
  add_compile_definitions(TRACY_ENABLE TRACY_FIBERS)
  if (WITH_TRACY_FORENSIC)
    message(STATUS "Tracy forensic per-command zones enabled.")
    add_compile_definitions(DFLY_TRACY_FORENSIC)
  endif()

  FetchContent_Declare(
    tracy
    GIT_REPOSITORY https://github.com/wolfpld/tracy.git
    GIT_TAG v0.11.1
    GIT_SHALLOW TRUE
    GIT_PROGRESS TRUE
  )
  FetchContent_MakeAvailable(tracy)
endif()

include_directories(src)
include_directories(helio)

add_subdirectory(helio)
add_subdirectory(src)
