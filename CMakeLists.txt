cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

include(CheckCXXCompilerFlag)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# AFL++ fuzzing support - must be set BEFORE project() command
option(USE_AFL "Enable AFL++ fuzzing" OFF)
if(USE_AFL)
  # Automatically set AFL++ compilers if not already set
  if(NOT CMAKE_C_COMPILER MATCHES "afl-" AND NOT CMAKE_CXX_COMPILER MATCHES "afl-")
    # Try to find afl-clang-fast
    find_program(AFL_CC afl-clang-lto)
    find_program(AFL_CXX afl-clang-lto++)

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
set(CMAKE_CXX_STANDARD 17)

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

# Check for Direct I/O support on the build host
include(CheckCXXSourceRuns)
set(DIRECT_IO_TEST_CODE "
#include <fcntl.h>
#include <unistd.h>
int main() {
    // Open file in current dir (which is the build dir during cmake checks)
    int fd = open(\"direct_io_probe.tmp\", O_CREAT | O_WRONLY | O_DIRECT, 0666);
    if (fd < 0) return 1;
    close(fd);
    unlink(\"direct_io_probe.tmp\");
    return 0;
}
")
# Check if it runs. Use CMAKE_BINARY_DIR to ensure we test the actual build partition.
set(OLD_CMAKE_REQUIRED_DEFINITIONS ${CMAKE_REQUIRED_DEFINITIONS})
list(APPEND CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE") # We might need _GNU_SOURCE for O_DIRECT on some older Linux headers
check_cxx_source_runs("${DIRECT_IO_TEST_CODE}" DIRECT_IO_SUPPORTED)
set(CMAKE_REQUIRED_DEFINITIONS ${OLD_CMAKE_REQUIRED_DEFINITIONS})

if (DIRECT_IO_SUPPORTED)
  message(STATUS "Build host supports O_DIRECT")
  add_compile_definitions(DIRECT_IO_SUPPORTED) # To be able to filter out test cases and not the whole test suite
else()
  message(STATUS "Build host does not support O_DIRECT (note: this check only validates the build host, target capabilities may differ when cross-compiling) - Tests will run using standard Buffered I/O.")
endif()

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

include(third_party)
include(internal)

include_directories(src)
include_directories(helio)

add_subdirectory(helio)
add_subdirectory(src)
