cmake_minimum_required(VERSION 3.15 FATAL_ERROR)
set(PROJECT_CONTACT romange@gmail.com)

enable_testing()

set(CMAKE_EXPORT_COMPILE_COMMANDS 1)

# Set targets in folders
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
project(DRAGONFLY C CXX)
set(CMAKE_CXX_STANDARD 17)

# We must define all the required variables from the root cmakefile, otherwise
# they just disappear.
set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/helio/cmake" ${CMAKE_MODULE_PATH})
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)

include(third_party)
include(internal)

add_third_party(
  lua
  URL https://github.com/lua/lua/archive/refs/tags/v5.4.4.tar.gz
  PATCH_COMMAND patch -p1 -i "${CMAKE_CURRENT_SOURCE_DIR}/patches/lua-v5.4.4.patch"
  CONFIGURE_COMMAND echo
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND cp <SOURCE_DIR>/liblua.a ${THIRD_PARTY_LIB_DIR}/lua/lib/
  COMMAND cp <SOURCE_DIR>/lualib.h <SOURCE_DIR>/lua.h <SOURCE_DIR>/lauxlib.h
          <SOURCE_DIR>/luaconf.h ${THIRD_PARTY_LIB_DIR}/lua/include
)

Message(STATUS "THIRD_PARTY_LIB_DIR ${THIRD_PARTY_LIB_DIR}")

include_directories(src)
include_directories(helio)

add_subdirectory(helio)
add_subdirectory(src)
