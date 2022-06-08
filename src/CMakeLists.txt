add_third_party(
  lua
  URL https://github.com/lua/lua/archive/refs/tags/v5.4.4.tar.gz
  PATCH_COMMAND patch -p1 -i "${CMAKE_SOURCE_DIR}/patches/lua-v5.4.4.patch"
  CONFIGURE_COMMAND echo
  BUILD_IN_SOURCE 1
  INSTALL_COMMAND cp <SOURCE_DIR>/liblua.a ${THIRD_PARTY_LIB_DIR}/lua/lib/
  COMMAND cp <SOURCE_DIR>/lualib.h <SOURCE_DIR>/lua.h <SOURCE_DIR>/lauxlib.h
          <SOURCE_DIR>/luaconf.h ${THIRD_PARTY_LIB_DIR}/lua/include
)

add_third_party(
  dconv
  URL https://github.com/google/double-conversion/archive/refs/tags/v3.2.0.tar.gz
  LIB libdouble-conversion.a
)

Message(STATUS "THIRD_PARTY_LIB_DIR ${THIRD_PARTY_LIB_DIR}")


option(ENABLE_GIT_VERSION "Build with Git metadata" OFF)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/" )

if(ENABLE_GIT_VERSION)
    include(GetGitRevisionDescription)
    get_git_head_revision(GIT_REFSPEC GIT_SHA1)
    git_local_changes(GIT_CLEAN_DIRTY)
    if("${GIT_CLEAN_DIRTY}" STREQUAL "DIRTY")
        set(GIT_CLEAN_DIRTY "-dirty")
        else()
        set(GIT_CLEAN_DIRTY "")
    endif()
    Message(STATUS "GIT_SHA1 ${GIT_SHA1}")
    git_describe(GIT_VER --always)
    Message(STATUS "GIT_VER ${GIT_VER}")
    string(TIMESTAMP PRJ_BUILD_TIME "%Y-%m-%d %H:%M:%S" UTC)
else(ENABLE_GIT_VERSION)
    set(GIT_VER "dev")
    set(GIT_SHA1 "0000000")
    set(GIT_CLEAN_DIRTY "-dev")
    set(PRJ_BUILD_TIME "bigbang")
endif(ENABLE_GIT_VERSION)

# the output file resides in the build directory.
configure_file(server/version.cc.in "${CMAKE_CURRENT_SOURCE_DIR}/server/version.cc" @ONLY)

add_subdirectory(redis)
add_subdirectory(core)
add_subdirectory(facade)
add_subdirectory(server)
