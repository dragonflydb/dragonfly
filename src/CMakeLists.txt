option(ENABLE_GIT_VERSION "Build with Git metadata" OFF)

option(USE_SIMSIMD "Enable SimSIMD vector optimizations" OFF)
option(WITH_SEARCH "Enable compilation of search module" ON)

option(USE_PROMETHEUS "Download prometheus binary for tests" OFF)

if ("${CMAKE_SYSTEM_NAME}" STREQUAL "FreeBSD")
  set(DFLY_TOOLS_MAKE "gmake")
else()
  set(DFLY_TOOLS_MAKE "make")
endif()

function(cur_gen_dir out_dir)
  file(RELATIVE_PATH _rel_folder "${PROJECT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")

  set(_tmp_dir ${ROOT_GEN_DIR}/${_rel_folder})
  set(${out_dir} ${_tmp_dir} PARENT_SCOPE)
  file(MAKE_DIRECTORY ${_tmp_dir})
endfunction()

set(ROOT_GEN_DIR ${CMAKE_SOURCE_DIR}/genfiles)
file(MAKE_DIRECTORY ${ROOT_GEN_DIR})
include_directories(${ROOT_GEN_DIR}/src)

function(gen_bison name)
  GET_FILENAME_COMPONENT(_in ${name}.y ABSOLUTE)
  cur_gen_dir(gen_dir)
  # add_library(${lib_name} ${gen_dir}/${name}.cc)
  set(full_path_cc ${gen_dir}/${name}.cc ${gen_dir}/${name}.hh)

  ADD_CUSTOM_COMMAND(
           OUTPUT ${full_path_cc}
           COMMAND mkdir -p ${gen_dir}
           COMMAND bison --language=c++ -o ${gen_dir}/${name}.cc ${name}.y -Wconflicts-sr
           DEPENDS ${_in}
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           COMMENT "Generating parser from ${name}.y" VERBATIM)
 set_source_files_properties(${name}.cc ${name}_base.h PROPERTIES GENERATED TRUE)
endfunction()


Message(STATUS "THIRD_PARTY_LIB_DIR ${THIRD_PARTY_LIB_DIR}")

include(external_libs.cmake)

if(ENABLE_GIT_VERSION)
    include(GetGitRevisionDescription.cmake)
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


function(gen_flex name)
  GET_FILENAME_COMPONENT(_in ${name}.lex ABSOLUTE)
  cur_gen_dir(gen_dir)

  ADD_CUSTOM_COMMAND(
           OUTPUT ${gen_dir}/${name}.cc ${gen_dir}/${name}.h
           COMMAND mkdir -p ${gen_dir}

           COMMAND ${REFLEX} -o ${gen_dir}/${name}.cc  --unicode --header-file=${gen_dir}/${name}.h
                             --bison-complete  --bison-locations  ${_in}
           DEPENDS ${_in} reflex_project
           WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
           COMMENT "Generating lexer from ${name}.lex" VERBATIM)

  set_source_files_properties(${gen_dir}/${name}.h ${gen_dir}/${name}.cc
                              PROPERTIES GENERATED TRUE)
endfunction()

# the output file resides in the build directory.
configure_file(server/version.cc.in "${CMAKE_CURRENT_SOURCE_DIR}/server/version.cc" @ONLY)

add_subdirectory(redis)
add_subdirectory(core)
add_subdirectory(facade)
add_subdirectory(server)
