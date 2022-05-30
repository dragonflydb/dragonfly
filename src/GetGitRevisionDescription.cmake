# - Returns a version string from Git
#
# These functions force a re-configure on each git commit so that you can
# trust the values of the variables in your build system.
#
#  get_git_head_revision(<refspecvar> <hashvar> [ALLOW_LOOKING_ABOVE_CMAKE_SOURCE_DIR])
#
# Returns the refspec and sha hash of the current head revision
#
#  git_describe(<var> [<additional arguments to git describe> ...])
#
# Returns the results of git describe on the source tree, and adjusting
# the output so that it tests false if an error occurs.
#
#  git_describe_working_tree(<var> [<additional arguments to git describe> ...])
#
# Returns the results of git describe on the working tree (--dirty option),
# and adjusting the output so that it tests false if an error occurs.
#
#  git_get_exact_tag(<var> [<additional arguments to git describe> ...])
#
# Returns the results of git describe --exact-match on the source tree,
# and adjusting the output so that it tests false if there was no exact
# matching tag.
#
#  git_local_changes(<var>)
#
# Returns either "CLEAN" or "DIRTY" with respect to uncommitted changes.
# Uses the return code of "git diff-index --quiet HEAD --".
# Does not regard untracked files.
#
# Requires CMake 2.6 or newer (uses the 'function' command)
#
# Original Author:
# 2009-2020 Ryan Pavlik <ryan.pavlik@gmail.com> <abiryan@ryand.net>
# http://academic.cleardefinition.com
#
# Copyright 2009-2013, Iowa State University.
# Copyright 2013-2020, Ryan Pavlik
# Copyright 2013-2020, Contributors
# SPDX-License-Identifier: BSL-1.0
# Distributed under the Boost Software License, Version 1.0.
# (See accompanying file LICENSE_1_0.txt or copy at
# http://www.boost.org/LICENSE_1_0.txt)

if(__get_git_revision_description)
    return()
endif()
set(__get_git_revision_description YES)

# We must run the following at "include" time, not at function call time,
# to find the path to this module rather than the path to a calling list file
get_filename_component(_gitdescmoddir ${CMAKE_CURRENT_LIST_FILE} PATH)

# Function _git_find_closest_git_dir finds the next closest .git directory
# that is part of any directory in the path defined by _start_dir.
# The result is returned in the parent scope variable whose name is passed
# as variable _git_dir_var. If no .git directory can be found, the
# function returns an empty string via _git_dir_var.
#
# Example: Given a path C:/bla/foo/bar and assuming C:/bla/.git exists and
# neither foo nor bar contain a file/directory .git. This wil return
# C:/bla/.git
#
function(_git_find_closest_git_dir _start_dir _git_dir_var)
    set(cur_dir "${_start_dir}")
    set(git_dir "${_start_dir}/.git")
    while(NOT EXISTS "${git_dir}")
        # .git dir not found, search parent directories
        set(git_previous_parent "${cur_dir}")
        get_filename_component(cur_dir "${cur_dir}" DIRECTORY)
        if(cur_dir STREQUAL git_previous_parent)
            # We have reached the root directory, we are not in git
            set(${_git_dir_var}
                ""
                PARENT_SCOPE)
            return()
        endif()
        set(git_dir "${cur_dir}/.git")
    endwhile()
    set(${_git_dir_var}
        "${git_dir}"
        PARENT_SCOPE)
endfunction()

function(get_git_head_revision _refspecvar _hashvar)
    _git_find_closest_git_dir("${CMAKE_CURRENT_SOURCE_DIR}" GIT_DIR)

    if("${ARGN}" STREQUAL "ALLOW_LOOKING_ABOVE_CMAKE_SOURCE_DIR")
        set(ALLOW_LOOKING_ABOVE_CMAKE_SOURCE_DIR TRUE)
    else()
        set(ALLOW_LOOKING_ABOVE_CMAKE_SOURCE_DIR FALSE)
    endif()
    if(NOT "${GIT_DIR}" STREQUAL "")
        file(RELATIVE_PATH _relative_to_source_dir "${CMAKE_SOURCE_DIR}"
             "${GIT_DIR}")
        if("${_relative_to_source_dir}" MATCHES "[.][.]" AND NOT ALLOW_LOOKING_ABOVE_CMAKE_SOURCE_DIR)
            # We've gone above the CMake root dir.
            set(GIT_DIR "")
        endif()
    endif()
    if("${GIT_DIR}" STREQUAL "")
        set(${_refspecvar}
            "GITDIR-NOTFOUND"
            PARENT_SCOPE)
        set(${_hashvar}
            "GITDIR-NOTFOUND"
            PARENT_SCOPE)
        return()
    endif()

    # Check if the current source dir is a git submodule or a worktree.
    # In both cases .git is a file instead of a directory.
    #
    if(NOT IS_DIRECTORY ${GIT_DIR})
        # The following git command will return a non empty string that
        # points to the super project working tree if the current
        # source dir is inside a git submodule.
        # Otherwise the command will return an empty string.
        #
        execute_process(
            COMMAND "${GIT_EXECUTABLE}" rev-parse
                    --show-superproject-working-tree
            WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
            OUTPUT_VARIABLE out
            ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
        if(NOT "${out}" STREQUAL "")
            # If out is empty, GIT_DIR/CMAKE_CURRENT_SOURCE_DIR is in a submodule
            file(READ ${GIT_DIR} submodule)
            string(REGEX REPLACE "gitdir: (.*)$" "\\1" GIT_DIR_RELATIVE
                                 ${submodule})
            string(STRIP ${GIT_DIR_RELATIVE} GIT_DIR_RELATIVE)
            get_filename_component(SUBMODULE_DIR ${GIT_DIR} PATH)
            get_filename_component(GIT_DIR ${SUBMODULE_DIR}/${GIT_DIR_RELATIVE}
                                   ABSOLUTE)
            set(HEAD_SOURCE_FILE "${GIT_DIR}/HEAD")
        else()
            # GIT_DIR/CMAKE_CURRENT_SOURCE_DIR is in a worktree
            file(READ ${GIT_DIR} worktree_ref)
            # The .git directory contains a path to the worktree information directory
            # inside the parent git repo of the worktree.
            #
            string(REGEX REPLACE "gitdir: (.*)$" "\\1" git_worktree_dir
                                 ${worktree_ref})
            string(STRIP ${git_worktree_dir} git_worktree_dir)
            _git_find_closest_git_dir("${git_worktree_dir}" GIT_DIR)
            set(HEAD_SOURCE_FILE "${git_worktree_dir}/HEAD")
        endif()
    else()
        set(HEAD_SOURCE_FILE "${GIT_DIR}/HEAD")
    endif()
    set(GIT_DATA "${CMAKE_CURRENT_BINARY_DIR}/CMakeFiles/git-data")
    if(NOT EXISTS "${GIT_DATA}")
        file(MAKE_DIRECTORY "${GIT_DATA}")
    endif()

    if(NOT EXISTS "${HEAD_SOURCE_FILE}")
        return()
    endif()
    set(HEAD_FILE "${GIT_DATA}/HEAD")
    configure_file("${HEAD_SOURCE_FILE}" "${HEAD_FILE}" COPYONLY)

    configure_file("${_gitdescmoddir}/GetGitRevisionDescription.cmake.in"
                   "${GIT_DATA}/grabRef.cmake" @ONLY)
    include("${GIT_DATA}/grabRef.cmake")

    set(${_refspecvar}
        "${HEAD_REF}"
        PARENT_SCOPE)
    set(${_hashvar}
        "${HEAD_HASH}"
        PARENT_SCOPE)
endfunction()

function(git_describe _var)
    if(NOT GIT_FOUND)
        find_package(Git QUIET)
    endif()
    get_git_head_revision(refspec hash)
    if(NOT GIT_FOUND)
        set(${_var}
            "GIT-NOTFOUND"
            PARENT_SCOPE)
        return()
    endif()
    if(NOT hash)
        set(${_var}
            "HEAD-HASH-NOTFOUND"
            PARENT_SCOPE)
        return()
    endif()

    # TODO sanitize
    #if((${ARGN}" MATCHES "&&") OR
    #	(ARGN MATCHES "||") OR
    #	(ARGN MATCHES "\\;"))
    #	message("Please report the following error to the project!")
    #	message(FATAL_ERROR "Looks like someone's doing something nefarious with git_describe! Passed arguments ${ARGN}")
    #endif()

    #message(STATUS "Arguments to execute_process: ${ARGN}")

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --tags --always ${hash} ${ARGN}
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        RESULT_VARIABLE res
        OUTPUT_VARIABLE out
        ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT res EQUAL 0)
        set(out "${out}-${res}-NOTFOUND")
    endif()

    set(${_var}
        "${out}"
        PARENT_SCOPE)
endfunction()

function(git_describe_working_tree _var)
    if(NOT GIT_FOUND)
        find_package(Git QUIET)
    endif()
    if(NOT GIT_FOUND)
        set(${_var}
            "GIT-NOTFOUND"
            PARENT_SCOPE)
        return()
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" describe --dirty ${ARGN}
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        RESULT_VARIABLE res
        OUTPUT_VARIABLE out
        ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT res EQUAL 0)
        set(out "${out}-${res}-NOTFOUND")
    endif()

    set(${_var}
        "${out}"
        PARENT_SCOPE)
endfunction()

function(git_get_exact_tag _var)
    git_describe(out --exact-match ${ARGN})
    set(${_var}
        "${out}"
        PARENT_SCOPE)
endfunction()

function(git_local_changes _var)
    if(NOT GIT_FOUND)
        find_package(Git QUIET)
    endif()
    get_git_head_revision(refspec hash)
    if(NOT GIT_FOUND)
        set(${_var}
            "GIT-NOTFOUND"
            PARENT_SCOPE)
        return()
    endif()
    if(NOT hash)
        set(${_var}
            "HEAD-HASH-NOTFOUND"
            PARENT_SCOPE)
        return()
    endif()

    execute_process(
        COMMAND "${GIT_EXECUTABLE}" diff-index --quiet HEAD --
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        RESULT_VARIABLE res
        OUTPUT_VARIABLE out
        ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(res EQUAL 0)
        set(${_var}
            "CLEAN"
            PARENT_SCOPE)
    else()
        set(${_var}
            "DIRTY"
            PARENT_SCOPE)
    endif()
endfunction()
