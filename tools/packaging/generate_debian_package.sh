#!/usr/bin/env bash

# Generate a debian package from a pre-build dragonfly bianry and set of files as well as generating change log from git history.
# The result is debian install package file (.deb file).
# This script accept 2 parameters:
#	1. Optioanl path to the location at which the binary file is located.
# this depends on
# * git
# * moreutils
# * debhelper
# e.g. apt update -y && apt install -y git moreutils debhelper
# Please note that is must run from main branch.
# Best running this from inside a container.
# The result are writing to the location from which you would execute the script (not where the script is located).
# Version number is the tag number.
# Params:
#	* optional location to the binary to place at the package


set -eu


if [ $# -ge 1 ]; then
    VERSION_FILE=$1
    if ! [ -f ${VERSION_FILE} ]; then
        echo "binary file ${VERSION_FILE} does not exist"
        exit 1
    fi

else
    echo "no binary file provided"
    exit 1
fi

SCRIPT_ABS_PATH=$(realpath $0)
SCRIPT_PATH=$(dirname ${SCRIPT_ABS_PATH})
PACKAGES_PATH=${SCRIPT_PATH}/debian
CHANGELOG_SCRIPT=generate_changelog.sh
ROOT_ABS_PATH=$(realpath $SCRIPT_PATH/../..)
TEMP_WORK_DIR=$(mktemp -d)
BASE_DIR=${TEMP_WORK_DIR}/packages
BASE_PATH=${BASE_DIR}/dragonfly
BINARY_TARGET_DIR=${BASE_PATH}/debian/bin

function cleanup {
    echo $@
    rm -rf ${TEMP_WORK_DIR}
    exit 1
}

mkdir -p ${BASE_PATH} || cleanup "failed to create working directory for building the package"

cp -r ${PACKAGES_PATH} ${BASE_PATH} || cleanup "failed to copy required files for the package build from ${PACKAGES_PATH}"

cp ${SCRIPT_PATH}/${CHANGELOG_SCRIPT} ${BASE_PATH} || cleanup "failed to copy changelog script to ${BASE_PATH}"

mkdir -p ${BINARY_TARGET_DIR} || cleanup "failed to create install directory for building the package"

cp ${VERSION_FILE} ${BINARY_TARGET_DIR}/dragonfly || cleanup "failed to copy binary to target dir"

${BASE_PATH}/${CHANGELOG_SCRIPT} ${ROOT_ABS_PATH} || cleanup "failed to generate changelog for package"

MY_DIR=${PWD}
cd ${BASE_PATH}
dpkg-buildpackage --build=binary || cleanup "failed to generate the package"

TEMP_RESULT_FILE=$(ls ../*.deb)
if [ "$TEMP_RESULT_FILE" = "" ]; then
    cleanup "failed to find debian file"
fi

for fl in ${TEMP_RESULT_FILE}; do
    destfile=$(basename ${fl} | sed 's/_\([0-9.]*_\)/_/')
    mv ${fl} ${MY_DIR}/${destfile}
done

cd ${MY_DIR}
RESULT_FILE=$(ls *.deb 2>/dev/null)
echo "successfully built the install package at ${MY_DIR}/${RESULT_FILE}"
rm -rf ${TEMP_WORK_DIR}
