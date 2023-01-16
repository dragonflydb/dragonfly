#!/usr/bin/env bash

# This would generate a change log required for build Debian installation package.
# Don't run this script on your local machine, run this inside docker
# you would need to install git client as well as moreutils
# apt install -y git moreutils
# note: This script should run on branch "main".

set -eu

if [ $# -ne 1 ]; then
	echo "usage: <git repo path> <target path>"
	exit 1
fi
SCRIPT_ABS_PATH=$(realpath $0)

THIS_DIR=$(dirname ${SCRIPT_ABS_PATH})
GIT_DIR=$1
PACKGE_DIR=${THIS_DIR}/debian
CHANGE_LOG=${PACKGE_DIR}/changelog
cd ${GIT_DIR}
git config --global --add safe.directory ${GIT_DIR}
has_tags=$(git tag -l v* | wc -l 2>/dev/null)
if [ "$has_tags" = "" -o "$has_tags" = "0" ]; then
	git fetch --all --tags || {
		echo "failed to fetch tags, cannot build changelog file"
		exit 1
	}
fi

>${CHANGE_LOG}
prevtag=v0.2.0
pkgname=`cat ${PACKGE_DIR}/control | grep '^Package: ' | sed 's/^Package: //'`
git tag -l v* | sort -V | while read tag; do
    (echo "$pkgname (${tag#v}) unstable; urgency=low"; git log --pretty=format:'  * %s' $prevtag..$tag; git log --pretty='format:%n%n -- %aN <%aE>  %aD%n%n' $tag^..$tag) | cat - ${CHANGE_LOG} | sponge ${CHANGE_LOG}
        prevtag=$tag
done
if [ -f ${CHANGE_LOG} ]; then
	haslnes=$(wc -l ${CHANGE_LOG} 2>/dev/null | awk '{print $1}')
	if [ "$haslnes" = "" ]; then
		echo "empty file ${CHANGE_LOG}, failed to generate changelog"
		exit 1
	fi
else
	echo "failed to generate ${CHANGE_LOG}"
	exit 1
fi
