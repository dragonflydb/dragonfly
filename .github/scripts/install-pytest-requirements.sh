#!/bin/bash
# https://peps.python.org/pep-0668/#keep-the-marker-file-in-container-images

set -eu

REQUIREMENTS_FILE="${GITHUB_WORKSPACE}/tests/dragonfly/requirements.txt"

if compgen -G '/usr/lib/python3.*/EXTERNALLY-MANAGED' > /dev/null; then
  pip3 install --break-system-packages -r "$REQUIREMENTS_FILE"
else
  pip3 install -r "$REQUIREMENTS_FILE"
fi
