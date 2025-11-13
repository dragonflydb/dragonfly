set -e

# GPG key must have been imported

echo "Signing RPMs with key id ${1}"

# The script fails in CI without an empty GPG_TTY
GPG_TTY=""
export GPG_TTY

for file in $(find _site/rpm -type f -name "*.rpm"); do
  echo "Signing ${file}"
  rpm --define "__gpg /usr/bin/gpg" --define "%_signature gpg" --define "%_gpg_name ${1}" --addsign "${file}"
done
