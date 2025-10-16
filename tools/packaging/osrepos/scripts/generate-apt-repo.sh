set -e

METADATA_ROOT=_site/deb
mkdir -pv ${METADATA_ROOT}/conf

cp -av reprepro-config/* ${METADATA_ROOT}/conf

reprepro -b ${METADATA_ROOT} createsymlinks
reprepro -b ${METADATA_ROOT} export

for file in $(find deb_tmp -type f -name "*.deb"); do
  reprepro -b ${METADATA_ROOT} includedeb noble "${file}"
done

rm -rf deb_tmp
