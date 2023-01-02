#!/bin/bash

DF_VERSION="0.12.0"


                echo "Building deb package for dragonflydb $DF_VERSION" 
                echo "Generating files"
                mkdir -p dragonfly_"$DF_VERSION"_amd64/usr/bin/
                mkdir -p dragonfly_"$DF_VERSION"_amd64/etc/systemd/system
                mkdir dragonfly_"$DF_VERSION"_amd64/DEBIAN
                touch dragonfly_"$DF_VERSION"_amd64/DEBIAN/control
                cat << EOF > dragonfly_"$DF_VERSION"_amd64/DEBIAN/control
Package: dragonfly
Version: 0.12.0
Architecture: amd64
Maintainer: DragonflyDB maintainers
Description: A modern replacement for Redis and Memcached       
EOF


                if test -f ../build-opt/dragonfly; then
                echo "Copying binary file"
                cp ../build-opt/dragonfly  dragonfly_"$DF_VERSION"_amd64/usr/bin/
                else
                echo "Binary not found"
                fi
                echo "Generating systemd config"
                touch dragonfly_"$DF_VERSION"_amd64/etc/systemd/system/dragonfly.service
                cat << EOF > dragonfly_"$DF_VERSION"_amd64/etc/systemd/system/dragonfly.service
[Unit]
Description=DragonflyDB

[Service]
ExecStart=/usr/bin/dragonfly

[Install]
WantedBy=multi-user.target
EOF

                echo "Generating deb package"
                dpkg-deb --build --root-owner-group dragonfly_"$DF_VERSION"_amd64 >> /dev/null
                cp dragonfly_"$DF_VERSION"_amd64.deb ../
                echo "Deb package generrated"
                echo "CLeaning build dir"
                rm -rf dragonfly_"$DF_VERSION"_amd64
                echo "Exiting ..."
~                                     
