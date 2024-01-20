%define     archive %{pkg_name}-%{_arch}.tar.gz

# How the package name looks like
%define     _build_name_fmt  %%{NAME}.%%{ARCH}.rpm

Name:       %{pkg_name}
Version:    %{version}
Release:    1%{?dist}
Summary:    DragonflyDB memory store
License:    BUSL-1.1
URL:        https://www.dragonflydb.io
Source0:    %{archive}
Source1:    dragonfly.service
Source2:    dragonfly.conf
Group:      Applications/System
Provides:   user(dfly)
Provides:   group(dfly)

%description
DragonflyDB is a vertically scalable and memory efficient in-memory store
that is compatible with Redis OSS and Memcached.

%pre

getent group dfly >/dev/null || groupadd -r dfly
getent passwd dfly >/dev/null || useradd -r -g dfly -M -s /sbin/nologin -c "User for DragonflyDB service" dfly

%prep

%build
tar xvfz %{SOURCE0}
mv ./{%pkg_name}-%{_arch} ./dragonfly

%install
mkdir -p %{buildroot}/usr/local/bin
mkdir -p %{buildroot}/etc/dragonfly

install -m 755 ./dragonfly %{buildroot}/usr/local/bin/
mkdir -p %{buildroot}/usr/lib/systemd/system
cp %{SOURCE1} %{buildroot}/usr/lib/systemd/system/
cp %{SOURCE2} %{buildroot}/etc/dragonfly/

%clean
rm -rf %{buildroot}
rm -rf %{_builddir}/*

%files
%attr(-,dfly,dfly) /usr/local/bin/dragonfly
%attr(-,dfly,dfly) /usr/lib/systemd/system/dragonfly.service
%attr(-,dfly,dfly) /etc/dragonfly/dragonfly.conf

%changelog
