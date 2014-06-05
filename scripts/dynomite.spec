Summary: Dynomite: generic replicator
Name: dynomite
Version: 0.1.19
Release: 1
URL: https://github.com/Netflix/Dynomite
Source0: %{name}-%{version}.tar.gz
License: Apache License 2.0
Group: System Environment/Libraries
Packager:  Tom Parrott <tomp@tomp.co.uk>
BuildRoot: %{_tmppath}/%{name}-root

%description
dynomite is thin replication layer for different storage.  Currently, we support memcached and redis protocol.
The goal is to use this as a caching system or a in-memory storage

%prep
%setup -q
autoreconf -fvi

%build

%configure
%__make

%install
[ %{buildroot} != "/" ] && rm -rf %{buildroot}

%makeinstall PREFIX=%{buildroot}

#Install init script
%{__install} -p -D -m 0755 scripts/%{name}.init %{buildroot}%{_initrddir}/%{name}

#Install example config file
%{__install} -p -D -m 0644 conf/%{name}.yml %{buildroot}%{_sysconfdir}/%{name}/%{name}.yml

%post
/sbin/chkconfig --add %{name}

%preun
if [ $1 = 0 ]; then
 /sbin/service %{name} stop > /dev/null 2>&1
 /sbin/chkconfig --del %{name}
fi

%clean
[ %{buildroot} != "/" ] && rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
/usr/bin/dynomite
%{_initrddir}/%{name}
%config(noreplace)%{_sysconfdir}/%{name}/%{name}.yml
