Name:           dynomite
Version:        0.3.0
Release:        1%{?dist}
Summary:        Netflix Dynomite

License:       Apache 2.0
URL:           https://github.com/Netflix/dynomite
Source0:       https://github.com/Netflix/dynomite/archive/v0.3.0.tar.gz
Patch0:	dynomite-aa
Patch1: dynomite-ab
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

Group: Custom
BuildRequires: gcc gcc-c++ make openssl-devel git
Requires:      make

%description
Netflix Dynomite.  Helps build redundant and cross-datacenter redis and memcache replication rings.

%prep
%setup -q -n %{name}-%{version}

%patch -P 0
%patch -P 1

%build
autoreconf -fvi

%configure --enable-debug=log

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
/usr/sbin/dynomite
%{_initrddir}/%{name}
%config(noreplace)%{_sysconfdir}/%{name}/%{name}.yml

%changelog

