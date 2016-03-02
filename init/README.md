# README

The `init` directory is the location for all System V init, Upstart, systemd, etc. initialization scripts.

## systemd on RHEL 7

Create the `dynomite` user.

```bash
mkdir -p /usr/share/dynomite

mkdir /var/run/dynomite

useradd -r -M -c "Dynomite server" -s /sbin/nologin -d /usr/share/dynomite dynomite

chown -R dynomite:dynomite /usr/share/dynomite

chown -R dynomite:dynomite /var/run/dynomite
```

Install the Dynomite service file and the associated sysconfig file.

```bash
cp init/systemd_environment__dynomite /etc/sysconfig/dynomite

cp init/systemd_service_rhel__dynomite.service /usr/lib/systemd/system/dynomite.service

systemctl daemon-reload

systemctl enable dynomite

systemctl status dynomite
```

## systemd on Ubuntu 15.10

Create the `dynomite` user.

```bash
mkdir -p /usr/share/dynomite

mkdir /var/run/dynomite

useradd -r -M -c "Dynomite server" -s /sbin/nologin -d /usr/share/dynomite dynomite

chown -R dynomite:dynomite /usr/share/dynomite

chown -R dynomite:dynomite /var/run/dynomite
```

Install the Dynomite service file and the associated sysconfig file.

```bash
cp init/systemd_environment__dynomite /etc/default/dynomite

cp init/systemd_service_ubuntu__dynomite.service /lib/systemd/system/dynomite.service

systemctl daemon-reload

systemctl enable dynomite

systemctl status dynomite
```
