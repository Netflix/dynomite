#!/bin/sh
set -ex

# this is a 3-AZ (3-rack in dynomite lingo) setup.
# I'm using non-standard port numbers purposefully to avoid conflicting with other configs by error

ulimit -n 32000  # increase file descriptor limit to 32k

redis-server --bind 127.0.1.2 --port 1212 &
src/dynomite --conf-file=conf-msf/conf/eu-west-1:eu-west-1a:0.yml &

redis-server --bind 127.0.1.3 --port 1212 &
src/dynomite --conf-file=conf-msf/conf/eu-west-1:eu-west-1b:0.yml &

redis-server --bind 127.0.1.4 --port 1212 &
src/dynomite --conf-file=conf-msf/conf/eu-west-1:eu-west-1c:0.yml &

