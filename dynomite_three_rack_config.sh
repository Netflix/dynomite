#!/bin/sh
set -ex

# this is a 3-AZ (3-rack in dynomite lingo) setup.
# I'm using non-standard port numbers purposefully to avoid conflicting with other configs by error

redis-server --port 33211 &
src/dynomite --conf-file=conf/msf_redis_node1.yml &

redis-server --port 33222 &
src/dynomite --conf-file=conf/msf_redis_node2.yml &

redis-server --port 33233 &
src/dynomite --conf-file=conf/msf_redis_node3.yml &

