#!/bin/bash

if [ -n "$TRAVIS" ]; then

    #python libs
    sudo pip install redis
    sudo pip install git+https://github.com/andymccurdy/redis-py.git@2.10.3
fi

#build Dynomite
CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make

# Create the environment

rm -rf test/_binaries/
mkdir test/_binaries
rm -rf test/logs
mkdir test/logs
rm test/conf
ln -s ../conf test/conf
cp `pwd`/src/dynomite test/_binaries/
cp `which redis-server` test/_binaries/
cp `which redis-cli` test/_binaries/
cd test

# launch processes
function launch_redis() {
    ./_binaries/redis-server --port 1212 > ./logs/redis_standalone.log &
    ./_binaries/redis-server --port 22121 > ./logs/redis_22121.log &
    ./_binaries/redis-server --port 22122 > ./logs/redis_22122.log &
    ./_binaries/redis-server --port 22123 > ./logs/redis_22123.log &
    ./_binaries/redis-server --port 22124 > ./logs/redis_22124.log &
    ./_binaries/redis-server --port 22125 > ./logs/redis_22125.log &
}
function launch_dynomite() {
    ./_binaries/dynomite -d -o ./logs/a_dc1.log \
                         -c ./conf/a_dc1.yml -s 22221 -M100000 -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack1_node1.log \
                         -c ./conf/a_dc2_rack1_node1.yml -s 22222 -M100000 -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack1_node2.log \
                         -c ./conf/a_dc2_rack1_node2.yml -s 22223 -M100000 -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack2_node1.log \
                         -c ./conf/a_dc2_rack2_node1.yml -s 22224 -M100000 -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack2_node2.log \
                         -c ./conf/a_dc2_rack2_node2.yml -s 22225 -M100000 -v6
}

function kill_redis() {
    killall redis-server
}

function kill_dynomite() {
    killall dynomite
}

declare -i RESULT
RESULT=0

function cleanup_and_exit() {
    kill_redis
    kill_dynomite
    exit $RESULT
}

launch_redis
launch_dynomite
DYNOMITE_NODES=`pgrep dynomite | wc -l`
REDIS_NODES=`pgrep redis-server | wc -l`

if [[ $DYNOMITE_NODES -ne 5 ]]; then
    print "Not all dynomite nodes are running"
    RESULT=1
    cleanup_and_exit
fi
if [[ $REDIS_NODES -ne 6 ]]; then
    print "Not all redis nodes are running"
    RESULT=1
    cleanup_and_exit
fi

sleep 10

./func_test.py
RESULT=$?
echo $RESULT

DYNOMITE_NODES=`pgrep dynomite | wc -l`
REDIS_NODES=`pgrep redis-server | wc -l`

if [[ $DYNOMITE_NODES -ne 5 ]]; then
    print "Not all dynomite nodes are running"
    RESULT=1
    cleanup_and_exit
fi
if [[ $REDIS_NODES -ne 6 ]]; then
    print "Not all redis nodes are running"
    RESULT=1
    cleanup_and_exit
fi

cleanup_and_exit

