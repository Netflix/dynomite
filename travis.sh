#!/bin/bash

if [ -n "$TRAVIS" ]; then

    #python libs
    sudo pip install redis
    sudo pip install git+https://github.com/andymccurdy/redis-py.git@2.10.3
fi


# parse options
rebuild=true
debug=false
while [ "$#" -gt 0 ]; do
    arg=$1
    case $1 in
        -n|--no-rebuild) shift; rebuild=false;;
        -d|--debug) shift; debug=true;;
        -*) usage_fatal "unknown option: '$1'";;
        *) break;; # reached the list of file names
    esac
done


#build Dynomite
if [[ "${rebuild}" == "true" ]]; then
    CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make
else
    echo "not rebuilding Dynomite"
fi

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
                         -c ./conf/a_dc1.yml -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack1_node1.log \
                         -c ./conf/a_dc2_rack1_node1.yml -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack1_node2.log \
                         -c ./conf/a_dc2_rack1_node2.yml -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack2_node1.log \
                         -c ./conf/a_dc2_rack2_node1.yml -v6
    ./_binaries/dynomite -d -o ./logs/a_dc2_rack2_node2.log \
                         -c ./conf/a_dc2_rack2_node2.yml -v6
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
    echo "Not all dynomite nodes are running" >&2
    RESULT=1
    cleanup_and_exit
fi
if [[ $REDIS_NODES -ne 6 ]]; then
    echo "Not all redis nodes are running" >&2
    RESULT=1
    cleanup_and_exit
fi

echo "Cluster Deployed....."
sleep 10

if [[ "${debug}" == "true" ]]; then
    ./func_test.py --debug
else
    sh -c ./func_test.py
fi
RESULT=$?
echo $RESULT

# check a single stats port
curl -s localhost:22222/info | python -mjson.tool > /dev/null
if [[ $? -ne 0 ]]; then
    echo "Stats are not working or not valid json" >&2
    RESULT=1
fi

DYNOMITE_NODES=`pgrep dynomite | wc -l`
REDIS_NODES=`pgrep redis-server | wc -l`

if [[ $DYNOMITE_NODES -ne 5 ]]; then
    echo "Not all dynomite nodes are running" >&2
    RESULT=1
    cleanup_and_exit
fi
if [[ $REDIS_NODES -ne 6 ]]; then
    echo "Not all redis nodes are running" >&2
    RESULT=1
    cleanup_and_exit
fi

cleanup_and_exit
