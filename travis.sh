#!/bin/bash

if [ -n "$TRAVIS" ]; then
    sudo pip install redis plumbum pyyaml
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

set -o errexit
set -o nounset
set -o pipefail


#build Dynomite
if [[ "${rebuild}" == "true" ]]; then
    CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make
else
    echo "not rebuilding Dynomite"
fi

exec test/cluster_generator.py
