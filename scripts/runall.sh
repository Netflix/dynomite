#!/bin/bash

N=$2
O=$1

if [ -z "$N" ]; then
  N=1
fi

if [ -z "$O" ]; then
  O=read
fi

echo "N=$N"
echo "O=$O"

for i in $( cat listnodes ); do
       echo 'processing: ' $i
       python dynomite/dyn_redis_test.py -P 22122 -H $i  -o $O -n $N
       sleep 1
done
