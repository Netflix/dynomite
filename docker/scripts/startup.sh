#!/bin/bash

# Setting overcommit for Redis to be able to do BGSAVE/BGREWRITEAOF
sysctl vm.overcommit_memory=1

#Start redis server on 22122
redis-server --port 22122 &

src/dynomite --conf-file=conf/redis_single.yml -v5
