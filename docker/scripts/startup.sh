#!/bin/bash

#Start redis server on 22122
redis-server --port 22122 /etc/redis/redis.conf &

src/dynomite $1 -v11
