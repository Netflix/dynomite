#!/bin/bash
set -e

docker build -t dynomite-ubuntu -f docker/Dockerfile-ubuntu-18.04 .
docker create -ti --name dummy dynomite-ubuntu bash
docker cp dummy:/dynomite-ubuntu-18.04.tgz .
docker rm -fv dummy

