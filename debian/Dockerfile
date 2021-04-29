FROM ubuntu:14.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    bzr-builddeb \
    bzr-git \
    dh-autoreconf \
    dh-exec \
    dh-make \
    git \
    libssl-dev \
  && rm -rf /var/lib/apt/lists/*

CMD /bin/bash
