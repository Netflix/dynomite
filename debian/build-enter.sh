#! /usr/bin/env bash

docker run -it \
	--rm \
	-u $(id -u):$(id -g) \
	-e HOME="$HOME" \
	-e USER="$USER" \
	-v "$HOME":"$HOME" \
	-v /etc/passwd:/etc/passwd \
	-v /etc/group:/etc/group \
	-w "$HOME" \
	local/debbuild
