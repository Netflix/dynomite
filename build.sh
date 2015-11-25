#!/bin/bash

sudo autoreconf -fvi ; sudo ./configure --enable-debug=log ; sudo make CFLAGS=-DFLORIDA_REQUEST='"\"GET /REST/v1/admin/get_seeds HTTP/1.0\r\nHost: 127.0.0.1\r\nUser-Agent: HTMLGET 1.0\r\n\r\n\""'

