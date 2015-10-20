#!/bin/bash
#file   : travis.sh
#author : ipapapa
#date   : 2015-10-20


#build Dynomite
CFLAGS="-ggdb3 -O0" autoreconf -fvi && ./configure --enable-debug=log && make 


