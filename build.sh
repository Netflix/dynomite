#!/bin/bash

#
# Dear Mac user, remember to setup your development environment. Install XCode
# then run the following commands:
#
# xcode-select --install
# brew install cmake autoconf libtool gcc automake openssl
# brew link openssl --force
#

OS=`uname -s`

if [ $OS == "Darwin" ] ; then
    SSL_LIBDIR=`pkg-config --variable=libdir openssl`
    SSL_INCLUDEDIR=`pkg-config --variable=includedir openssl`
fi

#make clean

autoreconf -fvi

if [ $OS == "Darwin" ] ; then
    ./configure --enable-debug=yes LDFLAGS="-L${SSL_LIBDIR}" CPPFLAGS="-I${SSL_INCLUDEDIR}"
else
    ./configure --enable-debug=full
fi

make
