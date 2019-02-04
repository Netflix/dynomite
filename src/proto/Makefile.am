MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libproto.a

noinst_HEADERS = dyn_proto.h

libproto_a_SOURCES =			\
	dyn_memcache.c			\
	dyn_redis.c					\
	dyn_redis_repair.c
