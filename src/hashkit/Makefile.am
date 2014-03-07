MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src -I $(top_srcdir)/contrib/murmur3

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libhashkit.a

noinst_HEADERS = dyn_hashkit.h

libhashkit_a_SOURCES =		\
	dyn_crc16.c		\
	dyn_crc32.c		\
	dyn_fnv.c		\
	dyn_hsieh.c		\
	dyn_jenkins.c		\
	dyn_ketama.c		\
	dyn_md5.c		\
	dyn_modula.c		\
	dyn_murmur.c		\
	dyn_one_at_a_time.c	\
	dyn_random.c		\
	dyn_vnode.c     \
	dyn_token.c	    \
	dyn_murmur3.c
