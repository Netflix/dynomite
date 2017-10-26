MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src -I $(top_srcdir)/contrib/murmur3

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wpointer-arith
AM_CFLAGS += -Winline
AM_CFLAGS += -Wunused-function -Wunused-variable -Wunused-value
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value -Wno-missing-prototypes -Wno-missing-declarations 
AM_CFLAGS += -Wconversion -Wsign-compare
AM_CFLAGS += -Wstrict-prototypes -Wredundant-decls -Werror=implicit-function-declaration

noinst_LIBRARIES = libhashkit.a

noinst_HEADERS = dyn_hashkit.h dyn_token.h

libhashkit_a_SOURCES =		\
	dyn_hashkit.c		\
	dyn_crc16.c		\
	dyn_crc32.c		\
	dyn_fnv.c		\
	dyn_hsieh.c		\
	dyn_jenkins.c		\
	dyn_md5.c		\
	dyn_murmur.c		\
	dyn_one_at_a_time.c	\
	dyn_token.c			\
	dyn_murmur3.c
