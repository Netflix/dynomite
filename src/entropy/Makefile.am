MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow -Werror
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value


noinst_LIBRARIES = libentropy.a

noinst_HEADERS = dyn_entropy.h

libentropy_a_SOURCES = 			\
	dyn_entropy_rcv.c			\
	dyn_entropy_snd.c			\
	dyn_entropy_util.c
