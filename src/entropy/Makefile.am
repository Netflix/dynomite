MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wpointer-arith
AM_CFLAGS += -Winline
AM_CFLAGS += -Wunused-function -Wunused-variable -Wunused-value
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value
AM_CFLAGS += -Wconversion -Wsign-compare
AM_CFLAGS += -Wstrict-prototypes -Wmissing-prototypes -Wredundant-decls -Wmissing-declarations -Wimplicit-function-declaration


noinst_LIBRARIES = libentropy.a

noinst_HEADERS = dyn_entropy.h

libentropy_a_SOURCES = 			\
	dyn_entropy_rcv.c			\
	dyn_entropy_snd.c			\
	dyn_entropy_util.c
