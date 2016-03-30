MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wpointer-arith
AM_CFLAGS += -Winline
AM_CFLAGS += -Wunused-function -Wunused-variable -Wunused-value
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value
AM_CFLAGS += -Wconversion -Wsign-compare
AM_CFLAGS += -Wstrict-prototypes -Wmissing-prototypes -Wredundant-decls -Wmissing-declarations -Werror=implicit-function-declaration

noinst_LIBRARIES = libevent.a

noinst_HEADERS = dyn_event.h

libevent_a_SOURCES =	\
	dyn_epoll.c	\
	dyn_kqueue.c	\
	dyn_evport.c

