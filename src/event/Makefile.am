MAINTAINERCLEANFILES = Makefile.in

AM_CPPFLAGS = -I $(top_srcdir)/src

AM_CFLAGS = -Wall -Wshadow
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value

noinst_LIBRARIES = libevent.a

noinst_HEADERS = dyn_event.h

libevent_a_SOURCES =	\
	dyn_epoll.c	\
	dyn_kqueue.c	\
	dyn_evport.c

