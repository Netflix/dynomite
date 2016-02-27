MAINTAINERCLEANFILES = Makefile.in
AUTOMAKE_OPTIONS = subdir-objects

AM_CPPFLAGS =
if !OS_SOLARIS
AM_CPPFLAGS += -D_GNU_SOURCE
endif
AM_CPPFLAGS += -I $(top_srcdir)/src
AM_CPPFLAGS += -I $(top_srcdir)/src/hashkit

AM_CFLAGS =
AM_CFLAGS += -Wall -Wshadow
AM_CFLAGS += -Wpointer-arith
AM_CFLAGS += -Winline
AM_CFLAGS += -Wunused-function -Wunused-variable -Wunused-value
AM_CFLAGS += -Wno-unused-parameter -Wno-unused-value
AM_CFLAGS += -Wconversion -Wsign-compare
AM_CFLAGS += -Wstrict-prototypes -Wmissing-prototypes -Wredundant-decls -Wmissing-declarations

AM_LDFLAGS =

if OS_SOLARIS
AM_LDFLAGS += -lnsl -lsocket
endif

sbin_PROGRAMS = dyn_hash_tool

dyn_hash_tool_SOURCES =                                                \
        dyn_hash_tool.c ../dyn_token.c ../dyn_log.c ../dyn_util.c ../dyn_array.c

dyn_hash_tool_LDADD = $(top_builddir)/src/hashkit/libhashkit.a
