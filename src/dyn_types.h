#pragma once

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef HAVE_DEBUG_LOG
#define DN_DEBUG_LOG 1
#endif

#ifdef HAVE_ASSERT_PANIC
#define DN_ASSERT_PANIC 1
#endif

#ifdef HAVE_ASSERT_LOG
#define DN_ASSERT_LOG 1
#endif

#ifdef HAVE_STATS
#define DN_STATS 1
#else
#define DN_STATS 0
#endif

#ifdef HAVE_EPOLL
#define DN_HAVE_EPOLL 1
#elif HAVE_KQUEUE
#define DN_HAVE_KQUEUE 1
#elif HAVE_EVENT_PORTS
#define DN_HAVE_EVENT_PORTS 1
#else
#error missing scalable I/O event notification mechanism
#endif

#ifdef HAVE_LITTLE_ENDIAN
#define DN_LITTLE_ENDIAN 1
#endif

#ifdef HAVE_BACKTRACE
#define DN_HAVE_BACKTRACE 1
#endif


#define DN_NOOPS 1
#define DN_OK 0
#define DN_ERROR -1
#define DN_EAGAIN -2
#define DN_ENOMEM -3
#define DN_ENO_IMPL -4

#define THROW_STATUS(s)                  \
  {                                      \
    rstatus_t __ret = (s);               \
    if (__ret != DN_OK) {                \
      log_debug(LOG_WARN, "failed " #s); \
      return __ret;                      \
    }                                    \
  }

#define IGNORE_RET_VAL(x) (void)x;

typedef uint64_t msgid_t;
typedef uint64_t msec_t;
typedef uint64_t usec_t;
typedef uint64_t sec_t;

typedef int rstatus_t; /* return type */
typedef int err_t;     /* error type */

typedef enum {
  SECURE_OPTION_NONE,
  SECURE_OPTION_RACK,
  SECURE_OPTION_DC,
  SECURE_OPTION_ALL,
} secure_server_option_t;

struct array;
struct string;
struct context;
struct conn;
struct conn_tqh;
struct msg;
struct msg_tqh;
struct server;
struct server_pool;
struct mbuf;
struct mhdr;
struct conf;
struct stats;
struct entropy_conn;
struct instance;
struct event_base;
struct datacenter;
struct rack;
struct dyn_ring;

extern void cleanup_charptr(char **ptr);

#define SCOPED_CHARPTR(var) \
  char *var __attribute__((__cleanup__(cleanup_charptr)))

typedef enum {
  OBJ_REQ,
  OBJ_RSP,
  OBJ_CONN,
  OBJ_CONN_POOL,
  OBJ_POOL,
  OBJ_DATASTORE,
  OBJ_NODE,
  OBJ_LAST
} object_type_t;

#define PRINT_BUF_SIZE 255

struct object;
typedef char *(*func_print_t)(const struct object *obj);
typedef struct object {
  uint16_t magic;
  object_type_t type;
  func_print_t func_print;
  char print_buff[PRINT_BUF_SIZE];
} object_t;

void init_object(object_t *obj, object_type_t type, func_print_t func_print);

char *print_obj(const void *ptr);
