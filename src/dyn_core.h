/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _DYN_CORE_H_
#define _DYN_CORE_H_

#ifdef HAVE_CONFIG_H
# include <config.h>
#endif

#ifdef HAVE_DEBUG_LOG
# define DN_DEBUG_LOG 1
#endif

#ifdef HAVE_ASSERT_PANIC
# define DN_ASSERT_PANIC 1
#endif

#ifdef HAVE_ASSERT_LOG
# define DN_ASSERT_LOG 1
#endif

#ifdef HAVE_STATS
# define DN_STATS 1
#else
# define DN_STATS 0
#endif

#ifdef HAVE_EPOLL
# define DN_HAVE_EPOLL 1
#elif HAVE_KQUEUE
# define DN_HAVE_KQUEUE 1
#elif HAVE_EVENT_PORTS
# define DN_HAVE_EVENT_PORTS 1
#else
# error missing scalable I/O event notification mechanism
#endif

#ifdef HAVE_LITTLE_ENDIAN
# define DN_LITTLE_ENDIAN 1
#endif

#ifdef HAVE_BACKTRACE
# define DN_HAVE_BACKTRACE 1
#endif

#define DN_NOOPS     1
#define DN_OK        0
#define DN_ERROR    -1
#define DN_EAGAIN   -2
#define DN_ENOMEM   -3

typedef int rstatus_t; /* return type */
typedef int err_t;     /* error type */

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
struct instance;
struct event_base;
struct rack;
struct dyn_ring;

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <netinet/in.h>

#include "dyn_array.h"
#include "dyn_string.h"
#include "dyn_queue.h"
#include "dyn_rbtree.h"
#include "dyn_log.h"
#include "dyn_util.h"
#include "dyn_stats.h"
#include "dyn_mbuf.h"
#include "dyn_message.h"
#include "dyn_connection.h"
#include "dyn_cbuf.h"
#include "dyn_ring_queue.h"

#include "event/dyn_event.h"


typedef enum dyn_state {
	INIT = 0,
	STANDBY = 1,
	WRITES_ONLY = 2,
	RESUMING = 3,
	NORMAL = 4,
	SUSPENDING = 5,
	LEAVING = 6,
	JOINING = 7,
	DOWN = 8,
	REMOVED = 9,
	EXITING = 10,
	UNKNOWN = 11
} dyn_state_t;


struct context {
    uint32_t           id;          /* unique context id */
    struct conf        *cf;         /* configuration */
    struct stats       *stats;      /* stats */

    struct array       pool;        /* server_pool[] */
    struct event_base  *evb;        /* event base */
    int                max_timeout; /* max timeout in msec */
    int                timeout;     /* timeout in msec */
    dyn_state_t        dyn_state;   /* state of the node.  Don't need volatile as
                                       it is ok to eventually get its new value */
};


struct instance {
    struct context  *ctx;                        /* active context */
    int             log_level;                   /* log level */
    char            *log_filename;               /* log filename */
    char            *conf_filename;              /* configuration filename */
    uint16_t        stats_port;                  /* stats monitoring port */
    int             stats_interval;              /* stats aggregation interval */
    char            *stats_addr;                 /* stats monitoring addr */
    char            hostname[DN_MAXHOSTNAMELEN]; /* hostname */
    size_t          mbuf_chunk_size;             /* mbuf chunk size */
    pid_t           pid;                         /* process id */
    char            *pid_filename;               /* pid filename */
    unsigned        pidfile:1;                   /* pid file created? */
};

struct context *core_start(struct instance *nci);
void core_stop(struct context *ctx);
rstatus_t core_core(void *arg, uint32_t events);
rstatus_t core_loop(struct context *ctx);
void core_debug(struct context *ctx);

#endif
