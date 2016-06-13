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

#define MAX_THREADS 3
#define THROW_STATUS(s)                                             \
                {                                                   \
                    rstatus_t __ret = (s);                          \
                    if (__ret != DN_OK) {                           \
                        log_debug(LOG_WARN, "failed "#s);           \
                        return __ret;                               \
                    }                                               \
                }

#define IGNORE_RET_VAL(x) x;

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
struct rack;
struct dyn_ring;
struct topology;
struct peer;

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

#include "dyn_error.h"
#include "dyn_types.h"
#include "dyn_array.h"
#include "dyn_dict.h"
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
#include "dyn_crypto.h"
#include "dyn_setting.h"


#include "entropy/dyn_entropy.h"
#include "event/dyn_event.h"
extern pthread_barrier_t datastore_preconnect_barr;

#define ENCRYPTION 0

struct thread_ctx;
typedef rstatus_t (*hash_t)(const char *, size_t, struct dyn_token *);
struct datacenter;

typedef enum dyn_state {
	INIT        = 0,
	STANDBY     = 1,
	WRITES_ONLY = 2,
	RESUMING    = 3,
	NORMAL      = 4,
	SUSPENDING  = 5,
	LEAVING     = 6,
	JOINING     = 7,
	DOWN        = 8,
	REMOVED     = 9,
	EXITING     = 10,
	RESET       = 11,
	UNKNOWN     = 12
} dyn_state_t;

typedef enum data_store {
	DATA_REDIS        = 0, /* Data store is Redis */
	DATA_MEMCACHE	  = 1  /* Data store is Memcache */
} data_store_t;

extern data_store_t g_data_store;

struct continuum {
	uint32_t index;  /* index in peers array in server_pool */
	uint32_t value;  /* hash value, used by ketama */
	struct dyn_token *token;  /* used in vnode/dyn_token situations */
};

struct instance {
    struct context  *ctx;                        /* active context */
    int             log_level;                   /* log level */
    char            *log_filename;               /* log filename */
    char            *conf_filename;              /* configuration filename */
    uint16_t        stats_port;                  /* stats monitoring port */
    msec_t          stats_interval;              /* stats aggregation interval */
    char            *stats_addr;                 /* stats monitoring addr */
    char            hostname[DN_MAXHOSTNAMELEN]; /* hostname */
    uint16_t        entropy_port;                  /* send reconciliation port */
    char            *entropy_addr;                 /* send reconciliation addr */
    size_t          mbuf_chunk_size;             /* mbuf chunk size */
    size_t			alloc_msgs_max;			 /* allocated messages buffer size */
    pid_t           pid;                         /* process id */
    char            *pid_filename;               /* pid filename */
    unsigned        pidfile:1;                   /* pid file created? */
};

struct endpoint {
    struct string      pname;         /* name:port:weight (ref in conf_server) */
    uint16_t           port;          /* port */
    uint32_t           weight;        /* weight */
    int                family;        /* socket family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in conf_server) */
};

struct datastore {
    uint32_t           idx;           /* server index */
    struct server_pool *owner;        /* owner pool */
    struct endpoint     endpoint;
    struct string      name;          /* name (ref in conf_server) */

    msec_t             next_retry;    /* next retry time in msec */
    uint32_t           failure_count; /* # consecutive failures */
};

struct server_pool {
    struct context     *ctx;                 /* owner context */
    struct conf_pool   *conf_pool;           /* back reference to conf_pool */

    struct conn        *p_conn;              /* proxy connection (listener) */
    uint32_t           dn_conn_q;            /* # client connection */
    struct conn_tqh    c_conn_q;             /* client connection q */

    struct datastore   *datastore;               /* underlying datastore */
    struct topology    *topo;                 /* current active topology */
    uint32_t           nlive_server;         /* # live server */
    uint64_t           next_rebuild;         /* next distribution rebuild time in usec */

    struct string      name;                 /* pool name (ref in conf_pool) */
    struct endpoint    proxy_endpoint;
    msec_t             timeout;              /* timeout in msec */
    int                backlog;              /* listen backlog */
    uint32_t           client_connections;   /* maximum # client connection */
    uint32_t           server_connections;   /* maximum # server connection */
    msec_t             server_retry_timeout_ms; /* server retry timeout in msec */
    uint32_t           server_failure_limit; /* server failure limit */
    unsigned           auto_eject_hosts:1;   /* auto_eject_hosts? */
    unsigned           preconnect:1;         /* preconnect? */

    /* dynomite */
    struct conn        *d_conn;              /* dnode connection (listener) */
    struct endpoint    dnode_proxy_endpoint;
    int                d_timeout;            /* peer timeout in msec */
    int                d_backlog;            /* listen backlog */
    int64_t            d_retry_timeout;      /* peer retry timeout in usec */
    uint32_t           d_failure_limit;      /* peer failure limit */
    uint32_t           peer_connections;     /* maximum # peer connections */
    struct string      rack_name;                 /* the rack for this node */
    struct array       tokens;               /* the DHT tokens for this server */

    int                g_interval;           /* gossip interval */
    struct string      dc_name;                   /* server's dc */
    struct datacenter  *my_dc;
    struct rack        *my_rack;
    struct string      env;                  /* aws, network, ect */
    /* none | datacenter | rack | all in order of increasing number of connections. (default is datacenter) */
    secure_server_option_t secure_server_option;
    struct string      pem_key_file;
    struct string      recon_key_file;       /* file with Key encryption in reconciliation */
	struct string      recon_iv_file;        /* file with Initialization Vector encryption in reconciliation */
};

struct context {
    struct instance    *instance;   /* back pointer to instance */
    struct conf        *cf;         /* configuration */
    struct stats       *stats;      /* stats */
    struct entropy     *entropy;    /* reconciliation connection */
    struct server_pool pool;        /* server_pool[] */
    msec_t              max_timeout; /* max timeout in msec */
    dyn_state_t        dyn_state;   /* state of the node.  Don't need volatile as
                                       it is ok to eventually get its new value */
    unsigned           enable_gossip:1;   /* enable/disable gossip */
    unsigned           admin_opt;   /* admin mode */
    struct array       thread_ctxs;  /* array of all thread contexts in the system*/
};

static inline struct server_pool *
ctx_get_pool(struct context *ctx)
{
    return &ctx->pool;
}

static inline struct topology *
ctx_get_topology(struct context *ctx)
{
    return ctx->pool.topo;
}

rstatus_t core_create(struct instance *nci);
void core_destroy(struct context *ctx);
rstatus_t core_loop(struct context *ctx);
struct thread_ctx *core_get_ptctx_for_conn(struct context *ctx, struct conn *conn);
struct thread_ctx *core_get_ptctx_for_peer(struct context *ctx, struct peer *peer);

#endif
