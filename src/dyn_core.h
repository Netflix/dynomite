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
#define DN_ENO_IMPL -4


typedef int rstatus_t; /* return type */
typedef int err_t;     /* error type */

#define THROW_STATUS(s)                                             \
                {                                                   \
                    rstatus_t __ret = (s);                          \
                    if (__ret != DN_OK) {                           \
                        log_debug(LOG_WARN, "failed "#s);           \
                        return __ret;                               \
                    }                                               \
                }

#define IGNORE_RET_VAL(x) x;

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

#define ENCRYPTION 1

typedef rstatus_t (*hash_t)(const char *, size_t, struct dyn_token *);

typedef enum dyn_state {
	INIT        = 0,
	STANDBY     = 1,
	WRITES_ONLY = 2,
	RESUMING    = 3,
	NORMAL      = 4,
	//SUSPENDING  = 5,
	//LEAVING     = 6,
	JOINING     = 7,
	DOWN        = 8,
	//REMOVED     = 9,
	//EXITING     = 10,
	RESET       = 11,
	UNKNOWN     = 12
} dyn_state_t;

static inline char*
get_state(dyn_state_t s) {
	switch(s)
	{
		case INIT: return "INIT";
		case STANDBY: return "STANDBY";
		case WRITES_ONLY: return "WRITES_ONLY";
		case RESUMING: return "RESUMING";
		case NORMAL: return "NORMAL";
		//case SUSPENDING: return "SUSPENDING";
		//case LEAVING: return "LEAVING";
		case JOINING: return "JOINING";
		case DOWN: return "DOWN";
		//case REMOVED: return "REMOVED";
		//case EXITING: return "EXITING";
		case RESET: return "RESET";
		case UNKNOWN: return "Unknown";
	}
	return "INVALID STATE";
}

typedef enum data_store {
	DATA_REDIS        = 0, /* Data store is Redis */
	DATA_MEMCACHE	  = 1  /* Data store is Memcache */
} data_store_t;

extern data_store_t g_data_store;

/** \struct instance
 * @brief An instance of the Dynomite server.
 *
 * Dynomite server properties including log level, log file, conf file,
 * statistics port and collection interval, statistics address, hostname, pid,
 * pid file and various other properties.
 */
struct instance {
    struct context  *ctx;                        /* active context */
    int             log_level;                   /* log level */
    char            *log_filename;               /* log filename */
    char            *conf_filename;              /* configuration filename */
    char            hostname[DN_MAXHOSTNAMELEN]; /* hostname */
    uint16_t        entropy_port;                /* send reconciliation port */
    char            *entropy_addr;               /* send reconciliation addr */
    size_t          mbuf_chunk_size;             /* mbuf chunk size */
    size_t          alloc_msgs_max;              /* allocated messages buffer size */
    pid_t           pid;                         /* process id */
    char            *pid_filename;               /* pid filename */
    unsigned        pidfile:1;                   /* pid file created? */
};


struct continuum {
	uint32_t index;  /* dyn_peer index */
	uint32_t value;  /* hash value, used by ketama */
	struct dyn_token *token;  /* used in vnode/dyn_token situations */
};

struct rack {
	struct string      *name;
	struct string      *dc;
	uint32_t           ncontinuum;           /* # continuum points */
	uint32_t           nserver_continuum;    /* # servers - live and dead on continuum (const) */
	struct continuum   *continuum;           /* continuum */
};


struct datacenter {
	struct string      *name;            /* datacenter name */
	struct array       racks;           /* list of racks in a datacenter */
    struct rack        *preselected_rack_for_replication;
	dict               *dict_rack;
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

    struct conn        *conn;         /* the only server connection */

    msec_t             next_retry_ms; /* next retry time in msec */
    sec_t              reconnect_backoff_sec; /* backoff time in seconds */
    uint32_t           failure_count; /* # consecutive failures */
};

/** \struct node
 * @brief Dynomite server node.
 */
struct node {
    uint32_t           idx;           /* server index */
    struct server_pool *owner;        /* owner pool */
    struct endpoint    endpoint;
    struct string      name;          /* name (ref in conf_server) */

    struct conn        *conn;         /* the only peer connection */

    msec_t             next_retry_ms;    /* next retry time in msec */
    sec_t              reconnect_backoff_sec; /* backoff time in seconds */
    uint32_t           failure_count; /* # consecutive failures */

    struct string      rack;          /* logical rack */
    struct string      dc;            /* server's dc */
    struct array       tokens;        /* DHT tokens this peer owns */
    bool               is_local;      /* is this peer the current running node?  */
    unsigned           is_seed:1;     /* seed? */
    unsigned           processed:1;   /* flag to indicate whether this has been processed */
    unsigned           is_secure:1;   /* is the connection to the server secure? */
    dyn_state_t        state;         /* state of the server - used mainly in peers  */
};

/** \struct server_pool
 * @brief Server pool.
 *
 * Server configuration including proxy connection, client connections, data
 * center and rack information, plus hash information such as distribution type
 * and hash type. Contains limits including client and server connection limits.
 * Contains cluster information such as seeds and seed provider, plus node
 * information such as dc, rack, node token and runtime environment.
 */
struct server_pool {
    struct context     *ctx;                 /* owner context */
    struct conf_pool   *conf_pool;           /* back reference to conf_pool */

    struct conn        *p_conn;              /* proxy connection (listener) */
    uint32_t           dn_conn_q;            /* # client connection */
    struct conn_tqh    c_conn_q;             /* client connection q */
    struct conn_tqh    ready_conn_q;         /* ready connection q */

    struct datastore   *datastore;           /* underlying datastore */
    struct array       datacenters;          /* racks info  */
    uint64_t           next_rebuild;         /* next distribution rebuild time in usec */

    struct string      name;                 /* pool name (ref in conf_pool) */
    struct endpoint    proxy_endpoint;
    int                key_hash_type;        /* key hash type (hash_type_t) */
    hash_t             key_hash;             /* key hasher */
    struct string      hash_tag;             /* key hash tag (ref in conf_pool) */
    msec_t             timeout;              /* timeout in msec */
    int                backlog;              /* listen backlog */
    uint32_t           client_connections;   /* maximum # client connection */
    msec_t             server_retry_timeout_ms; /* server retry timeout in msec */
    uint32_t           server_failure_limit; /* server failure limit */
    unsigned           auto_eject_hosts:1;   /* auto_eject_hosts? */
    unsigned           preconnect:1;         /* preconnect? */

    /* dynomite */
    struct string      seed_provider;
    struct array       seeds;                /*dyn seeds */
    struct array       peers;
    struct conn        *d_conn;              /* dnode connection (listener) */
    struct endpoint    dnode_proxy_endpoint;
    int                d_timeout;            /* peer timeout in msec */
    int                d_backlog;            /* listen backlog */
    int64_t            d_retry_timeout;      /* peer retry timeout in usec */
    uint32_t           d_failure_limit;      /* peer failure limit */
    uint32_t           peer_connections;     /* maximum # peer connections */
    struct string      rack;                 /* the rack for this node */
    struct array       tokens;               /* the DHT tokens for this server */

    int                g_interval;           /* gossip interval */
    struct string      dc;                   /* server's dc */
    struct string      env;                  /* aws, network, etc */
    /* none | datacenter | rack | all in order of increasing number of connections. (default is datacenter) */
    secure_server_option_t secure_server_option;
    struct string      pem_key_file;
    struct string      recon_key_file;       /* file with Key encryption in reconciliation */
    struct string      recon_iv_file;        /* file with Initialization Vector encryption in reconciliation */
    struct endpoint    stats_endpoint;       /* stats_listen: socket info for stats */
    int                stats_interval;       /* stats aggregation interval */
    bool               enable_gossip;        /* enable/disable gossip */
    size_t             mbuf_size;            /* mbuf chunk size */
    size_t             alloc_msgs_max;       /* allocated messages buffer size */
};

/** \struct context
 * @brief Context of the Dynomite process.
 *
 * Context of the Dynomite process including it's configuration including
 * dynomite itself plus statistics, entropy, the server pool (i.e. connections),
 * the event base, timeout, dynomite state, gossip and whether or not the admin
 * functionality is enabled/disabled.
 */
struct context {
    struct instance    *instance;   /* back pointer to instance */
    struct conf        *cf;         /* configuration */
    struct stats       *stats;      /* stats */
    struct entropy     *entropy;    /* reconciliation connection */
    struct server_pool pool;        /* server_pool[] */
    struct event_base  *evb;        /* event base */
    int                max_timeout; /* max timeout in msec */
    int                timeout;     /* timeout in msec */
    dyn_state_t        dyn_state;   /* state of the node.  Don't need volatile as
                                       it is ok to eventually get its new value */
    unsigned           admin_opt;   /* admin mode */
};



rstatus_t core_start(struct instance *nci);
void core_stop(struct context *ctx);
rstatus_t core_core(void *arg, uint32_t events);
rstatus_t core_loop(struct context *ctx);
void core_debug(struct context *ctx);

#endif
