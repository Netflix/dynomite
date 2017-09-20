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
 *_stats_pool_set_ts
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "dyn_core.h"
#include "dyn_histogram.h"


#ifndef _DYN_STATS_H_
#define _DYN_STATS_H_



#define STATS_POOL_CODEC(ACTION)                                                                                          \
    /* client behavior */                                                                                                 \
    ACTION( client_eof,                   STATS_COUNTER,      "# eof on client connections")                              \
    ACTION( client_err,                   STATS_COUNTER,      "# errors on client connections")                           \
    ACTION( client_connections,           STATS_GAUGE,        "# active client connections")                              \
    ACTION( client_read_requests,         STATS_COUNTER,      "# client read requests")                                   \
    ACTION( client_write_requests,        STATS_COUNTER,      "# client write responses")                                 \
    ACTION( client_dropped_requests,      STATS_COUNTER,      "# client dropped requests")                                \
    ACTION( client_non_quorum_w_responses,STATS_COUNTER,      "# client non quorum write responses")                      \
    ACTION( client_non_quorum_r_responses,STATS_COUNTER,      "# client non quorum read responses")                       \
    /* pool behavior */                                                                                                   \
    ACTION( server_ejects,                STATS_COUNTER,      "# times backend server was ejected")                       \
    /* dnode client behavior */                                                                                           \
    ACTION( dnode_client_eof,             STATS_COUNTER,      "# eof on dnode client connections")                        \
    ACTION( dnode_client_err,             STATS_COUNTER,      "# errors on dnode client connections")                     \
    ACTION( dnode_client_connections,     STATS_GAUGE,        "# active dnode client connections")                        \
    ACTION( dnode_client_in_queue,        STATS_GAUGE,        "# dnode client requests in incoming queue")                \
    ACTION( dnode_client_in_queue_bytes,  STATS_GAUGE,        "current dnode client request bytes in incoming queue")     \
    ACTION( dnode_client_out_queue,       STATS_GAUGE,        "# dnode client requests in outgoing queue")                \
    ACTION( dnode_client_out_queue_bytes, STATS_GAUGE,        "current dnode client request bytes in outgoing queue")     \
    /* peer behavior */                                                                                                   \
    ACTION( peer_dropped_requests,        STATS_COUNTER,      "# local dc peer dropped requests")                         \
    ACTION( peer_timedout_requests,       STATS_COUNTER,      "# local dc peer timedout requests")                        \
    ACTION( remote_peer_dropped_requests, STATS_COUNTER,      "# remote dc peer dropped requests")                        \
    ACTION( remote_peer_timedout_requests,STATS_COUNTER,      "# remote dc peer timedout requests")                       \
    ACTION( remote_peer_failover_requests,STATS_COUNTER,      "# remote dc peer failover requests")                       \
    ACTION( peer_eof,                     STATS_COUNTER,      "# eof on peer connections")                                \
    ACTION( peer_err,                     STATS_COUNTER,      "# errors on peer connections")                             \
    ACTION( peer_timedout,                STATS_COUNTER,      "# timeouts on local dc peer connections")                           \
    ACTION( remote_peer_timedout,         STATS_COUNTER,      "# timeouts on remote dc peer connections")                           \
    ACTION( peer_connections,             STATS_GAUGE,        "# active peer connections")                                \
    ACTION( peer_forward_error,           STATS_GAUGE,        "# times we encountered a peer forwarding error")           \
    ACTION( peer_requests,                STATS_COUNTER,      "# peer requests")                                          \
    ACTION( peer_request_bytes,           STATS_COUNTER,      "total peer request bytes")                                 \
    ACTION( peer_responses,               STATS_COUNTER,      "# peer respones")                                          \
    ACTION( peer_response_bytes,          STATS_COUNTER,      "total peer response bytes")                                \
    ACTION( peer_ejected_at,              STATS_TIMESTAMP,    "timestamp when peer was ejected")                               \
    ACTION( peer_ejects,                  STATS_COUNTER,      "# times a peer was ejected")                               \
    ACTION( peer_in_queue,                STATS_GAUGE,        "# local dc peer requests in incoming queue")                        \
    ACTION( remote_peer_in_queue,         STATS_GAUGE,        "# remote dc peer requests in incoming queue")                        \
    ACTION( peer_in_queue_bytes,          STATS_GAUGE,        "current peer request bytes in incoming queue")             \
    ACTION( remote_peer_in_queue_bytes,   STATS_GAUGE,        "current peer request bytes in incoming queue to remote DC")             \
    ACTION( peer_out_queue,               STATS_GAUGE,        "# local dc peer requests in outgoing queue")                        \
    ACTION( remote_peer_out_queue,        STATS_GAUGE,        "# remote dc peer requests in outgoing queue")                        \
    ACTION( peer_out_queue_bytes,         STATS_GAUGE,        "current peer request bytes in outgoing queue")             \
    ACTION( remote_peer_out_queue_bytes,  STATS_GAUGE,        "current peer request bytes in outgoing queue to remote DC")             \
    ACTION( peer_mismatch_requests,       STATS_COUNTER,      "current dnode peer mismatched messages")                   \
    /* forwarder behavior */                                                                                              \
    ACTION( forward_error,                STATS_COUNTER,      "# times we encountered a forwarding error")                \
    ACTION( fragments,                    STATS_COUNTER,      "# fragments created from a multi-vector request")          \
    ACTION( stats_count,                  STATS_COUNTER,      "# stats request")                                          \

#define STATS_SERVER_CODEC(ACTION)                                                                                             \
    /* server behavior */                                                                                                      \
    ACTION( server_eof,                   STATS_COUNTER,           "# eof on server connections")                              \
    ACTION( server_err,                   STATS_COUNTER,           "# errors on server connections")                           \
    ACTION( server_timedout,              STATS_COUNTER,           "# timeouts on server connections")                         \
    ACTION( server_ejected_at,            STATS_TIMESTAMP,         "timestamp when server was ejected in usec since epoch")    \
    ACTION( server_dropped_requests,      STATS_COUNTER,           "# server dropped requests")                                \
    ACTION( server_timedout_requests,     STATS_COUNTER,           "# server timedout requests")                               \
    /* data behavior */                                                                                                        \
    ACTION( read_requests,                STATS_COUNTER,           "# read requests")                                          \
    ACTION( read_request_bytes,           STATS_COUNTER,           "total read request bytes")                                 \
    ACTION( write_requests,               STATS_COUNTER,           "# write requests")                                         \
    ACTION( write_request_bytes,          STATS_COUNTER,           "total write request bytes")                                \
    ACTION( read_responses,               STATS_COUNTER,           "# read respones")                                          \
    ACTION( read_response_bytes,          STATS_COUNTER,           "total read response bytes")                                \
    ACTION( write_responses,              STATS_COUNTER,           "# write respones")                                         \
    ACTION( write_response_bytes,         STATS_COUNTER,           "total write response bytes")                               \
    ACTION( in_queue,                     STATS_GAUGE,             "# requests in incoming queue")                             \
    ACTION( in_queue_bytes,               STATS_GAUGE,             "current request bytes in incoming queue")                  \
    ACTION( out_queue,                    STATS_GAUGE,             "# requests in outgoing queue")                             \
    ACTION( out_queue_bytes,              STATS_GAUGE,             "current request bytes in outgoing queue")                  \
    /* Redis */																											  \
	ACTION( redis_req_get,				  STATS_COUNTER,	  "# Redis get")											  \
	ACTION( redis_req_set,				  STATS_COUNTER,	  "# Redis set")											  \
	ACTION( redis_req_del,				  STATS_COUNTER,	  "# Redis del")											  \
	ACTION( redis_req_incr_decr,		  STATS_COUNTER,	  "# Redis incr or decr")									  \
	ACTION( redis_req_keys,				  STATS_COUNTER,	  "# Redis keys")											  \
	ACTION( redis_req_mget,				  STATS_COUNTER,	  "# Redis mget")											  \
	ACTION( redis_req_scan,				  STATS_COUNTER,	  "# Redis scan")											  \
	ACTION( redis_req_sort,				  STATS_COUNTER,	  "# Redis sort")											  \
	ACTION( redis_req_lreqm,			  STATS_COUNTER,	  "# Redis lreqm")											  \
	ACTION( redis_req_sunion,			  STATS_COUNTER,	  "# Redis sunion")											  \
	ACTION( redis_req_ping,				  STATS_COUNTER,	  "# Redis ping")											  \
	ACTION( redis_req_lists,			  STATS_COUNTER,	  "# Redis lists")											  \
	ACTION( redis_req_sets,				  STATS_COUNTER,	  "# Redis sets")											  \
	ACTION( redis_req_hashes,			  STATS_COUNTER,	  "# Redis hashes")											  \
	ACTION( redis_req_sortedsets,		  STATS_COUNTER,	  "# Redis sortedsets")										  \
	ACTION( redis_req_other,			  STATS_COUNTER,	  "# Redis other")											  \


typedef enum stats_type {
    STATS_INVALID,
    STATS_COUNTER,    /* monotonic accumulator */
    STATS_GAUGE,      /* non-monotonic accumulator */
    STATS_TIMESTAMP,  /* monotonic timestamp (in nsec) */
    STATS_STRING,
    STATS_SENTINEL
} stats_type_t;

typedef enum {
    CMD_UNKNOWN,
    CMD_HELP,
    CMD_INFO,
    CMD_PING,
    CMD_DESCRIBE,
    CMD_STANDBY,
    CMD_WRITES_ONLY,
    CMD_RESUMING,
    CMD_NORMAL,
    CMD_BOOTSTRAPING,
    CMD_LEAVING,
    CMD_PEER_DOWN,
    CMD_PEER_UP,
    CMD_PEER_RESET,
    CMD_SET_LOG_LEVEL,
    CMD_LOG_LEVEL_UP,
    CMD_LOG_LEVEL_DOWN,
    CMD_HISTO_RESET,
    CMD_CL_DESCRIBE,  /* cluster_describe */
    CMD_SET_CONSISTENCY,
    CMD_GET_CONSISTENCY,
    CMD_GET_TIMEOUT_FACTOR,
    CMD_SET_TIMEOUT_FACTOR,
    CMD_GET_STATE,
} stats_cmd_t;

struct stats_metric {
    stats_type_t  type;         /* type */
    struct string name;         /* name (ref) */
    union {
        int64_t   counter;      /* accumulating counter */
        int64_t   timestamp;    /* monotonic timestamp */
        struct string str;     /* store string value */
    } value;
};

struct stats_dnode {
    struct string name;   /* dnode server name (ref) */
    struct array  metric; /* stats_metric[] for dnode server codec */
};

struct stats_server {
    struct string name;   /* server name (ref) */
    struct array  metric; /* stats_metric[] for server codec */
};

struct stats_pool {
    struct string name;   /* pool name (ref) */
    struct array  metric; /* stats_metric[] for pool codec */
    struct stats_server server; /* stats for datastore */
};

struct stats_buffer {
    size_t   len;   /* buffer length */
    uint8_t  *data; /* buffer data */
    size_t   size;  /* buffer alloc size */
};

/** \struct stats
 * Dynomite server performance statistics.
 */
struct stats {
    struct context           *ctx;
    uint16_t                  port;           /* stats monitoring port */
    int                       interval;       /* stats aggregation interval */
    struct string             addr;           /* stats monitoring address */

    int64_t                   start_ts;       /* start timestamp of dynomite */
    struct stats_buffer       buf;            /* info buffer */
    struct stats_buffer       clus_desc_buf;  /* cluster_describe buffer */

    struct stats_pool         current;        /* stats_pool[] (a) */
    struct stats_pool         shadow;         /* stats_pool[] (b) */
    struct stats_pool         sum;            /* stats_pool[] (c = a + b) */

    pthread_t                 tid;            /* stats aggregator thread */
    int                       sd;             /* stats descriptor */

    struct string             service_str;    /* service string */
    struct string             service;        /* service */
    struct string             source_str;     /* source string */
    struct string             source;         /* source */
    struct string             version_str;    /* version string */
    struct string             version;        /* version */
    struct string             uptime_str;     /* uptime string */
    struct string             timestamp_str;  /* timestamp string */
    struct string             latency_999th_str;
    struct string             latency_99th_str;
    struct string             latency_95th_str;
    struct string             latency_mean_str;
    struct string             latency_max_str;

    struct string             payload_size_999th_str;
    struct string             payload_size_99th_str;
    struct string             payload_size_95th_str;
    struct string             payload_size_mean_str;
    struct string             payload_size_max_str;

    struct string             cross_region_avg_rtt;
    struct string             cross_region_99_rtt;

    struct string             client_out_queue_99;
    struct string             server_in_queue_99;
    struct string             server_out_queue_99;
    struct string             dnode_client_out_queue_99;
    struct string             peer_in_queue_99;
    struct string             peer_out_queue_99;
    struct string             remote_peer_in_queue_99;
    struct string             remote_peer_out_queue_99;

    struct string             alloc_msgs_str;
    struct string             free_msgs_str;
    struct string             alloc_mbufs_str;
    struct string             free_mbufs_str;
    struct string			  dyn_memory_str;

    struct string             rack_str;
    struct string             rack;

    struct string             dc_str;
    struct string             dc;

    volatile int              aggregate;      /* shadow (b) aggregate? */
    volatile int              updated;        /* current (a) updated? */
    volatile bool             reset_histogram;
    volatile struct histogram latency_histo;
    volatile struct histogram payload_size_histo;

    volatile struct histogram server_latency_histo;
    volatile struct histogram cross_zone_latency_histo;
    volatile struct histogram cross_region_latency_histo;

    volatile struct histogram server_queue_wait_time_histo;
    volatile struct histogram cross_zone_queue_wait_time_histo;
    volatile struct histogram cross_region_queue_wait_time_histo;

    volatile struct histogram client_out_queue;
    volatile struct histogram server_in_queue;
    volatile struct histogram server_out_queue;
    volatile struct histogram dnode_client_out_queue;
    volatile struct histogram peer_in_queue;
    volatile struct histogram peer_out_queue;
    volatile struct histogram remote_peer_in_queue;
    volatile struct histogram remote_peer_out_queue;

    size_t           alloc_msgs;
    size_t           free_msgs;
    uint64_t         alloc_mbufs;
    uint64_t         free_mbufs;
    uint64_t         dyn_memory;

};


#define DEFINE_ACTION(_name, _type, _desc) STATS_POOL_##_name,
typedef enum stats_pool_field {
    STATS_POOL_CODEC(DEFINE_ACTION)
    STATS_POOL_NFIELD
} stats_pool_field_t;
#undef DEFINE_ACTION

#define DEFINE_ACTION(_name, _type, _desc) STATS_SERVER_##_name,
typedef enum stats_server_field {
    STATS_SERVER_CODEC(DEFINE_ACTION)
    STATS_SERVER_NFIELD
} stats_server_field_t;
#undef DEFINE_ACTION

struct stats_cmd {
	stats_cmd_t cmd;
	struct string req_data;
};


#if defined DN_STATS && DN_STATS == 1

#define stats_pool_incr(_ctx, _name) do {                        \
    _stats_pool_incr(_ctx, STATS_POOL_##_name);                  \
} while (0)

#define stats_pool_decr(_ctx, _name) do {                        \
    _stats_pool_decr(_ctx, STATS_POOL_##_name);                  \
} while (0)

#define stats_pool_incr_by(_ctx, _name, _val) do {               \
    _stats_pool_incr_by(_ctx, STATS_POOL_##_name, _val);         \
} while (0)

#define stats_pool_decr_by(_ctx, _name, _val) do {               \
    _stats_pool_decr_by(_ctx, STATS_POOL_##_name, _val);         \
} while (0)

#define stats_pool_set_ts(_ctx, _name, _val) do {                \
    _stats_pool_set_ts(_ctx, STATS_POOL_##_name, _val);          \
} while (0)

#define stats_pool_get_ts(_ctx, _name)                           \
    _stats_pool_get_ts(_ctx, STATS_POOL_##_name)

#define stats_pool_set_val(_ctx, _name, _val) do {                \
    _stats_pool_set_val(_ctx, STATS_POOL_##_name, _val);          \
} while (0)

#define stats_pool_get_val(_ctx, _name)                          \
    _stats_pool_get_val(_ctx, STATS_POOL_##_name)

#define stats_server_incr(_ctx, _name) do {                    \
    _stats_server_incr(_ctx, STATS_SERVER_##_name);            \
} while (0)

#define stats_server_decr(_ctx, _name) do {                    \
    _stats_server_decr(_ctx, STATS_SERVER_##_name);            \
} while (0)

#define stats_server_incr_by(_ctx, _name, _val) do {           \
    _stats_server_incr_by(_ctx, STATS_SERVER_##_name, _val);   \
} while (0)

#define stats_server_decr_by(_ctx, _name, _val) do {           \
    _stats_server_decr_by(_ctx, STATS_SERVER_##_name, _val);   \
} while (0)

#define stats_server_set_ts(_ctx, _name, _val) do {            \
     _stats_server_set_ts(_ctx, STATS_SERVER_##_name, _val);   \
} while (0)

#define stats_server_get_ts(_ctx, _name)                       \
     _stats_server_get_ts(_ctx, STATS_SERVER_##_name)

#define stats_server_set_val(_ctx, _name, _val) do {           \
     _stats_server_set_val(_ctx, STATS_SERVER_##_name, _val);  \
} while (0)

#define stats_server_get_val(_ctx, _name)                      \
     _stats_server_get_val(_ctx, STATS_SERVER_##_name)


#else

#define stats_pool_incr(_ctx, _name)

#define stats_pool_decr(_ctx, _name)

#define stats_pool_incr_by(_ctx, _name, _val)

#define stats_pool_decr_by(_ctx, _name, _val)

#define stats_pool_set_val(_ctx, _name, _val)

#define stats_pool_get_val(_ctx, _name)

#define stats_server_incr(_ctx, _name)

#define stats_server_decr(_ctx, _name)

#define stats_server_incr_by(_ctx, _name, _val)

#define stats_server_decr_by(_ctx, _name, _val)

#define stats_server_set_ts(_ctx, _name, _val)

#define stats_server_get_ts(_ctx, _name)

#define stats_server_set_val(_ctx, _name, _val)

#define stats_server_get_val(_ctx, _name)

#endif

#define stats_enabled   DN_STATS

void stats_describe(void);

void _stats_pool_incr(struct context *ctx, stats_pool_field_t fidx);
void _stats_pool_decr(struct context *ctx, stats_pool_field_t fidx);
void _stats_pool_incr_by(struct context *ctx, stats_pool_field_t fidx, int64_t val);
void _stats_pool_decr_by(struct context *ctx, stats_pool_field_t fidx, int64_t val);
void _stats_pool_set_ts(struct context *ctx, stats_pool_field_t fidx, int64_t val);
uint64_t _stats_pool_get_ts(struct context *ctx,stats_pool_field_t fidx);
void _stats_pool_set_val(struct context *ctx,stats_pool_field_t fidx, int64_t val);
int64_t _stats_pool_get_val(struct context *ctx,
		                 stats_pool_field_t fidx);

void _stats_server_incr(struct context *ctx, stats_server_field_t fidx);
void _stats_server_decr(struct context *ctx, stats_server_field_t fidx);
void _stats_server_incr_by(struct context *ctx, stats_server_field_t fidx, int64_t val);
void _stats_server_decr_by(struct context *ctx, stats_server_field_t fidx, int64_t val);
void _stats_server_set_ts(struct context *ctx, stats_server_field_t fidx, uint64_t val);
uint64_t _stats_server_get_ts(struct context *ctx, stats_server_field_t fidx);
void _stats_server_set_val(struct context *ctx, stats_server_field_t fidx, int64_t val);
int64_t _stats_server_get_val(struct context *ctx, stats_server_field_t fidx);

struct stats * stats_create(uint16_t stats_port, struct string pname, int stats_interval,
             char *source, struct server_pool *sp, struct context *ctx);

void stats_destroy(struct stats *stats);
void stats_swap(struct stats *stats);


void stats_histo_add_latency(struct context *ctx, uint64_t val);
void stats_histo_add_payloadsize(struct context *ctx, uint64_t val);


#endif

