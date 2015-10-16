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

#ifndef _DYN_MESSAGE_H_
#define _DYN_MESSAGE_H_

#include "dyn_core.h"
#include "dyn_dnode_msg.h"

#define ALLOC_MSGS					  200000
#define MIN_ALLOC_MSGS		     	  100000
#define MAX_ALLOC_MSGS			      1000000

#define MAX_ALLOWABLE_PROCESSED_MSGS  500
#define MAX_REPLICAS_PER_DC           3

typedef void (*func_msg_parse_t)(struct msg *);
typedef rstatus_t (*func_msg_post_splitcopy_t)(struct msg *);
typedef void (*func_msg_coalesce_t)(struct msg *r);
typedef rstatus_t (*msg_response_handler_t)(struct msg *req, struct msg *rsp);
typedef uint64_t msgid_t;
typedef rstatus_t (*func_msg_reply_t)(struct msg *r);
typedef bool (*func_msg_failure_t)(struct msg *r);


typedef enum msg_parse_result {
    MSG_PARSE_OK,                         /* parsing ok */
    MSG_PARSE_ERROR,                      /* parsing error */
    MSG_PARSE_REPAIR,                     /* more to parse -> repair parsed & unparsed data */
    MSG_PARSE_FRAGMENT,                   /* multi-vector request -> fragment */
    MSG_PARSE_AGAIN,                      /* incomplete -> parse again */
    MSG_OOM_ERROR
} msg_parse_result_t;

typedef enum msg_type {
    MSG_UNKNOWN,
    MSG_REQ_MC_GET,                       /* memcache retrieval requests */
    MSG_REQ_MC_GETS,
    MSG_REQ_MC_DELETE,                    /* memcache delete request */
    MSG_REQ_MC_CAS,                       /* memcache cas request and storage request */
    MSG_REQ_MC_SET,                       /* memcache storage request */
    MSG_REQ_MC_ADD,
    MSG_REQ_MC_REPLACE,
    MSG_REQ_MC_APPEND,
    MSG_REQ_MC_PREPEND,
    MSG_REQ_MC_INCR,                      /* memcache arithmetic request */
    MSG_REQ_MC_DECR,
    MSG_REQ_MC_TOUCH,                     /* memcache touch request */
    MSG_REQ_MC_QUIT,                      /* memcache quit request */
    MSG_RSP_MC_NUM,                       /* memcache arithmetic response */
    MSG_RSP_MC_STORED,                    /* memcache cas and storage response */
    MSG_RSP_MC_NOT_STORED,
    MSG_RSP_MC_EXISTS,
    MSG_RSP_MC_NOT_FOUND,
    MSG_RSP_MC_END,
    MSG_RSP_MC_VALUE,
    MSG_RSP_MC_DELETED,                   /* memcache delete response */
    MSG_RSP_MC_TOUCHED,                   /* memcachd touch response */
    MSG_RSP_MC_ERROR,                     /* memcache error responses */
    MSG_RSP_MC_CLIENT_ERROR,
    MSG_RSP_MC_SERVER_ERROR,
    MSG_REQ_REDIS_DEL,                    /* redis commands - keys */
    MSG_REQ_REDIS_EXISTS,
    MSG_REQ_REDIS_EXPIRE,
    MSG_REQ_REDIS_EXPIREAT,
    MSG_REQ_REDIS_PEXPIRE,
    MSG_REQ_REDIS_PEXPIREAT,
    MSG_REQ_REDIS_PERSIST,
    MSG_REQ_REDIS_PTTL,
    MSG_REQ_REDIS_SCAN,
    MSG_REQ_REDIS_SORT,
    MSG_REQ_REDIS_TTL,
    MSG_REQ_REDIS_TYPE,
    MSG_REQ_REDIS_APPEND,                 /* redis requests - string */
    MSG_REQ_REDIS_BITCOUNT,
    MSG_REQ_REDIS_DECR,
    MSG_REQ_REDIS_DECRBY,
    MSG_REQ_REDIS_DUMP,
    MSG_REQ_REDIS_GET,
    MSG_REQ_REDIS_GETBIT,
    MSG_REQ_REDIS_GETRANGE,
    MSG_REQ_REDIS_GETSET,
    MSG_REQ_REDIS_INCR,
    MSG_REQ_REDIS_INCRBY,
    MSG_REQ_REDIS_INCRBYFLOAT,
    MSG_REQ_REDIS_MSET,
    MSG_REQ_REDIS_MGET,
    MSG_REQ_REDIS_PSETEX,
    MSG_REQ_REDIS_RESTORE,
    MSG_REQ_REDIS_SET,
    MSG_REQ_REDIS_SETBIT,
    MSG_REQ_REDIS_SETEX,
    MSG_REQ_REDIS_SETNX,
    MSG_REQ_REDIS_SETRANGE,
    MSG_REQ_REDIS_STRLEN,
    MSG_REQ_REDIS_HDEL,                   /* redis requests - hashes */
    MSG_REQ_REDIS_HEXISTS,
    MSG_REQ_REDIS_HGET,
    MSG_REQ_REDIS_HGETALL,
    MSG_REQ_REDIS_HINCRBY,
    MSG_REQ_REDIS_HINCRBYFLOAT,
    MSG_REQ_REDIS_HKEYS,
    MSG_REQ_REDIS_HLEN,
    MSG_REQ_REDIS_HMGET,
    MSG_REQ_REDIS_HMSET,
    MSG_REQ_REDIS_HSET,
    MSG_REQ_REDIS_HSETNX,
    MSG_REQ_REDIS_HSCAN,
    MSG_REQ_REDIS_HVALS,
    MSG_REG_REDIS_KEYS,
    MSG_REG_REDIS_INFO,
    MSG_REQ_REDIS_LINDEX,                 /* redis requests - lists */
    MSG_REQ_REDIS_LINSERT,
    MSG_REQ_REDIS_LLEN,
    MSG_REQ_REDIS_LPOP,
    MSG_REQ_REDIS_LPUSH,
    MSG_REQ_REDIS_LPUSHX,
    MSG_REQ_REDIS_LRANGE,
    MSG_REQ_REDIS_LREM,
    MSG_REQ_REDIS_LSET,
    MSG_REQ_REDIS_LTRIM,
    MSG_REQ_REDIS_PING,
    MSG_REQ_REDIS_QUIT,                                                                         \
    MSG_REQ_REDIS_RPOP,
    MSG_REQ_REDIS_RPOPLPUSH,
    MSG_REQ_REDIS_RPUSH,
    MSG_REQ_REDIS_RPUSHX,
    MSG_REQ_REDIS_SADD,                   /* redis requests - sets */
    MSG_REQ_REDIS_SCARD,
    MSG_REQ_REDIS_SDIFF,
    MSG_REQ_REDIS_SDIFFSTORE,
    MSG_REQ_REDIS_SINTER,
    MSG_REQ_REDIS_SINTERSTORE,
    MSG_REQ_REDIS_SISMEMBER,
    MSG_REQ_REDIS_SLAVEOF,
    MSG_REQ_REDIS_SMEMBERS,
    MSG_REQ_REDIS_SMOVE,
    MSG_REQ_REDIS_SPOP,
    MSG_REQ_REDIS_SRANDMEMBER,
    MSG_REQ_REDIS_SREM,
    MSG_REQ_REDIS_SUNION,
    MSG_REQ_REDIS_SUNIONSTORE,
    MSG_REQ_REDIS_SSCAN,
    MSG_REQ_REDIS_ZADD,                   /* redis requests - sorted sets */
    MSG_REQ_REDIS_ZCARD,
    MSG_REQ_REDIS_ZCOUNT,
    MSG_REQ_REDIS_ZINCRBY,
    MSG_REQ_REDIS_ZINTERSTORE,
    MSG_REQ_REDIS_ZRANGE,
    MSG_REQ_REDIS_ZRANGEBYSCORE,
    MSG_REQ_REDIS_ZRANK,
    MSG_REQ_REDIS_ZREM,
    MSG_REQ_REDIS_ZREMRANGEBYRANK,
    MSG_REQ_REDIS_ZREMRANGEBYSCORE,
    MSG_REQ_REDIS_ZREVRANGE,
    MSG_REQ_REDIS_ZREVRANGEBYSCORE,
    MSG_REQ_REDIS_ZREVRANK,
    MSG_REQ_REDIS_ZSCORE,
    MSG_REQ_REDIS_ZUNIONSTORE,
    MSG_REQ_REDIS_ZSCAN,
    MSG_REQ_REDIS_EVAL,                   /* redis requests - eval */
    MSG_REQ_REDIS_EVALSHA,
    MSG_RSP_REDIS_STATUS,                 /* redis response */
    MSG_RSP_REDIS_INTEGER,
    MSG_RSP_REDIS_BULK,
    MSG_RSP_REDIS_MULTIBULK,
    MSG_RSP_REDIS_ERROR,
    MSG_RSP_REDIS_ERROR_ERR,
    MSG_RSP_REDIS_ERROR_OOM,
    MSG_RSP_REDIS_ERROR_BUSY,
    MSG_RSP_REDIS_ERROR_NOAUTH,
    MSG_RSP_REDIS_ERROR_LOADING,
    MSG_RSP_REDIS_ERROR_BUSYKEY,
    MSG_RSP_REDIS_ERROR_MISCONF,
    MSG_RSP_REDIS_ERROR_NOSCRIPT,
    MSG_RSP_REDIS_ERROR_READONLY,
    MSG_RSP_REDIS_ERROR_WRONGTYPE,
    MSG_RSP_REDIS_ERROR_EXECABORT,
    MSG_RSP_REDIS_ERROR_MASTERDOWN,
    MSG_RSP_REDIS_ERROR_NOREPLICAS,
    MSG_SENTINEL
} msg_type_t;


typedef enum dyn_error {
    UNKNOWN_ERROR,
    PEER_CONNECTION_REFUSE,
    STORAGE_CONNECTION_REFUSE,
    BAD_FORMAT
} dyn_error_t;

/* This is a wrong place for this typedef. But adding to core has some
 * dependency issues - FixIt someother day :(
 */
typedef enum consistency {
    DC_ONE = 0,
    DC_QUORUM = 1
} consistency_t;

static inline char*
get_consistency_string(consistency_t cons)
{
    switch(cons)
    {
        case DC_ONE: return "DC_ONE";
        case DC_QUORUM: return "DC_QUORUM";
    }
    return "INVALID CONSISTENCY";
}

#define DEFAULT_READ_CONSISTENCY DC_ONE
#define DEFAULT_WRITE_CONSISTENCY DC_ONE
extern consistency_t g_write_consistency;
extern consistency_t g_read_consistency;

struct response_mgr {
    bool        is_read;
    bool        done;
    /* we could use the dynamic array
       here. But we have only 3 ASGs */
    struct msg  *responses[MAX_REPLICAS_PER_DC];
    uint8_t     good_responses;     // non-error responses received
    uint8_t     max_responses;      // max responses expected
    uint8_t     quorum_responses;   // responses expected to form a quorum
    uint8_t     error_responses;    // error responses received
    struct msg  *err_rsp;           // first error response
    struct conn *conn;
    struct msg *msg;
};

void init_response_mgr(struct response_mgr *rspmgr, struct msg*, bool is_read,
                       uint8_t max_responses, struct conn *conn);
// DN_OK if response was accepted
rstatus_t rspmgr_submit_response(struct response_mgr *rspmgr, struct msg *rsp);
bool rspmgr_is_done(struct response_mgr *rspmgr);
struct msg* rspmgr_get_response(struct response_mgr *rspmgr);
void rspmgr_free_response(struct response_mgr *rspmgr, struct msg *dont_free);

struct msg {
    TAILQ_ENTRY(msg)     c_tqe;           /* link in client q */
    TAILQ_ENTRY(msg)     s_tqe;           /* link in server q */
    TAILQ_ENTRY(msg)     m_tqe;           /* link in send q / free q */

    msgid_t              id;              /* message id */
    struct msg           *peer;           /* message peer */
    struct conn          *owner;          /* message owner - client | server */
    int64_t              stime_in_microsec;  /* start time in microsec */
    uint8_t              awaiting_rsps;
    struct msg           *selected_rsp;

    struct rbnode        tmo_rbe;         /* entry in rbtree */

    struct mhdr          mhdr;            /* message mbuf header */
    uint32_t             mlen;            /* message length */

    int                  state;           /* current parser state */
    uint8_t              *pos;            /* parser position marker */
    uint8_t              *token;          /* token marker */

    func_msg_parse_t     parser;          /* message parser */
    msg_parse_result_t   result;          /* message parsing result */

    func_mbuf_copy_t     pre_splitcopy;   /* message pre-split copy */
    func_msg_post_splitcopy_t post_splitcopy;  /* message post-split copy */
    func_msg_coalesce_t  pre_coalesce;    /* message pre-coalesce */
    func_msg_coalesce_t  post_coalesce;   /* message post-coalesce */

    msg_type_t           type;            /* message type */

    uint8_t              *key_start;      /* key start */
    uint8_t              *key_end;        /* key end */

    uint32_t             vlen;            /* value length (memcache) */
    uint8_t              *end;            /* end marker (memcache) */

    uint8_t              *narg_start;     /* narg start (redis) */
    uint8_t              *narg_end;       /* narg end (redis) */
    uint32_t             narg;            /* # arguments (redis) */
    uint32_t             rnarg;           /* running # arg used by parsing fsa (redis) */
    uint32_t             rlen;            /* running length in parsing fsa (redis) */
    uint32_t             integer;         /* integer reply value (redis) */

    struct msg           *frag_owner;     /* owner of fragment message */
    uint32_t             nfrag;           /* # fragment */
    uint64_t             frag_id;         /* id of fragmented message */

    err_t                err;             /* errno on error? */
    unsigned             error:1;         /* error? */
    unsigned             ferror:1;        /* one or more fragments are in error? */
    unsigned             request:1;       /* request? or response? */
    unsigned             quit:1;          /* quit request? */
    unsigned             noreply:1;       /* noreply? */
    unsigned             done:1;          /* done? */
    unsigned             fdone:1;         /* all fragments are done? */
    unsigned             first_fragment:1;/* first fragment? */
    unsigned             last_fragment:1; /* last fragment? */
    unsigned             swallow:1;       /* swallow response? */
    unsigned             rsp_sent:1;      /* is a response sent for this request?*/

    int					 data_store;
    //dynomite
    struct dmsg          *dmsg;          /* dyn message */
    int                  dyn_state;
    dyn_error_t          dyn_error;      /* error code for dynomite */
    uint8_t              msg_type;       /* for special message types
                                              0 : normal,
                                              1 : local cmd only no matter what
                                              2 : cmd to all nodes in same RACK no matter whats
                                              3 : cmd to all RACKs (one node from each RACK)
                                          */
    unsigned             is_read:1;       /*  0 : write
                                              1 : read */
    msg_response_handler_t rsp_handler;
    consistency_t        consistency;
    msgid_t              parent_id;       /* parent message id */
    struct response_mgr  rspmgr;
};

TAILQ_HEAD(msg_tqh, msg);

static inline void
msg_incr_awaiting_rsps(struct msg *req)
{
    req->awaiting_rsps++;
    return;
}

static inline void
msg_decr_awaiting_rsps(struct msg *req)
{
    req->awaiting_rsps--;
    return;
}

static inline rstatus_t
msg_handle_response(struct msg *req, struct msg *rsp)
{
    return req->rsp_handler(req, rsp);
}

uint32_t msg_free_queue_size(void);

struct msg *msg_tmo_min(void);
void msg_tmo_insert(struct msg *msg, struct conn *conn);
void msg_tmo_delete(struct msg *msg);

void msg_init(struct instance *nci);
rstatus_t msg_clone(struct msg *src, struct mbuf *mbuf_start, struct msg *target);
void msg_deinit(void);
struct msg *msg_get(struct conn *conn, bool request, int data_store);
void msg_put(struct msg *msg);
uint32_t msg_mbuf_size(struct msg *msg);
uint32_t msg_length(struct msg *msg);
struct msg *msg_get_error(int data_store, dyn_error_t dyn_err, err_t err);
void msg_dump(struct msg *msg);
bool msg_empty(struct msg *msg);
rstatus_t msg_recv(struct context *ctx, struct conn *conn);
rstatus_t msg_send(struct context *ctx, struct conn *conn);
uint32_t msg_alloc_msgs(void);
uint32_t msg_payload_crc32(struct msg *msg);
struct msg *msg_get_rsp_integer(int data_store);
struct mbuf *msg_ensure_mbuf(struct msg *msg, size_t len);
rstatus_t msg_append(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_prepend(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_prepend_format(struct msg *msg, const char *fmt, ...);


struct msg *req_get(struct conn *conn);
void req_put(struct msg *msg);
bool req_done(struct conn *conn, struct msg *msg);
bool req_error(struct conn *conn, struct msg *msg);
void req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *req_send_next(struct context *ctx, struct conn *conn);
void req_send_done(struct context *ctx, struct conn *conn, struct msg *msg);

struct msg *rsp_get(struct conn *conn);
void rsp_put(struct msg *msg);
struct msg *rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void server_rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *rsp_send_next(struct context *ctx, struct conn *conn);
void rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg);


/* for dynomite  */
struct msg *dnode_req_get(struct conn *conn);
void dnode_req_peer_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void dnode_req_peer_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void dnode_req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void dnode_req_peer_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void dnode_req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void dnode_req_peer_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
struct msg *dnode_req_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void dnode_req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *dnode_req_send_next(struct context *ctx, struct conn *conn);
void dnode_req_send_done(struct context *ctx, struct conn *conn, struct msg *msg);

struct msg *dnode_rsp_get(struct conn *conn);
void dnode_rsp_put(struct msg *msg);
struct msg *dnode_rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void dnode_rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);
struct msg *dnode_rsp_send_next(struct context *ctx, struct conn *conn);
void dnode_rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg);


void dnode_rsp_gos_syn(struct context *ctx, struct conn *p_conn, struct msg *msg);


void remote_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg,
		                struct rack *rack, uint8_t *key, uint32_t keylen);
void local_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg, uint8_t *key, uint32_t keylen);
void dnode_peer_req_forward(struct context *ctx, struct conn *c_conn, struct conn *p_conn,
		                struct msg *msg, struct rack *rack, uint8_t *key, uint32_t keylen);

//void peer_gossip_forward(struct context *ctx, struct conn *conn, int data_store, struct string *data);
void dnode_peer_gossip_forward(struct context *ctx, struct conn *conn, int data_store, struct mbuf *data);

rstatus_t client_handle_response(struct conn *conn, msgid_t msg, struct msg *rsp);
rstatus_t dnode_client_handle_response(struct conn *conn, msgid_t msg, struct msg *rsp);

#endif
