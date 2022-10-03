/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
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

#include <string.h>

#include "dyn_dict.h"
#include "dyn_dnode_msg.h"
#include "dyn_mbuf.h"
#include "dyn_queue.h"
#include "dyn_rbtree.h"
#include "dyn_response_mgr.h"
#include "dyn_types.h"

#define ALLOC_MSGS 200000
#define MIN_ALLOC_MSGS 100000
#define MAX_ALLOC_MSGS 1000000

#define MAX_ALLOWABLE_PROCESSED_MSGS 500

#define MSG_TYPE_CODEC(ACTION)                                                 \
  ACTION(UNKNOWN)                                                              \
  ACTION(REQ_MC_GET) /* memcache retrieval requests */                         \
  ACTION(REQ_MC_GETS)                                                          \
  ACTION(REQ_MC_DELETE) /* memcache delete request */                          \
  ACTION(REQ_MC_CAS)    /* memcache cas request and storage request */         \
  ACTION(REQ_MC_SET)    /* memcache storage request */                         \
  ACTION(REQ_MC_ADD)                                                           \
  ACTION(REQ_MC_REPLACE)                                                       \
  ACTION(REQ_MC_APPEND)                                                        \
  ACTION(REQ_MC_PREPEND)                                                       \
  ACTION(REQ_MC_INCR) /* memcache arithmetic request */                        \
  ACTION(REQ_MC_DECR)                                                          \
  ACTION(REQ_MC_TOUCH)  /* memcache touch request */                           \
  ACTION(REQ_MC_QUIT)   /* memcache quit request */                            \
  ACTION(RSP_MC_NUM)    /* memcache arithmetic response */                     \
  ACTION(RSP_MC_STORED) /* memcache cas and storage response */                \
  ACTION(RSP_MC_NOT_STORED)                                                    \
  ACTION(RSP_MC_EXISTS)                                                        \
  ACTION(RSP_MC_NOT_FOUND)                                                     \
  ACTION(RSP_MC_END)                                                           \
  ACTION(RSP_MC_VALUE)                                                         \
  ACTION(RSP_MC_DELETED) /* memcache delete response */                        \
  ACTION(RSP_MC_TOUCHED) /* memcache touch response */                         \
  ACTION(RSP_MC_ERROR)   /* memcache error responses */                        \
  ACTION(RSP_MC_CLIENT_ERROR)                                                  \
  ACTION(RSP_MC_SERVER_ERROR)                                                  \
  ACTION(REQ_REDIS_DEL) /* redis commands - keys */                            \
  ACTION(REQ_REDIS_EXISTS)                                                     \
  ACTION(REQ_REDIS_EXPIRE)                                                     \
  ACTION(REQ_REDIS_EXPIREAT)                                                   \
  ACTION(REQ_REDIS_PEXPIRE)                                                    \
  ACTION(REQ_REDIS_PEXPIREAT)                                                  \
  ACTION(REQ_REDIS_PERSIST)                                                    \
  ACTION(REQ_REDIS_PTTL)                                                       \
  ACTION(REQ_REDIS_SCAN)                                                       \
  ACTION(REQ_REDIS_SORT)                                                       \
  ACTION(REQ_REDIS_TTL)                                                        \
  ACTION(REQ_REDIS_TYPE)                                                       \
  ACTION(REQ_REDIS_APPEND) /* redis requests - string */                       \
  ACTION(REQ_REDIS_BITCOUNT)                                                   \
  ACTION(REQ_REDIS_BITPOS)                                                     \
  ACTION(REQ_REDIS_DECR)                                                       \
  ACTION(REQ_REDIS_DECRBY)                                                     \
  ACTION(REQ_REDIS_DUMP)                                                       \
  ACTION(REQ_REDIS_GET)                                                        \
  ACTION(REQ_REDIS_GETBIT)                                                     \
  ACTION(REQ_REDIS_GETRANGE)                                                   \
  ACTION(REQ_REDIS_GETSET)                                                     \
  ACTION(REQ_REDIS_INCR)                                                       \
  ACTION(REQ_REDIS_INCRBY)                                                     \
  ACTION(REQ_REDIS_INCRBYFLOAT)                                                \
  ACTION(REQ_REDIS_MSET)                                                       \
  ACTION(REQ_REDIS_MGET)                                                       \
  ACTION(REQ_REDIS_PSETEX)                                                     \
  ACTION(REQ_REDIS_RESTORE)                                                    \
  ACTION(REQ_REDIS_SET)                                                        \
  ACTION(REQ_REDIS_SETBIT)                                                     \
  ACTION(REQ_REDIS_SETEX)                                                      \
  ACTION(REQ_REDIS_SETNX)                                                      \
  ACTION(REQ_REDIS_SETRANGE)                                                   \
  ACTION(REQ_REDIS_STRLEN)                                                     \
  ACTION(REQ_REDIS_HDEL) /* redis requests - hashes */                         \
  ACTION(REQ_REDIS_HEXISTS)                                                    \
  ACTION(REQ_REDIS_HGET)                                                       \
  ACTION(REQ_REDIS_HGETALL)                                                    \
  ACTION(REQ_REDIS_HINCRBY)                                                    \
  ACTION(REQ_REDIS_HINCRBYFLOAT)                                               \
  ACTION(REQ_REDIS_HKEYS)                                                      \
  ACTION(REQ_REDIS_HLEN)                                                       \
  ACTION(REQ_REDIS_HMGET)                                                      \
  ACTION(REQ_REDIS_HMSET)                                                      \
  ACTION(REQ_REDIS_HSET)                                                       \
  ACTION(REQ_REDIS_HSETNX)                                                     \
  ACTION(REQ_REDIS_HSCAN)                                                      \
  ACTION(REQ_REDIS_HVALS)                                                      \
  ACTION(REQ_REDIS_HSTRLEN)                                                    \
  ACTION(REQ_REDIS_KEYS)                                                       \
  ACTION(REQ_REDIS_INFO)                                                       \
  ACTION(REQ_REDIS_LINDEX) /* redis requests - lists */                        \
  ACTION(REQ_REDIS_LINSERT)                                                    \
  ACTION(REQ_REDIS_LLEN)                                                       \
  ACTION(REQ_REDIS_LPOP)                                                       \
  ACTION(REQ_REDIS_LPUSH)                                                      \
  ACTION(REQ_REDIS_LPUSHX)                                                     \
  ACTION(REQ_REDIS_LRANGE)                                                     \
  ACTION(REQ_REDIS_LREM)                                                       \
  ACTION(REQ_REDIS_LSET)                                                       \
  ACTION(REQ_REDIS_LTRIM)                                                      \
  ACTION(REQ_REDIS_PING)                                                       \
  ACTION(REQ_REDIS_QUIT)                                                       \
  ACTION(REQ_REDIS_RPOP)                                                       \
  ACTION(REQ_REDIS_RPOPLPUSH)                                                  \
  ACTION(REQ_REDIS_RPUSH)                                                      \
  ACTION(REQ_REDIS_RPUSHX)                                                     \
  ACTION(REQ_REDIS_SADD) /* redis requests - sets */                           \
  ACTION(REQ_REDIS_SCARD)                                                      \
  ACTION(REQ_REDIS_SDIFF)                                                      \
  ACTION(REQ_REDIS_SDIFFSTORE)                                                 \
  ACTION(REQ_REDIS_SINTER)                                                     \
  ACTION(REQ_REDIS_SINTERSTORE)                                                \
  ACTION(REQ_REDIS_SISMEMBER)                                                  \
  ACTION(REQ_REDIS_SLAVEOF)                                                    \
  ACTION(REQ_REDIS_SMEMBERS)                                                   \
  ACTION(REQ_REDIS_SMOVE)                                                      \
  ACTION(REQ_REDIS_SPOP)                                                       \
  ACTION(REQ_REDIS_SRANDMEMBER)                                                \
  ACTION(REQ_REDIS_SREM)                                                       \
  ACTION(REQ_REDIS_SUNION)                                                     \
  ACTION(REQ_REDIS_SUNIONSTORE)                                                \
  ACTION(REQ_REDIS_SSCAN)                                                      \
  ACTION(REQ_REDIS_ZADD) /* redis requests - sorted sets */                    \
  ACTION(REQ_REDIS_ZCARD)                                                      \
  ACTION(REQ_REDIS_ZCOUNT)                                                     \
  ACTION(REQ_REDIS_ZINCRBY)                                                    \
  ACTION(REQ_REDIS_ZINTERSTORE)                                                \
  ACTION(REQ_REDIS_ZLEXCOUNT)                                                  \
  ACTION(REQ_REDIS_ZRANGE)                                                     \
  ACTION(REQ_REDIS_ZRANGEBYLEX)                                                \
  ACTION(REQ_REDIS_ZRANGEBYSCORE)                                              \
  ACTION(REQ_REDIS_ZRANK)                                                      \
  ACTION(REQ_REDIS_ZREM)                                                       \
  ACTION(REQ_REDIS_ZREMRANGEBYRANK)                                            \
  ACTION(REQ_REDIS_ZREMRANGEBYLEX)                                             \
  ACTION(REQ_REDIS_ZREMRANGEBYSCORE)                                           \
  ACTION(REQ_REDIS_ZREVRANGE)                                                  \
  ACTION(REQ_REDIS_ZREVRANGEBYLEX)                                             \
  ACTION(REQ_REDIS_ZREVRANGEBYSCORE)                                           \
  ACTION(REQ_REDIS_ZREVRANK)                                                   \
  ACTION(REQ_REDIS_ZSCORE)                                                     \
  ACTION(REQ_REDIS_ZUNIONSTORE)                                                \
  ACTION(REQ_REDIS_ZSCAN)                                                      \
  ACTION(REQ_REDIS_EVAL) /* redis requests - eval */                           \
  ACTION(REQ_REDIS_EVALSHA)                                                    \
  ACTION(REQ_REDIS_GEOADD) /* redis geo requests */                            \
  ACTION(REQ_REDIS_GEORADIUS)                                                  \
  ACTION(REQ_REDIS_GEODIST)                                                    \
  ACTION(REQ_REDIS_GEOHASH)                                                    \
  ACTION(REQ_REDIS_GEOPOS)                                                     \
  ACTION(REQ_REDIS_GEORADIUSBYMEMBER)                                          \
  ACTION(REQ_REDIS_UNLINK)                                                     \
  ACTION(REQ_REDIS_JSONSET)                                                    \
  ACTION(REQ_REDIS_JSONGET)                                                    \
  ACTION(REQ_REDIS_JSONDEL)                                                    \
  ACTION(REQ_REDIS_JSONTYPE)                                                   \
  ACTION(REQ_REDIS_JSONMGET)                                                   \
  ACTION(REQ_REDIS_JSONARRAPPEND)                                              \
  ACTION(REQ_REDIS_JSONARRINSERT)                                              \
  ACTION(REQ_REDIS_JSONARRLEN)                                                 \
  ACTION(REQ_REDIS_JSONOBJKEYS)                                                \
  ACTION(REQ_REDIS_JSONOBJLEN)                                                 \
  /* ACTION(REQ_REDIS_AUTH) */                                                 \
  /* ACTION(REQ_REDIS_SELECT)*/ /* only during init */                         \
  ACTION(REQ_REDIS_PFADD)        /* redis requests - hyperloglog */            \
  ACTION(REQ_REDIS_PFCOUNT)                                                    \
  ACTION(REQ_REDIS_CONFIG)                                                     \
  ACTION(REQ_REDIS_SCRIPT)                                                     \
  ACTION(REQ_REDIS_SCRIPT_LOAD)                                                \
  ACTION(REQ_REDIS_SCRIPT_EXISTS)                                              \
  ACTION(REQ_REDIS_SCRIPT_FLUSH)                                               \
  ACTION(REQ_REDIS_SCRIPT_KILL)                                                \
  ACTION(RSP_REDIS_STATUS) /* redis response */                                \
  ACTION(RSP_REDIS_INTEGER)                                                    \
  ACTION(RSP_REDIS_BULK)                                                       \
  ACTION(RSP_REDIS_MULTIBULK)                                                  \
  ACTION(RSP_REDIS_ERROR)                                                      \
  ACTION(RSP_REDIS_ERROR_ERR)                                                  \
  ACTION(RSP_REDIS_ERROR_OOM)                                                  \
  ACTION(RSP_REDIS_ERROR_BUSY)                                                 \
  ACTION(RSP_REDIS_ERROR_NOAUTH)                                               \
  ACTION(RSP_REDIS_ERROR_LOADING)                                              \
  ACTION(RSP_REDIS_ERROR_BUSYKEY)                                              \
  ACTION(RSP_REDIS_ERROR_MISCONF)                                              \
  ACTION(RSP_REDIS_ERROR_NOSCRIPT)                                             \
  ACTION(RSP_REDIS_ERROR_READONLY)                                             \
  ACTION(RSP_REDIS_ERROR_WRONGTYPE)                                            \
  ACTION(RSP_REDIS_ERROR_EXECABORT)                                            \
  ACTION(RSP_REDIS_ERROR_MASTERDOWN)                                           \
  ACTION(RSP_REDIS_ERROR_NOREPLICAS)                                           \
  ACTION(HACK_SETTING_CONN_CONSISTENCY)                                        \
  ACTION(SENTINEL)                                                             \
  ACTION(END_IDX)                                                              \
  /* ACTION( REQ_REDIS_AUTH) */                                                \
  /* ACTION( REQ_REDIS_SELECT)*/ /* only during init */                        \

#define DEFINE_ACTION(_name) MSG_##_name,
typedef enum msg_type { MSG_TYPE_CODEC(DEFINE_ACTION) } msg_type_t;
#undef DEFINE_ACTION

typedef void (*func_msg_parse_t)(struct msg *, struct context *ctx);
typedef rstatus_t (*func_msg_fragment_t)(struct msg *, struct server_pool *,
                                         struct rack *, struct msg_tqh *);
typedef rstatus_t (*func_msg_verify_t)(struct msg *, struct server_pool *,
                                       struct rack *);
typedef void (*func_msg_coalesce_t)(struct msg *r);
typedef rstatus_t (*msg_response_handler_t)(struct context *ctx, struct msg *req,
                                            struct msg *rsp);
typedef bool (*func_msg_failure_t)(struct msg *r);
typedef bool (*func_is_multikey_request)(struct msg *r);
typedef struct msg *(*func_reconcile_responses)(struct response_mgr *rspmgr);
typedef rstatus_t (*func_msg_rewrite_t)(struct msg *orig_msg,
                                        struct context *ctx, bool *did_rewrite,
                                        struct msg **new_msg_ptr);
typedef rstatus_t (*func_msg_repair_t)(struct context *ctx, struct response_mgr *rspmgr,
    struct msg **new_msg_ptr);
typedef rstatus_t (*func_clear_repair_md_t)(struct context *ctx, struct msg *req,
    struct msg **new_msg_ptr);
typedef void (*func_init_datastore_t)();

extern func_msg_coalesce_t g_pre_coalesce;  /* message pre-coalesce */
extern func_msg_coalesce_t g_post_coalesce; /* message post-coalesce */
extern func_msg_fragment_t g_fragment;      /* message fragment */
extern func_msg_verify_t g_verify_request;  /* message verify */
extern func_is_multikey_request g_is_multikey_request;
extern func_reconcile_responses g_reconcile_responses;
extern func_msg_rewrite_t
    g_rewrite_query; /* rewrite query in a msg if necessary */
extern func_msg_rewrite_t
    g_rewrite_query_with_timestamp_md;
extern func_msg_repair_t g_make_repair_query; /* Create a repair msg. */
extern func_clear_repair_md_t g_clear_repair_md_for_key;

void set_datastore_ops(void);

typedef enum msg_parse_result {
  // Parsing OK
  MSG_PARSE_OK,
  // Parsing error
  MSG_PARSE_ERROR,
  // More to parse -> Repair parsed & unparsed data
  MSG_PARSE_REPAIR,
  // Multi-vector request -> fragment
  MSG_PARSE_FRAGMENT,
  // Incomplete, parse again.
  MSG_PARSE_AGAIN,
  // Parsing done, but do nothing after
  MSG_PARSE_NOOP,
  // Parsing done, command was a dynomite configuration
  MSG_PARSE_DYNO_CONFIG,
  // OOM error during parsing (TODO: consider removing)
  MSG_OOM_ERROR
} msg_parse_result_t;

typedef enum dyn_error {
  DYNOMITE_OK,
  DYNOMITE_UNKNOWN_ERROR,
  DYNOMITE_INVALID_STATE,
  DYNOMITE_INVALID_ADMIN_REQ,
  PEER_CONNECTION_REFUSE,
  PEER_HOST_DOWN,
  PEER_HOST_NOT_CONNECTED,
  STORAGE_CONNECTION_REFUSE,
  BAD_FORMAT,
  DYNOMITE_NO_QUORUM_ACHIEVED,
  DYNOMITE_SCRIPT_SPANS_NODES,
  DYNOMITE_PAYLOAD_TOO_LARGE,
} dyn_error_t;

static inline char *dn_strerror(dyn_error_t err) {
  switch (err) {
    case DYNOMITE_OK:
      return "Success";
    case DYNOMITE_UNKNOWN_ERROR:
      return "Unknown Error";
    case DYNOMITE_INVALID_STATE:
      return "Dynomite's current state does not allow this request";
    case DYNOMITE_INVALID_ADMIN_REQ:
      return "Invalid request in Dynomite's admin mode";
    case PEER_CONNECTION_REFUSE:
      return "Peer Node refused connection";
    case PEER_HOST_DOWN:
      return "Peer Node is down";
    case PEER_HOST_NOT_CONNECTED:
      return "Peer Node is not connected";
    case STORAGE_CONNECTION_REFUSE:
      return "Datastore refused connection";
    case DYNOMITE_NO_QUORUM_ACHIEVED:
      return "Failed to achieve Quorum";
    case DYNOMITE_SCRIPT_SPANS_NODES:
      return "Keys in the script cannot span multiple nodes";
    case DYNOMITE_PAYLOAD_TOO_LARGE:
      return "MSET/MGET/SCAN payload too large";
    default:
      return strerror(err);
  }
}

static inline char *dyn_error_source(dyn_error_t err) {
  switch (err) {
    case DYNOMITE_INVALID_ADMIN_REQ:
    case DYNOMITE_INVALID_STATE:
    case DYNOMITE_NO_QUORUM_ACHIEVED:
    case DYNOMITE_SCRIPT_SPANS_NODES:
    case DYNOMITE_PAYLOAD_TOO_LARGE:
      return "Dynomite:";
    case PEER_CONNECTION_REFUSE:
    case PEER_HOST_DOWN:
    case PEER_HOST_NOT_CONNECTED:
      return "Peer:";
    case STORAGE_CONNECTION_REFUSE:
      return "Storage:";
    default:
      return "unknown:";
  }
}
/* This is a wrong place for this typedef. But adding to core has some
 * dependency issues - FixIt someother day :(
 */
typedef enum consistency {
  DC_ONE = 0,
  DC_QUORUM,
  DC_SAFE_QUORUM,
  DC_EACH_SAFE_QUORUM,
} consistency_t;

static inline char *get_consistency_string(consistency_t cons) {
  switch (cons) {
    case DC_ONE:
      return "DC_ONE";
    case DC_QUORUM:
      return "DC_QUORUM";
    case DC_SAFE_QUORUM:
      return "DC_SAFE_QUORUM";
    case DC_EACH_SAFE_QUORUM:
      return "DC_EACH_SAFE_QUORUM";
  }
  return "INVALID CONSISTENCY";
}

static inline consistency_t get_consistency_enum_from_string(char *cons) {
  if (dn_strcasecmp(cons, "DC_ONE") == 0) {
    return DC_ONE;
  } else if (dn_strcasecmp(cons, "DC_QUORUM") == 0) {
    return DC_QUORUM;
  } else if (dn_strcasecmp(cons, "DC_SAFE_QUORUM") == 0) {
    return DC_SAFE_QUORUM;
  } else if (dn_strcasecmp(cons, "DC_EACH_SAFE_QUORUM") == 0) {
    return DC_EACH_SAFE_QUORUM;
  }
  return -1;
}

#define DEFAULT_READ_CONSISTENCY DC_ONE
#define DEFAULT_WRITE_CONSISTENCY DC_ONE
extern consistency_t g_write_consistency;
extern consistency_t g_read_consistency;
extern uint8_t g_timeout_factor;

extern bool g_read_repairs_enabled;

typedef enum msg_routing {
  ROUTING_NORMAL = 0,
  // Ignore the key hashing
  ROUTING_LOCAL_NODE_ONLY = 1,
  // Apply key hashing, but only for the local rack.
  ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY = 2,
  // Ignore key hashing, but only for the local rack.
  ROUTING_ALL_NODES_LOCAL_RACK_ONLY = 3,
  // Ignore key hashing, and send to all nodes in all racks in all DCs.
  ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS = 4,
} msg_routing_t;

static inline char *get_msg_routing_string(msg_routing_t route) {
  switch (route) {
    case ROUTING_NORMAL:
      return "ROUTING_NORMAL";
    case ROUTING_LOCAL_NODE_ONLY:
      return "ROUTING_LOCAL_NODE_ONLY";
    case ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY:
      return "ROUTING_TOKEN_OWNER_LOCAL_RACK_ONLY";
    case ROUTING_ALL_NODES_LOCAL_RACK_ONLY:
      return "ROUTING_ALL_NODES_LOCAL_RACK_ONLY";
    case ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS:
      return "ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS";
  }
  return "INVALID MSG ROUTING TYPE";
}

struct keypos {
  uint8_t *start;     /* key start pos */
  uint8_t *end;       /* key end pos */
  uint8_t *tag_start; /* hashtagged key start pos */
  uint8_t *tag_end;   /* hashtagged key end pos */
};

struct argpos {
  uint8_t *start;     // Argument start position
  uint8_t *end;       // Argument end position
};

// This struct is used when 'read_repairs' is enabled. It holds information required to
// set the metadata of every write that will be used in the case of a quorum mismatch in
// order to repair a query.
struct write_with_ts {
  msg_type_t cmd_type;
  char *add_set;
  char *rem_set;
  uint64_t ts;
  int num_keys;
  struct array *keys;
  int num_fields;
  struct array *fields;
  int num_values;
  struct array *values;
  int num_optionals;
  struct array *optionals;
  const char* rewrite_script;
  int total_num_tokens;
};

struct msg {
  object_t object;
  TAILQ_ENTRY(msg) c_tqe; /* link in client q */
  TAILQ_ENTRY(msg) s_tqe; /* link in server q */
  TAILQ_ENTRY(msg) m_tqe; /* link in send q / free q */

  msgid_t id;                             /* message id */
  struct msg *peer;                       /* message peer */
  struct conn *owner;                     /* message owner - client | server */
  usec_t stime_in_microsec;               /* start time in microsec */
  usec_t request_inqueue_enqueue_time_us; /* when message was enqueued in
                                             inqueue, either to the data store
                                             or remote region or cross rack */
  usec_t request_send_time; /* when message was sent: either to the data store
                               or remote region or cross rack */
  uint32_t awaiting_rsps;
  struct msg *selected_rsp;

  struct rbnode tmo_rbe; /* entry in rbtree */

  struct mhdr mhdr; /* message mbuf header */
  uint32_t mlen;    /* message length */

  int state;      /* current parser state */
  uint8_t *pos;   /* parser position marker */
  uint8_t *token; /* token marker */
  int latest_parsed_mbuf_idx; /* Most recent idx of mbuf parsed in 'mhdr' linked list */

  func_msg_parse_t parser;   /* message parser */
  msg_parse_result_t result; /* message parsing result */

  msg_type_t type; /* message type */
  msg_type_t orig_type; /* Original message type. Only used on a query rewrite. */

  struct array *keys; /* array of keypos, for req */
  struct array *args; /* array of keypos, for req */

  uint32_t vlen; /* value length (memcache) */
  uint8_t *end;  /* end marker (memcache) */

  uint8_t *ntoken_start; /* ntoken start (redis) */
  uint8_t *ntoken_end;   /* ntoken end (redis) */
  uint32_t ntokens;       /* # tokens (redis) */
  uint32_t nkeys;      /* # keys in script (redis EVAL/EVALSHA) */
  uint32_t rntokens;      /* running # tokens used by parsing fsa (redis) */
  uint32_t rlen;       /* running length in parsing fsa (redis) */
  uint32_t integer;    /* integer reply value (redis) */

  struct msg *frag_owner; /* owner of fragment message */
  uint32_t nfrag;         /* # fragment */
  uint32_t nfrag_done;    /* # fragment done */
  uint64_t frag_id;       /* id of fragmented message */
  struct msg *
      *frag_seq; /* sequence of fragment message, map from keys to fragments*/

  err_t error_code;                    /* errno on error? */
  unsigned is_error : 1;               /* error? */
  unsigned is_ferror : 1;              /* one or more fragments are in error? */
  unsigned is_request : 1;             /* request? or response? */
  unsigned quit : 1;                   /* quit request? */
  unsigned expect_datastore_reply : 1; /* expect datastore reply */
  unsigned done : 1;                   /* done? */
  unsigned fdone : 1;                  /* all fragments are done? */
  unsigned swallow : 1;                /* swallow response? */
  /* We need a way in dnode_rsp_send_next to remember if we already
   * did a dmsg_write of a dnode header in this message. If we do not remember
   * it, then if the same message gets attempted to be sent twice in
   * msg_send_chain, (due to lack of space in the previous attempt), we will
   * prepend another header and we will have corrupted message at the
   * destination */
  unsigned dnode_header_prepended : 1;
  unsigned rsp_sent : 1; /* is a response sent for this request?*/
  uint64_t timestamp;   // Timestamp of request. Used only if 'read_repiars' is enabled.

  // Some 'msg's are not possible to rewrite.
  // Currently, the main reason is if an arg is across mbuf's.
  bool rewrite_with_ts_possible;
  bool needs_repair;    // If 'true', a repair msg will be sent to 'owner'.
  struct write_with_ts msg_info;
  struct msg *orig_msg; // The original message if a rewrite took place.

  // dynomite
  struct dmsg *dmsg; /* dyn message */
  dyn_parse_state_t dyn_parse_state;
  dyn_error_t dyn_error_code; /* error code for dynomite */
  msg_routing_t msg_routing;
  unsigned is_read : 1; /*  0 : write
                            1 : read */
  msg_response_handler_t rsp_handler;
  consistency_t consistency;
  msgid_t parent_id; /* parent message id */

  // Primary response_mgr for this instance's DC.
  struct response_mgr rspmgr;

  // Additional response_mgrs if we choose to use DC_EACH_SAFE_QUORUM
  struct response_mgr **additional_each_rspmgrs;

  // Indicates whether the rspmgr and additional_each_rspmgrs(if applicable)
  // are init-ed.
  bool rspmgrs_inited;
};

TAILQ_HEAD(msg_tqh, msg);

static inline void msg_incr_awaiting_rsps(struct msg *req) {
  req->awaiting_rsps++;
  return;
}

static inline void msg_decr_awaiting_rsps(struct msg *req) {
  req->awaiting_rsps--;
  return;
}

static inline rstatus_t msg_handle_response(struct context *ctx, struct msg *req, struct msg *rsp) {
  return req->rsp_handler(ctx, req, rsp);
}

size_t msg_free_queue_size(void);

struct msg *msg_tmo_min(void);
void msg_tmo_insert(struct msg *msg, struct conn *conn);
void msg_tmo_delete(struct msg *msg);

void msg_init(size_t alloc_msgs_max);
rstatus_t msg_clone(struct msg *src, struct mbuf *mbuf_start,
                    struct msg *target);
void msg_deinit(void);
struct string *msg_type_string(msg_type_t type);
struct msg *msg_get(struct conn *conn, bool request, const char *const caller);
void msg_put(struct msg *msg);
uint32_t msg_mbuf_size(struct msg *msg);
uint32_t msg_length(struct msg *msg);
struct msg *msg_get_error(struct conn *conn, dyn_error_t dyn_err, err_t err);
void msg_dump(int level, struct msg *msg);
bool msg_empty(struct msg *msg);
rstatus_t msg_recv(struct context *ctx, struct conn *conn);
rstatus_t msg_send(struct context *ctx, struct conn *conn);
uint64_t msg_gen_frag_id(void);
size_t msg_alloc_msgs(void);
uint32_t msg_payload_crc32(struct msg *msg);
struct msg *msg_get_rsp_integer(struct conn *conn);
struct mbuf *msg_ensure_mbuf(struct msg *msg, size_t len);
rstatus_t msg_append(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_append_format(struct msg *msg, const char *fmt, int num_args, ...);
rstatus_t msg_prepend(struct msg *msg, uint8_t *pos, size_t n);
rstatus_t msg_prepend_format(struct msg *msg, const char *fmt, ...);

uint8_t *msg_get_tagged_key(struct msg *req, uint32_t key_index,
                            uint32_t *keylen);
uint8_t *msg_get_full_key(struct msg *req, uint32_t key_index,
                          uint32_t *keylen);
uint8_t *msg_get_full_key_copy(struct msg *msg, int idx, uint32_t *keylen);
uint8_t *msg_get_arg_copy(struct msg *msg, int idx, uint32_t *arglen);

struct msg *req_get(struct conn *conn);
void req_put(struct msg *msg);
bool req_done(struct conn *conn, struct msg *msg);
bool req_error(struct conn *conn, struct msg *msg);
struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
                   struct msg *nmsg);
rstatus_t req_make_reply(struct context *ctx, struct conn *conn,
                         struct msg *req);
struct msg *req_send_next(struct context *ctx, struct conn *conn);
void req_send_done(struct context *ctx, struct conn *conn, struct msg *msg);

struct msg *rsp_get(struct conn *conn);
void rsp_put(struct msg *msg);
struct msg *rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc);
void server_rsp_recv_done(struct context *ctx, struct conn *conn,
                          struct msg *msg, struct msg *nmsg);
struct msg *rsp_send_next(struct context *ctx, struct conn *conn);
void rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg);

/* for dynomite  */
void dnode_rsp_gos_syn(struct context *ctx, struct conn *p_conn,
                       struct msg *msg);

void req_forward_error(struct context *ctx, struct conn *conn, struct msg *req,
                       err_t error_code, err_t dyn_error_code);
void req_forward_all_racks_for_dc(struct context *ctx, struct conn *c_conn,
                                 struct msg *req, struct mbuf *orig_mbuf,
                                 uint8_t *key, uint32_t keylen,
                                 struct datacenter *dc);
rstatus_t req_forward_local_datastore(struct context *ctx, struct conn *c_conn,
                            struct msg *msg, uint8_t *key, uint32_t keylen,
                            dyn_error_t *dyn_error_code);
rstatus_t dnode_peer_req_forward(struct context *ctx, struct conn *c_conn,
                                 struct conn *p_conn, struct msg *msg,
                                 uint8_t *key, uint32_t keylen,
                                 dyn_error_t *dyn_error_code);

// void peer_gossip_forward(struct context *ctx, struct conn *conn, struct
// string *data);
void dnode_peer_gossip_forward(struct context *ctx, struct conn *conn,
                               struct mbuf *data);

/*
 * Simulates a successful response as though the datastore sent it.
 * Also, does the necessary to make sure that the response path is
 * able to send this response back to the client.
 *
 * Returns DN_OK on success and an appropriate error otherwise.
 */
rstatus_t simulate_ok_rsp(struct context *ctx, struct conn *conn,
    struct msg *msg);

// Returns 'true' if 'msg_type' is a Dynomite configuration command.
bool is_msg_type_dyno_config(msg_type_t msg_type);

#endif
