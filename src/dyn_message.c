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

#include <stdio.h>
#include <stdlib.h>

#include <sys/uio.h>

#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_server.h"
#include "hashkit/dyn_hashkit.h"
#include "proto/dyn_proto.h"

#if (IOV_MAX > 128)
#define DN_IOV_MAX 128
#else
#define DN_IOV_MAX IOV_MAX
#endif

/*
 *            dyn_message.[ch]
 *         message (struct msg)
 *            +        +            .
 *            |        |            .
 *            /        \            .
 *         Request    Response      .../ dyn_mbuf.[ch]  (message buffers)
 *      dyn_request.c  dyn_response.c .../ dyn_memcache.c; dyn_redis.c (message
 * parser)
 *
 * Messages in dynomite are manipulated by a chain of processing handlers,
 * where each handler is responsible for taking the input and producing an
 * output for the next handler in the chain. This mechanism of processing
 * loosely conforms to the standard chain-of-responsibility design pattern
 *
 * At the high level, each handler takes in a message: request or response
 * and produces the message for the next handler in the chain. The input
 * for a handler is either a request or response, but never both and
 * similarly the output of an handler is either a request or response or
 * nothing.
 *
 * Each handler itself is composed of two processing units:
 *
 * 1). filter: manipulates output produced by the handler, usually based
 *     on a policy. If needed, multiple filters can be hooked into each
 *     location.
 * 2). forwarder: chooses one of the backend servers to send the request
 *     to, usually based on the configured distribution and key hasher.
 *
 * Handlers are registered either with Client or Server or Proxy
 * connections. A Proxy connection only has a read handler as it is only
 * responsible for accepting new connections from client. Read handler
 * (conn_recv_t) registered with client is responsible for reading requests,
 * while that registered with server is responsible for reading responses.
 * Write handler (conn_send_t) registered with client is responsible for
 * writing response, while that registered with server is responsible for
 * writing requests.
 *
 * Note that in the above discussion, the terminology send is used
 * synonymously with write or OUT event. Similarly recv is used synonymously
 * with read or IN event
 *
 *             Client+             Proxy           Server+
 *                              (dynomite)
 *                                   .
 *       msg_recv {read event}       .       msg_recv {read event}
 *         +                         .                         +
 *         |                         .                         |
 *         \                         .                         /
 *         req_recv_next             .             rsp_recv_next
 *           +                       .                       +
 *           |                       .                       |       Rsp
 *           req_recv_done           .           rsp_recv_done      <===
 *             +                     .                     +
 *             |                     .                     |
 *    Req      \                     .                     /
 *    ===>     req_filter*           .           *rsp_filter
 *               +                   .                   +
 *               |                   .                   |
 *               \                   .                   /
 *               req_forward-//  (a) . (c)  \\-rsp_forward
 *                                   .
 *                                   .
 *       msg_send {write event}      .      msg_send {write event}
 *         +                         .                         +
 *         |                         .                         |
 *    Rsp' \                         .                         /     Req'
 *   <===  rsp_send_next             .             req_send_next     ===>
 *           +                       .                       +
 *           |                       .                       |
 *           \                       .                       /
 *           rsp_send_done-//    (d) . (b)    //-req_send_done
 *
 *
 * (a) -> (b) -> (c) -> (d) is the normal flow of transaction consisting
 * of a single request response, where (a) and (b) handle request from
 * client, while (c) and (d) handle the corresponding response from the
 * server.
 */

/* Changes to message for consistency:
 * In order to implement consistency, following changes have been made to
 * message peer: Previously there was a one to one relation between request and
 * a response both of which is struct message unfortunately. And due to the fact
 * that some requests are forwarded as is to the underlying server while
 * some are copied, the notion of 'peer' gets complicated. hence I changed
 * its meaning somewhat. response->peer points to request that this
 * response belongs to. Right now request->peer does not have any meaning
 * other than some code in redis which does coalescing etc, and some other
 * code just for the sake of it. awaiting_rsps: This is a counter of the
 * number of responses that a request is still expecting. For DC_ONE consistency
 * this is immaterial. For DC_QUORUM, this is the total number of responses
 * expected. We wait for them to arrive before we free the request. A
 * client connection in turn waits for all the requests to finish before
 * freeing itself. (Look for waiting_to_unref). selected_rsp : A
 * request->selected_rsp is the response selected for a given request. All code
 * related to sending response should look at selected_rsp. rsp_sent : Due
 * to consistency DC_QUORUM, we would have sent the response for a request even
 * before all the responses arrive. The responses coming after rsp_sent are
 * extra and can be swallowed. Also at this time we know that the response
 * is sent and the request can be deleted from the client hash table
 * outstanding_msgs_dict.
 *
 * So generally request->selected_rsp & response->peer is valid. Eventually it
 * will be good to have different structures for request and response.
 */
static uint64_t msg_id;          /* message id counter */
static uint64_t frag_id;         /* fragment id counter */
static struct msg_tqh free_msgq; /* free msg q */
static struct rbtree tmo_rbt;    /* timeout rbtree */
static struct rbnode tmo_rbs;    /* timeout rbtree sentinel */
static size_t alloc_msgs_max; /* maximum number of allowed allocated messages */
uint8_t g_timeout_factor = 1;

func_msg_coalesce_t g_pre_coalesce;     /* message pre-coalesce */
func_msg_coalesce_t g_post_coalesce;    /* message post-coalesce */
func_msg_fragment_t g_fragment;         /* message post-coalesce */
func_msg_verify_t g_verify_request;     /* message post-coalesce */
func_is_multikey_request g_is_multikey_request;
func_reconcile_responses g_reconcile_responses;
func_msg_rewrite_t g_rewrite_query;     /* rewrite query in a msg if necessary */
/* rewrite query as script that updates both data and metadata */
func_msg_rewrite_t g_rewrite_query_with_timestamp_md;
func_msg_repair_t g_make_repair_query;  /* Send a repair msg. */
func_clear_repair_md_t g_clear_repair_md_for_key; /* Clear repair metadata for a key */

#define DEFINE_ACTION(_name) string(#_name),
static struct string msg_type_strings[] = {MSG_TYPE_CODEC(DEFINE_ACTION)
                                               null_string};
#undef DEFINE_ACTION

// Determines if read repairs is enabled or not.
bool g_read_repairs_enabled = false;

static char *print_req(const struct object *obj) {
  ASSERT(obj->type == OBJ_REQ);
  struct msg *req = (struct msg *)obj;
  struct string *req_type = msg_type_string(req->type);
  snprintf(obj->print_buff, PRINT_BUF_SIZE,
           "<REQ %p %lu:%lu::%lu %.*s, len:%u>", req, req->id, req->parent_id,
           req->frag_id, req_type->len, req_type->data, req->mlen);
  return obj->print_buff;
}

static char *print_rsp(const struct object *obj) {
  ASSERT(obj->type == OBJ_RSP);
  struct msg *rsp = (struct msg *)obj;
  struct string *rsp_type = msg_type_string(rsp->type);
  snprintf(obj->print_buff, PRINT_BUF_SIZE, "<RSP %p %lu:%lu %.*s len:%u>", rsp,
           rsp->id, rsp->parent_id, rsp_type->len, rsp_type->data, rsp->mlen);
  return obj->print_buff;
}

void set_datastore_ops(void) {
  switch (g_data_store) {
    case DATA_REDIS:
      g_pre_coalesce = redis_pre_coalesce;
      g_post_coalesce = redis_post_coalesce;
      g_fragment = redis_fragment;
      g_verify_request = redis_verify_request;
      g_is_multikey_request = redis_is_multikey_request;
      g_reconcile_responses = redis_reconcile_responses;
      g_rewrite_query = redis_rewrite_query;
      g_rewrite_query_with_timestamp_md = redis_rewrite_query_with_timestamp_md;
      g_make_repair_query = redis_make_repair_query;
      g_clear_repair_md_for_key = redis_clear_repair_md_for_key;
      break;
    case DATA_MEMCACHE:
      g_pre_coalesce = memcache_pre_coalesce;
      g_post_coalesce = memcache_post_coalesce;
      g_fragment = memcache_fragment;
      g_verify_request = memcache_verify_request;
      g_is_multikey_request = memcache_is_multikey_request;
      g_reconcile_responses = memcache_reconcile_responses;
      g_rewrite_query = memcache_rewrite_query;
      g_rewrite_query_with_timestamp_md = memcache_rewrite_query_with_timestamp_md;
      g_make_repair_query = memcache_make_repair_query;
      g_clear_repair_md_for_key = memcache_clear_repair_md_for_key;
      break;
    default:
      return;
  }
}

static inline rstatus_t msg_cant_handle_response(struct msg *req,
                                                 struct msg *rsp) {
  return DN_ENO_IMPL;
}

static struct msg *msg_from_rbe(struct rbnode *node) {
  struct msg *req;
  int offset;

  offset = offsetof(struct msg, tmo_rbe);
  req = (struct msg *)((char *)node - offset);

  return req;
}

struct msg *msg_tmo_min(void) {
  struct rbnode *node;

  node = rbtree_min(&tmo_rbt);
  if (node == NULL) {
    return NULL;
  }

  return msg_from_rbe(node);
}

void msg_tmo_insert(struct msg *req, struct conn *conn) {
  struct rbnode *node;
  msec_t timeout;

  // ASSERT(req->is_request);
  ASSERT(!req->quit && req->expect_datastore_reply);

  timeout =
      conn->dyn_mode ? dnode_peer_timeout(req, conn) : server_timeout(conn);
  if (timeout <= 0) {
    return;
  }
  timeout = timeout * g_timeout_factor;

  node = &req->tmo_rbe;
  node->timeout = timeout;
  node->key = dn_msec_now() + timeout;
  node->data = conn;

  rbtree_insert(&tmo_rbt, node);

  if (log_loggable(LOG_VERB)) {
    log_debug(LOG_VERB,
              "insert req %" PRIu64
              " into tmo rbt with expiry of "
              "%d msec",
              req->id, timeout);
  }
}

void msg_tmo_delete(struct msg *req) {
  struct rbnode *node;

  node = &req->tmo_rbe;

  /* already deleted */

  if (node->data == NULL) {
    return;
  }

  rbtree_delete(&tmo_rbt, node);

  if (log_loggable(LOG_VERB)) {
    log_debug(LOG_VERB, "delete req %" PRIu64 " from tmo rbt", req->id);
  }
}

static size_t alloc_msg_count = 0;

static struct msg *_msg_get(struct conn *conn, bool request,
                            const char *const caller) {
  struct msg *msg;

  if (!TAILQ_EMPTY(&free_msgq)) {
    ASSERT(TAILQ_COUNT(&free_msgq));

    msg = TAILQ_FIRST(&free_msgq);
    TAILQ_REMOVE(&free_msgq, msg, m_tqe);
    goto done;
  }

  // protect our server in the slow network and high traffics.
  // we drop client requests but still honor our peer requests
  if (alloc_msg_count >= alloc_msgs_max) {
    log_debug(LOG_WARN, "allocated #msgs %lu hit max allowable limit",
              alloc_msg_count);
    return NULL;
  }

  alloc_msg_count++;

  if (alloc_msg_count % 1000 == 0)
    log_warn("alloc_msg_count: %lu caller: %s %s", alloc_msg_count, caller,
             print_obj(conn));
  else
    log_info("alloc_msg_count: %lu caller: %s %s", alloc_msg_count, caller,
             print_obj(conn));

  msg = dn_alloc(sizeof(*msg));
  if (msg == NULL) {
    return NULL;
  }

done:
  /* c_tqe, s_tqe, and m_tqe are left uninitialized */
  if (request) {
    init_object(&msg->object, OBJ_REQ, print_req);
  } else {
    init_object(&msg->object, OBJ_RSP, print_rsp);
  }

  msg->id = ++msg_id;
  msg->parent_id = 0;
  msg->peer = NULL;
  msg->owner = NULL;
  msg->stime_in_microsec = 0ULL;
  msg->request_send_time = 0L;
  msg->request_inqueue_enqueue_time_us = 0L;
  msg->awaiting_rsps = 0;
  msg->selected_rsp = NULL;

  rbtree_node_init(&msg->tmo_rbe);

  STAILQ_INIT(&msg->mhdr);
  msg->mlen = 0;

  msg->state = 0;
  msg->pos = NULL;
  msg->token = NULL;
  msg->latest_parsed_mbuf_idx = -1;

  msg->parser = NULL;
  msg->result = MSG_PARSE_OK;

  msg->type = MSG_UNKNOWN;

  msg->keys = array_create(1, sizeof(struct keypos));
  if (msg->keys == NULL) {
    dn_free(msg);
    return NULL;
  }

  msg->args = array_create(1, sizeof(struct argpos));
  if (msg->args == NULL) {
    dn_free(msg);
    return NULL;
  }

  msg->vlen = 0;
  msg->end = NULL;

  msg->frag_owner = NULL;
  msg->frag_seq = NULL;
  msg->nfrag = 0;
  msg->nfrag_done = 0;
  msg->frag_id = 0;

  msg->ntoken_start = NULL;
  msg->ntoken_end = NULL;
  msg->ntokens = 0;
  msg->rntokens = 0;
  msg->nkeys = 0;
  msg->rlen = 0;
  msg->integer = 0;

  msg->error_code = 0;
  msg->is_error = 0;
  msg->is_ferror = 0;
  msg->is_request = 0;
  msg->quit = 0;
  msg->expect_datastore_reply = 1;
  msg->done = 0;
  msg->fdone = 0;
  msg->swallow = 0;
  msg->dnode_header_prepended = 0;
  msg->rsp_sent = 0;

  // dynomite
  msg->is_read = 1;
  msg->dyn_parse_state = 0;
  msg->dmsg = NULL;
  msg->msg_routing = ROUTING_NORMAL;
  msg->dyn_error_code = 0;
  msg->rsp_handler = msg_local_one_rsp_handler;
  msg->consistency = DC_ONE;
  msg->timestamp = 0;
  msg->orig_type = MSG_UNKNOWN;
  msg->orig_msg = NULL;
  msg->needs_repair = false;
  msg->rewrite_with_ts_possible = true;
  msg->additional_each_rspmgrs = NULL;
  msg->rspmgrs_inited = false;

  // Init the write_with_ts struct:
  struct write_with_ts *minfo = &msg->msg_info;
  minfo->add_set = NULL;
  minfo->rem_set = NULL;
  minfo->keys = NULL;
  minfo->num_keys = 0;
  minfo->fields = NULL;
  minfo->num_fields = 0;
  minfo->values = NULL;
  minfo->num_values = 0;
  minfo->optionals = NULL;
  minfo->num_optionals = 0;
  minfo->rewrite_script = NULL;
  minfo->total_num_tokens = 0;
  return msg;
}

size_t msg_alloc_msgs() { return alloc_msg_count; }

size_t msg_free_queue_size(void) { return TAILQ_COUNT(&free_msgq); }

struct msg *msg_get(struct conn *conn, bool request, const char *const caller) {
  struct msg *msg;

  msg = _msg_get(conn, request, caller);
  if (msg == NULL) {
    return NULL;
  }

  msg->owner = conn;
  msg->is_request = request ? 1 : 0;

  if (g_data_store == DATA_REDIS) {
    if (request) {
      if (conn->dyn_mode) {
        msg->parser = dyn_parse_req;
      } else {
        msg->parser = redis_parse_req;
      }
    } else {
      if (conn->dyn_mode) {
        msg->parser = dyn_parse_rsp;
      } else {
        msg->parser = redis_parse_rsp;
      }
    }
  } else if (g_data_store == DATA_MEMCACHE) {
    if (request) {
      if (conn->dyn_mode) {
        msg->parser = dyn_parse_req;
      } else {
        msg->parser = memcache_parse_req;
      }
    } else {
      if (conn->dyn_mode) {
        msg->parser = dyn_parse_rsp;
      } else {
        msg->parser = memcache_parse_rsp;
      }
    }
  } else {
    log_debug(LOG_VVERB, "incorrect selection of data store %d", g_data_store);
    exit(0);
  }

  if (log_loggable(LOG_VVERB)) {
    log_debug(LOG_VVERB, "get msg %p id %" PRIu64 " request %d owner sd %d",
              msg, msg->id, msg->is_request, conn->sd);
  }

  return msg;
}

rstatus_t msg_clone(struct msg *src, struct mbuf *mbuf_start,
                    struct msg *target) {
  target->parent_id = src->id;
  target->owner = src->owner;
  target->is_request = src->is_request;

  target->parser = src->parser;
  target->expect_datastore_reply = src->expect_datastore_reply;
  target->swallow = src->swallow;
  target->type = src->type;
  target->mlen = src->mlen;
  target->pos = src->pos;
  target->vlen = src->vlen;
  target->is_read = src->is_read;
  target->consistency = src->consistency;
  target->msg_routing = src->msg_routing;

  struct mbuf *mbuf, *nbuf;
  bool started = false;
  STAILQ_FOREACH(mbuf, &src->mhdr, next) {
    if (!started && mbuf != mbuf_start) {
      continue;
    } else {
      started = true;
    }
    nbuf = mbuf_get();
    if (nbuf == NULL) {
      return DN_ENOMEM;
    }

    uint32_t len = mbuf_length(mbuf);
    mbuf_copy(nbuf, mbuf->pos, len);
    mbuf_insert(&target->mhdr, nbuf);
  }

  return DN_OK;
}

struct msg *msg_get_error(struct conn *conn, dyn_error_t dyn_error_code,
                          err_t error_code) {
  struct msg *rsp;
  struct mbuf *mbuf;
  int n;
  char *errstr = dyn_error_code ? dn_strerror(dyn_error_code) : "unknown";
  char *protstr = g_data_store == DATA_REDIS ? "-ERR" : "SERVER_ERROR";
  char *source = dyn_error_source(dyn_error_code);

  rsp = _msg_get(conn, false, __FUNCTION__);
  if (rsp == NULL) {
    return NULL;
  }

  rsp->state = 0;
  rsp->is_error = true;
  rsp->error_code = error_code;
  rsp->dyn_error_code = dyn_error_code;
  rsp->type = g_data_store == DATA_REDIS ? MSG_RSP_REDIS_ERROR
                                         : MSG_RSP_MC_SERVER_ERROR;

  mbuf = mbuf_get();
  if (mbuf == NULL) {
    msg_put(rsp);
    return NULL;
  }
  mbuf_insert(&rsp->mhdr, mbuf);

  n = dn_scnprintf(mbuf->last, mbuf_remaining_space(mbuf), "%s %s %s" CRLF, protstr,
                   source, errstr);
  mbuf->last += n;
  rsp->mlen = (uint32_t)n;

  if (log_loggable(LOG_VVERB)) {
    log_debug(LOG_VVERB,
              "get rsp %p id %" PRIu64 " len %" PRIu32 " err %d error '%s'",
              rsp, rsp->id, rsp->mlen, error_code, errstr);
  }

  return rsp;
}

struct msg *msg_get_rsp_integer(struct conn *conn) {
  struct msg *rsp;
  struct mbuf *mbuf;
  int n;

  rsp = _msg_get(conn, false, __FUNCTION__);
  if (rsp == NULL) {
    return NULL;
  }

  rsp->state = 0;
  rsp->type = MSG_RSP_REDIS_INTEGER;

  mbuf = mbuf_get();
  if (mbuf == NULL) {
    msg_put(rsp);
    return NULL;
  }
  mbuf_insert(&rsp->mhdr, mbuf);

  n = dn_scnprintf(mbuf->last, mbuf_remaining_space(mbuf), ":0\r\n");
  mbuf->last += n;
  rsp->mlen = (uint32_t)n;

  if (log_loggable(LOG_VVERB)) {
    log_debug(LOG_VVERB, "get rsp %p id %" PRIu64 " len %" PRIu32 " ", rsp,
              rsp->id, rsp->mlen);
  }

  return rsp;
}

static void msg_free(struct msg *msg) {
  ASSERT(STAILQ_EMPTY(&msg->mhdr));

  if (log_loggable(LOG_VVERB)) {
    log_debug(LOG_VVERB, "free msg %p id %" PRIu64 "", msg, msg->id);
  }
  dn_free(msg);
}

void msg_put(struct msg *msg) {
  if (msg == NULL) {
    log_debug(
        LOG_ERR,
        "Unable to put a null msg - probably due to memory hard-set limit");
    return;
  }

  if (msg->is_request && msg->awaiting_rsps != 0 && msg->expect_datastore_reply !=0) {
    log_error("Not freeing req %d, awaiting_rsps = %u", msg->id,
              msg->awaiting_rsps);
    return;
  }

  struct dmsg *dmsg = msg->dmsg;
  if (dmsg != NULL) {
    dmsg_put(dmsg);
    msg->dmsg = NULL;
  }

  while (!STAILQ_EMPTY(&msg->mhdr)) {
    struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
    mbuf_remove(&msg->mhdr, mbuf);
    mbuf_put(mbuf);
  }

  if (msg->frag_seq) {
    dn_free(msg->frag_seq);
    msg->frag_seq = NULL;
  }

  if (msg->keys) {
    array_destroy(msg->keys);
    msg->keys = NULL;
  }

  if (msg->args) {
    array_destroy(msg->args);
    msg->args = NULL;
  }

  if (msg->orig_msg) {
    msg_put(msg->orig_msg);
    msg->orig_msg = NULL;
  }

  if (msg->msg_info.keys) {
    array_destroy(msg->msg_info.keys);
  }
  if (msg->msg_info.fields) {
    array_destroy(msg->msg_info.fields);
  }
  if (msg->msg_info.values) {
    array_destroy(msg->msg_info.values);
  }
  if (msg->msg_info.optionals) {
    array_destroy(msg->msg_info.optionals);
  }

  if (msg->additional_each_rspmgrs) {
    ASSERT(msg->consistency == DC_EACH_SAFE_QUORUM);
    // Only requests have their connection's owner as the 'struct server_pool' object,
    // and only requests would have 'additional_each_rspmgrs', so it's safe to cast to
    // 'struct server_pool'.
    struct server_pool *sp = msg->owner->owner;
    uint8_t num_dcs = array_n(&sp->datacenters);

    int i;
    // Skip the 0th index as that points back to the statically allocated 'rspmgr' struct
    // in 'msg'.
    for (i = 1; i < num_dcs; ++i) {
      dn_free(msg->additional_each_rspmgrs[i]);
    }
    dn_free(msg->additional_each_rspmgrs);
  }
  TAILQ_INSERT_HEAD(&free_msgq, msg, m_tqe);
}

uint32_t msg_mbuf_size(struct msg *msg) {
  uint32_t count = 0;
  struct mbuf *mbuf;

  STAILQ_FOREACH(mbuf, &msg->mhdr, next) { count++; }

  return count;
}

uint32_t msg_length(struct msg *msg) {
  uint32_t count = 0;
  struct mbuf *mbuf;

  STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
    ASSERT(mbuf->last >= mbuf->start);
    count += (uint32_t)(mbuf->last - mbuf->start);
  }

  return count;
}

void msg_dump(int level, struct msg *msg) {
  if (!log_loggable(level)) {
    return;
  }
  struct mbuf *mbuf;

  if (msg == NULL) {
    loga("msg is NULL - cannot display its info");
    return;
  }

  loga("msg dump id %" PRIu64 " request %d len %" PRIu32
       " type %d done %d "
       "error %d (err %d)",
       msg->id, msg->is_request, msg->mlen, msg->type, msg->done, msg->is_error,
       msg->error_code);

  STAILQ_FOREACH(mbuf, &msg->mhdr, next) { mbuf_dump(mbuf); }
  loga("=================================================");
}

/**
 * Initialize the message queue.
 * @param[in] alloc_msgs_max Dynomite instance.
 */
void msg_init(size_t msgs_max) {
  log_debug(LOG_DEBUG, "msg size %d", sizeof(struct msg));
  msg_id = 0;
  frag_id = 0;
  alloc_msgs_max = msgs_max;
  TAILQ_INIT(&free_msgq);
  rbtree_init(&tmo_rbt, &tmo_rbs);

}

void msg_deinit(void) {
  struct msg *msg, *nmsg;

  for (msg = TAILQ_FIRST(&free_msgq); msg != NULL; msg = nmsg) {
    ASSERT(TAILQ_COUNT(&free_msgq));
    nmsg = TAILQ_NEXT(msg, m_tqe);
    msg_free(msg);
  }
  ASSERT(TAILQ_COUNT(&free_msgq) == 0);
}

struct string *msg_type_string(msg_type_t type) {
  return &msg_type_strings[type];
}

bool msg_empty(struct msg *msg) {
  return msg->mlen == 0 ? true
                        : (msg->dyn_error_code == BAD_FORMAT ? true : false);
}

static uint8_t *msg_get_key(struct msg *req, uint32_t key_index,
                            uint32_t *keylen, bool tagged_only) {
  *keylen = 0;
  if (array_n(req->keys) == 0) return NULL;
  ASSERT_LOG(key_index < array_n(req->keys), "%s has %u keys", print_obj(req),
             array_n(req->keys));

  struct keypos *kpos = array_get(req->keys, key_index);
  uint8_t *key_start = tagged_only ? kpos->tag_start : kpos->start;
  uint8_t *key_end = tagged_only ? kpos->tag_end : kpos->end;
  *keylen = (uint32_t)(key_end - key_start);
  return key_start;
}

uint8_t *msg_get_full_key(struct msg *req, uint32_t key_index,
                          uint32_t *keylen) {
  return msg_get_key(req, key_index, keylen, false);
}
uint8_t *msg_get_tagged_key(struct msg *req, uint32_t key_index,
                            uint32_t *keylen) {
  return msg_get_key(req, key_index, keylen, true);
}

/*
 * Returns the 'idx' key in 'msg'.
 *
 * Transfers ownership of returned buffer to the caller, so the caller must
 * take the responsibility of freeing it.
 *
 * Returns NULL if key does not exist or if we're unable to allocate memory.
 */
uint8_t *msg_get_full_key_copy(struct msg *msg, int idx, uint32_t *keylen) {
  // Get a pointer to the required key in 'msg'.
  uint8_t *key_ptr = msg_get_full_key(msg, idx, keylen);

  // Allocate a new buffer for the key.
  uint8_t *copied_key = dn_alloc((size_t)(*keylen + 1));
  if (copied_key == NULL) return NULL;

  // Copy contents of the key from 'msg' to our new buffer.
  dn_memcpy(copied_key, key_ptr, *keylen);
  copied_key[*keylen] = '\0';

  return copied_key;
}

static uint8_t *msg_get_arg(struct msg *req, uint32_t arg_index,
                            uint32_t *arglen) {
  *arglen = 0;
  if (array_n(req->args) == 0) return NULL;
  ASSERT_LOG(arg_index < array_n(req->args), "%s has %u keys", print_obj(req),
             array_n(req->args));

  struct argpos *argpos = array_get(req->args, arg_index);
  uint8_t *arg_start = argpos->start;
  uint8_t *arg_end = argpos->end;
  *arglen = (uint32_t)(arg_end - arg_start);
  return arg_start;
}

/*
 * Returns the 'idx' arg in 'msg'.
 *
 * Transfers ownership of returned buffer to the caller, so the caller must
 * take the responsibility of freeing it.
 *
 * Returns NULL if key does not exist or if we're unable to allocate memory.
 */
uint8_t *msg_get_arg_copy(struct msg *msg, int idx, uint32_t *arglen) {
  // Get a pointer to the required arg in 'msg'.
  uint8_t *arg_ptr = msg_get_arg(msg, idx, arglen);

  // Allocate a new buffer for the key.
  uint8_t *copied_arg = dn_alloc((size_t)(*arglen + 1));
  if (copied_arg == NULL) return NULL;

  // Copy contents of the key from 'msg' to our new buffer.
  dn_memcpy(copied_arg, arg_ptr, *arglen);
  copied_arg[*arglen] = '\0';

  return copied_arg;
}

uint32_t msg_payload_crc32(struct msg *rsp) {
  ASSERT(rsp != NULL);
  // take a continuous buffer crc
  uint32_t crc = 0;
  struct mbuf *mbuf;
  /* Since we want to checksum only the payload, we have to start from the
     payload offset. which is somewhere in the mbufs. Skip the mbufs till we
     find the start of the payload. If there is no dyno header, we start from
     the beginning of the first mbuf */
  bool start_found = rsp->dmsg ? false : true;

  // If the message is from another DC, the mbufs will have the decrypted
  // payload without the Dynomite header, so we do have the start.
  // rsp->dmsg->payload for cross DC msgs will have the encrypted payload.
  if (rsp->dmsg && !rsp->owner->same_dc) start_found = true;

  STAILQ_FOREACH(mbuf, &rsp->mhdr, next) {
    uint8_t *start = mbuf->start;
    uint8_t *end = mbuf->last;
    if (!start_found) {
      // if payload start is within this mbuf
      if ((mbuf->start <= rsp->dmsg->payload) &&
          (rsp->dmsg->payload < mbuf->last)) {
        start = rsp->dmsg->payload;
        start_found = true;
      } else {
        // else skip this mbuf
        continue;
      }
    }

    crc = crc32_sz((char *)start, (size_t)(end - start), crc);
  }
  return crc;
}

inline uint64_t msg_gen_frag_id(void) { return ++frag_id; }

static rstatus_t msg_parsed(struct context *ctx, struct conn *conn,
                            struct msg *msg) {
  struct msg *nmsg;
  struct mbuf *mbuf, *nbuf;

  mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);

  if (msg->pos == mbuf->last) {
    /* no more data to parse */
    conn_recv_done(ctx, conn, msg, NULL);
    return DN_OK;
  }

  /*
   * Input mbuf has un-parsed data. Split mbuf of the current message msg
   * into (mbuf, nbuf), where mbuf is the portion of the message that has
   * been parsed and nbuf is the portion of the message that is un-parsed.
   * Parse nbuf as a new message nmsg in the next iteration.
   */
  nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
  if (nbuf == NULL) {
    return DN_ENOMEM;
  }

  nmsg = msg_get(msg->owner, msg->is_request, __FUNCTION__);
  if (nmsg == NULL) {
    mbuf_put(nbuf);
    return DN_ENOMEM;
  }
  mbuf_insert(&nmsg->mhdr, nbuf);
  nmsg->pos = nbuf->pos;

  /* update length of current (msg) and new message (nmsg) */
  nmsg->mlen = mbuf_length(nbuf);
  msg->mlen -= nmsg->mlen;

  conn_recv_done(ctx, conn, msg, nmsg);

  return DN_OK;
}

static rstatus_t msg_repair(struct context *ctx, struct conn *conn,
                            struct msg *msg) {
  struct mbuf *nbuf, *mbuf;

  nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
  if (nbuf == NULL) {
    return DN_ENOMEM;
  }

  // This was added to handle a specific case which doesn't seem reproducible
  // now. Revisit if things seem off.
  //mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
  //mbuf_remove(&msg->mhdr, mbuf);
  mbuf_insert(&msg->mhdr, nbuf);
  msg->pos = nbuf->pos;

  return DN_OK;
}

/*
 * Crafts a success response message for the respective datastore.
 *
 * TODO: This currently does only Redis. The Redis specific code should
 *       be moved out of this file.
 *
 * Returns a 'msg' with the expected success response.
 */
static struct msg *craft_ok_rsp(struct context *ctx, struct conn *conn,
    struct msg *req) {

  ASSERT(req->is_request);

  rstatus_t ret_status = DN_OK;
  const char *QUIT_FMT_STRING = "+OK\r\n";

  struct msg *rsp = msg_get(conn, false, __FUNCTION__);
  if (rsp == NULL) {
    conn->err = errno;
    return NULL;
  }

  rstatus_t append_status = msg_append(rsp, QUIT_FMT_STRING, strlen(QUIT_FMT_STRING));
  if (append_status != DN_OK) {
    rsp_put(rsp);
    return NULL;
  }

  rsp->peer = req;
  rsp->is_request = 0;

  req->done = 1;

  return rsp;
}

rstatus_t simulate_ok_rsp(struct context *ctx, struct conn *conn,
    struct msg *msg) {
  // Create an OK response.
  struct msg *ok_rsp = craft_ok_rsp(ctx, conn, msg);

  // Add it to the outstanding messages dictionary, so that 'conn_handle_response'
  // can process it appropriately.
  dictAdd(conn->outstanding_msgs_dict, &msg->id, msg);

  // Enqueue the message in the outbound queue so that the code on the response
  // path can find it.
  conn_enqueue_outq(ctx, conn, msg);

  THROW_STATUS(conn_handle_response(ctx, conn,
      msg->parent_id ? msg->parent_id : msg->id, ok_rsp));

  return DN_OK;
}

/*
 * If the command sent to Dynomite was a special Dynomite configuration
 * command, we process and apply the configuration here.
 *
 * Returns: DN_OK on successful application, DN_ERROR otherwise.
 */
static rstatus_t msg_apply_config(struct context *ctx, struct conn *conn,
    struct msg *msg) {

  // We only support one type of configuration now.
  // TODO: If we support more, convert this to a switch case.
  ASSERT(msg->type == MSG_HACK_SETTING_CONN_CONSISTENCY);

  struct argpos *consistency_string = (struct argpos*) array_get(msg->args, 0);

  // We must have a consistency string, else we wouldn't have reached here.
  ASSERT(consistency_string != NULL);

  consistency_t cons = get_consistency_enum_from_string(consistency_string->start);
  if (cons == -1) return DN_ERROR;

  conn_set_read_consistency(conn, cons);
  conn_set_write_consistency(conn, cons);

  // Set the consistency to DC_ONE, since this is just a configuration setting.
  msg->consistency = DC_ONE;

  THROW_STATUS(simulate_ok_rsp(ctx, conn, msg));

  return DN_OK;
}

static rstatus_t msg_parse(struct context *ctx, struct conn *conn,
                           struct msg *msg) {
  rstatus_t status;

  if (msg_empty(msg)) {
    /* no data to parse */
    conn_recv_done(ctx, conn, msg, NULL);
    return DN_OK;
  }

  msg->parser(msg, ctx);

  switch (msg->result) {
    case MSG_PARSE_OK:
      status = msg_parsed(ctx, conn, msg);
      break;
    case MSG_PARSE_REPAIR:
      status = msg_repair(ctx, conn, msg);
      break;
    case MSG_PARSE_AGAIN:
      status = DN_OK;
      break;
    case MSG_PARSE_DYNO_CONFIG:
      status = msg_apply_config(ctx, conn, msg);

      // No more data to parse.
      conn_recv_done(ctx, conn, msg, NULL);
      break;

    case MSG_PARSE_NOOP:
      status = DN_NOOPS;
      break;

    default:
      /*
      if (!conn->dyn_mode) {
          status = DN_ERROR;
          conn->err = errno;
      } else {
          log_debug(LOG_VVERB, "Parsing error in dyn_mode");
          status = DN_OK;
      }
      */
      status = DN_ERROR;
      conn->err = errno;
      break;
  }

  return conn->err != 0 ? DN_ERROR : status;
}

static rstatus_t msg_recv_chain(struct context *ctx, struct conn *conn,
                                struct msg *msg) {
  rstatus_t status;
  struct msg *nmsg;
  struct mbuf *mbuf;
  size_t msize;
  ssize_t n;
  bool encryption_detected = (msg->dyn_parse_state == DYN_DONE ||
                              msg->dyn_parse_state == DYN_POST_DONE) &&
                             (msg->dmsg->flags & 0x1);

  mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
  /* This logic is unncessarily complicated. Ideally a connection should read
   * the entire payload of an encrypted message before it starts decrypting.
   * However the code tries to check if a buffer is full and decrypts it before
   * moving to the next buffer. So at any given point, a message large enough
   * can have some buffers decrypted and the last one either decrypted or
   * encrypted. We start decrypting a buffer if we finish reading the payload
   * (dmsg->plen) or we reach till mbuf->end_extra. If a buffer is encrypted, it
   * can span till mbuf->end_extra. If this buffer gets decrypted, it can span
   * till mbuf_full() i.e mbuf->end. which is 16 bytes (one cypher block) less
   * than mbuf->end_extra
   *
   * However there is no way to tell if a buffer that is filled till mbuf->end
   * is encrypted or decrypted so we cannot know if we should continue writing
   * to that buffer from mbuf->end till mbuf->end_extra (which is 16 bytes) or
   * whether we just decrypted a buffer and now it spans till mbuf->end and we
   * should create a new buffer to start receiving new encrypted data. Hence,
   * I created a new flag MBUF_FLAG_JUST_DECRYPTED which is solely for this
   * purpose. One should not write a code like this. We should receive the
   * entire payload first and then decrypt it. Its slightly slow but worth the
   * simplicity
   *
   * Create a new buffer if:
   * 1) mbuf is NULL
   * 2) unencrypted case and mbuf is full
   * 3) encrypted case and
   *      a) mbuf is full till end_extra
   *      b) mbuf is full till mbuf->end (mbuf_full) and we just decrypted that
   * buffer.
   */
  if (mbuf == NULL || ((!encryption_detected) && mbuf_full(mbuf)) ||
      (encryption_detected && mbuf->last == mbuf->end_extra) ||
      (encryption_detected && mbuf_full(mbuf) &&
       (mbuf->flags & MBUF_FLAGS_JUST_DECRYPTED))) {
    mbuf = mbuf_get();
    if (mbuf == NULL) {
      return DN_ENOMEM;
    }
    mbuf_insert(&msg->mhdr, mbuf);

    msg->pos = mbuf->pos;
  }

  ASSERT(mbuf->end_extra - mbuf->last > 0);

  if (!encryption_detected) {
    msize = mbuf_remaining_space(mbuf);
  } else {
    msize = (size_t)MIN(msg->dmsg->plen, mbuf->end_extra - mbuf->last);
  }

  if (msize != 0) {
    n = conn_recv_data(conn, mbuf->last, msize);
  } else {
    // We may have got an event notification even though we received all the data.
    // In that case, we don't want to read off the socket again.
    n = 0;
  }

  if (n < 0) {
    if (n == DN_EAGAIN) {
      return DN_OK;
    }
    return DN_ERROR;
  }

  ASSERT((mbuf->last + n) <= mbuf->end_extra);
  mbuf->last += n;
  msg->mlen += (uint32_t)n;

  // Only used in encryption case
  if (encryption_detected) {
    if ((n >= msg->dmsg->plen && n != 0) || mbuf->end_extra == mbuf->last) {
      // log_debug(LOG_VERB, "About to decrypt this mbuf as it is full or
      // eligible!");
      struct mbuf *nbuf = NULL;

      if (n >= msg->dmsg->plen) {
        nbuf = mbuf_get();

        if (nbuf == NULL) {
          loga("Not enough memory error!!!");
          return DN_ENOMEM;
        }

        status =
            dyn_aes_decrypt(mbuf->start, (size_t)(mbuf->last - mbuf->start),
                            nbuf, msg->owner->aes_key);
        if (status >= DN_OK) {
          int remain = n - msg->dmsg->plen;
          uint8_t *pos = mbuf->last - remain;
          mbuf_copy(nbuf, pos, remain);
        }

      } else if (mbuf->end_extra == mbuf->last) {
        nbuf = mbuf_get();

        if (nbuf == NULL) {
          loga("Not enough memory error!!!");
          return DN_ENOMEM;
        }

        status = dyn_aes_decrypt(mbuf->start, mbuf->last - mbuf->start, nbuf,
                                 msg->owner->aes_key);
      }

      if (status >= 0 && nbuf != NULL) {
        nbuf->flags |= MBUF_FLAGS_JUST_DECRYPTED;
        nbuf->flags |= MBUF_FLAGS_READ_FLIP;
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_insert(&msg->mhdr, nbuf);
        msg->pos = nbuf->start;

        msg->mlen -= mbuf->last - mbuf->start;
        msg->mlen += nbuf->last - nbuf->start;

        mbuf_put(mbuf);
      } else {  // clean up the mess and recover it
        mbuf_insert(&msg->mhdr, nbuf);
        msg->pos = nbuf->last;
        msg->dyn_error_code = BAD_FORMAT;
      }
    }

    msg->dmsg->plen -= n;
  }

  for (;;) {
    status = msg_parse(ctx, conn, msg);
    if (status != DN_OK) {
      return status;
    }

    /* get next message to parse */
    nmsg = conn_recv_next(ctx, conn, false);
    if (nmsg == NULL || nmsg == msg) {
      /* no more data to parse */
      break;
    }

    msg = nmsg;
  }

  return DN_OK;
}

rstatus_t msg_recv(struct context *ctx, struct conn *conn) {
  rstatus_t status;
  struct msg *msg;

  ASSERT(conn->recv_active);
  conn->recv_ready = 1;

  do {
    msg = conn_recv_next(ctx, conn, true);
    if (msg == NULL) {
      return DN_OK;
    }

    status = msg_recv_chain(ctx, conn, msg);
    if (status != DN_OK) {
      return status;
    }

  } while (conn->recv_ready);

  return DN_OK;
}

static rstatus_t msg_send_chain(struct context *ctx, struct conn *conn,
                                struct msg *msg) {
  struct msg_tqh send_msgq;            /* send msg q */
  struct msg *nmsg;                    /* next msg */
  struct mbuf *mbuf, *nbuf;            /* current and next mbuf */
  size_t mlen;                         /* current mbuf data length */
  struct iovec *ciov, iov[DN_IOV_MAX]; /* current iovec */
  struct array sendv;                  /* send iovec */
  size_t nsend, nsent;                 /* bytes to send; bytes sent */
  size_t limit;                        /* bytes to send limit */
  ssize_t n = 0;                       /* bytes sent by sendv */

  if (log_loggable(LOG_VVERB)) {
    loga("About to dump out the content of msg");
    msg_dump(LOG_VVERB, msg);
  }

  TAILQ_INIT(&send_msgq);

  array_set(&sendv, iov, sizeof(iov[0]), DN_IOV_MAX);

  /* preprocess - build iovec */

  nsend = 0;
  /*
   * readv() and writev() returns EINVAL if the sum of the iov_len values
   * overflows an ssize_t value Or, the vector count iovcnt is less than
   * zero or greater than the permitted maximum.
   */
  limit = SSIZE_MAX;

  for (;;) {
    ASSERT(conn->smsg == msg);

    TAILQ_INSERT_TAIL(&send_msgq, msg, m_tqe);

    STAILQ_FOREACH(mbuf, &msg->mhdr, next) {
      if (!(array_n(&sendv) < DN_IOV_MAX) && (nsend < limit)) break;

      if (mbuf_empty(mbuf)) {
        continue;
      }

      mlen = mbuf_length(mbuf);
      if ((nsend + mlen) > limit) {
        mlen = limit - nsend;
      }

      ciov = array_push(&sendv);
      ciov->iov_base = mbuf->pos;
      ciov->iov_len = mlen;

      nsend += mlen;
    }

    if (array_n(&sendv) >= DN_IOV_MAX || nsend >= limit) {
      break;
    }

    msg = conn_send_next(ctx, conn);
    if (msg == NULL) {
      break;
    }
  }

  conn->smsg = NULL;

  if (nsend != 0) n = conn_sendv_data(conn, &sendv, nsend);

  nsent = n > 0 ? (size_t)n : 0;

  /* postprocess - process sent messages in send_msgq */
  TAILQ_FOREACH_SAFE(msg, &send_msgq, m_tqe, nmsg) {
    TAILQ_REMOVE(&send_msgq, msg, m_tqe);

    if (nsent == 0) {
      if (msg->mlen == 0) {
        conn_send_done(ctx, conn, msg);
      }
      continue;
    }

    /* adjust mbufs of the sent message */
    for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = nbuf) {
      nbuf = STAILQ_NEXT(mbuf, next);

      if (mbuf_empty(mbuf)) {
        continue;
      }

      mlen = mbuf_length(mbuf);
      if (nsent < mlen) {
        /* mbuf was sent partially; process remaining bytes later */
        mbuf->pos += nsent;
        ASSERT(mbuf->pos < mbuf->last);
        nsent = 0;
        break;
      }

      /* mbuf was sent completely; mark it empty */
      mbuf->pos = mbuf->last;
      nsent -= mlen;
    }

    /* message has been sent completely, finalize it */
    if (mbuf == NULL) {
      conn_send_done(ctx, conn, msg);
    }
  }

  ASSERT(TAILQ_EMPTY(&send_msgq));

  if (n > 0) {
    return DN_OK;
  }

  return (n == DN_EAGAIN) ? DN_OK : DN_ERROR;
}

rstatus_t msg_send(struct context *ctx, struct conn *conn) {
  rstatus_t status;
  struct msg *msg;

  ASSERT_LOG(conn->send_active, "%s is not active", print_obj(conn));

  conn->send_ready = 1;
  do {
    msg = conn_send_next(ctx, conn);
    if (msg == NULL) {
      /* nothing to send */
      return DN_OK;
    }

    status = msg_send_chain(ctx, conn, msg);
    if (status != DN_OK) {
      return status;
    }

    if (TAILQ_COUNT(&conn->omsg_q) > MAX_CONN_QUEUE_SIZE) {
      conn->send_ready = 0;
      conn->err = ENOTRECOVERABLE;
      log_error("%s Setting ENOTRECOVERABLE happens here!", print_obj(conn));
    }

  } while (conn->send_ready);

  return DN_OK;
}

struct mbuf *msg_ensure_mbuf(struct msg *msg, size_t len) {
  struct mbuf *mbuf;

  if (STAILQ_EMPTY(&msg->mhdr) ||
      mbuf_remaining_space(STAILQ_LAST(&msg->mhdr, mbuf, next)) < len) {
    mbuf = mbuf_get();
    if (mbuf == NULL) {
      return NULL;
    }
    mbuf_insert(&msg->mhdr, mbuf);
  } else {
    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
  }

  return mbuf;
}

/*
 * Append n bytes of data, with n <= mbuf_remaining_space(mbuf)
 * into mbuf
 */
rstatus_t msg_append(struct msg *msg, uint8_t *pos, size_t n) {
  struct mbuf *mbuf;

  ASSERT(n <= mbuf_data_size());

  mbuf = msg_ensure_mbuf(msg, n);
  if (mbuf == NULL) {
    return DN_ENOMEM;
  }

  ASSERT(n <= mbuf_remaining_space(mbuf));

  mbuf_copy(mbuf, pos, n);
  msg->mlen += (uint32_t)n;

  return DN_OK;
}

/*
 * Prepend n bytes of data, with n <= mbuf_remaining_space(mbuf)
 * into mbuf
 */
rstatus_t msg_prepend(struct msg *msg, uint8_t *pos, size_t n) {
  struct mbuf *mbuf;

  mbuf = mbuf_get();
  if (mbuf == NULL) {
    return DN_ENOMEM;
  }

  ASSERT(n <= mbuf_remaining_space(mbuf));

  mbuf_copy(mbuf, pos, n);
  msg->mlen += (uint32_t)n;

  STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);

  return DN_OK;
}

/*
 * Prepend a formatted string into msg. Returns an error if the formatted
 * string does not fit in a single mbuf.
 */
rstatus_t msg_prepend_format(struct msg *msg, const char *fmt, ...) {
  struct mbuf *mbuf;
  int n;
  uint32_t size;
  va_list args;

  mbuf = mbuf_get();
  if (mbuf == NULL) {
    return DN_ENOMEM;
  }

  size = mbuf_remaining_space(mbuf);

  va_start(args, fmt);
  n = dn_vscnprintf(mbuf->last, size, fmt, args);
  va_end(args);
  if (n <= 0 || n >= (int)size) {
    return DN_ERROR;
  }

  mbuf->last += n;
  msg->mlen += (uint32_t)n;
  STAILQ_INSERT_HEAD(&msg->mhdr, mbuf, next);

  return DN_OK;
}

rstatus_t parse_int_arg_for_formatting(int arg, struct msg *msg, struct mbuf **mbuf_ptr,
    char* current_fmt_string, uint32_t *required_space_ptr,
    uint32_t *remaining_space_ptr) {
  struct mbuf *mbuf = *mbuf_ptr;

  *required_space_ptr = *required_space_ptr + (size_t) count_digits(arg);
  int n = snprintf(mbuf->last, *required_space_ptr + 1, current_fmt_string, arg);
  if (n < 0) return DN_ERROR;
  mbuf->last += n;
  msg->mlen += (uint32_t)n;
  *remaining_space_ptr = *remaining_space_ptr - n;
  return DN_OK;
}

rstatus_t parse_llu_arg_for_formatting(uint64_t arg, struct msg *msg,
    struct mbuf **mbuf_ptr, char* current_fmt_string, uint32_t *required_space_ptr,
    uint32_t *remaining_space_ptr) {
  struct mbuf *mbuf = *mbuf_ptr;

  *required_space_ptr = *required_space_ptr + (size_t) count_digits(arg);
  int n = snprintf(mbuf->last, *required_space_ptr + 1, current_fmt_string, arg);
  if (n < 0) return DN_ERROR;
  mbuf->last += n;
  msg->mlen += (uint32_t)n;
  *remaining_space_ptr = *remaining_space_ptr - n;
  return DN_OK;
}

rstatus_t parse_string_arg_for_formatting(char* arg, struct msg *msg,
    struct mbuf **mbuf_ptr, int given_fixed_len, char* current_fmt_string,
    int *cur_fmt_str_len_ptr, uint32_t *required_space_ptr,
    uint32_t *remaining_space_ptr) {

  struct mbuf *mbuf = *mbuf_ptr;

  *required_space_ptr += (given_fixed_len > 0) ? given_fixed_len : strlen(arg);
  int arg_offset = 0;
  if (*required_space_ptr > *remaining_space_ptr) {
    while (*required_space_ptr > *remaining_space_ptr) {

      // If we're already using a fixed len format specifier, skip this bit.
      if (given_fixed_len == 0) {
        // First fill in the remaining space in the existing mbuf by converting the
        // format string to a string that takes a string length.
        strncpy(current_fmt_string + (*cur_fmt_str_len_ptr) - 1, ".*s", 3);
        *cur_fmt_str_len_ptr += 2;
        current_fmt_string[*cur_fmt_str_len_ptr] = '\0';
      }

      // This is the arg length to print without the rest of the format string.
      int arglen = *remaining_space_ptr - (*cur_fmt_str_len_ptr - 4);

      // Write 'remaining_space' bytes to the mbuf.
      int n = snprintf(mbuf->last, *remaining_space_ptr + 1, current_fmt_string,
          arglen + 1, arg + arg_offset);
      if (n < 0) return DN_ERROR;

      // Subtract 1 from 'n' to un-account for the '\0' put by snprintf().
      // (see 'man snprintf').
      *required_space_ptr -= n - 1;
      mbuf->last += n - 1;
      msg->mlen += (uint32_t)n - 1;
      arg_offset += arglen;
      if (given_fixed_len > 0) {
        given_fixed_len -= n - 1;
      }

      // Get a new mbuf.
      mbuf = mbuf_get();
      if (mbuf == NULL) {
        return DN_ENOMEM;
      }
      // Insert it into the 'msg' struct.
      STAILQ_INSERT_TAIL(&msg->mhdr, mbuf, next);

      *mbuf_ptr = mbuf;
      *remaining_space_ptr = mbuf_remaining_space(mbuf);

      // Adjust the format string for the rest of the arg.
      char remaining_fmt_specifier[10];
      if (given_fixed_len > 0) {
        strcpy(remaining_fmt_specifier, "%.*s");
        remaining_fmt_specifier[4] = '\0';
      } else {
        strcpy(remaining_fmt_specifier, "%s");
        remaining_fmt_specifier[2] = '\0';
      }
      strncpy(current_fmt_string, remaining_fmt_specifier, strlen(remaining_fmt_specifier));
      *cur_fmt_str_len_ptr = strlen(remaining_fmt_specifier);
      current_fmt_string[*cur_fmt_str_len_ptr] = '\0';
    }
  }

  int n;
  // Copy the remaining part of the argument into the mbuf.
  if (given_fixed_len > 0) {
    n = snprintf(mbuf->last, *required_space_ptr + 1, current_fmt_string, given_fixed_len,
        arg + arg_offset);
  } else {
    n = snprintf(mbuf->last, *required_space_ptr + 1, current_fmt_string,
        arg + arg_offset);
  }
  if (n < 0) return DN_ERROR;
  mbuf->last += n;
  msg->mlen += (uint32_t)n;
  *remaining_space_ptr -= n;

  return DN_OK;
}

/*
 * Prepend a formatted string into msg.
 *
 * This currently only supports the %d and %s format specifiers.
 * The complicated logic is due to supporting prepending multi-mbuf payloads to
 * 'msg'.
 *
 */
rstatus_t msg_append_format(struct msg *msg, const char *fmt, int num_args, ...) {
  struct mbuf *mbuf;
  int n;
  uint32_t remaining_space;
  va_list args;

  // Check if an mbuf with free space already exists in this 'msg'.
  mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
  if (mbuf == NULL || mbuf_full(mbuf)) {
    mbuf = mbuf_get();
    if (mbuf == NULL) {
      return DN_ENOMEM;
    }
    STAILQ_INSERT_TAIL(&msg->mhdr, mbuf, next);
  }

  remaining_space = mbuf_remaining_space(mbuf);

  va_start(args, num_args);
  uint32_t required_space = 0;
  const char* start_from = fmt;
  char current_fmt_string[512];
  int i;

  // Iterate through the arguments and map them onto the format specifiers in the
  // format string.
  for (i = 0; i < num_args; ++i) {
    char *fmt_specifier = strstr(start_from, "%") + 1;
    if (fmt_specifier == NULL) {
      log_error("Number of arguments do not match format string.");
      return DN_ERROR;
    }

    // Calculate the space required for all the non replaceable chars in the format
    // string (i.e. chars not prepended with a '%').
    required_space = fmt_specifier - start_from - 1;

    switch ((char) *fmt_specifier) {
      case 'd':
        {
          int cur_fmt_str_len = fmt_specifier - start_from + 1;
          strncpy(current_fmt_string, start_from, cur_fmt_str_len);
          current_fmt_string[cur_fmt_str_len] = '\0';
          int arg = va_arg(args, int);
          if (parse_int_arg_for_formatting(arg, msg, &mbuf, current_fmt_string,
              &required_space, &remaining_space) != DN_OK) {
            return DN_ERROR;
          }
          // Start from right after the format specifier we just processed.
          start_from = fmt_specifier + 1;
          break;
        }
      // Case assumes '%llu'
      case 'l':
        {
          int cur_fmt_str_len = fmt_specifier - start_from + 3; // 3 because 'llu'
          strncpy(current_fmt_string, start_from, cur_fmt_str_len);
          current_fmt_string[cur_fmt_str_len] = '\0';
          uint64_t arg = va_arg(args, uint64_t);
          if (parse_llu_arg_for_formatting(arg, msg, &mbuf, current_fmt_string,
              &required_space, &remaining_space) != DN_OK) {
            return DN_ERROR;
          }
          // Start from right after the format specifier we just processed.
          start_from = fmt_specifier + 3;
          break;
        }
      case 's':
        {
          int cur_fmt_str_len = fmt_specifier - start_from + 1;
          strncpy(current_fmt_string, start_from, cur_fmt_str_len);
          current_fmt_string[cur_fmt_str_len] = '\0';
          char* arg = va_arg(args, char*);
          if (parse_string_arg_for_formatting(arg, msg, &mbuf, 0, current_fmt_string,
              &cur_fmt_str_len, &required_space, &remaining_space) != DN_OK) {
            return DN_ERROR;
          }
          // Start from right after the format specifier we just processed.
          start_from = fmt_specifier + 1;
          break;
        }
      // Case assumes '%.*s'
      case '.':
        {
          int cur_fmt_str_len = fmt_specifier - start_from + 3; // 3 because '.*s'
          strncpy(current_fmt_string, start_from, cur_fmt_str_len);
          current_fmt_string[cur_fmt_str_len] = '\0';
          int arg_fixed_len = va_arg(args, int);
          char* arg = va_arg(args, char*);
          ++i; // Skip one index since we parsed 2 args here.
          if (parse_string_arg_for_formatting(arg, msg, &mbuf, arg_fixed_len,
              current_fmt_string, &cur_fmt_str_len, &required_space,
              &remaining_space) != DN_OK) {
            return DN_ERROR;
          }

          // Start from right after the format specifier we just processed.
          start_from = fmt_specifier + 3;
          break;
        }
      default:
        log_error("Unsupported format string");
        return DN_ERROR;
    }
  }
  va_end(args);

  // Copy the remaining part of the format string.
  int string_epilogue_len = (fmt + strlen(fmt)) - start_from;
  if (string_epilogue_len != 0) {
    strcpy(mbuf->last, start_from);
    mbuf->last += string_epilogue_len;
    msg->mlen += string_epilogue_len;
  }

  return DN_OK;
}

bool is_msg_type_dyno_config(msg_type_t msg_type) {
  // TODO: Convert to a switch case if we support more.
  if (msg_type == MSG_HACK_SETTING_CONN_CONSISTENCY) return true;
  return false;
}
