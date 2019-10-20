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

#include <stdlib.h>
#include <unistd.h>

#include "dyn_conf.h"
#include "dyn_core.h"
#include "dyn_dnode_peer.h"
#include "dyn_server.h"
#include "dyn_token.h"
#include "dyn_util.h"

static char *_print_datastore(const struct object *obj) {
  ASSERT(obj->type == OBJ_DATASTORE);
  struct datastore *ds = (struct datastore *)obj;
  snprintf((char*)obj->print_buff, PRINT_BUF_SIZE, "<DATASTORE %p %.*s>", ds,
           ds->endpoint.pname.len, ds->endpoint.pname.data);
  return (char*)obj->print_buff;
}

static void server_ref(struct conn *conn, void *owner) {
  struct datastore *datastore = owner;

  ASSERT(conn->type == CONN_SERVER);
  ASSERT(conn->owner == NULL);

  conn->family = datastore->endpoint.family;
  conn->addrlen = datastore->endpoint.addrlen;
  conn->addr = datastore->endpoint.addr;
  string_duplicate(&conn->pname, &datastore->endpoint.pname);

  conn->owner = datastore;

  log_debug(LOG_VVERB, "ref conn %p owner %p into '%.*s", conn, datastore,
            datastore->endpoint.pname.len, datastore->endpoint.pname.data);
}

static void server_unref(struct conn *conn) {
  struct datastore *server;

  ASSERT(conn->type == CONN_SERVER);
  ASSERT(conn->owner != NULL);

  conn_event_del_conn(conn);
  server = conn->owner;
  conn->owner = NULL;

  log_debug(LOG_VVERB, "unref conn %p owner %p from '%.*s'", conn, server,
            server->endpoint.pname.len, server->endpoint.pname.data);
}

msec_t server_timeout(struct conn *conn) {
  struct datastore *server;
  struct server_pool *pool;

  ASSERT(conn->type == CONN_SERVER);

  server = conn->owner;
  pool = server->owner;

  return pool->timeout;
}

static bool server_active(struct conn *conn) {
  ASSERT(conn->type == CONN_SERVER);

  if (!TAILQ_EMPTY(&conn->imsg_q)) {
    log_debug(LOG_VVERB, "s %d is active", conn->sd);
    return true;
  }

  if (!TAILQ_EMPTY(&conn->omsg_q)) {
    log_debug(LOG_VVERB, "s %d is active", conn->sd);
    return true;
  }

  if (conn->rmsg != NULL) {
    log_debug(LOG_VVERB, "s %d is active", conn->sd);
    return true;
  }

  if (conn->smsg != NULL) {
    log_debug(LOG_VVERB, "s %d is active", conn->sd);
    return true;
  }

  log_debug(LOG_VVERB, "s %d is inactive", conn->sd);

  return false;
}

static void server_deinit(struct datastore *pdatastore) {
  if (!pdatastore) return;
  if (pdatastore->conn_pool) {
    conn_pool_destroy(pdatastore->conn_pool);
    pdatastore->conn_pool = NULL;
  }
}

static struct conn *server_conn(struct datastore *datastore, int tag) {
  return conn_pool_get(datastore->conn_pool, tag);
}

static rstatus_t datastore_preconnect(struct datastore *datastore) {
  return conn_pool_preconnect(datastore->conn_pool);
}

static void server_failure(struct context *ctx, struct datastore *server) {
  conn_pool_notify_conn_errored(server->conn_pool);
  if (ctx->stats) {
    stats_server_set_ts(ctx, server_ejected_at, dn_msec_now());
    stats_pool_incr(ctx, server_ejects);
  }
}

static void server_close_stats(struct context *ctx, struct datastore *server,
                               err_t err, unsigned eof, unsigned connected) {
  if (eof) {
    stats_server_incr(ctx, server_eof);
    return;
  }

  switch (err) {
    case ETIMEDOUT:
      stats_server_incr(ctx, server_timedout);
      break;
    case EPIPE:
    case ECONNRESET:
    case ECONNABORTED:
    case ECONNREFUSED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
      stats_server_incr(ctx, server_err);
      break;
  }
}

static void server_ack_err(struct context *ctx, struct conn *conn,
                           struct msg *req) {
  // I want to make sure we do not have swallow here.
  // ASSERT_LOG(!req->swallow, "req %d:%d has swallow set??", req->id,
  // req->parent_id);
  if ((req->swallow && !req->expect_datastore_reply) ||
      (req->swallow && (req->consistency == DC_ONE)) ||
      (req->swallow &&
       ((req->consistency == DC_QUORUM) ||
        (req->consistency == DC_SAFE_QUORUM)) &&
       (!conn->same_dc))) {
    log_info("%s SWALLOW %s len %" PRIu32, print_obj(conn), print_obj(req),
             req->mlen);
    req_put(req);
    return;
  }
  struct conn *c_conn = req->owner;
  // At other connections, these responses would be swallowed.
  ASSERT_LOG(
      (c_conn->type == CONN_CLIENT) || (c_conn->type == CONN_DNODE_PEER_CLIENT),
      "c_conn %s", print_obj(c_conn));

  // Create an appropriate response for the request so its propagated up;
  // This response gets dropped in rsp_make_error anyways. But since this is
  // an error path its ok with the overhead.
  struct msg *rsp = msg_get_error(conn, STORAGE_CONNECTION_REFUSE, conn->err);
  if (rsp == NULL) {
    // TODO: It's not clear how the client should behave if we hit this error
    // condition. Return an appropriate error instead.
    log_warn("Could not allocate msg for notifying an error to the client.");
    return;
  }
  req->done = 1;
  rsp->peer = req;
  rsp->is_error = req->is_error = 1;
  rsp->error_code = req->error_code = conn->err;
  rsp->dyn_error_code = req->dyn_error_code = STORAGE_CONNECTION_REFUSE;
  rsp->dmsg = NULL;
  log_debug(LOG_DEBUG, "%s <-> %s", print_obj(req), print_obj(rsp));

  log_info("close %s req %s len %" PRIu32 " from %s %c %s", print_obj(conn),
           print_obj(req), req->mlen, print_obj(c_conn), conn->err ? ':' : ' ',
           conn->err ? strerror(conn->err) : " ");
  rstatus_t status = conn_handle_response(ctx,
      c_conn, req->parent_id ? req->parent_id : req->id, rsp);
  IGNORE_RET_VAL(status);
  if (req->swallow) req_put(req);
}

static void server_close(struct context *ctx, struct conn *conn) {
  struct msg *req, *nmsg; /* current and next message */

  ASSERT(conn->type == CONN_SERVER);
  struct datastore *datastore = conn->owner;

  if (ctx->stats) {
    server_close_stats(ctx, datastore, conn->err, conn->eof, conn->connected);
  }

  if (conn->sd < 0) {
    conn_unref(conn);
    conn_put(conn);
    server_failure(ctx, datastore);
    return;
  }

  uint32_t out_counter = 0;
  for (req = TAILQ_FIRST(&conn->omsg_q); req != NULL; req = nmsg) {
    nmsg = TAILQ_NEXT(req, s_tqe);

    /* dequeue the message (request) from server outq */
    conn_dequeue_outq(ctx, conn, req);
    server_ack_err(ctx, conn, req);
    out_counter++;
  }
  ASSERT(TAILQ_EMPTY(&conn->omsg_q));

  uint32_t in_counter = 0;
  for (req = TAILQ_FIRST(&conn->imsg_q); req != NULL; req = nmsg) {
    nmsg = TAILQ_NEXT(req, s_tqe);

    /* dequeue the message (request) from server inq */
    conn_dequeue_inq(ctx, conn, req);
    // We should also remove the req from the timeout rbtree.
    msg_tmo_delete(req);
    server_ack_err(ctx, conn, req);
    in_counter++;

    if (ctx->stats) stats_server_incr(ctx, server_dropped_requests);
  }
  ASSERT(TAILQ_EMPTY(&conn->imsg_q));

  log_warn("close %s Dropped %u outqueue & %u inqueue requests",
           print_obj(conn), out_counter, in_counter);

  struct msg *rsp = conn->rmsg;
  if (rsp != NULL) {
    conn->rmsg = NULL;

    ASSERT(!rsp->is_request);
    ASSERT(rsp->peer == NULL);

    rsp_put(rsp);

    log_info("close %s discarding rsp %s len %" PRIu32
             " "
             "in error",
             print_obj(conn), print_obj(rsp), rsp->mlen);
  }

  ASSERT(conn->smsg == NULL);

  conn_unref(conn);

  rstatus_t status = close(conn->sd);
  if (status < 0) {
    log_error("close s %s failed, ignored: %s", print_obj(conn),
              strerror(errno));
  }
  conn->sd = -1;

  conn_put(conn);

  server_failure(ctx, datastore);
}

static void server_connected(struct context *ctx, struct conn *conn) {
  ASSERT(conn->type == CONN_SERVER);
  ASSERT(conn->connecting && !conn->connected);

  conn->connecting = 0;
  conn->connected = 1;
  conn_pool_connected(conn->conn_pool, conn);

  log_notice("%s connected ", print_obj(conn));
}

static void server_ok(struct context *ctx, struct conn *conn) {
  struct datastore *server = conn->owner;

  ASSERT(conn->type == CONN_SERVER);
  ASSERT(conn->connected);

  if (log_loggable(LOG_VERB)) {
    log_debug(LOG_VERB,
              "reset server '%.*s' failure count from %" PRIu32 " to 0",
              server->endpoint.pname.len, server->endpoint.pname.data,
              server->failure_count);
  }
  server->failure_count = 0;
  server->next_retry_ms = 0ULL;
}

static rstatus_t datastore_check_autoeject(struct datastore *datastore) {
  struct server_pool *pool = datastore->owner;
  if (!pool->auto_eject_hosts) {
    return DN_OK;
  }

  msec_t now_ms = dn_msec_now();
  if (now_ms == 0) {
    return DN_ERROR;
  }

  if (now_ms <= datastore->next_retry_ms) {
    errno = ECONNREFUSED;
    return DN_ERROR;
  }

  return DN_OK;
}

struct conn *get_datastore_conn(struct context *ctx, struct server_pool *pool,
                                int tag) {
  rstatus_t status;
  struct datastore *datastore = pool->datastore;
  struct conn *conn;

  ASSERT(datastore);
  status = datastore_check_autoeject(datastore);
  if (status != DN_OK) {
    return NULL;
  }

  /* pick a connection to a given server */
  conn = server_conn(datastore, tag);
  if (conn == NULL) {
    return NULL;
  }

  status = conn_connect(ctx, conn);
  if (status != DN_OK) {
    conn_close(ctx, conn);
    return NULL;
  }

  return conn;
}

rstatus_t server_pool_preconnect(struct context *ctx) {
  if (!ctx->pool.preconnect) {
    return DN_OK;
  }
  return datastore_preconnect(ctx->pool.datastore);
}

void server_pool_disconnect(struct context *ctx) {
  struct datastore *datastore = ctx->pool.datastore;
  if (datastore->conn_pool) {
    conn_pool_destroy(datastore->conn_pool);
    datastore->conn_pool = NULL;
  }
}

/**
 * Initialize the server pool.
 * @param[in,out] sp Server pool configuration.
 * @param[in] cp Connection pool configuration.
 * @param[in] ctx Context.
 * @return rstatus_t Return status code.
 */
rstatus_t server_pool_init(struct server_pool *sp, struct conf_pool *cp,
                           struct context *ctx) {
  ASSERT(cp->valid);

  memset(sp, 0, sizeof(struct server_pool));
  init_object(&sp->object, OBJ_POOL, print_server_pool);
  sp->ctx = ctx;
  sp->p_conn = NULL;
  TAILQ_INIT(&sp->c_conn_q);
  TAILQ_INIT(&sp->ready_conn_q);

  array_null(&sp->datacenters);
  /* sp->ncontinuum = 0; */
  /* sp->nserver_continuum = 0; */
  /* sp->continuum = NULL; */
  sp->next_rebuild = 0ULL;

  sp->name = cp->name;
  sp->proxy_endpoint.pname = cp->listen.pname;
  sp->proxy_endpoint.port = (uint16_t)cp->listen.port;

  sp->proxy_endpoint.family = cp->listen.info.family;
  sp->proxy_endpoint.addrlen = cp->listen.info.addrlen;
  sp->proxy_endpoint.addr = (struct sockaddr *)&cp->listen.info.addr;

  sp->key_hash_type = cp->hash;
  sp->key_hash = get_hash_func(cp->hash);
  sp->hash_tag = cp->hash_tag;

  g_data_store = cp->data_store;
  if ((g_data_store != DATA_REDIS) && (g_data_store != DATA_MEMCACHE)) {
    log_error("Invalid datastore in conf file");
    return DN_ERROR;
  }
  set_datastore_ops();
  sp->timeout = cp->timeout;
  sp->backlog = cp->backlog;

  sp->client_connections = (uint32_t)cp->client_connections;

  sp->server_retry_timeout_ms = cp->server_retry_timeout_ms;
  sp->server_failure_limit = (uint8_t)cp->server_failure_limit;
  sp->auto_eject_hosts = cp->auto_eject_hosts ? 1 : 0;
  sp->preconnect = cp->preconnect ? 1 : 0;

  sp->datastore = dn_zalloc(sizeof(*sp->datastore));
  init_object(&(sp->datastore->obj), OBJ_DATASTORE, _print_datastore);
  THROW_STATUS(conf_datastore_transform(sp->datastore, cp, cp->conf_datastore));
  sp->datastore->owner = sp;
  log_debug(LOG_DEBUG, "init datastore in pool '%.*s'", sp->name.len,
            sp->name.data);

  /* dynomite init */
  sp->seed_provider = cp->dyn_seed_provider;
  sp->dnode_proxy_endpoint.pname = cp->dyn_listen.pname;
  sp->dnode_proxy_endpoint.port = (uint16_t)cp->dyn_listen.port;
  sp->dnode_proxy_endpoint.family = cp->dyn_listen.info.family;
  sp->dnode_proxy_endpoint.addrlen = cp->dyn_listen.info.addrlen;
  sp->dnode_proxy_endpoint.addr = (struct sockaddr *)&cp->dyn_listen.info.addr;
  sp->max_local_peer_connections = cp->local_peer_connections;
  sp->max_remote_peer_connections = cp->remote_peer_connections;
  sp->rack = cp->rack;
  sp->dc = cp->dc;
  sp->tokens = cp->tokens;
  sp->env = cp->env;
  sp->enable_gossip = cp->enable_gossip;

  /* dynomite stats init */
  sp->stats_endpoint.pname = cp->stats_listen.pname;
  sp->stats_endpoint.port = (uint16_t)cp->stats_listen.port;
  sp->stats_endpoint.family = cp->stats_listen.info.family;
  sp->stats_endpoint.addrlen = cp->stats_listen.info.addrlen;
  sp->stats_endpoint.addr = (struct sockaddr *)&cp->stats_listen.info.addr;
  sp->stats_interval = cp->stats_interval;
  sp->mbuf_size = cp->mbuf_size;
  sp->alloc_msgs_max = cp->alloc_msgs_max;

  sp->secure_server_option =
      get_secure_server_option(&cp->secure_server_option);
  sp->pem_key_file = cp->pem_key_file;
  sp->recon_key_file = cp->recon_key_file;
  sp->recon_iv_file = cp->recon_iv_file;

  array_null(&sp->peers);
  array_init(&sp->datacenters, 1, sizeof(struct datacenter));
  sp->conf_pool = cp;

  /* gossip */
  sp->g_interval = cp->gos_interval;

  set_msgs_per_sec(cp->conn_msg_rate);

  log_debug(LOG_VERB, "transform to pool '%.*s'", sp->name.len, sp->name.data);

  sp->ctx = ctx;
  struct datastore *datastore = sp->datastore;
  datastore->conn_pool = conn_pool_create(
      ctx, datastore, datastore->max_connections, init_server_conn,
      sp->server_failure_limit, sp->server_retry_timeout_ms / 1000);
  log_debug(LOG_DEBUG, "Initialized server pool");
  return DN_OK;
}

/**
 * Deinitialize the server pool which includes deinitialization of the backend
 * data store and setting the number of live backend servers to 0.
 * @param[in,out] sp Server pool.
 */
void server_pool_deinit(struct server_pool *sp) {
  ASSERT(sp->p_conn == NULL);
  ASSERT(TAILQ_EMPTY(&sp->c_conn_q));

  server_deinit(sp->datastore);
  dn_free(sp->datastore);
  sp->datastore = NULL;
  log_debug(LOG_DEBUG, "deinit pool '%.*s'", sp->name.len, sp->name.data);
}

dictType dc_string_dict_type = {
    dict_string_hash,        /* hash function */
    NULL,                    /* key dup */
    NULL,                    /* val dup */
    dict_string_key_compare, /* key compare */
    dict_string_destructor,  /* key destructor */
    NULL                     /* val destructor */
};

static rstatus_t rack_init(struct rack *rack) {

  // TODO: Initialize the array to the size of the ring instead of to 1.
  THROW_STATUS(array_init(&rack->continuums, 1, sizeof(struct continuum)));
  rack->ncontinuum = 0;
  rack->nserver_continuum = 0;
  rack->name = dn_alloc(sizeof(struct string));
  string_init(rack->name);

  rack->dc = dn_alloc(sizeof(struct string));
  string_init(rack->dc);

  return DN_OK;
}

static rstatus_t rack_deinit(struct rack *rack) {
  array_deinit(&rack->continuums);

  return DN_OK;
}

static rstatus_t dc_init(struct datacenter *dc) {
  rstatus_t status;

  dc->dict_rack = dictCreate(&dc_string_dict_type, NULL);
  dc->name = dn_alloc(sizeof(struct string));
  string_init(dc->name);
  dc->preselected_rack_for_replication = NULL;

  status = array_init(&dc->racks, 3, sizeof(struct rack));

  return status;
}

static rstatus_t rack_destroy(void *elem) {
  struct rack *rack = elem;
  return rack_deinit(rack);
}

static rstatus_t dc_deinit(struct datacenter *dc) {
  array_each(&dc->racks, rack_destroy);
  string_deinit(dc->name);
  // dictRelease(dc->dict_rack);
  return DN_OK;
}

rstatus_t datacenter_destroy(void *elem, void *data) {
  struct datacenter *dc = elem;
  dc_deinit(dc);

  return DN_OK;
}

struct datacenter *server_get_dc(struct server_pool *pool,
                                 struct string *dcname) {
  struct datacenter *dc;
  uint32_t i, len;

  if (log_loggable(LOG_DEBUG)) {
    log_debug(LOG_DEBUG, "server_get_dc dc '%.*s'", dcname->len, dcname->data);
  }

  for (i = 0, len = array_n(&pool->datacenters); i < len; i++) {
    dc = (struct datacenter *)array_get(&pool->datacenters, i);
    ASSERT(dc != NULL);
    ASSERT(dc->name != NULL);

    if (string_compare(dc->name, dcname) == 0) {
      return dc;
    }
  }

  dc = array_push(&pool->datacenters);
  dc_init(dc);
  string_copy(dc->name, dcname->data, dcname->len);

  if (log_loggable(LOG_DEBUG)) {
    log_debug(LOG_DEBUG, "server_get_dc about to exit dc '%.*s'", dc->name->len,
              dc->name->data);
  }

  return dc;
}

struct rack *server_get_rack(struct datacenter *dc, struct string *rackname) {
  ASSERT(dc != NULL);
  ASSERT(dc->dict_rack != NULL);
  ASSERT(dc->name != NULL);

  if (log_loggable(LOG_DEBUG)) {
    log_debug(LOG_DEBUG, "server_get_rack   '%.*s'", rackname->len,
              rackname->data);
  }
  /*
struct rack *rack = dictFetchValue(dc->dict_rack, rackname);
if (rack == NULL) {
rack = array_push(&dc->racks);
rack_init(rack);
string_copy(rack->name, rackname->data, rackname->len);
string_copy(rack->dc, dc->name->data, dc->name->len);
rack->continuum = dn_alloc(sizeof(struct continuum));

  dictAdd(dc->dict_rack, rackname, rack);
}
   */

  struct rack *rack;
  uint32_t i, len;
  for (i = 0, len = array_n(&dc->racks); i < len; i++) {
    rack = (struct rack *)array_get(&dc->racks, i);

    if (string_compare(rack->name, rackname) == 0) {
      return rack;
    }
  }

  rack = array_push(&dc->racks);
  rack_init(rack);
  string_copy(rack->name, rackname->data, rackname->len);
  string_copy(rack->dc, dc->name->data, dc->name->len);

  if (log_loggable(LOG_DEBUG)) {
    log_debug(LOG_DEBUG, "server_get_rack exiting  '%.*s'", rack->name->len,
              rack->name->data);
  }

  return rack;
}

struct rack *server_get_rack_by_dc_rack(struct server_pool *sp,
                                        struct string *rackname,
                                        struct string *dcname) {
  struct datacenter *dc = server_get_dc(sp, dcname);
  return server_get_rack(dc, rackname);
}

struct msg *rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc) {
  struct msg *rsp;

  ASSERT((conn->type == CONN_DNODE_PEER_SERVER) || (conn->type == CONN_SERVER));

  if (conn->eof) {
    rsp = conn->rmsg;

    /* server sent eof before sending the entire request */
    if (rsp != NULL) {
      conn->rmsg = NULL;

      ASSERT(rsp->peer == NULL);
      ASSERT(!rsp->is_request);

      log_error("%s EOF discarding incomplete rsp %s len %" PRIu32,
                print_obj(conn), print_obj(rsp), rsp->mlen);

      rsp_put(rsp);
    }

    /*
     * We treat TCP half-close from a server different from how we treat
     * those from a client. On a FIN from a server, we close the connection
     * immediately by sending the second FIN even if there were outstanding
     * or pending requests. This is actually a tricky part in the FA, as
     * we don't expect this to happen unless the server is misbehaving or
     * it crashes
     */
    conn->done = 1;
    log_debug(LOG_DEBUG, "s %d active %d is done", conn->sd, conn_active(conn));

    return NULL;
  }

  rsp = conn->rmsg;
  if (rsp != NULL) {
    ASSERT(!rsp->is_request);
    return rsp;
  }

  if (!alloc) {
    return NULL;
  }

  rsp = rsp_get(conn);
  if (rsp != NULL) {
    conn->rmsg = rsp;
  }

  // Record timetamps if repairs are enabled.
  // TODO: Consider requests that span multiple mbufs.
  if (is_read_repairs_enabled()) {
    rsp->timestamp = current_timestamp_in_millis();
  }
  return rsp;
}

static bool server_rsp_filter(struct context *ctx, struct conn *conn,
                              struct msg *rsp) {
  struct msg *req;

  ASSERT(conn->type == CONN_SERVER);

  if (msg_empty(rsp)) {
    ASSERT(conn->rmsg == NULL);
    log_debug(LOG_VERB, "filter empty rsp %" PRIu64 " on s %d", rsp->id,
              conn->sd);
    rsp_put(rsp);
    return true;
  }

  req = TAILQ_FIRST(&conn->omsg_q);
  if (req == NULL) {
    log_debug(LOG_VERB, "filter stray rsp %" PRIu64 " len %" PRIu32 " on s %d",
              rsp->id, rsp->mlen, conn->sd);
    rsp_put(rsp);
    return true;
  }

  if (!req->expect_datastore_reply) {
    conn_dequeue_outq(ctx, conn, req);
    req_put(req);
    rsp_put(rsp);
    return true;
  }

  ASSERT(req->is_request);

  if (req->swallow) {
    conn_dequeue_outq(ctx, conn, req);
    req->done = 1;

    log_debug(LOG_DEBUG,
              "swallow rsp %" PRIu64 " len %" PRIu32
              " of req "
              "%" PRIu64 " on s %d",
              rsp->id, rsp->mlen, req->id, conn->sd);

    rsp_put(rsp);
    req_put(req);
    return true;
  }

  return false;
}

static void server_rsp_forward_stats(struct context *ctx, struct msg *rsp) {
  ASSERT(!rsp->is_request);

  if (rsp->is_read) {
    stats_server_incr(ctx, read_responses);
    stats_server_incr_by(ctx, read_response_bytes, rsp->mlen);
  } else {
    stats_server_incr(ctx, write_responses);
    stats_server_incr_by(ctx, write_response_bytes, rsp->mlen);
  }
}

static void server_rsp_forward(struct context *ctx, struct conn *s_conn,
                               struct msg *rsp) {
  rstatus_t status;
  struct msg *req;
  struct conn *c_conn;
  ASSERT(s_conn->type == CONN_SERVER);

  /* response from server implies that server is ok and heartbeating */
  server_ok(ctx, s_conn);

  /* dequeue peer message (request) from server */
  req = TAILQ_FIRST(&s_conn->omsg_q);
  ASSERT(req->is_request);
  if (req->request_send_time) {
    struct stats *st = ctx->stats;
    uint64_t delay = dn_usec_now() - req->request_send_time;
    histo_add(&st->server_latency_histo, delay);
  }
  conn_dequeue_outq(ctx, s_conn, req);

  c_conn = req->owner;
  log_info("%s %s RECEIVED %s", print_obj(c_conn), print_obj(req),
           print_obj(rsp));

  ASSERT((c_conn->type == CONN_CLIENT) ||
         (c_conn->type == CONN_DNODE_PEER_CLIENT));

  server_rsp_forward_stats(ctx, rsp);
  // handler owns the response now
  status = conn_handle_response(ctx, c_conn, req->id, rsp);
  IGNORE_RET_VAL(status);
}

static void rsp_recv_done(struct context *ctx, struct conn *conn,
                          struct msg *rsp, struct msg *nmsg) {
  ASSERT(conn->type == CONN_SERVER);
  ASSERT(rsp != NULL && conn->rmsg == rsp);
  ASSERT(!rsp->is_request);
  ASSERT(rsp->owner == conn);
  ASSERT(nmsg == NULL || !nmsg->is_request);

  /* enqueue next message (response), if any */
  conn->rmsg = nmsg;

  if (server_rsp_filter(ctx, conn, rsp)) {
    return;
  }
  server_rsp_forward(ctx, conn, rsp);
}

struct msg *req_send_next(struct context *ctx, struct conn *conn) {
  rstatus_t status;
  struct msg *req, *nmsg; /* current and next message */

  ASSERT((conn->type == CONN_SERVER) || (conn->type == CONN_DNODE_PEER_SERVER));

  if (conn->connecting) {
    if (conn->type == CONN_SERVER) {
      server_connected(ctx, conn);
    } else if (conn->type == CONN_DNODE_PEER_SERVER) {
      dnode_peer_connected(ctx, conn);
    }
  }

  nmsg = TAILQ_FIRST(&conn->imsg_q);
  if (nmsg == NULL) {
    /* nothing to send as the server inq is empty */
    status = conn_event_del_out(conn);
    if (status != DN_OK) {
      conn->err = errno;
    }

    return NULL;
  }

  req = conn->smsg;

  if (req != NULL) {
    ASSERT(req->is_request && !req->done);
    nmsg = TAILQ_NEXT(req, s_tqe);
  }

  conn->smsg = nmsg;

  if (nmsg == NULL) {
    return NULL;
  }

  ASSERT(nmsg->is_request && !nmsg->done);

  if (log_loggable(LOG_VVERB)) {
    log_debug(LOG_VVERB,
              "send next req %" PRIu64 " len %" PRIu32
              " type %d on "
              "s %d",
              nmsg->id, nmsg->mlen, nmsg->type, conn->sd);
  }

  return nmsg;
}

void req_send_done(struct context *ctx, struct conn *conn, struct msg *req) {
  ASSERT((conn->type == CONN_SERVER) || (conn->type == CONN_DNODE_PEER_SERVER));
  ASSERT(req != NULL && conn->smsg == NULL);
  ASSERT(req->is_request && !req->done);
  // ASSERT(req->owner == conn);

  if (log_loggable(LOG_VVERB)) {
    log_debug(LOG_VVERB,
              "send done req %" PRIu64 " len %" PRIu32
              " type %d on "
              "s %d",
              req->id, req->mlen, req->type, conn->sd);
  }

  /* dequeue the message (request) from server inq */
  conn_dequeue_inq(ctx, conn, req);
  req->request_send_time = dn_usec_now();

  /*
   * expect_datastore_reply request instructs the server to send response. So,
   * enqueue message (request) in server outq, if response is expected.
   * Otherwise, free the request
   */
  if (req->expect_datastore_reply || (conn->type == CONN_SERVER)) {
    conn_enqueue_outq(ctx, conn, req);
  } else {
    req_put(req);
  }
}

static void req_server_enqueue_imsgq(struct context *ctx, struct conn *conn,
                                     struct msg *req) {
  ASSERT(req->is_request);
  ASSERT(conn->type == CONN_SERVER);
  req->request_inqueue_enqueue_time_us = dn_usec_now();

  /*
   * timeout clock starts ticking the instant the message is enqueued into
   * the server in_q; the clock continues to tick until it either expires
   * or the message is dequeued from the server out_q
   *
   * expect_datastore_reply request have timeouts because client is expecting
   * a response
   */
  if (req->expect_datastore_reply) {
    msg_tmo_insert(req, conn);
  }

  TAILQ_INSERT_TAIL(&conn->imsg_q, req, s_tqe);
  log_debug(LOG_VERB, "conn %p enqueue inq %d:%d", conn, req->id,
            req->parent_id);

  histo_add(&ctx->stats->server_in_queue, TAILQ_COUNT(&conn->imsg_q));
  stats_server_incr(ctx, in_queue);
  stats_server_incr_by(ctx, in_queue_bytes, req->mlen);
}

static void req_server_dequeue_imsgq(struct context *ctx, struct conn *conn,
                                     struct msg *req) {
  ASSERT(req->is_request);
  ASSERT(conn->type == CONN_SERVER);

  TAILQ_REMOVE(&conn->imsg_q, req, s_tqe);
  log_debug(LOG_VERB, "conn %p dequeue inq %d:%d", conn, req->id,
            req->parent_id);
  usec_t delay = dn_usec_now() - req->request_inqueue_enqueue_time_us;
  histo_add(&ctx->stats->server_queue_wait_time_histo, delay);

  histo_add(&ctx->stats->server_in_queue, TAILQ_COUNT(&conn->imsg_q));
  stats_server_decr(ctx, in_queue);
  stats_server_decr_by(ctx, in_queue_bytes, req->mlen);
}

static void req_server_enqueue_omsgq(struct context *ctx, struct conn *conn,
                                     struct msg *req) {
  ASSERT(req->is_request);
  ASSERT(conn->type == CONN_SERVER);

  TAILQ_INSERT_TAIL(&conn->omsg_q, req, s_tqe);
  log_debug(LOG_VERB, "conn %p enqueue outq %d:%d", conn, req->id,
            req->parent_id);

  histo_add(&ctx->stats->server_out_queue, TAILQ_COUNT(&conn->omsg_q));
  stats_server_incr(ctx, out_queue);
  stats_server_incr_by(ctx, out_queue_bytes, req->mlen);
}

static void req_server_dequeue_omsgq(struct context *ctx, struct conn *conn,
                                     struct msg *req) {
  ASSERT(req->is_request);
  ASSERT(conn->type == CONN_SERVER);

  msg_tmo_delete(req);

  TAILQ_REMOVE(&conn->omsg_q, req, s_tqe);
  log_debug(LOG_VERB, "conn %p dequeue outq %d:%d", conn, req->id,
            req->parent_id);

  histo_add(&ctx->stats->server_out_queue, TAILQ_COUNT(&conn->omsg_q));
  stats_server_decr(ctx, out_queue);
  stats_server_decr_by(ctx, out_queue_bytes, req->mlen);
}

struct conn_ops server_ops = {msg_recv,
                              rsp_recv_next,
                              rsp_recv_done,
                              msg_send,
                              req_send_next,
                              req_send_done,
                              server_close,
                              server_active,
                              server_ref,
                              server_unref,
                              req_server_enqueue_imsgq,
                              req_server_dequeue_imsgq,
                              req_server_enqueue_omsgq,
                              req_server_dequeue_omsgq,
                              conn_cant_handle_response};

void init_server_conn(struct conn *conn) {
  conn->dyn_mode = 0;
  conn->type = CONN_SERVER;
  conn->ops = &server_ops;
}
