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
#include "dyn_dnode_proxy.h"
#include "dyn_gossip.h"
#include "dyn_proxy.h"
#include "dyn_server.h"
#include "dyn_task.h"
#include "event/dyn_event.h"

uint32_t admin_opt = 0;

static void core_print_peer_status(void *arg1) {
  struct context *ctx = arg1;
  struct server_pool *sp = &ctx->pool;
  // iterate over all peers
  uint32_t dc_cnt = array_n(&sp->datacenters);
  uint32_t dc_index;
  for (dc_index = 0; dc_index < dc_cnt; dc_index++) {
    struct datacenter *dc = array_get(&sp->datacenters, dc_index);
    if (!dc) log_panic("DC is null. Topology not inited proerly");
    uint8_t rack_cnt = (uint8_t)array_n(&dc->racks);
    uint8_t rack_index;
    for (rack_index = 0; rack_index < rack_cnt; rack_index++) {
      struct rack *rack = array_get(&dc->racks, rack_index);
      uint8_t i = 0;
      for (i = 0; i < rack->ncontinuum; i++) {
        struct continuum *c = (struct continuum*) array_get(&rack->continuums, i);
        ASSERT(c != NULL);
        uint32_t peer_index = c->index;
        struct node *peer = *(struct node **)array_get(&sp->peers, peer_index);
        if (!peer) log_panic("peer is null. Topology not inited proerly");

        log_notice("%u)%p %.*s %.*s %.*s %s", peer_index, peer, dc->name->len,
                   dc->name->data, rack->name->len, rack->name->data,
                   peer->endpoint.pname.len, peer->endpoint.pname.data,
                   get_state(peer->state));
      }
    }
  }
}

void core_set_local_state(struct context *ctx, dyn_state_t state) {
  struct server_pool *sp = &ctx->pool;
  struct node *peer = *(struct node **)array_get(&sp->peers, 0);
  ctx->dyn_state = state;
  peer->state = state;
}

static rstatus_t core_init_last(struct context *ctx) {
  core_debug(ctx);
  preselect_remote_rack_for_replication(ctx);
  // Print the network health once after 30 secs
  schedule_task_1(core_print_peer_status, ctx, 30000);
  return DN_OK;
}

static rstatus_t core_gossip_pool_init(struct context *ctx) {
  // init ring msg queue
  CBUF_Init(C2G_InQ);
  CBUF_Init(C2G_OutQ);

  THROW_STATUS(gossip_pool_init(ctx));
  return DN_OK;
}

static rstatus_t core_dnode_peer_pool_preconnect(struct context *ctx) {
  rstatus_t status = dnode_peer_pool_preconnect(ctx);
  IGNORE_RET_VAL(status);
  return status;
}
static rstatus_t core_dnode_peer_init(struct context *ctx) {
  /* initialize peers */
  THROW_STATUS(dnode_initialize_peers(ctx));
  return DN_OK;
}

static rstatus_t core_dnode_proxy_init(struct context *ctx) {
  /* initialize dnode listener per server pool */
  THROW_STATUS(dnode_proxy_init(ctx));

  ctx->dyn_state = JOINING;  // TODOS: change this to JOINING
  return DN_OK;
}

static rstatus_t core_proxy_init(struct context *ctx) {
  /* initialize proxy per server pool */
  THROW_STATUS(proxy_init(ctx));
  return DN_OK;
}

static rstatus_t core_server_pool_preconnect(struct context *ctx) {
  rstatus_t status = server_pool_preconnect(ctx);
  IGNORE_RET_VAL(status);

  return DN_OK;
}

static rstatus_t core_event_base_create(struct context *ctx) {
  /* initialize event handling for client, proxy and server */
  ctx->evb = event_base_create(EVENT_SIZE, &core_core);
  if (ctx->evb == NULL) {
    log_error("Failed to create socket event handling!!!");
    return DN_ERROR;
  }
  return DN_OK;
}

/**
 * Initialize anti-entropy.
 * @param[in,out] ctx Context.
 * @return rstatus_t Return status code.
 */
static rstatus_t core_entropy_init(struct context *ctx) {
  struct instance *nci = ctx->instance;
  /* initializing anti-entropy */
  ctx->entropy = entropy_init(ctx, nci->entropy_port, nci->entropy_addr);
  if (ctx->entropy == NULL) {
    log_error("Failed to create entropy!!!");
  }

  return DN_OK;
}

/**
 * Create the Dynomite server performance statistics and assign it to the
 * context, plus initialize anti-entropy.
 * @param[in,out] ctx Context.
 * @return rstatus_t Return status code.
 */
static rstatus_t core_stats_create(struct context *ctx) {
  struct instance *nci = ctx->instance;
  struct server_pool *sp = &ctx->pool;

  ctx->stats = stats_create(sp->stats_endpoint.port, sp->stats_endpoint.pname,
                            sp->stats_interval, nci->hostname, &ctx->pool, ctx);
  if (ctx->stats == NULL) {
    log_error("Failed to create stats!!!");
    return DN_ERROR;
  }

  return DN_OK;
}

/**
 * Initialize crypto and create the Dynomite server performance statistics.
 * @param[in,out] ctx Dynomite server context.
 * @return rstatus_t Return status code.
 */
static rstatus_t core_crypto_init(struct context *ctx) {
  /* crypto init */
  THROW_STATUS(crypto_init(&ctx->pool));
  return DN_OK;
}

/**
 * Initialize the server pool.
 * @param[in,out] ctx Context.
 * @return rstatus_t Return status code.
 */
static rstatus_t core_server_pool_init(struct context *ctx) {
  THROW_STATUS(server_pool_init(&ctx->pool, &ctx->cf->pool, ctx));
  return DN_OK;
}

/**
 * Create a context for the dynomite process.
 * @param[in,out] nci Dynomite instance.
 * @return rstatus_t Return status code.
 */
static rstatus_t core_ctx_create(struct instance *nci) {
  struct context *ctx;

  srand((unsigned)time(NULL));

  ctx = dn_alloc(sizeof(*ctx));
  if (ctx == NULL) {
    loga("Failed to create context!!!");
    return DN_ERROR;
  }

  nci->ctx = ctx;
  ctx->instance = nci;
  ctx->cf = NULL;
  ctx->stats = NULL;
  ctx->evb = NULL;
  ctx->dyn_state = INIT;
  ctx->admin_opt = admin_opt;

  /* parse and create configuration */
  ctx->cf = conf_create(nci->conf_filename);
  if (ctx->cf == NULL) {
    loga("Failed to create conf!!!");
    dn_free(ctx);
    return DN_ERROR;
  }

  struct conf_pool *cp = &ctx->cf->pool;
  ctx->max_timeout = cp->stats_interval;
  ctx->timeout = ctx->max_timeout;

  return DN_OK;
}

static void core_ctx_destroy(struct context *ctx) {
  proxy_deinit(ctx);
  server_pool_disconnect(ctx);
  event_base_destroy(ctx->evb);
  stats_destroy(ctx->stats);
  server_pool_deinit(&ctx->pool);
  conf_destroy(ctx->cf);
  dn_free(ctx);
}

/**
 * Initialize memory buffers, message queue, and connections.
 * @param[in] nci Dynomite instance.
 * @return rstatus_t Return status code.
 */
rstatus_t core_start(struct instance *nci) {
  conn_init();
  task_mgr_init();

  rstatus_t status = core_ctx_create(nci);
  if (status != DN_OK) {
    goto error;
  }

  struct context *ctx = nci->ctx;
  ASSERT(ctx != NULL);

  status = core_server_pool_init(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_crypto_init(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_stats_create(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_entropy_init(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_event_base_create(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_server_pool_preconnect(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_proxy_init(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_dnode_proxy_init(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_dnode_peer_init(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_dnode_peer_pool_preconnect(ctx);
  if (status != DN_OK) {
    goto error;
  }

  status = core_init_last(ctx);
  if (status != DN_OK) {
    goto error;
  }
  // XXX: Gossip is currently not maintained actively, so ignore any failures.
  IGNORE_RET_VAL(core_gossip_pool_init(ctx));

  // Set the repairs flag.
  g_read_repairs_enabled = ctx->cf->pool.read_repairs_enabled;

  /**
   * Providing mbuf_size and alloc_msgs through the command line
   * has been deprecated. For backward compatibility
   * we support both ways here: One through nci (command line)
   * and one through the YAML file (server_pool).
   */
  struct server_pool *sp = &ctx->pool;

  if (sp->mbuf_size == UNSET_NUM) {
    loga("mbuf_size not in YAML: using deprecated way  %d",
         nci->mbuf_chunk_size);
    mbuf_init(nci->mbuf_chunk_size);
  } else {
    loga("YAML provided mbuf_size: %d", sp->mbuf_size);
    mbuf_init(sp->mbuf_size);
  }
  if (sp->alloc_msgs_max == UNSET_NUM) {
    loga("max_msgs not in YAML: using deprecated way %d", nci->alloc_msgs_max);
    msg_init(nci->alloc_msgs_max);
  } else {
    loga("YAML provided max_msgs: %d", sp->alloc_msgs_max);
    msg_init(sp->alloc_msgs_max);
  }

  return DN_OK;

error:
  // If we hit an error, undo everything in the reverse order as it was setup to maintain
  // symmetric setup/teardown semantics.
  if (ctx != NULL) {
    //gossip_pool_deinit(ctx);   // XXX: Gossip not actively maintained.
    dnode_peer_pool_disconnect(ctx);
    dnode_peer_deinit(&ctx->pool.peers);
    dnode_proxy_deinit(ctx);
    proxy_deinit(ctx);
    server_pool_disconnect(ctx);
    if (ctx->evb) event_base_destroy(ctx->evb);
    if (ctx->entropy) entropy_conn_destroy(ctx->entropy);
    if (ctx->stats) stats_destroy(ctx->stats);
    crypto_deinit();
    server_pool_deinit(&ctx->pool);
    if (ctx->cf) conf_destroy(ctx->cf);
    dn_free(ctx);
  }
  conn_deinit();
  return status;
}

char *print_server_pool(const struct object *obj) {
  ASSERT(obj->type == OBJ_POOL);
  struct server_pool *sp = (struct server_pool *)obj;
  snprintf(obj->print_buff, PRINT_BUF_SIZE, "<POOL %p '%.*s'>", sp,
           sp->name.len, sp->name.data);
  return obj->print_buff;
}

/**
 * Deinitialize connections, message queue, memory buffers and destroy the
 * context.
 * @param[in] ctx Dynomite process context.
 */
void core_stop(struct context *ctx) {
  conn_deinit();
  msg_deinit();
  dmsg_deinit();
  mbuf_deinit();
  core_ctx_destroy(ctx);
}

static rstatus_t core_recv(struct context *ctx, struct conn *conn) {
  rstatus_t status;

  status = conn_recv(ctx, conn);
  if (status != DN_OK) {
    log_info("%s recv failed: %s", print_obj(conn), strerror(errno));
  }

  return status;
}

static rstatus_t core_send(struct context *ctx, struct conn *conn) {
  rstatus_t status;

  status = conn_send(ctx, conn);
  if (status != DN_OK) {
    log_info("%s send failed: %s", print_obj(conn), strerror(errno));
  }

  return status;
}

static void core_close(struct context *ctx, struct conn *conn) {
  rstatus_t status;

  ASSERT(conn->sd > 0);

  log_debug(LOG_NOTICE,
            "close %s on event %04" PRIX32
            " eof %d done "
            "%d rb %zu sb %zu%c %s",
            print_obj(conn), conn->events, conn->eof, conn->done,
            conn->recv_bytes, conn->send_bytes, conn->err ? ':' : ' ',
            conn->err ? strerror(conn->err) : "");

  status = conn_event_del_conn(conn);
  if (status < 0) {
    log_warn("event del conn %d failed, ignored: %s", conn->sd,
             strerror(errno));
  }

  conn_close(ctx, conn);
}

static void core_error(struct context *ctx, struct conn *conn) {
  rstatus_t status;

  status = dn_get_soerror(conn->sd);
  if (status < 0) {
    log_warn("get soerr on %s failed, ignored: %s", print_obj(conn),
             strerror(errno));
  }
  conn->err = errno;

  core_close(ctx, conn);
}

static void core_timeout(struct context *ctx) {
  for (;;) {
    struct msg *req;
    struct conn *conn;
    msec_t now, then;

    req = msg_tmo_min();
    if (req == NULL) {
      ctx->timeout = ctx->max_timeout;
      return;
    }

    /* skip over req that are in-error or done */

    if (req->is_error || req->done) {
      msg_tmo_delete(req);
      continue;
    }

    /*
     * timeout expired req and all the outstanding req on the timing
     * out server
     */

    conn = req->tmo_rbe.data;
    then = req->tmo_rbe.key;

    now = dn_msec_now();
    if (now < then) {
      msec_t delta = (msec_t)(then - now);
      ctx->timeout = MIN(delta, ctx->max_timeout);
      return;
    }

    log_warn("%s on %s timedout, timeout was %d", print_obj(req),
             print_obj(conn), req->tmo_rbe.timeout);

    msg_tmo_delete(req);

    if (conn->dyn_mode) {
      if (conn->type == CONN_DNODE_PEER_SERVER) {  // outgoing peer requests
        if (conn->same_dc)
          stats_pool_incr(ctx, peer_timedout_requests);
        else
          stats_pool_incr(ctx, remote_peer_timedout_requests);
      }
    } else {
      if (conn->type == CONN_SERVER) {  // storage server requests
        stats_server_incr(ctx, server_dropped_requests);
      }
    }

    conn->err = ETIMEDOUT;

    core_close(ctx, conn);
  }
}

rstatus_t core_core(void *arg, uint32_t events) {
  rstatus_t status;
  struct conn *conn = arg;
  struct context *ctx = conn_to_ctx(conn);

  log_debug(LOG_VVERB, "event %04" PRIX32 " on %s", events, print_obj(conn));

  conn->events = events;

  /* error takes precedence over read | write */
  if (events & EVENT_ERR) {
    if (conn->err && conn->dyn_mode) {
      loga("conn err on dnode EVENT_ERR: %d", conn->err);
    }
    core_error(ctx, conn);

    return DN_ERROR;
  }

  /* read takes precedence over write */
  if (events & EVENT_READ) {
    status = core_recv(ctx, conn);

    if (status != DN_OK || conn->done || conn->err) {
      if (conn->dyn_mode) {
        if (conn->err) {
          loga("conn err on dnode EVENT_READ: %d", conn->err);
          core_close(ctx, conn);
          return DN_ERROR;
        }
        core_close(ctx, conn);
        return DN_OK;
      }

      core_close(ctx, conn);
      return DN_ERROR;
    }
  }

  if (events & EVENT_WRITE) {
    status = core_send(ctx, conn);
    if (status != DN_OK || conn->done || conn->err) {
      if (conn->dyn_mode) {
        if (conn->err) {
          loga("conn err on dnode EVENT_WRITE: %d", conn->err);
          core_close(ctx, conn);
          return DN_ERROR;
        }
        return DN_OK;
      }

      core_close(ctx, conn);
      return DN_ERROR;
    }
  }

  return DN_OK;
}

void core_debug(struct context *ctx) {
  log_debug(LOG_VERB, "=====================Peers info=====================");
  struct server_pool *sp = &ctx->pool;
  log_debug(LOG_VERB, "Server pool          : '%.*s'", sp->name);
  uint32_t j, n;
  for (j = 0, n = array_n(&sp->peers); j < n; j++) {
    log_debug(LOG_VERB, "==============================================");
    struct node *peer = *(struct node **)array_get(&sp->peers, j);
    log_debug(LOG_VERB, "\tPeer DC            : '%.*s'", peer->dc);
    log_debug(LOG_VERB, "\tPeer Rack          : '%.*s'", peer->rack);

    log_debug(LOG_VERB, "\tPeer name          : '%.*s'", peer->name);
    log_debug(LOG_VERB, "\tPeer pname         : '%.*s'", peer->endpoint.pname);

    log_debug(LOG_VERB, "\tPeer state         : %s", get_state(peer->state));
    log_debug(LOG_VERB, "\tPeer port          : %" PRIu32 "",
              peer->endpoint.port);
    log_debug(LOG_VERB, "\tPeer is_local      : %" PRIu32 " ", peer->is_local);
    log_debug(LOG_VERB, "\tPeer failure_count : %" PRIu32 " ",
              peer->failure_count);
    log_debug(LOG_VERB, "\tPeer num tokens    : %d", array_n(&peer->tokens));

    uint32_t k;
    for (k = 0; k < array_n(&peer->tokens); k++) {
      struct dyn_token *token = (struct dyn_token *)array_get(&peer->tokens, k);
      print_dyn_token(token, 12);
    }
  }

  log_debug(LOG_VERB,
            "Peers Datacenters/racks/nodes "
            ".................................................");
  uint32_t dc_index, dc_len;
  for (dc_index = 0, dc_len = array_n(&sp->datacenters); dc_index < dc_len;
       dc_index++) {
    struct datacenter *dc = array_get(&sp->datacenters, dc_index);
    log_debug(LOG_VERB, "Peer datacenter........'%.*s'", dc->name->len,
              dc->name->data);
    uint32_t rack_index, rack_len;
    for (rack_index = 0, rack_len = array_n(&dc->racks); rack_index < rack_len;
         rack_index++) {
      struct rack *rack = array_get(&dc->racks, rack_index);
      log_debug(LOG_VERB, "\tPeer rack........'%.*s'", rack->name->len,
                rack->name->data);
      log_debug(LOG_VERB, "\tPeer rack ncontinuumm    : %d", rack->ncontinuum);
      log_debug(LOG_VERB, "\tPeer rack nserver_continuum    : %d",
                rack->nserver_continuum);
    }
  }
  log_debug(LOG_VERB,
            ".................................................................."
            ".............");
}

/**
 * Process elements in the circular buffer.
 * @return rstatus_t Return status code.
 */
static rstatus_t core_process_messages(void) {
  log_debug(LOG_VVVERB, "length of C2G_OutQ : %d", CBUF_Len(C2G_OutQ));

  // Continue to process messages while the circular buffer is not empty
  while (!CBUF_IsEmpty(C2G_OutQ)) {
    // Get an element from the beginning of the circular buffer
    struct ring_msg *ring_msg = (struct ring_msg *)CBUF_Pop(C2G_OutQ);
    if (ring_msg != NULL && ring_msg->cb != NULL) {
      // CBUF_Push
      // ./src/dyn_dnode_msg.c
      // ./src/dyn_gossip.c
      ring_msg->cb(ring_msg);
      core_debug(ring_msg->sp->ctx);
      ring_msg_deinit(ring_msg);
    }
  }

  return DN_OK;
}

/**
 * Primary loop for the Dynomite server process.
 * @param[in] ctx Dynomite process context.
 * @return rstatus_t Return status code.
 */
rstatus_t core_loop(struct context *ctx) {
  int nsd;

  core_process_messages();

  core_timeout(ctx);
  execute_expired_tasks(0);
  ctx->timeout = MIN(ctx->timeout, time_to_next_task());
  nsd = event_wait(ctx->evb, (int)ctx->timeout);
  if (nsd < 0) {
    return nsd;
  }

  // go through all the ready queue and send each of them
  /*struct server_pool *sp = &ctx->pool;
  struct conn *conn, *nconn;
  TAILQ_FOREACH_SAFE(conn, &sp->ready_conn_q, ready_tqe, nconn) {
              rstatus_t status = core_send(ctx, conn);
      if (status == DN_OK) {
          log_debug(LOG_VVERB, "Flushing writes on %s", print_obj(conn));
          conn_event_del_out(conn);
      } else {
          TAILQ_REMOVE(&sp->ready_conn_q, conn, ready_tqe);
      }
  }*/
  stats_swap(ctx->stats);

  return DN_OK;
}

// TODO: Does this belong here?
bool is_read_repairs_enabled() {
  return g_read_repairs_enabled &&
        (g_read_consistency > DC_ONE) &&
        (g_write_consistency > DC_ONE);
}
