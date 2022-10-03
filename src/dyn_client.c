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

/**
 * This is the client connection. It receives requests from the client, and
 * forwards it to the corresponding peers and local data store server (if this
 * node owns the token).
 * There is fair amount of machinery involved here mainly for consistency
 * feature It acts more of a co-ordinator than a mere client connection handler.
 * - outstanding_msgs_dict : This is a hash table (HT) of request id to request
 *   mapping. When it receives a request, it adds the message to the HT, and
 *   removes it when it finished responding. We need a hash table mainly for
 *   implementing consistency. When a response is received from a peer, it is
 *   handed over to the client connection. It uses this HT to get the request &
 *   calls the request's response handler.
 * - waiting_to_unref: Now that we distribute messages to multiple nodes and
 * that we have consistency, there is a need for the responses to refer back to
 * the original requests. This makes cleaning up and connection tear down
 * fairly complex. The client connection has to wait for all responses (either
 * a good response or a error response due to timeout). Hence the client
 * connection should wait for the above HT outstanding_msgs_dict to get empty.
 * This flag waiting_to_unref indicates that the client connection is ready to
 * close and just waiting for the outstanding messages to finish.
 */

#include "dyn_client.h"
#include "dyn_core.h"
#include "dyn_dict_msg_id.h"
#include "dyn_dnode_peer.h"
#include "dyn_server.h"
#include "dyn_util.h"

static rstatus_t msg_quorum_rsp_handler(struct context *ctx, struct msg *req, struct msg *rsp);
static rstatus_t msg_each_quorum_rsp_handler(struct context *ctx, struct msg *req,
    struct msg *rsp);
static msg_response_handler_t msg_get_rsp_handler(struct context *ctx, struct msg *req);

static rstatus_t rewrite_query_if_necessary(struct msg **req,
                                            struct context *ctx);
static rstatus_t fragment_query_if_necessary(struct msg *req, struct conn *conn,
                                             struct msg_tqh *frag_msgq);

static void client_ref(struct conn *conn, void *owner) {
  struct server_pool *pool = owner;

  ASSERT(conn->type == CONN_CLIENT);
  ASSERT(conn->owner == NULL);

  /*
   * We use null pointer as the sockaddr argument in the accept() call as
   * we are not interested in the address of the peer for the accepted
   * connection
   */
  conn->family = 0;
  conn->addrlen = 0;
  conn->addr = NULL;

  TAILQ_INSERT_TAIL(&pool->c_conn_q, conn, conn_tqe);

  /* owner of the client connection is the server pool */
  conn->owner = owner;
  conn->outstanding_msgs_dict = dictCreate(&msg_table_dict_type, NULL);
  conn->waiting_to_unref = 0;

  log_debug(LOG_VVERB, "%s ref owner %p into pool '%.*s'", print_obj(conn),
            pool, pool->name.len, pool->name.data);
}

static void client_unref_internal_try_put(struct conn *conn) {
  ASSERT(conn->waiting_to_unref);
  unsigned long msgs = dictSize(conn->outstanding_msgs_dict);
  if (msgs != 0) {
    log_warn("%s Waiting for %lu outstanding messages", print_obj(conn), msgs);
    return;
  }
  ASSERT(conn->owner != NULL);
  conn_event_del_conn(conn);
  log_warn("%s unref owner %s", print_obj(conn), print_obj(conn->owner));
  conn->owner = NULL;
  dictRelease(conn->outstanding_msgs_dict);
  conn->outstanding_msgs_dict = NULL;
  conn->waiting_to_unref = 0;
  conn_put(conn);
}

static void client_unref_and_try_put(struct conn *conn) {
  ASSERT(conn->type == CONN_CLIENT);

  struct server_pool *pool;
  pool = conn->owner;
  ASSERT(conn->owner != NULL);
  ASSERT(TAILQ_COUNT(&pool->c_conn_q) != 0);
  TAILQ_REMOVE(&pool->c_conn_q, conn, conn_tqe);
  conn->waiting_to_unref = 1;
  client_unref_internal_try_put(conn);
}

static void client_unref(struct conn *conn) { client_unref_and_try_put(conn); }

static bool client_active(struct conn *conn) {
  ASSERT(conn->type == CONN_CLIENT);

  ASSERT(TAILQ_EMPTY(&conn->imsg_q));

  if (!TAILQ_EMPTY(&conn->omsg_q)) {
    log_debug(LOG_VVERB, "%s is active", print_obj(conn));
    return true;
  }

  if (conn->rmsg != NULL) {
    log_debug(LOG_VVERB, "%s is active", print_obj(conn));
    return true;
  }

  if (conn->smsg != NULL) {
    log_debug(LOG_VVERB, "%s is active", print_obj(conn));
    return true;
  }

  log_debug(LOG_VVERB, "%s is inactive", print_obj(conn));

  return false;
}

static void client_close_stats(struct context *ctx, struct server_pool *pool,
                               err_t err, unsigned eof) {
  stats_pool_decr(ctx, client_connections);

  if (eof) {
    stats_pool_incr(ctx, client_eof);
    return;
  }

  switch (err) {
    case EPIPE:
    case ETIMEDOUT:
    case ECONNRESET:
    case ECONNABORTED:
    case ENOTCONN:
    case ENETDOWN:
    case ENETUNREACH:
    case EHOSTDOWN:
    case EHOSTUNREACH:
    default:
      stats_pool_incr(ctx, client_err);
      break;
  }
}

static void client_close(struct context *ctx, struct conn *conn) {
  rstatus_t status;
  struct msg *req, *nreq; /* current and next message */

  ASSERT(conn->type == CONN_CLIENT);

  client_close_stats(ctx, conn->owner, conn->err, conn->eof);

  if (conn->sd < 0) {
    client_unref(conn);
    return;
  }

  req = conn->rmsg;
  if (req != NULL) {
    conn->rmsg = NULL;

    ASSERT(req->selected_rsp == NULL);
    ASSERT(req->is_request && !req->done);

    log_info("%s close, discarding pending %s len %" PRIu32, print_obj(conn),
             print_obj(req), req->mlen);

    req_put(req);
  }

  ASSERT(conn->smsg == NULL);
  ASSERT(TAILQ_EMPTY(&conn->imsg_q));

  for (req = TAILQ_FIRST(&conn->omsg_q); req != NULL; req = nreq) {
    nreq = TAILQ_NEXT(req, c_tqe);

    /* dequeue the message (request) from client outq */
    conn_dequeue_outq(ctx, conn, req);

    if (req->done || req->selected_rsp) {
      log_info("%s close, discarding %s %s len %" PRIu32, print_obj(conn),
               req->is_error ? "error" : "completed", print_obj(req),
               req->mlen);
      req_put(req);
    } else {
      req->swallow = 1;

      ASSERT(req->is_request);
      ASSERT(req->selected_rsp == NULL);

      log_info("%s close, schedule swallow of %s len %" PRIu32, print_obj(conn),
               print_obj(req), req->mlen);
    }

    stats_pool_incr(ctx, client_dropped_requests);
  }
  ASSERT(TAILQ_EMPTY(&conn->omsg_q));

  status = close(conn->sd);
  if (status < 0) {
    log_error("close %s failed, ignored: %s", print_obj(conn), strerror(errno));
  }
  conn->sd = -1;
  client_unref(conn);
}

/* Handle a response to a given request. if this is a quorum setting, choose the
 * right response. Then make sure all the requests are satisfied in a fragmented
 * request scenario and then use the post coalesce logic to cook up a combined
 * response
 */
static rstatus_t client_handle_response(struct context *ctx, struct conn *conn, msgid_t reqid,
                                        struct msg *rsp) {


  // now the handler owns the response.
  ASSERT(conn->type == CONN_CLIENT);
  // Fetch the original request
  struct msg *req = dictFetchValue(conn->outstanding_msgs_dict, &reqid);
  if (!req) {
    log_notice("looks like we already cleanedup the request for %d", reqid);
    rsp_put(rsp);
    return DN_OK;
  }
  // we have to submit the response irrespective of the unref status.
  rstatus_t status = msg_handle_response(ctx, req, rsp);
  if (conn->waiting_to_unref) {
    // don't care about the status.
    if (req->awaiting_rsps) return DN_OK;
    // all responses received
    dictDelete(conn->outstanding_msgs_dict, &reqid);
    log_info("%s Putting %s", print_obj(conn), print_obj(req));
    req_put(req);
    client_unref_internal_try_put(conn);
    return DN_OK;
  }
  if (status == DN_NOOPS) {
    // by now the response is dropped
    if (!req->awaiting_rsps) {
      // if we have sent the response for this request or the connection
      // is closed and we are just waiting to drain off the messages.
      if (req->rsp_sent) {
        dictDelete(conn->outstanding_msgs_dict, &reqid);
        log_info("%s Putting %s", print_obj(conn), print_obj(req));
        req_put(req);
      }
    }
  } else if (status == DN_OK) {
    g_pre_coalesce(req->selected_rsp);
    if (req_done(conn, req)) {
      status = conn_event_add_out(conn);
      if (status != DN_OK) {
        conn->err = errno;
      }
    }
  }
  return status;
}

struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc) {
  struct msg *req;

  ASSERT((conn->type == CONN_DNODE_PEER_CLIENT) || (conn->type = CONN_CLIENT));

  if (conn->eof) {
    req = conn->rmsg;

    // if (conn->dyn_mode) {
    //    if (conn->non_bytes_recv > MAX_CONN_ALLOWABLE_NON_RECV) {
    //        conn->err = EPIPE;
    //        return NULL;
    //    }
    //    conn->eof = 0;
    //    return req;
    //}

    /* client sent eof before sending the entire request */
    if (req != NULL) {
      conn->rmsg = NULL;

      ASSERT(req->selected_rsp == NULL);
      ASSERT(req->is_request && !req->done);

      log_error("%s EOF discarding incomplete %s %" PRIu32 "", print_obj(conn),
                print_obj(req), req->mlen);

      req_put(req);
    }

    /*
     * TCP half-close enables the client to terminate its half of the
     * connection (i.e. the client no longer sends data), but it still
     * is able to receive data from the proxy. The proxy closes its
     * half (by sending the second FIN) when the client has no
     * outstanding requests
     */
    if (!conn_active(conn)) {
      conn->done = 1;
      log_debug(LOG_INFO, "%s DONE", print_obj(conn));
    }

    return NULL;
  }

  req = conn->rmsg;
  if (req != NULL) {
    ASSERT(req->is_request);
    return req;
  }

  if (!alloc) {
    return NULL;
  }

  req = req_get(conn);
  if (req != NULL) {
    conn->rmsg = req;
  }

  // Record timetamps if repairs are enabled.
  if (is_read_repairs_enabled()) {
    req->timestamp = current_timestamp_in_millis();
  }

  return req;
}

static bool req_filter(struct context *ctx, struct conn *conn,
                       struct msg *req) {
  ASSERT(conn->type == CONN_CLIENT);

  if (msg_empty(req)) {
    ASSERT(conn->rmsg == NULL);
    log_debug(LOG_VERB, "%s filter empty %s", print_obj(conn), print_obj(req));
    req_put(req);
    return true;
  }

  /*
   * Handle "quit\r\n", which is the protocol way of doing a
   * passive close
   */
  if (req->quit) {
    ASSERT(conn->rmsg == NULL);
    log_debug(LOG_VERB, "%s filter quit %s", print_obj(conn), print_obj(req));

    // The client expects to receive an "+OK\r\n" response, so make sure
    // to do that.
    IGNORE_RET_VAL(simulate_ok_rsp(ctx, conn, req));

    conn->eof = 1;
    conn->recv_ready = 0;
    return true;
  }

  // If this is a Dynomite configuration message, don't forward it.
  if (is_msg_type_dyno_config(req->type)) {
    return true;
  }

  return false;
}

/*
static void
send_rsp_integer(struct context *ctx, struct conn *c_conn, struct msg *req)
{
    //do nothing
    struct msg *rsp = msg_get_rsp_integer(c_conn);
    req->selected_rsp = rsp;
    rsp->peer = req;
    req->selected_rsp = rsp;

    req->done = 1;
    //req->pre_coalesce(req);
    rstatus_t status = conn_event_add_out(c_conn);
    IGNORE_RET_VAL(status);
}
*/

/* Expects req to be in the conn's outq already */
void req_forward_error(struct context *ctx, struct conn *conn, struct msg *req,
                       err_t error_code, err_t dyn_error_code) {
  log_info("%s FORWARD FAILED %s len %" PRIu32 ": %d:%s", print_obj(conn),
           print_obj(req), req->mlen, error_code, dn_strerror(error_code));

  // Nothing to do if request is not expecting a reply.
  // The higher layer will take care of freeing the request
  if (!req->expect_datastore_reply) {
    return;
  }

  // Create an appropriate response for the request so its propagated up;
  // This response gets dropped in rsp_make_error anyways. But since this is
  // an error path its ok with the overhead.
  struct msg *rsp = msg_get_error(conn, dyn_error_code, error_code);
  rsp->peer = req;
  // TODO: Check if this is required in response
  rsp->dmsg = dmsg_get();
  rsp->dmsg->id = req->id;

  rstatus_t status = conn_handle_response(ctx,
      conn, req->parent_id ? req->parent_id : req->id, rsp);
  IGNORE_RET_VAL(status);
}

static void req_redis_stats(struct context *ctx, struct msg *req) {
  switch (req->type) {
    case MSG_REQ_REDIS_GET:
      stats_server_incr(ctx, redis_req_get);
      break;
    case MSG_REQ_REDIS_SET:
      stats_server_incr(ctx, redis_req_set);
      break;
    case MSG_REQ_REDIS_DEL:
      stats_server_incr(ctx, redis_req_del);
      break;
    case MSG_REQ_REDIS_INCR:
    case MSG_REQ_REDIS_DECR:
      stats_server_incr(ctx, redis_req_incr_decr);
      break;
    case MSG_REQ_REDIS_KEYS:
      stats_server_incr(ctx, redis_req_keys);
      break;
    case MSG_REQ_REDIS_MGET:
      stats_server_incr(ctx, redis_req_mget);
      break;
    case MSG_REQ_REDIS_SCAN:
      stats_server_incr(ctx, redis_req_scan);
      break;
    case MSG_REQ_REDIS_SORT:
      stats_server_incr(ctx, redis_req_sort);
      break;
    case MSG_REQ_REDIS_PING:
      stats_server_incr(ctx, redis_req_ping);
      break;
    case MSG_REQ_REDIS_LREM:
      stats_server_incr(ctx, redis_req_lreqm);
      /* do not break as this is a list operation as the following.
       * We count twice the LREM because it is an intensive operation/
       *  */
    case MSG_REQ_REDIS_LRANGE:
    case MSG_REQ_REDIS_LSET:
    case MSG_REQ_REDIS_LTRIM:
    case MSG_REQ_REDIS_LINDEX:
    case MSG_REQ_REDIS_LPUSHX:
      stats_server_incr(ctx, redis_req_lists);
      break;
    case MSG_REQ_REDIS_SUNION:
      stats_server_incr(ctx, redis_req_sunion);
      /* do not break as this is a set operation as the following.
       * We count twice the SUNION because it is an intensive operation/
       *  */
    case MSG_REQ_REDIS_SETBIT:
    case MSG_REQ_REDIS_SETEX:
    case MSG_REQ_REDIS_SETRANGE:
    case MSG_REQ_REDIS_SADD:
    case MSG_REQ_REDIS_SDIFF:
    case MSG_REQ_REDIS_SDIFFSTORE:
    case MSG_REQ_REDIS_SINTER:
    case MSG_REQ_REDIS_SINTERSTORE:
    case MSG_REQ_REDIS_SREM:
    case MSG_REQ_REDIS_SUNIONSTORE:
    case MSG_REQ_REDIS_SSCAN:
      stats_server_incr(ctx, redis_req_set);
      break;
    case MSG_REQ_REDIS_ZADD:
    case MSG_REQ_REDIS_ZINTERSTORE:
    case MSG_REQ_REDIS_ZRANGE:
    case MSG_REQ_REDIS_ZRANGEBYSCORE:
    case MSG_REQ_REDIS_ZREM:
    case MSG_REQ_REDIS_ZREVRANGE:
    case MSG_REQ_REDIS_ZREVRANGEBYSCORE:
    case MSG_REQ_REDIS_ZUNIONSTORE:
    case MSG_REQ_REDIS_ZSCAN:
    case MSG_REQ_REDIS_ZCOUNT:
    case MSG_REQ_REDIS_ZINCRBY:
    case MSG_REQ_REDIS_ZLEXCOUNT:
    case MSG_REQ_REDIS_ZRANGEBYLEX:
    case MSG_REQ_REDIS_ZREMRANGEBYLEX:
    case MSG_REQ_REDIS_ZREMRANGEBYRANK:
    case MSG_REQ_REDIS_ZREMRANGEBYSCORE:
    case MSG_REQ_REDIS_ZREVRANGEBYLEX:
      stats_server_incr(ctx, redis_req_sortedsets);
      break;
    case MSG_REQ_REDIS_HINCRBY:
    case MSG_REQ_REDIS_HINCRBYFLOAT:
    case MSG_REQ_REDIS_HSET:
    case MSG_REQ_REDIS_HSETNX:
      stats_server_incr(ctx, redis_req_hashes);
      break;
    default:
      stats_server_incr(ctx, redis_req_other);
      break;
  }
}

static void req_forward_stats(struct context *ctx, struct msg *req) {
  ASSERT(req->is_request);

  if (req->is_read) {
    stats_server_incr(ctx, read_requests);
    stats_server_incr_by(ctx, read_request_bytes, req->mlen);
  } else {
    stats_server_incr(ctx, write_requests);
    stats_server_incr_by(ctx, write_request_bytes, req->mlen);
  }
}

rstatus_t req_forward_local_datastore(struct context *ctx, struct conn *c_conn,
                            struct msg *req, uint8_t *key, uint32_t keylen,
                            dyn_error_t *dyn_error_code) {
  rstatus_t status;
  struct conn *s_conn;

  ASSERT((c_conn->type == CONN_CLIENT) ||
         (c_conn->type == CONN_DNODE_PEER_CLIENT));

  s_conn = get_datastore_conn(ctx, c_conn->owner, c_conn->sd);
  log_debug(LOG_VERB, "c_conn %p got server conn %p", c_conn, s_conn);
  if (s_conn == NULL) {
    *dyn_error_code = STORAGE_CONNECTION_REFUSE;
    return errno;
  }
  ASSERT(s_conn->type == CONN_SERVER);

  log_info("%s FORWARD %s to storage conn %s", print_obj(c_conn),
           print_obj(req), print_obj(s_conn));

  if (ctx->dyn_state == NORMAL) {
    /* enqueue the message (request) into server inq */
    if (TAILQ_EMPTY(&s_conn->imsg_q)) {
      status = conn_event_add_out(s_conn);

      if (status != DN_OK) {
        *dyn_error_code = DYNOMITE_UNKNOWN_ERROR;
        s_conn->err = errno;
        return DN_ERROR;
      }
    }
  } else if (ctx->dyn_state == STANDBY) {  // no reads/writes from peers/clients
    log_debug(LOG_INFO, "Node is in STANDBY state. Drop write/read requests");
    *dyn_error_code = DYNOMITE_INVALID_STATE;
    return DN_ERROR;
  } else if (ctx->dyn_state == WRITES_ONLY && req->is_read) {
    // no reads from peers/clients but allow writes from peers/clients
    log_debug(LOG_INFO, "Node is in WRITES_ONLY state. Drop read requests");
    *dyn_error_code = DYNOMITE_INVALID_STATE;
    return DN_ERROR;
  } else if (ctx->dyn_state == RESUMING) {
    log_debug(LOG_INFO,
              "Node is in RESUMING state. Still drop read requests and flush "
              "out all the queued writes");
    if (req->is_read) {
      *dyn_error_code = DYNOMITE_INVALID_STATE;
      return DN_ERROR;
    }

    status = conn_event_add_out(s_conn);

    if (status != DN_OK) {
      *dyn_error_code = DYNOMITE_UNKNOWN_ERROR;
      s_conn->err = errno;
      return DN_ERROR;
    }
  }

  conn_enqueue_inq(ctx, s_conn, req);
  req_forward_stats(ctx, req);
  if (g_data_store == DATA_REDIS) {
    req_redis_stats(ctx, req);
  }

  log_debug(LOG_VERB, "%s local forward %s to %s len %" PRIu32 " key '%.*s'",
            print_obj(c_conn), print_obj(req), print_obj(s_conn), req->mlen,
            keylen, key);
  *dyn_error_code = 0;
  return DN_OK;
}

static rstatus_t admin_req_forward_local_datastore(struct context *ctx,
                                         struct conn *c_conn, struct msg *req,
                                         struct rack *rack, uint8_t *key,
                                         uint32_t keylen,
                                         dyn_error_t *dyn_error_code) {
  ASSERT((c_conn->type == CONN_CLIENT) ||
         (c_conn->type == CONN_DNODE_PEER_CLIENT));

  struct node *peer = dnode_peer_pool_server(ctx, c_conn->owner, rack, key,
                                             keylen, req->msg_routing);
  if (!peer->is_local) {
    *dyn_error_code = DYNOMITE_INVALID_ADMIN_REQ;
    return DN_ERROR;
  }

  log_debug(LOG_NOTICE, "%s Need to delete [%.*s] ", print_obj(c_conn), keylen,
            key);
  return req_forward_local_datastore(ctx, c_conn, req, key, keylen, dyn_error_code);
}

rstatus_t req_forward_to_peer(struct context *ctx, struct conn *c_conn,
    struct msg *req, struct node *peer, uint8_t* key, uint32_t keylen,
    struct mbuf *orig_mbuf, bool force_copy, bool force_swallow,
    dyn_error_t *dyn_error_code) {

  rstatus_t status;

  bool same_dc = false;
  bool same_rack = false;
  struct msg *rack_msg = NULL;

  if (peer->is_local) {
    // Since the peer is the same as this node, it's the same DC and rack.
    same_dc = same_rack = true;
    // If the peer is pointing to the current node itself, forward it directly to the
    // datastore.
    status = req_forward_local_datastore(ctx, c_conn, req, key, keylen, dyn_error_code);
    if (status != DN_OK) {
      log_error("Failed to forward request to local datastore.");
      goto error;
    }
    return status;
  }

  struct server_pool *pool = c_conn->owner;
  ASSERT(pool != NULL);

  // Figure out if the peer is in the same DC or rack.
  same_dc = peer->is_same_dc;
  same_rack = !(string_compare(&peer->rack, &pool->rack)) && same_dc;
  if (force_copy || !same_rack) {
    // Make a copy of the message if forced or if the peer is not on the same rack or DC.
    rack_msg = msg_get(c_conn, req->is_request, __FUNCTION__);
    if (rack_msg == NULL) {
      log_error("Failed to allocate memory for inter-node message.");
      *dyn_error_code = DYNOMITE_UNKNOWN_ERROR;
      status = DN_ENOMEM;
      goto error;
    }
    msg_clone(req, orig_mbuf, rack_msg);
  } else {
    rack_msg = req;
  }

  if (!(same_dc && same_rack) || force_swallow) {
    // Swallow responses from remote racks or DCs.
    rack_msg->swallow = true;
  }

  // Get a connection to the node.
  struct conn *p_conn = dnode_peer_get_conn(ctx, peer, c_conn->sd);
  if (p_conn == NULL) {
    status = DN_ERROR;
    *dyn_error_code = PEER_HOST_NOT_CONNECTED;
    goto error;
  }

  // Finally, forward the message to a peer DNODE.
  status = dnode_peer_req_forward(
      ctx, c_conn, p_conn, rack_msg, key, keylen, dyn_error_code);
  if (status != DN_OK) {
    log_error("Failure in forwarding a request to a dnode");
    goto error;
  }

  return status;

 error:
  // Forward errors only if we failed to talk to the same DC. We currently ignore cross-DC
  // errors.
  if (same_dc) {
    // We forward the error if the target was in the same rack, or if it was in a remote
    // rack and we're expecting a quorum response.
    if ((!same_rack && req->consistency != DC_ONE) || same_rack) {
      req_forward_error(ctx, c_conn,
          (rack_msg ? rack_msg : req),
          status, *dyn_error_code);
    } else if (!same_rack && req->consistency == DC_ONE) {
      // We won't receive a response from one host, so account for that
      // (even though it's effectively a no-op as we wait only for one response in
      // DC_ONE).
      req->rspmgr.max_responses--;
      log_error("Swallowing cross rack error due to DC_ONE. Error: %d '%s'",
          status, dyn_error_source(*dyn_error_code));
    }
  }
  // Release the copy if we made one above..
  if (rack_msg != NULL && (force_copy || !same_rack)) {
    req_put(rack_msg);
  }
  return status;
}

void req_forward_all_racks_for_dc(struct context *ctx, struct conn *c_conn,
                                 struct msg *req, struct mbuf *orig_mbuf,
                                 uint8_t *key, uint32_t keylen,
                                 struct datacenter *dc) {
  uint8_t rack_cnt = (uint8_t)array_n(&dc->racks);
  uint8_t rack_index;

  if (req->rspmgrs_inited == false) {
    if (req->consistency == DC_EACH_SAFE_QUORUM) {
      init_response_mgr_all_dcs(ctx, req, c_conn, dc);
    } else {
      init_response_mgr(req, &req->rspmgr, rack_cnt, c_conn);
    }
  }
  log_info("%s %s same DC racks:%d expect replies %d", print_obj(c_conn),
           print_obj(req), rack_cnt, req->rspmgr.max_responses);

  for (rack_index = 0; rack_index < rack_cnt; ++rack_index) {
    struct rack *rack = array_get(&dc->racks, rack_index);
    // Pick the token owner peer from the selected rack.
    struct node *peer = dnode_peer_pool_server(ctx, c_conn->owner, rack, key,
                                               keylen, req->msg_routing);
    dyn_error_t dyn_error_code = DYNOMITE_OK;

    // Forward the message to the peer.
    rstatus_t status = req_forward_to_peer(ctx, c_conn, req, peer, key, keylen,
        orig_mbuf, false /* force_copy? */, false /* force swallow? */,
        &dyn_error_code);

    // We ignore the return value since the callee will take care of forwarding errors.
    IGNORE_RET_VAL(status);
  }
}

static bool request_send_to_all_dcs(struct msg *req) {
  // There is a routing override
  if (req->msg_routing != ROUTING_NORMAL) return false;

  // Under DC_EACH_SAFE_QUORUM, we need to send reads and writes to all
  // DCs.
  if (req->consistency == DC_EACH_SAFE_QUORUM) return true;

  // Reads are not propagated
  if (req->is_read) return false;

  return true;
}

/**
 * Determine if a request should be forwarded to all replicas within the local
 * DC.
 * @param[in] req Message.
 * @return bool True if message should be forwarded to all local replicas, else
 * false
 */
static bool request_send_to_all_local_racks(struct msg *req) {
  /* There is a routing override set by the parser on this message. Do not
   * propagate it to other racks irrespective of the consistency setting */
  if (req->msg_routing != ROUTING_NORMAL) return false;

  // A write should go to all racks
  if (!req->is_read) return true;

  if ((req->consistency == DC_QUORUM)
    || (req->consistency == DC_SAFE_QUORUM)
    || (req->consistency == DC_EACH_SAFE_QUORUM)) {
    return true;
  }
  return false;
}

/**
 * Forward 'req' to all DCs, racks and nodes.
 *
 */
static rstatus_t req_forward_all_dcs_all_racks_all_nodes(struct context *ctx,
    struct conn *c_conn, struct msg *req, struct mbuf *orig_mbuf, uint8_t *key,
    uint32_t keylen, dyn_error_t *dyn_error_code) {
  rstatus_t status = DN_OK;
  struct server_pool *pool = c_conn->owner;
  uint32_t peer_cnt = array_n(&pool->peers);

  req->rsp_handler = msg_get_rsp_handler(ctx, req);

  // Ennumerate every node (or 'peer') in the cluster and send 'req' to each of them.
  uint32_t peer_idx = 0;
  for (peer_idx = 0; peer_idx < peer_cnt; ++peer_idx) {
    struct node *peer = *(struct node **)array_get(&pool->peers, peer_idx);

    // Force a swallow for all the peers since we don't care about matching the return
    // values for each one.
    status = req_forward_to_peer(ctx, c_conn, req, peer, key, keylen,
        orig_mbuf, true /* force_copy */, true /* force swallow */, dyn_error_code);
    // We ignore the return value since the callee will take care of forwarding errors.
    IGNORE_RET_VAL(status);
  }

  return status;
}

static void req_forward_remote_dc(struct context *ctx, struct conn *c_conn,
                                  struct msg *req, struct mbuf *orig_mbuf,
                                  uint8_t *key, uint32_t keylen,
                                  struct datacenter *dc) {
  const uint32_t rack_cnt = array_n(&dc->racks);
  if (rack_cnt == 0) return;

  if (req->consistency == DC_EACH_SAFE_QUORUM) {
    // Under 'DC_EACH_SAFE_QUORUM', we want to hear back from at least
    // quorum racks in each DC, so send it to all racks in remote DCs.
    req_forward_all_racks_for_dc(ctx, c_conn, req, orig_mbuf, key, keylen, dc);
    return;
  }

  // If we're not expecting a consistency level of 'DC_EACH_SAFE_QUORUM', then
  // we send it to only to the preselected rack in the remote DCs. If that's not
  // reachable, we failover to another in the remote DC.

  // Pick the preferred pre-selected rack for this DC.
  struct rack *rack = dc->preselected_rack_for_replication;
  if (rack == NULL) rack = array_get(&dc->racks, 0);

  log_info("%s %s Forwarding to remote DC; rack name:%.*s", print_obj(c_conn),
           print_obj(req), rack->name->len, rack->name->data);

  // Pick the token owner peer from the selected rack.
  struct node *peer = dnode_peer_pool_server(ctx, c_conn->owner, rack, key,
                                             keylen, req->msg_routing);

  dyn_error_t dyn_error_code = DYNOMITE_OK;
  // Forward the message to the peer.
  rstatus_t status = req_forward_to_peer(ctx, c_conn, req, peer, key, keylen,
      orig_mbuf, true /* force_copy */, false /* force swallow? */,
      &dyn_error_code);

  // If we succeeded in sending it to the preselected rack in the preferred remote DC,
  // then we return, else we go ahead to try other racks in the remote DC.
  if (status == DN_OK) return;

  // Start over with another rack.
  uint8_t rack_index;
  for (rack_index = 0; rack_index < rack_cnt; rack_index++) {
    rack = array_get(&dc->racks, rack_index);
    peer = dnode_peer_pool_server(ctx, c_conn->owner, rack, key,
                                  keylen, req->msg_routing);

    if (rack == dc->preselected_rack_for_replication) continue;
    log_info("%s FAILOVER forwarding msg %s to remote dc rack '%.*s'",
             print_obj(c_conn), print_obj(req), rack->name->len,
             rack->name->data);

    status = req_forward_to_peer(ctx, c_conn, req, peer, key, keylen,
        orig_mbuf, true /* force_copy */, false /* force swallow */,
        &dyn_error_code);
    if (status == DN_OK) {
      stats_pool_incr(ctx, remote_peer_failover_requests);
      return;
    }
  }
  stats_pool_incr(ctx, remote_peer_dropped_requests);
}

static void req_forward_local_dc(struct context *ctx, struct conn *c_conn,
                                 struct msg *req, struct mbuf *orig_mbuf,
                                 uint8_t *key, uint32_t keylen,
                                 struct datacenter *dc) {
  struct server_pool *pool = c_conn->owner;
  req->rsp_handler = msg_get_rsp_handler(ctx, req);
  if (request_send_to_all_local_racks(req)) {
    // send request to all local racks
    req_forward_all_racks_for_dc(ctx, c_conn, req, orig_mbuf, key, keylen, dc);
  } else {
    // send request to only local token owner
    struct rack *rack =
        server_get_rack_by_dc_rack(pool, &pool->rack, &pool->dc);
    // Pick the token owner peer from the selected rack.
    struct node *peer = dnode_peer_pool_server(ctx, c_conn->owner, rack, key,
                                               keylen, req->msg_routing);

    dyn_error_t dyn_error_code = 0;
    // Forward the message to the peer.
    rstatus_t status = req_forward_to_peer(ctx, c_conn, req, peer, key, keylen,
        orig_mbuf, false /* force_copy? */, false /* force swallow? */,
        &dyn_error_code);
    IGNORE_RET_VAL(status);
  }
}

static void req_forward(struct context *ctx, struct conn *c_conn,
                        struct msg *req) {
  struct server_pool *pool = c_conn->owner;
  dyn_error_t dyn_error_code = DYNOMITE_OK;
  rstatus_t s = DN_OK;

  ASSERT(c_conn->type == CONN_CLIENT);

  if (req->is_read) {
    if (req->type != MSG_REQ_REDIS_PING) stats_pool_incr(ctx, client_read_requests);
  } else {
    stats_pool_incr(ctx, client_write_requests);
  }

  uint32_t keylen = 0;
  uint8_t *key = msg_get_tagged_key(req, 0, &keylen);
  uint32_t full_keylen = 0;
  uint8_t *full_key = msg_get_full_key(req, 0, &full_keylen);

  log_info(
      ">>>>>>>>>>>>>>>>>>>>>>> %s RECEIVED %s key '%.*s' tagged key '%.*s'",
      print_obj(c_conn), print_obj(req), full_keylen, full_key, keylen, key);
  // add the message to the dict
  dictAdd(c_conn->outstanding_msgs_dict, &req->id, req);

  // need to capture the initial mbuf location as once we add in the dynomite
  // headers (as mbufs to the src req), that will bork the request sent to
  // secondary racks
  struct mbuf *orig_mbuf = STAILQ_FIRST(&req->mhdr);

  // Enqueue message 'req' into the client outq, if a response is expected.
  if (req->expect_datastore_reply) {
    conn_enqueue_outq(ctx, c_conn, req);
  }

  // Make sure that this is a valid request according to Dynomite.
  s = g_verify_request(
      req, pool, server_get_rack_by_dc_rack(pool, &pool->rack, &pool->dc));
  if (s != DN_OK) {
    // If this was an invalid request, we forward an error.
    req_forward_error(ctx, c_conn, req, DN_OK, s);
    return;
  }

  if (ctx->admin_opt == 1) {
    if (req->type == MSG_REQ_REDIS_DEL || req->type == MSG_REQ_MC_DELETE) {
      struct rack *rack =
          server_get_rack_by_dc_rack(pool, &pool->rack, &pool->dc);
      s = admin_req_forward_local_datastore(ctx, c_conn, req, rack, key, keylen,
                                  &dyn_error_code);
      if (s != DN_OK) {
        req_forward_error(ctx, c_conn, req, s, dyn_error_code);
      }
      return;
    }
  }

  req->consistency = req->is_read ? conn_get_read_consistency(c_conn)
                                  : conn_get_write_consistency(c_conn);

  if (req->msg_routing == ROUTING_LOCAL_NODE_ONLY) {
    // Strictly local host only
    req->consistency = DC_ONE;
    req->rsp_handler = msg_local_one_rsp_handler;

    s = req_forward_local_datastore(ctx, c_conn, req, key, keylen, &dyn_error_code);
    if (s != DN_OK) {
      req_forward_error(ctx, c_conn, req, s, dyn_error_code);
    }
    return;
  }

  if (req->msg_routing == ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS) {
    // Under this routing mechanism, it doesn't make sense to check for quorum, so we set the
    // consistency to DC_ONE regardless of the configuration.
    req->consistency = DC_ONE;

    // Send 'req' to every node in the cluster.
    s = req_forward_all_dcs_all_racks_all_nodes(ctx, c_conn, req, orig_mbuf, key, keylen,
        &dyn_error_code);
    if (s != DN_OK) {
      log_error("Failure in forwarding a ROUTING_ALL_NODES_ALL_RACKS_ALL_DCS "\
                "request to one or more nodes");
      // Errors would have already been forwarded by callee so we do nothing else here.
    }
    return;
  }

  /* forward the request based on other 'msg_routing' policies. */
  uint32_t dc_cnt = array_n(&pool->datacenters);
  uint32_t dc_index;
  for (dc_index = 0; dc_index < dc_cnt; dc_index++) {
    struct datacenter *dc = array_get(&pool->datacenters, dc_index);
    if (dc == NULL) {
      log_error("FATAL ERROR: Server pool lost track of existing DCs.");
      return;
    }

    if (string_compare(dc->name, &pool->dc) == 0) {
      req_forward_local_dc(ctx, c_conn, req, orig_mbuf, key, keylen, dc);
    } else if (request_send_to_all_dcs(req)) {
      req_forward_remote_dc(ctx, c_conn, req, orig_mbuf, key, keylen, dc);
    }
  }
}

/*
 * Rewrites a query if necessary.
 *
 * If a rewrite occured, it will replace '*req' with the new 'msg' that contains
 * the new query and free up the original msg.
 *
 */
rstatus_t rewrite_query_if_necessary(struct msg **req, struct context *ctx) {
  bool did_rewrite = false;
  struct msg *new_req = NULL;

  msg_type_t orig_msg_type = (*req)->type;
  rstatus_t ret_status = g_rewrite_query(*req, ctx, &did_rewrite, &new_req);
  THROW_STATUS(ret_status);

  if (did_rewrite) {
    // If we successfully did a rewrite, we need to recycle the memory used by
    // the original request and point it to the 'new_req'.
    msg_put(*req);
    *req = new_req;
    (*req)->orig_type = orig_msg_type;
  }
  return DN_OK;
}

/*
 * Rewrites a query as a script that updates both the data and metadata.
 *
 * If a rewrite occured, it will replace '*req' with the new 'msg' that contains
 * the new query and free up the original msg.
 *
 */
static rstatus_t rewrite_query_with_timestamp_md(struct msg **req, struct context *ctx) {

  if (is_read_repairs_enabled() == false) return DN_OK;

  bool did_rewrite = false;
  struct msg *new_req = NULL;

  msg_type_t orig_msg_type = (*req)->type;
  rstatus_t ret_status = g_rewrite_query_with_timestamp_md(
      *req, ctx, &did_rewrite, &new_req);
  THROW_STATUS(ret_status);

  if (did_rewrite) {
    // If we successfully did a rewrite, we neet to make sure that the 'new_req' is the
    // msg considered from here on, and record the original msg for later reference.
    new_req->orig_msg = *req;
    *req = new_req;
    (*req)->orig_type = orig_msg_type;
  }
  return DN_OK;
}

/*
 * Fragments a query if applicable.
 * 'frag_msgq' will be non-empty if the query is fragmented.
 */
rstatus_t fragment_query_if_necessary(struct msg *req, struct conn *conn,
                                      struct msg_tqh *frag_msgq) {
  struct server_pool *pool = conn->owner;
  struct rack *rack = server_get_rack_by_dc_rack(pool, &pool->rack, &pool->dc);
  return g_fragment(req, pool, rack, frag_msgq);
}

void req_recv_done(struct context *ctx, struct conn *conn, struct msg *req,
                   struct msg *nreq) {
  ASSERT(conn->type == CONN_CLIENT);
  ASSERT(req->is_request);
  ASSERT(req->owner == conn);
  ASSERT(conn->rmsg == req);
  ASSERT(nreq == NULL || nreq->is_request);

  if (!req->is_read) stats_histo_add_payloadsize(ctx, req->mlen);

  /* enqueue next message (request), if any */
  conn->rmsg = nreq;

  if (req_filter(ctx, conn, req)) {
    return;
  }

  req->stime_in_microsec = dn_usec_now();
  struct msg_tqh frag_msgq;
  TAILQ_INIT(&frag_msgq);

  rstatus_t status = rewrite_query_if_necessary(&req, ctx);
  if (status != DN_OK) goto error;

  status = fragment_query_if_necessary(req, conn, &frag_msgq);
  if (status == DYNOMITE_PAYLOAD_TOO_LARGE) {
    dictAdd(conn->outstanding_msgs_dict, &req->id, req);
    req->nfrag = 0; // Since we failed to fragment the payload, we don't have any fragments for this request
    goto error;
  }

  if (status != DN_OK) goto error;

  status = rewrite_query_with_timestamp_md(&req, ctx);
  if (status != DN_OK) goto error;

  /* if no fragment happened */
  if (TAILQ_EMPTY(&frag_msgq)) {
    req_forward(ctx, conn, req);
    return;
  }

  status = req_make_reply(ctx, conn, req);
  if (status != DN_OK) goto error;

  struct msg *sub_msg, *tmsg;
  for (sub_msg = TAILQ_FIRST(&frag_msgq); sub_msg != NULL; sub_msg = tmsg) {
    tmsg = TAILQ_NEXT(sub_msg, m_tqe);

    TAILQ_REMOVE(&frag_msgq, sub_msg, m_tqe);
    log_info("Forwarding split request %s", print_obj(sub_msg));
    req_forward(ctx, conn, sub_msg);
  }
  ASSERT(TAILQ_EMPTY(&frag_msgq));
  return;

error:
  if (req->expect_datastore_reply) {
    conn_enqueue_outq(ctx, conn, req);
  }
  req_forward_error(ctx, conn, req, DN_OK, status);  // TODO: CHeck error code
  return;
}

static msg_response_handler_t msg_get_rsp_handler(struct context *ctx, struct msg *req) {
  if (request_send_to_all_local_racks(req)) {
    // Request is being braoadcasted
    // Check if its quorum
    if ((req->consistency == DC_QUORUM) || (req->consistency == DC_SAFE_QUORUM)) {
      return msg_quorum_rsp_handler;
    } else if (req->consistency == DC_EACH_SAFE_QUORUM) {
      return msg_each_quorum_rsp_handler;
    }
  }

  return msg_local_one_rsp_handler;
}

rstatus_t msg_local_one_rsp_handler(struct context *ctx, struct msg *req, struct msg *rsp) {
  ASSERT_LOG(!req->selected_rsp,
             "Received more than one response for dc_one.\
               %s prev %s new rsp %s",
             print_obj(req), print_obj(req->selected_rsp), print_obj(rsp));

  req->awaiting_rsps = 0;
  rsp->peer = req;
  req->is_error = rsp->is_error;
  req->error_code = rsp->error_code;
  req->dyn_error_code = rsp->dyn_error_code;
  req->selected_rsp = rsp;
  log_info("%d SELECTED %d", print_obj(req), print_obj(rsp));
  return DN_OK;
}

static rstatus_t swallow_extra_rsp(struct msg *req, struct msg *rsp) {
  log_info("%s SWALLOW %s awaiting %d", print_obj(req), print_obj(rsp),
           req->awaiting_rsps);
  ASSERT_LOG(req->awaiting_rsps, "%s has no awaiting rsps, received %s",
             print_obj(req), print_obj(rsp));
  // drop this response.
  rsp_put(rsp);
  msg_decr_awaiting_rsps(req);
  return DN_NOOPS;
}

static rstatus_t msg_quorum_rsp_handler(struct context *ctx, struct msg *req,
    struct msg *rsp) {
  if (req->rspmgr.done) {
    rstatus_t swallow_status = swallow_extra_rsp(req, rsp);
    if (is_read_repairs_enabled()) {
      struct msg *cleanup_msg = NULL;
      // Check if we can delete tombstone metadata.
      rstatus_t status = g_clear_repair_md_for_key(ctx, req, &cleanup_msg);
      if (status == DN_OK) {
        req_forward(ctx, req->owner, cleanup_msg);
      }
      return DN_NOOPS;
    }
    return swallow_status;
  }
  rspmgr_submit_response(&req->rspmgr, rsp);
  if (!rspmgr_check_is_done(&req->rspmgr)) return DN_EAGAIN;
  // rsp is absorbed by rspmgr. so we can use that variable
  rsp = rspmgr_get_response(ctx, &req->rspmgr);
  ASSERT(rsp);
  rspmgr_free_other_responses(&req->rspmgr, rsp);
  rsp->peer = req;
  req->selected_rsp = rsp;
  req->error_code = rsp->error_code;
  req->is_error = rsp->is_error;
  req->dyn_error_code = rsp->dyn_error_code;
  return DN_OK;
}

static int find_rspmgr_idx(struct context *ctx, struct response_mgr **rspmgrs,
    struct string *target_dc_name) {
  int num_dcs = (int) array_n(&ctx->pool.datacenters);

  int i = 0;
  for (i = 0; i < num_dcs; ++i) {
    struct response_mgr *rspmgr = rspmgrs[i];
    if (string_compare(&rspmgr->dc_name, target_dc_name) == 0) {
      return i;
    }
  }
  return -1;
}

static bool all_rspmgrs_done(struct context *ctx, struct response_mgr **rspmgrs) {
  int num_dcs = (int) array_n(&ctx->pool.datacenters);
  int i = 0;
  for (i = 0; i < num_dcs; ++i) {
    struct response_mgr *rspmgr = rspmgrs[i];
    if (!rspmgr->done) return false;
  }

  return true;
}

static struct msg *all_rspmgrs_get_response(struct context *ctx, struct msg *req) {
  int num_dcs = (int) array_n(&ctx->pool.datacenters);
  struct msg *rsp = NULL;
  int i;
  for (i = 0; i < num_dcs; ++i) {
    struct response_mgr *rspmgr = req->additional_each_rspmgrs[i];
    struct msg *dc_rsp = NULL;
    if (!rsp) {
      rsp = rspmgr_get_response(ctx, rspmgr);
      ASSERT(rsp);
    } else if (rsp->is_error) {
      // If any of the DCs errored out, we just clean up responses from the
      // remaining DCs.
      rspmgr_free_other_responses(rspmgr, NULL);
      continue;
    } else {
      ASSERT(rsp->is_error == false);

      // If the DCs we've processed so far have not seen errors, we need to
      // make sure that the remaining DCs don't have errors too.
      dc_rsp = rspmgr_get_response(ctx, rspmgr);
      ASSERT(dc_rsp);
      if (dc_rsp->is_error) {
        rsp_put(rsp);
        rsp = dc_rsp;
      } else {
        // If it's not an error, clear all responses from this DC.
        rspmgr_free_other_responses(rspmgr, NULL);
        continue;
      }
    }

    rspmgr_free_other_responses(rspmgr, rsp);
    rsp->peer = req;
    req->selected_rsp = rsp;
    req->error_code = rsp->error_code;
    req->is_error = rsp->is_error;
    req->dyn_error_code = rsp->dyn_error_code;

  }

  return rsp;
}

static rstatus_t msg_each_quorum_rsp_handler(struct context *ctx, struct msg *req,
    struct msg *rsp) {

  if (all_rspmgrs_done(ctx, req->additional_each_rspmgrs)) {
    return swallow_extra_rsp(req, rsp);
  }

  int rspmgr_idx = -1;
  struct conn *rsp_conn = rsp->owner;
  if (rsp_conn == NULL) {
    // TODO: We should remove this case. Test and confirm.
    rspmgr_idx = 0;
  } else if (rsp_conn->type == CONN_DNODE_PEER_SERVER) {
    struct node *peer_instance = (struct node*) rsp_conn->owner;
    struct string *peer_dc_name = &peer_instance->dc;
    rspmgr_idx = find_rspmgr_idx(ctx, req->additional_each_rspmgrs, peer_dc_name);
    if (rspmgr_idx == -1) {
      log_error("Could not find which DC response was from");
    }
  } else if (rsp_conn->type == CONN_SERVER) {
    // If this is a 'CONN_SERVER' connection, then it is from the same DC.
    rspmgr_idx = 0;
  }

  struct response_mgr *rspmgr = req->additional_each_rspmgrs[rspmgr_idx];
  rspmgr_submit_response(rspmgr, rsp);
  if (!rspmgr_check_is_done(rspmgr)) return DN_EAGAIN;
  if (!all_rspmgrs_done(ctx, req->additional_each_rspmgrs)) return DN_EAGAIN;

  rsp = all_rspmgrs_get_response(ctx, req);
  return DN_OK;
}

static void req_client_enqueue_omsgq(struct context *ctx, struct conn *conn,
                                     struct msg *req) {
  ASSERT(req->is_request);
  ASSERT(conn->type == CONN_CLIENT);

  TAILQ_INSERT_TAIL(&conn->omsg_q, req, c_tqe);
  histo_add(&ctx->stats->client_out_queue, TAILQ_COUNT(&conn->omsg_q));
  log_debug(LOG_VERB, "%s enqueue outq %s", print_obj(conn), print_obj(req));
}

static void req_client_dequeue_omsgq(struct context *ctx, struct conn *conn,
                                     struct msg *req) {
  ASSERT(req->is_request);
  ASSERT(conn->type == CONN_CLIENT);

  if (req->stime_in_microsec) {
    usec_t latency = dn_usec_now() - req->stime_in_microsec;
    stats_histo_add_latency(ctx, latency);
  }
  TAILQ_REMOVE(&conn->omsg_q, req, c_tqe);
  histo_add(&ctx->stats->client_out_queue, TAILQ_COUNT(&conn->omsg_q));
  log_debug(LOG_VERB, "%s dequeue outq %s", print_obj(conn), print_obj(req));
}

struct conn_ops client_ops = {msg_recv,
                              req_recv_next,
                              req_recv_done,
                              msg_send,
                              rsp_send_next,
                              rsp_send_done,
                              client_close,
                              client_active,
                              client_ref,
                              client_unref,
                              NULL,
                              NULL,
                              req_client_enqueue_omsgq,
                              req_client_dequeue_omsgq,
                              client_handle_response};

void init_client_conn(struct conn *conn) {
  conn->dyn_mode = 0;
  conn->type = CONN_CLIENT;
  conn->ops = &client_ops;
}
