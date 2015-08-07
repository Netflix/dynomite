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

#include "dyn_core.h"
#include "dyn_server.h"

struct msg *
rsp_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT((!conn->client && !conn->proxy) || (!conn->dnode_client && !conn->dnode_server));

    msg = msg_get(conn, false, conn->data_store);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
rsp_put(struct msg *msg)
{
    if (!msg)
        return;
    ASSERT(!msg->request);
    ASSERT(msg->peer == NULL);
    msg_put(msg);
}

static struct msg *
rsp_make_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;        /* peer message (response) */
    struct msg *cmsg, *nmsg; /* current and next message (request) */
    uint64_t id;
    err_t err;

    ASSERT((conn->client && !conn->proxy) || (conn->dnode_client && !conn->dnode_server));
    ASSERT(msg->request && req_error(conn, msg));
    ASSERT(msg->owner == conn);

    id = msg->frag_id;
    if (id != 0) {
        for (err = 0, cmsg = TAILQ_NEXT(msg, c_tqe);
             cmsg != NULL && cmsg->frag_id == id;
             cmsg = nmsg) {
            nmsg = TAILQ_NEXT(cmsg, c_tqe);

            /* dequeue request (error fragment) from client outq */
            conn->dequeue_outq(ctx, conn, cmsg);
            if (err == 0 && cmsg->err != 0) {
                err = cmsg->err;
            }

            req_put(cmsg);
        }
    } else {
        err = msg->err;
    }

    pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }

    return msg_get_error(conn->data_store, msg->dyn_error, err);
}

struct msg *
rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT((!conn->client && !conn->proxy) || (!conn->dnode_client && !conn->dnode_server));

    if (conn->eof) {
        msg = conn->rmsg;

        if (conn->dyn_mode) {
            if (conn->non_bytes_recv > MAX_CONN_ALLOWABLE_NON_RECV) {
                conn->err = EPIPE;
                return NULL;
            }
            conn->eof = 0;
            return msg;
        }

        /* server sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(!msg->request);

            log_error("eof s %d discarding incomplete rsp %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            rsp_put(msg);
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
        log_debug(LOG_DEBUG, "s %d active %d is done", conn->sd, conn->active(conn));

        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(!msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = rsp_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static bool
server_rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    ASSERT(!conn->client && !conn->proxy);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty rsp %"PRIu64" on s %d", msg->id,
                  conn->sd);
        rsp_put(msg);
        return true;
    }

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL) {
        log_debug(LOG_VERB, "filter stray rsp %"PRIu64" len %"PRIu32" on s %d",
                  msg->id, msg->mlen, conn->sd);
        rsp_put(msg);
        return true;
    }

    if (pmsg->noreply) {
         conn->dequeue_outq(ctx, conn, pmsg);
         rsp_put(pmsg);
         rsp_put(msg);
         return true;
    }

    ASSERT(pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    if (pmsg->swallow) {
        conn->dequeue_outq(ctx, conn, pmsg);
        pmsg->done = 1;

        log_debug(LOG_DEBUG, "swallow rsp %"PRIu64" len %"PRIu32" of req "
                  "%"PRIu64" on s %d", msg->id, msg->mlen, pmsg->id,
                  conn->sd);

        rsp_put(msg);
        req_put(pmsg);
        return true;
    }

    return false;
}

static void
server_rsp_forward_stats(struct context *ctx, struct server *server,
                         struct msg *msg)
{
	ASSERT(!msg->request);

	if (msg->is_read) {
		stats_server_incr(ctx, server, read_responses);
		stats_server_incr_by(ctx, server, read_response_bytes, msg->mlen);
	} else {
		stats_server_incr(ctx, server, write_responses);
		stats_server_incr_by(ctx, server, write_response_bytes, msg->mlen);
	}
}

static void
server_rsp_forward(struct context *ctx, struct conn *s_conn, struct msg *rsp)
{
    rstatus_t status;
    struct msg *req;
    struct conn *c_conn;
    ASSERT(s_conn->type == CONN_SERVER);
    ASSERT(!s_conn->client && !s_conn->proxy);

    /* response from server implies that server is ok and heartbeating */
    server_ok(ctx, s_conn);

    /* dequeue peer message (request) from server */
    req = TAILQ_FIRST(&s_conn->omsg_q);
    ASSERT(req != NULL && req->peer == NULL);
    ASSERT(req->request && !req->done);

    s_conn->dequeue_outq(ctx, s_conn, req);
    req->done = 1;

    /* establish rsp <-> req (response <-> request) link */
    log_debug(LOG_VERB, "%d:%d <-> %d:%d", req->id, req->parent_id,
               rsp->id, rsp->parent_id);
    req->peer = rsp;
    rsp->peer = req;

    rsp->pre_coalesce(rsp);

    c_conn = req->owner;
    
    ASSERT((c_conn->type == CONN_CLIENT) ||
           (c_conn->type == CONN_DNODE_PEER_CLIENT));

    server_rsp_forward_stats(ctx, s_conn->owner, rsp);
    // this should really be the message's response handler be doing it
    if (req_done(c_conn, req)) {
        // handler owns the response now
        log_debug(LOG_INFO, "handle rsp %d:%d for req %d:%d conn %p", rsp->id,
                   rsp->parent_id, req->id, req->parent_id, c_conn);
        rstatus_t status = DN_OK;
        status = conn_handle_response(c_conn, c_conn->type == CONN_CLIENT ?
                                      req->id : req->parent_id, rsp);
        IGNORE_RET_VAL(status);
     }
}

void
server_rsp_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
                     struct msg *nmsg)
{
    ASSERT(!conn->client && !conn->proxy);
    ASSERT(msg != NULL && conn->rmsg == msg);
    ASSERT(!msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->request);

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

    if (server_rsp_filter(ctx, conn, msg)) {
        return;
    }
    server_rsp_forward(ctx, conn, msg);
}

struct msg *
rsp_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *pmsg; /* response and it's peer request */

    ASSERT((conn->client && !conn->proxy) ||
           (conn->dnode_client && !conn->dnode_server));

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL || !req_done(conn, pmsg)) {
        /* nothing is outstanding, initiate close? */
        if (pmsg == NULL && conn->eof) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }

        status = event_del_out(ctx->evb, conn);
        if (status != DN_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(!msg->request && msg->peer != NULL);
        ASSERT(req_done(conn, msg->peer));
        pmsg = TAILQ_NEXT(msg->peer, c_tqe);
    }

    if (pmsg == NULL || !req_done(conn, pmsg)) {
        conn->smsg = NULL;
        return NULL;
    }
    ASSERT(pmsg->request && !pmsg->swallow);

    if (req_error(conn, pmsg)) {
        msg = rsp_make_error(ctx, conn, pmsg);
        if (msg == NULL) {
            conn->err = errno;
            return NULL;
        }
        msg->peer = pmsg;
        pmsg->peer = msg;
        if (conn->dyn_mode) {
      	  stats_pool_incr(ctx, conn->owner, peer_forward_error);
        } else {
      	  stats_pool_incr(ctx, conn->owner, forward_error);
        }
    } else {
        msg = pmsg->peer;
    }
    ASSERT(!msg->request);

    conn->smsg = msg;

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "send next rsp %"PRIu64" on c %d", msg->id, conn->sd);
    }

    return msg;
}

void
rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg; /* peer message (request) */

    ASSERT(conn->client && !conn->proxy);
    ASSERT(conn->smsg == NULL);

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "send done rsp %"PRIu64" on c %d", msg->id, conn->sd);
    }

    log_debug(LOG_VERB, "conn %p msg %p done", conn, msg);
    pmsg = msg->peer;

    ASSERT(!msg->request && pmsg->request);
    ASSERT(pmsg->peer == msg);
    ASSERT(pmsg->done && !pmsg->swallow);

    /* dequeue request from client outq */
    conn->dequeue_outq(ctx, conn, pmsg);

    // Remove it from the dict
    struct msg *req = msg->peer;
    log_debug(LOG_VERB, "conn %p removing message %d:%d", conn, req->id, req->parent_id);
    dictDelete(conn->outstanding_msgs_dict, &req->id);
    req_put(pmsg);
}

