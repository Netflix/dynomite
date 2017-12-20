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
    struct msg *rsp;

    ASSERT((conn->type == CONN_DNODE_PEER_SERVER) ||
           (conn->type == CONN_SERVER));

    rsp = msg_get(conn, false, __FUNCTION__);
    if (rsp == NULL) {
        conn->err = errno;
    }

    return rsp;
}

void
rsp_put(struct msg *rsp)
{
    if (!rsp)
        return;
    ASSERT(!rsp->is_request);
    //ASSERT(rsp->peer == NULL);
    msg_put(rsp);
}

static struct msg *
rsp_make_error(struct context *ctx, struct conn *conn, struct msg *req)
{
    struct msg *rsp;        /* peer message (response) */
    struct msg *cmsg, *nmsg; /* current and next message (request) */
    uint64_t id;
    err_t error_code = 0, dyn_error_code = 0;

    ASSERT((conn->type == CONN_CLIENT) ||
           (conn->type == CONN_DNODE_PEER_CLIENT));
    ASSERT(req->is_request && req_error(conn, req));
    ASSERT(req->owner == conn);

    // first grab the error from the current req
    error_code = req->error_code;
    dyn_error_code = req->dyn_error_code;

    id = req->frag_id;
    if (id != 0) {
        for (cmsg = TAILQ_NEXT(req, c_tqe);
             cmsg != NULL && cmsg->frag_id == id;
             cmsg = nmsg) {
            nmsg = TAILQ_NEXT(cmsg, c_tqe);

            /* dequeue request (error fragment) from client outq */
            conn_dequeue_outq(ctx, conn, cmsg);
            if (!error_code && cmsg->error_code != 0) {
                error_code = cmsg->error_code;
                dyn_error_code = cmsg->dyn_error_code;
            }
            req_put(cmsg);
        }
    }

    rsp = req->selected_rsp;
    if (rsp != NULL) {
        ASSERT(!rsp->is_request && rsp->peer == req);
        req->selected_rsp = NULL;
        rsp->peer = NULL;
        rsp_put(rsp);
    }

    return msg_get_error(conn, dyn_error_code, error_code);
}

struct msg *
rsp_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *rsp, *req; /* response and it's peer request */

    ASSERT_LOG((conn->type == CONN_DNODE_PEER_CLIENT) ||
               (conn->type = CONN_CLIENT), "conn %s", print_obj(conn));

    req = TAILQ_FIRST(&conn->omsg_q);
    if (req == NULL || !req_done(conn, req)) {
        /* nothing is outstanding, initiate close? */
        if (req == NULL && conn->eof) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }

        status = conn_event_del_out(conn);
        if (status != DN_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    rsp = conn->smsg;
    if (rsp != NULL) {
        ASSERT(!rsp->is_request);
        ASSERT(rsp->peer != NULL);
        req = TAILQ_NEXT(rsp->peer, c_tqe);
    }

    if (req == NULL || !req_done(conn, req)) {
        conn->smsg = NULL;
        return NULL;
    }
    ASSERT(req->is_request && !req->swallow);

    if (req_error(conn, req)) {
        rsp = rsp_make_error(ctx, conn, req);
        if (rsp == NULL) {
            conn->err = errno;
            return NULL;
        }
        rsp->peer = req;
        req->selected_rsp = rsp;
        log_debug(LOG_VERB, "creating new error rsp %s", print_obj(rsp));
        if (conn->dyn_mode) {
      	  stats_pool_incr(ctx, peer_forward_error);
        } else {
      	  stats_pool_incr(ctx, forward_error);
        }
    } else {
        rsp = req->selected_rsp;
    }
    ASSERT(!rsp->is_request);

    conn->smsg = rsp;

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "send next rsp %"PRIu64" on c %d", rsp->id, conn->sd);
    }

    return rsp;
}

void
rsp_send_done(struct context *ctx, struct conn *conn, struct msg *rsp)
{

    ASSERT(conn->type == CONN_CLIENT);
    ASSERT(conn->smsg == NULL);

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "send done rsp %"PRIu64" on c %d", rsp->id, conn->sd);
    }

    log_debug(LOG_VERB, "conn %p rsp %p done", conn, rsp);
    struct msg *req = rsp->peer;
    ASSERT_LOG(req, "response %d does not have a corresponding request", rsp->id);
    ASSERT_LOG(!req->rsp_sent, "request %d:%d already had a response sent",
               req->id, req->parent_id);

    ASSERT(!rsp->is_request && req->is_request);
    ASSERT(req->selected_rsp == rsp);
    req->rsp_sent = 1;

    /* dequeue request from client outq */
    conn_dequeue_outq(ctx, conn, req);

    // Remove it from the dict
    if (!req->awaiting_rsps) {
        log_debug(LOG_VERB, "conn %p removing message %d:%d", conn, req->id, req->parent_id);
        dictDelete(conn->outstanding_msgs_dict, &req->id);
        req_put(req);
    } else {
        log_info("req %d:%d still awaiting rsps %d", req->id, req->parent_id,
                  req->awaiting_rsps);
    }
}

