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
#include "dyn_thread_ctx.h"

struct msg *
rsp_get(struct conn *conn)
{
    struct msg *msg;

    ASSERT((conn->p.type == CONN_DNODE_PEER_SERVER) ||
           (conn->p.type == CONN_SERVER));

    msg = msg_get(conn, false, __FUNCTION__);
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
    //ASSERT(msg->peer == NULL);
    msg_put(msg);
}

static struct msg *
rsp_make_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;        /* peer message (response) */
    struct msg *cmsg, *nmsg; /* current and next message (request) */
    uint64_t id;
    err_t err;

    ASSERT((conn->p.type == CONN_CLIENT) ||
           (conn->p.type == CONN_DNODE_PEER_CLIENT));
    ASSERT(msg->request && req_error(conn, msg));
    ASSERT(msg->owner == conn);

    id = msg->frag_id;
    if (id != 0) {
        for (err = 0, cmsg = TAILQ_NEXT(msg, c_tqe);
             cmsg != NULL && cmsg->frag_id == id;
             cmsg = nmsg) {
            nmsg = TAILQ_NEXT(cmsg, c_tqe);

            /* dequeue request (error fragment) from client outq */
            conn_dequeue_outq(ctx, conn, cmsg);
            if (err == 0 && cmsg->err != 0) {
                err = cmsg->err;
            }

            req_put(cmsg);
        }
    } else {
        err = msg->err;
    }

    pmsg = msg->selected_rsp;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->selected_rsp = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }

    return msg_get_error(conn, msg->dyn_error, err);
}

struct msg *
rsp_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *rsp, *req; /* response and it's peer request */

    ASSERT_LOG((conn->p.type == CONN_DNODE_PEER_CLIENT) ||
               (conn->p.type = CONN_CLIENT), "conn %s", conn_get_type_string(conn));

    req = TAILQ_FIRST(&conn->omsg_q);
    if (req == NULL || (!req->selected_rsp && !req_done(conn, req))) {
        /* nothing is outstanding, initiate close? */
        if (req == NULL && conn->eof) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->p.sd);
        }

        status = thread_ctx_del_out(conn->ptctx, conn_get_pollable(conn));
        if (status != DN_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    rsp = conn->smsg;
    if (rsp != NULL) {
        ASSERT(!rsp->request);
        ASSERT_LOG(rsp->peer != NULL, "rsp:%p %lu:%lu reqid: %lu", rsp, rsp->id, rsp->parent_id, rsp->req_id);
        req = TAILQ_NEXT(rsp->peer, c_tqe);
    }

    if (req == NULL || !req_done(conn, req)) {
        conn->smsg = NULL;
        return NULL;
    }
    ASSERT(req->request && !req->swallow);

    if (req_error(conn, req)) {
        rsp = rsp_make_error(ctx, conn, req);
        if (rsp == NULL) {
            conn->err = errno;
            return NULL;
        }
        rsp->peer = req;
        req->selected_rsp = rsp;
        log_error("creating new error rsp %p", rsp);
        if (conn->dyn_mode) {
      	  stats_pool_incr(ctx, peer_forward_error);
        } else {
      	  stats_pool_incr(ctx, forward_error);
        }
    } else {
        rsp = req->selected_rsp;
    }
    ASSERT(!rsp->request);

    conn->smsg = rsp;

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "send next rsp %"PRIu64" on c %d", rsp->id, conn->p.sd);
    }

    return rsp;
}
