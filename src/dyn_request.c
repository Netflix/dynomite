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
#include "dyn_dnode_peer.h"

struct msg *
req_get(struct conn *conn)
{
    struct msg *req;

    ASSERT((conn->type == CONN_CLIENT) ||
           (conn->type == CONN_DNODE_PEER_CLIENT));

    req = msg_get(conn, true, __FUNCTION__);
    if (req == NULL) {
        conn->err = errno;
    }

    return req;
}

void
req_put(struct msg *req)
{
    struct msg *rsp; /* peer message (response) */

    ASSERT(req->is_request);

    rsp = req->selected_rsp;
    if (rsp != NULL) {
        ASSERT(!rsp->is_request && rsp->peer == req);
        req->selected_rsp = NULL;
        rsp->peer = NULL;
        rsp_put(rsp);
    }

    msg_tmo_delete(req);

    msg_put(req);
}

/*
 * Return true if request is done, false otherwise
 *
 * A request is done, if we received response for the given request.
 * A request vector is done if we received responses for all its
 * fragments.
 */
bool
req_done(struct conn *conn, struct msg *req)
{
    struct msg *cmsg; /* current and previous message */
    uint64_t id;             /* fragment id */
    uint32_t nfragment;      /* # fragment */

    ASSERT((conn->type == CONN_CLIENT) ||
           (conn->type == CONN_DNODE_PEER_CLIENT));

    if (req == NULL)
        return false;

    ASSERT(req->is_request);

    if (!req->selected_rsp)
        return false;

    id = req->frag_id;
    if (id == 0) {
        return true;
    }

    if (req->fdone) {
        /* request has already been marked as done */
        return true;
    }

    struct msg *frag_owner = req->frag_owner;
    if (frag_owner->nfrag_done < frag_owner->nfrag)
        return false;

    // check all fragments of the given request vector are done.
    for (cmsg = TAILQ_PREV(req, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (!cmsg->selected_rsp)
            return false;
    }

    for (cmsg = TAILQ_NEXT(req, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (!cmsg->selected_rsp)
            return false;
    }

    /*
     * At this point, all the fragments including the last fragment have
     * been received.
     *
     * Mark all fragments of the given request vector to be done to speed up
     * future req_done calls for any of fragments of this request
     */

    req->fdone = 1;
    nfragment = 0;

    for (cmsg = TAILQ_PREV(req, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(req, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    ASSERT(req->frag_owner->nfrag == nfragment);

    g_post_coalesce(req->frag_owner);

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is done", conn->sd, id, nfragment);

    return true;
}

rstatus_t
req_make_reply(struct context *ctx, struct conn *conn, struct msg *req)
{
    struct msg *rsp = msg_get(conn, false, __FUNCTION__);
    if (rsp == NULL) {
        conn->err = errno;
        return DN_ENOMEM;
    }

    req->selected_rsp = rsp;
    rsp->peer = req;
    rsp->is_request = 0;

    req->done = 1;
    conn_enqueue_outq(ctx, conn, req);

    return DN_OK;
}

/*
 * Return true if request is in error, false otherwise
 *
 * A request is in error, if there was an error in receiving response for the
 * given request. A multiget request is in error if there was an error in
 * receiving response for any its fragments.
 */
bool
req_error(struct conn *conn, struct msg *req)
{
    struct msg *cmsg; /* current message */
    uint64_t id;
    uint32_t nfragment;

    ASSERT(req->is_request && req_done(conn, req));

    if (req->is_error) {
        return true;
    }

    id = req->frag_id;
    if (id == 0) {
        return false;
    }

    if (req->is_ferror) {
        /* request has already been marked to be in error */
        return true;
    }

    /* check if any of the fragments of the given request are in error */

    for (cmsg = TAILQ_PREV(req, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (cmsg->is_error) {
            goto ferror;
        }
    }

    for (cmsg = TAILQ_NEXT(req, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (cmsg->is_error) {
            goto ferror;
        }
    }

    return false;

ferror:

    /*
     * Mark all fragments of the given request to be in error to speed up
     * future req_error calls for any of fragments of this request
     */

    req->is_ferror = 1;
    nfragment = 1;

    for (cmsg = TAILQ_PREV(req, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->is_ferror = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(req, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->is_ferror = 1;
        nfragment++;
    }

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is in error", conn->sd, id, nfragment);

    return true;
}
