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
    struct msg *msg;

    ASSERT((conn->client && !conn->proxy) || (conn->dnode_client && !conn->dnode_server));

    msg = msg_get(conn, true, conn->redis);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
req_put(struct msg *msg)
{
    struct msg *pmsg; /* peer message (response) */

    ASSERT(msg->request);

    pmsg = msg->peer;
    if (pmsg != NULL) {
        ASSERT(!pmsg->request && pmsg->peer == msg);
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }

    msg_tmo_delete(msg);

    msg_put(msg);
}

/*
 * Return true if request is done, false otherwise
 *
 * A request is done, if we received response for the given request.
 * A request vector is done if we received responses for all its
 * fragments.
 */
bool
req_done(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg, *pmsg; /* current and previous message */
    uint64_t id;             /* fragment id */
    uint32_t nfragment;      /* # fragment */

    ASSERT((conn->client && !conn->proxy) || (conn->dnode_client && !conn->dnode_server));
    ASSERT(msg->request);

    if (!msg->done) {
        return false;
    }

    id = msg->frag_id;
    if (id == 0) {
        return true;
    }

    if (msg->fdone) {
        /* request has already been marked as done */
        return true;
    }

    /* check all fragments of the given request vector are done */

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (!cmsg->done) {
            return false;
        }
    }

    if (!pmsg->last_fragment) {
        return false;
    }

    /*
     * At this point, all the fragments including the last fragment have
     * been received.
     *
     * Mark all fragments of the given request vector to be done to speed up
     * future req_done calls for any of fragments of this request
     */

    msg->fdone = 1;
    nfragment = 1;

    for (pmsg = msg, cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    for (pmsg = msg, cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         pmsg = cmsg, cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->fdone = 1;
        nfragment++;
    }

    ASSERT(msg->frag_owner->nfrag == nfragment);

    msg->post_coalesce(msg->frag_owner);

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is done", conn->sd, id, nfragment);

    return true;
}

/*
 * Return true if request is in error, false otherwise
 *
 * A request is in error, if there was an error in receiving response for the
 * given request. A multiget request is in error if there was an error in
 * receiving response for any its fragments.
 */
bool
req_error(struct conn *conn, struct msg *msg)
{
    struct msg *cmsg; /* current message */
    uint64_t id;
    uint32_t nfragment;

    ASSERT(msg->request && req_done(conn, msg));

    if (msg->error) {
        return true;
    }

    id = msg->frag_id;
    if (id == 0) {
        return false;
    }

    if (msg->ferror) {
        /* request has already been marked to be in error */
        return true;
    }

    /* check if any of the fragments of the given request are in error */

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {

        if (cmsg->error) {
            goto ferror;
        }
    }

    return false;

ferror:

    /*
     * Mark all fragments of the given request to be in error to speed up
     * future req_error calls for any of fragments of this request
     */

    msg->ferror = 1;
    nfragment = 1;

    for (cmsg = TAILQ_PREV(msg, msg_tqh, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_PREV(cmsg, msg_tqh, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    for (cmsg = TAILQ_NEXT(msg, c_tqe);
         cmsg != NULL && cmsg->frag_id == id;
         cmsg = TAILQ_NEXT(cmsg, c_tqe)) {
        cmsg->ferror = 1;
        nfragment++;
    }

    log_debug(LOG_DEBUG, "req from c %d with fid %"PRIu64" and %"PRIu32" "
              "fragments is in error", conn->sd, id, nfragment);

    return true;
}

void
req_server_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT((!conn->client && !conn->proxy) || (!conn->dnode_client && !conn->dnode_server));

    /*
     * timeout clock starts ticking the instant the message is enqueued into
     * the server in_q; the clock continues to tick until it either expires
     * or the message is dequeued from the server out_q
     *
     * noreply request are free from timeouts because client is not interested
     * in the reponse anyway!
     */
    if (!msg->noreply) {
        msg_tmo_insert(msg, conn);
    }

    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);

    if (!conn->dyn_mode) {
       stats_server_incr(ctx, conn->owner, in_queue);
       stats_server_incr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
    }
}

void
req_server_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, in_queue);
    stats_server_decr_by(ctx, conn->owner, in_queue_bytes, msg->mlen);
}

void
req_client_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

void
req_server_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);

    stats_server_incr(ctx, conn->owner, out_queue);
    stats_server_incr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

void
req_client_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->client && !conn->proxy);

    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
}

void
req_server_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(!conn->client && !conn->proxy);

    msg_tmo_delete(msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);

    stats_server_decr(ctx, conn->owner, out_queue);
    stats_server_decr_by(ctx, conn->owner, out_queue_bytes, msg->mlen);
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    ASSERT((conn->client && !conn->proxy) || (conn->dnode_client && !conn->dnode_server));

    if (conn->eof) {
        msg = conn->rmsg;

        /* client sent eof before sending the entire request */
        if (msg != NULL) {
            conn->rmsg = NULL;

            ASSERT(msg->peer == NULL);
            ASSERT(msg->request && !msg->done);

            log_error("eof c %d discarding incomplete req %"PRIu64" len "
                      "%"PRIu32"", conn->sd, msg->id, msg->mlen);

            req_put(msg);
        }

        /*
         * TCP half-close enables the client to terminate its half of the
         * connection (i.e. the client no longer sends data), but it still
         * is able to receive data from the proxy. The proxy closes its
         * half (by sending the second FIN) when the client has no
         * outstanding requests
         */
        if (!conn->active(conn)) {
            conn->done = 1;
            log_debug(LOG_INFO, "c %d is done", conn->sd);
        }
        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        ASSERT(msg->request);
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = req_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(conn->client && !conn->proxy);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "filter empty req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        req_put(msg);
        return true;
    }

    /*
     * Handle "quit\r\n", which is the protocol way of doing a
     * passive close
     */
    if (msg->quit) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_INFO, "filter quit req %"PRIu64" from c %d", msg->id,
                  conn->sd);
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put(msg);
        return true;
    }

    return false;
}

static void
req_forward_error(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    ASSERT(conn->client && !conn->proxy);

    log_debug(LOG_INFO, "forward req %"PRIu64" len %"PRIu32" type %d from "
              "c %d failed: %s", msg->id, msg->mlen, msg->type, conn->sd,
              strerror(errno));

    msg->done = 1;
    msg->error = 1;
    msg->err = errno;

    /* noreply request don't expect any response */
    if (msg->noreply) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->evb, conn);
        if (status != DN_OK) {
            conn->err = errno;
        }
    }
}

static void
req_forward_stats(struct context *ctx, struct server *server, struct msg *msg)
{
    ASSERT(msg->request);

    stats_server_incr(ctx, server, requests);
    stats_server_incr_by(ctx, server, request_bytes, msg->mlen);
}

void
local_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg,
		               uint8_t *key, uint32_t keylen)
{
    rstatus_t status;
    struct conn *s_conn;

    ASSERT((c_conn->client || c_conn->dnode_client) && !c_conn->proxy && !c_conn->dnode_server);

    /* enqueue message (request) into client outq, if response is expected */
    if (!msg->noreply) {
        c_conn->enqueue_outq(ctx, c_conn, msg);
    }

    s_conn = server_pool_conn(ctx, c_conn->owner, key, keylen);
    if (s_conn == NULL) {
        req_forward_error(ctx, c_conn, msg);
        return;
    }
    ASSERT(!s_conn->client && !s_conn->proxy);


    if (ctx->dyn_state == NORMAL) {
        /* enqueue the message (request) into server inq */
        if (TAILQ_EMPTY(&s_conn->imsg_q)) {
            status = event_add_out(ctx->evb, s_conn);

            if (status != DN_OK) {
                req_forward_error(ctx, c_conn, msg);
                s_conn->err = errno;
                return;
            }
        }
    } else if (ctx->dyn_state == STANDBY) {  //no reads/writes from peers/clients
    	log_debug(LOG_VERB, "Node is in STANDBY state. Drop write/read requests");
    	req_forward_error(ctx, c_conn, msg);
    	return;
    } else if (ctx->dyn_state == WRITES_ONLY && msg->is_read) {
    	//no reads from peers/clients but allow writes from peers/clients
    	log_debug(LOG_VERB, "Node is in WRITES_ONLY state. Drop read requests");
    	req_forward_error(ctx, c_conn, msg);
        return;
    } else if (ctx->dyn_state == RESUMING) {
    	log_debug(LOG_VERB, "Node is in RESUMING state. Still drop read requests and flush out all the queued writes");
    	if (msg->is_read) {
    		req_forward_error(ctx, c_conn, msg);
    		return;
    	}

    	status = event_add_out(ctx->evb, s_conn);

    	if (status != DN_OK) {
    	    req_forward_error(ctx, c_conn, msg);
    	    s_conn->err = errno;
    	    return;
    	}
    }

    s_conn->enqueue_inq(ctx, s_conn, msg);
    //req_forward_stats(ctx, s_conn->owner, msg);


    log_debug(LOG_VERB, "local forward from c %d to s %d req %"PRIu64" len %"PRIu32
              " type %d with key '%.*s'", c_conn->sd, s_conn->sd, msg->id,
              msg->mlen, msg->type, keylen, key);
}


/*
 * TODOs: Should replace these by using msg_type in struct msg
 */
static bool
request_send_to_all_datacenters(struct msg *msg) {
    msg_type_t t = msg->type;

    if (msg->redis) {
        return t == MSG_REQ_REDIS_SET || t == MSG_REQ_REDIS_DEL || t == MSG_REQ_REDIS_DECR || t == MSG_REQ_REDIS_HDEL ||
        	   t == MSG_REQ_REDIS_HSET || t == MSG_REQ_REDIS_INCR || t == MSG_REQ_REDIS_LPOP || t == MSG_REQ_REDIS_LREM ||
        	   t == MSG_REQ_REDIS_LSET || t == MSG_REQ_REDIS_RPOP || t == MSG_REQ_REDIS_SADD || t == MSG_REQ_REDIS_SPOP ||
        	   t == MSG_REQ_REDIS_SREM || t == MSG_REQ_REDIS_ZADD || t == MSG_REQ_REDIS_ZREM || t == MSG_REQ_REDIS_HMSET ||
        	   t == MSG_REQ_REDIS_LPUSH || t == MSG_REQ_REDIS_LTRIM || t == MSG_REQ_REDIS_RPUSH || t == MSG_REQ_REDIS_SETEX ||
        	   t == MSG_REQ_REDIS_SETNX;
    }

    // yeah, there's probably a better way to do this...
    return t == MSG_REQ_MC_SET || t == MSG_REQ_MC_CAS || t == MSG_REQ_MC_DELETE || t == MSG_REQ_MC_ADD ||
           t == MSG_REQ_MC_REPLACE || t == MSG_REQ_MC_APPEND || t == MSG_REQ_MC_PREPEND || t == MSG_REQ_MC_INCR ||
           t == MSG_REQ_MC_DECR;
}


void 
remote_req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg, 
                        struct datacenter *dc, uint8_t *key, uint32_t keylen)
{
    struct conn *s_conn;

    ASSERT(c_conn->client);

    s_conn = dnode_peer_pool_conn(ctx, c_conn->owner, dc, key, keylen, msg->msg_type);
    if (s_conn == NULL) {
        req_forward_error(ctx, c_conn, msg);
        return;
    }

    //jeb - check if s_conn is _this_ node, and if so, get conn from server_pool_conn instead
    struct server *peer = s_conn->owner;

    if (peer->is_local) {
        local_req_forward(ctx, c_conn, msg, key, keylen);
        return;
    } else {
        peer_req_forward(ctx, c_conn, s_conn, msg, dc, key, keylen);
    }
}


static void
req_forward(struct context *ctx, struct conn *c_conn, struct msg *msg)
{
    struct server_pool *pool;
    uint8_t *key;
    uint32_t keylen;

    ASSERT(c_conn->client && !c_conn->proxy);

    pool = c_conn->owner;
    key = NULL;
    keylen = 0;

    if (!string_empty(&pool->hash_tag)) {
        struct string *tag = &pool->hash_tag;
        uint8_t *tag_start, *tag_end;

        tag_start = dn_strchr(msg->key_start, msg->key_end, tag->data[0]);
        if (tag_start != NULL) {
            tag_end = dn_strchr(tag_start + 1, msg->key_end, tag->data[1]);
            if (tag_end != NULL) {
                key = tag_start + 1;
                keylen = (uint32_t)(tag_end - key);
            }
        }
    }

    if (keylen == 0) {
        key = msg->key_start;
        keylen = (uint32_t)(msg->key_end - msg->key_start);
    }

    struct datacenter *dc;
    uint32_t dc_cnt = array_n(&pool->datacenter);
    if (dc_cnt > 1 && request_send_to_all_datacenters(msg)) {
        // need to capture the initial mbuf location as once we add in the dynomite headers (as mbufs to the src msg), 
        // that will bork the request sent to secondary dcs
        struct mbuf *mbuf_start = STAILQ_FIRST(&msg->mhdr);

        uint32_t i;
        for (i = 0; i < dc_cnt; i++) {
            dc = array_get(&pool->datacenter, i);
            struct msg *dc_msg;
            
            // clone the msg struct if not the current dc
            if (string_compare(dc->name, &pool->dc) != 0) {
                dc_msg = msg_get(c_conn, msg->request, msg->redis);
                if (dc_msg == NULL) {
                    log_debug(LOG_VERB, "whelp, looks like yer screwed now, buddy. no inter-dc messages for you!");
                    continue;
                }

                msg_clone(msg, mbuf_start, dc_msg);
                dc_msg->noreply = true;
            } else {
                dc_msg = msg;
            }

            remote_req_forward(ctx, c_conn, dc_msg, dc, key, keylen);
        }
    } else {
        dc = server_get_datacenter(pool, &pool->dc);
        remote_req_forward(ctx, c_conn, msg, dc, key, keylen);
    }
}


void
req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    ASSERT(conn->client && !conn->proxy);
    ASSERT(msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(conn->rmsg == msg);
    ASSERT(nmsg == NULL || nmsg->request);

    /* enqueue next message (request), if any */
    conn->rmsg = nmsg;

    if (req_filter(ctx, conn, msg)) {
        return;
    }

    req_forward(ctx, conn, msg);
}

struct msg *
req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    ASSERT((!conn->client && !conn->proxy) || (!conn->dnode_client && !conn->dnode_server));

    if (conn->connecting) {
        if (!conn->dyn_mode && !conn->client)
           server_connected(ctx, conn);
        else if (conn->dyn_mode && !conn->dnode_client)
           dnode_peer_connected(ctx, conn);
    }

    nmsg = TAILQ_FIRST(&conn->imsg_q);
    if (nmsg == NULL) {
        /* nothing to send as the server inq is empty */
        status = event_del_out(ctx->evb, conn);
        if (status != DN_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        ASSERT(msg->request && !msg->done);
        nmsg = TAILQ_NEXT(msg, s_tqe);
    }

    conn->smsg = nmsg;

    if (nmsg == NULL) {
        return NULL;
    }

    ASSERT(nmsg->request && !nmsg->done);

    log_debug(LOG_VVERB, "send next req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", nmsg->id, nmsg->mlen, nmsg->type, conn->sd);

    return nmsg;
}

void
req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT((!conn->client && !conn->proxy) || (!conn->dnode_client && !conn->dnode_server));
    ASSERT(msg != NULL && conn->smsg == NULL);
    ASSERT(msg->request && !msg->done);
    ASSERT(msg->owner != conn);

    log_debug(LOG_VVERB, "send done req %"PRIu64" len %"PRIu32" type %d on "
              "s %d", msg->id, msg->mlen, msg->type, conn->sd);

    /* dequeue the message (request) from server inq */
    conn->dequeue_inq(ctx, conn, msg);

    /*
     * noreply request instructs the server not to send any response. So,
     * enqueue message (request) in server outq, if response is expected.
     * Otherwise, free the noreply request
     */
    if (!msg->noreply) {
        conn->enqueue_outq(ctx, conn, msg);
    } else {
        req_put(msg);
    }
}

