/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "dyn_core.h"
#include "dyn_topology.h"
#include "dyn_conf.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"
#include "dyn_node_snitch.h"
#include "dyn_token.h"
#include "dyn_thread_ctx.h"


static void forward_response_upstream(struct conn *c_conn, struct msg *req, struct msg *rsp);

bool
peer_is_same_dc(struct peer *peer_node)
{
    struct server_pool *sp = peer_node->owner;
    return string_compare(&sp->dc_name, &peer_node->dc) == 0;
}

static bool
is_conn_secured(struct peer *peer_node)
{
    //ASSERT(peer_server != NULL);
    //ASSERT(sp != NULL);

    // if dc-secured mode then communication only between nodes in different dc is secured
    struct server_pool *sp = peer_node->owner;
    switch (sp->secure_server_option)
    {
        case SECURE_OPTION_NONE:
            return false;
        case SECURE_OPTION_RACK:
            // if rack-secured mode then communication only between nodes in different rack is secured.
            // communication secured between nodes if they are in rack with same name across dcs.
            if (string_compare(&sp->rack_name, &peer_node->rack) != 0
                    || string_compare(&sp->dc_name, &peer_node->dc) != 0) {
                return true;
            }
            return false;
        case SECURE_OPTION_DC:
            // if dc-secured mode then communication only between nodes in different dc is secured     
            if (string_compare(&sp->dc_name, &peer_node->dc) != 0) {
                return true;
            }
            return false;
        case SECURE_OPTION_ALL:
            return true;
    }
    return false;
}

static void
dnode_peer_ref(struct conn *conn, void *owner)
{
    struct peer *peer = owner;

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->owner == NULL);

    conn->family = peer->endpoint.family;
    conn->addrlen = peer->endpoint.addrlen;
    conn->addr = peer->endpoint.addr;
    string_duplicate(&conn->pname, &peer->endpoint.pname);

    ASSERT(peer->conn == NULL);
    peer->conn = conn;

    if (is_conn_secured(peer)) {
        conn->dnode_secured = 1;
        conn->dnode_crypto_state = 0; //need to do a encryption handshake
    }

    conn->same_dc = peer_is_same_dc(peer)? 1 : 0;

    conn->owner = owner;
    conn->ptctx = core_get_ptctx_for_conn(peer->owner->ctx, conn->p.type);

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "dyn: ref peer conn %p owner %p into '%.*s", conn, peer,
                   peer->endpoint.pname.len, peer->endpoint.pname.data);
    }
}

static void
dnode_peer_unref(struct conn *conn)
{
    struct peer *peer;

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->owner != NULL);

    peer = conn->owner;
    conn->owner = NULL;

    peer->conn = NULL;

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "dyn: unref peer conn %p owner %p from '%.*s'", conn, peer,
               peer->endpoint.pname.len, peer->endpoint.pname.data);
    }
}

msec_t
dnode_peer_timeout(struct msg *msg, struct conn *conn)
{
    struct peer *server;
    struct server_pool *pool;

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    server = conn->owner;
    pool = server->owner;
    msec_t additional_timeout = 0;

   if (conn->same_dc)
       additional_timeout = 200;
   else
       additional_timeout = 5000;

   if (!msg->is_read) //make sure write request has a longer timeout so we almost never want to drop it
       additional_timeout += 20000;

    return pool->timeout + additional_timeout;
}

static bool
dnode_peer_active(struct conn *conn)
{
    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->p.sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->p.sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->p.sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->p.sd);
        return true;
    }

    log_debug(LOG_VVERB, "dyn: s %d is inactive", conn->p.sd);

    return false;
}

rstatus_t
dnode_peer_each_set_owner(void *elem, void *data)
{
    struct peer *s = elem;
    struct server_pool *sp = data;

    s->owner = sp;

    return DN_OK;
}

rstatus_t
dnode_peer_each_set_ptctx(void *elem, void *data)
{
    struct peer *s = elem;
    struct context *ctx = data;

    s->ptctx = core_get_ptctx_for_peer(ctx, s);

    return DN_OK;
}

rstatus_t
dnode_peer_add_local(struct server_pool *pool, struct peer *self)
{
    ASSERT(self != NULL);
    self->idx = 0; /* this might be psychotic, trying it for now */

    struct string *p_pname = &pool->dnode_proxy_endpoint.pname;
    self->endpoint.pname = *p_pname;

    uint8_t *p = p_pname->data + p_pname->len - 1;
    uint8_t *start = p_pname->data;
    string_copy(&self->name, start, (uint32_t)(dn_strrchr(p, start, ':') - start));

    //self->name = pool->d_addrstr;
    self->endpoint.port = pool->dnode_proxy_endpoint.port;

    self->endpoint.weight = 0;  /* hacking this out of the way for now */
    string_copy(&self->rack, pool->rack_name.data, pool->rack_name.len);
    self->is_local = true;
    //TODO-jeb might need to copy over tokens, not sure if this is good enough
    self->tokens = pool->tokens;

    self->endpoint.family = pool->dnode_proxy_endpoint.family;
    self->endpoint.addrlen = pool->dnode_proxy_endpoint.addrlen;
    self->endpoint.addr = pool->dnode_proxy_endpoint.addr;
    self->conn = NULL;
    self->ptctx = NULL;

    self->next_retry = 0ULL;
    self->failure_count = 0;
    self->is_seed = 1;
    self->processed = 0;
    string_copy(&self->dc, pool->dc_name.data, pool->dc_name.len);
    self->owner = pool;

    log_debug(LOG_VERB, "dyn: transform to local node to peer %"PRIu32" '%.*s'",
            self->idx, pool->name.len, pool->name.data);

    return DN_OK;
}

void
dnode_peer_deinit(struct array *nodes)
{
    uint32_t i, nnode;

    for (i = 0, nnode = array_n(nodes); i < nnode; i++) {
        struct peer *peer;

        peer = array_pop(nodes);
        IGNORE_RET_VAL(peer);
        ASSERT(peer->conn == NULL);
    }
    array_deinit(nodes);
}

static struct conn *
dnode_peer_conn(struct peer *peer)
{
    if (peer->conn != NULL)
        return peer->conn;

    peer->conn = conn_get_peer(peer, false);
    return peer->conn;
}

static void
dnode_peer_ack_err(struct context *ctx, struct conn *conn, struct msg *req)
{
    if ((req->swallow && !req->expect_datastore_reply) || // no reply
        (req->swallow && (req->consistency == DC_ONE)) || // dc one
        (req->swallow && (req->consistency == DC_QUORUM) // remote dc request
                      && (!conn->same_dc)) ||
        (req->owner == conn)) // a gossip message that originated on this conn
    {
        log_info("close %s %d swallow req %u:%u len %"PRIu32
                 " type %d", conn_get_type_string(conn), conn->p.sd, req->id,
                 req->parent_id, req->mlen, req->type);
        req_put(req);
        return;
    }
    struct conn *c_conn = req->owner;
    // At other connections, these responses would be swallowed.
    ASSERT_LOG((c_conn->p.type == CONN_CLIENT) ||
               (c_conn->p.type == CONN_DNODE_PEER_CLIENT),
               "conn:%s c_conn:%s, req %d:%d", conn_get_type_string(conn),
               conn_get_type_string(c_conn), req->id, req->parent_id);

    // Create an appropriate response for the request so its propagated up;
    // This response gets dropped in rsp_make_error anyways. But since this is
    // an error path its ok with the overhead.
    struct msg *rsp = msg_get(conn, false, __FUNCTION__);
    req->done = 1;
    rsp->error = req->error = 1;
    rsp->err = req->err = conn->err;
    rsp->dyn_error = req->dyn_error = PEER_CONNECTION_REFUSE;
    rsp->dmsg = dmsg_get();
    rsp->dmsg->id =  req->id;

    log_info("close %s %d req %u:%u "
             "len %"PRIu32" type %d from c %d%c %s", conn_get_type_string(conn),
             conn->p.sd, req->id, req->parent_id, req->mlen, req->type,
             c_conn->p.sd, conn->err ? ':' : ' ',
             conn->err ? strerror(conn->err): " ");
    forward_response_upstream(c_conn, req, rsp);
}


static void
dnode_peer_failure(struct context *ctx, struct peer *server)
{
    struct server_pool *pool = server->owner;
    msec_t now;
    rstatus_t status;

    server->failure_count++;

    if (log_loggable(LOG_VERB)) {
       log_debug(LOG_VERB, "dyn: peer '%.*s' failure count %"PRIu32" ",
                  server->endpoint.pname.len, server->endpoint.pname.data, server->failure_count);
    }

    now = dn_msec_now();
    if (now < 0) {
        return;
    }

    if (log_loggable(LOG_INFO)) {
       log_debug(LOG_INFO, "dyn: update peer pool '%.*s' for peer '%.*s' "
               "for next %"PRIu32" secs", pool->name.len,
               pool->name.data, server->endpoint.pname.len, server->endpoint.pname.data,
               pool->server_retry_timeout_ms/1000);
    }

    stats_pool_incr(ctx, peer_ejects);

    //if (server->failure_count == 3)
    //   server->next_retry = now + WAIT_BEFORE_RECONNECT_IN_MILLIS;

    status = topo_update_now(ctx_get_topology(ctx));
    if (status != DN_OK) {
        log_error("dyn: updating peer pool '%.*s' failed: %s",
                pool->name.len, pool->name.data, strerror(errno));
    }
}

static void
dnode_peer_close_stats(struct context *ctx, struct conn *conn)
{
    if (conn->connected) {
        stats_pool_decr(ctx, peer_connections);
    }

    if (conn->eof) {
        stats_pool_incr(ctx, peer_eof);
        return;
    }

    switch (conn->err) {
    case ETIMEDOUT:
        if (conn->same_dc)
            stats_pool_incr(ctx, peer_timedout);
        else
            stats_pool_incr(ctx, remote_peer_timedout);
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
        stats_pool_incr(ctx, peer_err);
        break;
    }
}


static void
dnode_peer_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg; /* current and next message */

    struct peer *peer = conn->owner;

    log_debug(LOG_WARN, "dyn: dnode_peer_close on peer '%.*s'", peer->endpoint.pname.len,
            peer->endpoint.pname.data);

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    dnode_peer_close_stats(ctx, conn);

    if (conn->p.sd < 0) {
        dnode_peer_failure(ctx, conn->owner);
        conn_unref(conn);
        conn_put(conn);
        return;
    }
    uint32_t out_counter = 0;
    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from peer outq */
        conn_dequeue_outq(ctx, conn, msg);
        dnode_peer_ack_err(ctx, conn, msg);
        out_counter++;
    }

    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    uint32_t in_counter = 0;
    for (msg = TAILQ_FIRST(&conn->imsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, s_tqe);

        /* dequeue the message (request) from peer inq */
        conn_dequeue_inq(ctx, conn, msg);
        // We should also remove the msg from the timeout rbtree.
        // for outq, its already taken care of
        pthread_ctx ptctx = conn->ptctx;
        msg_tmo_delete(&ptctx->tmo, msg);
        dnode_peer_ack_err(ctx, conn, msg);
        in_counter++;

        stats_pool_incr(ctx, peer_dropped_requests);
    }

    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    log_warn("close %s %d Dropped %u outqueue & %u inqueue requests",
             conn_get_type_string(conn), conn->p.sd, out_counter, in_counter);
    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        ASSERT(!msg->request);
        ASSERT(msg->peer == NULL);

        rsp_put(msg);

        log_debug(LOG_INFO, "dyn: close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                "in error", conn->p.sd, msg->id, msg->mlen);
    }

    ASSERT(conn->smsg == NULL);

    dnode_peer_failure(ctx, conn->owner);

    conn_unref(conn);

    status = close(conn->p.sd);
    if (status < 0) {
        log_error("dyn: close s %d failed, ignored: %s", conn->p.sd, strerror(errno));
    }
    conn->p.sd = -1;

    conn_put(conn);
}



static rstatus_t
dnode_peer_each_preconnect(void *elem, void *data)
{
    rstatus_t status;
    struct peer *peer;
    struct server_pool *sp;
    struct conn *conn;

    peer = elem;
    sp = peer->owner;

    if (peer->is_local)  //don't bother to connect if it is a self-connection
        return DN_OK;

    conn = dnode_peer_conn(peer);
    if (conn == NULL) {
        return DN_ENOMEM;
    }

    status = conn_connect(sp->ctx, conn);
    if (status != DN_OK) {
        log_warn("dyn: connect to peer '%.*s' failed, ignored: %s",
                peer->endpoint.pname.len, peer->endpoint.pname.data, strerror(errno));
        dnode_peer_close(sp->ctx, conn);
    }

    return DN_OK;
}

static rstatus_t
dnode_peer_each_disconnect(void *elem, void *data)
{
    struct peer *peer = elem;
    struct server_pool *pool;

    pool = peer->owner;

    if (peer->conn != NULL)
        conn_close(pool->ctx, peer->conn);

    return DN_OK;
}

static void
dnode_peer_close_socket(struct conn *conn)
{
    rstatus_t status;
    log_debug(LOG_VERB, "In %s", __FUNCTION__);

    if ((conn != NULL) && (conn->p.sd >= 0)) {
        status = close(conn->p.sd);
        if (status < 0) {
            log_error("dyn: close s %d failed, ignored: %s", conn->p.sd, strerror(errno));
        }
    }

    conn->dnode_crypto_state = 0;
    conn->p.sd = -1;
}


rstatus_t
dnode_peer_forward_state(void *rmsg)
{
    rstatus_t status;
    struct ring_msg *msg = rmsg;
    struct server_pool *sp = msg->sp;
    struct topology *topo = sp->topo;

    log_debug(LOG_VVERB, "dnode_peer_forward_state: forwarding");

    //we assume one mbuf is enough for now - will enhance with multiple mbufs later
    struct mbuf *mbuf = mbuf_get();
    if (mbuf == NULL) {
        log_debug(LOG_VVERB, "Too bad, not enough memory!");
        return DN_ENOMEM;
    }

    mbuf_copy(mbuf, msg->data, msg->len);

    struct array *peers = &topo->peers;
    uint32_t nelem = array_n(peers);

    //pick a random peer
    uint32_t ran_index = (uint32_t)rand() % nelem;

    if (ran_index == 0)
       ran_index += 1;

    struct peer *peer = (struct peer *) array_get(peers, ran_index);

    //log_debug(LOG_VVERB, "Gossiping to node  '%.*s'", peer->name.len, peer->name.data);

    struct conn * conn = dnode_peer_conn(peer);
    if (conn == NULL) {
        //running out of connection due to memory exhaust
        log_debug(LOG_ERR, "Unable to obtain a connection object");
        return DN_ERROR;
    }

    status = conn_connect(sp->ctx, conn);
    if (status != DN_OK ) {
        dnode_peer_close(sp->ctx, conn);
        log_debug(LOG_ERR, "Error happened in connecting on conn %d", conn->p.sd);
        return DN_ERROR;
    }

    dnode_peer_gossip_forward(sp->ctx, conn, mbuf);

    //free this as nobody else will do
    //mbuf_put(mbuf);

    return status;
}


rstatus_t
dnode_peer_handshake_announcing(void *rmsg)
{
    rstatus_t status;
    struct ring_msg *msg = rmsg;
    struct server_pool *sp = msg->sp;
    struct topology *topo = sp->topo;
    log_debug(LOG_VVERB, "dyn: handshaking peers");
    struct array *peers = &topo->peers;

    uint32_t i,nelem;
    nelem = array_n(peers);

    //we assume one mbuf is enough for now - will enhance with multiple mbufs later
    struct mbuf *mbuf = mbuf_get();
    if (mbuf == NULL) {
        log_debug(LOG_VVERB, "Too bad, not enough memory!");
        return DN_ENOMEM;
    }

    //annoucing myself by sending msg: 'dc$rack$token,started_ts,node_state,node_dns'
    mbuf_write_string(mbuf, &sp->dc_name);
    mbuf_write_char(mbuf, '$');
    mbuf_write_string(mbuf, &sp->rack_name);
    mbuf_write_char(mbuf, '$');
    struct dyn_token *token = (struct dyn_token *) array_get(&sp->tokens, 0);
    if (token == NULL) {
        log_debug(LOG_VVERB, "Why? This should not be null!");
        mbuf_put(mbuf);
        return DN_ERROR;
    }

    mbuf_write_uint32(mbuf, token->mag[0]);
    mbuf_write_char(mbuf, ',');
    int64_t cur_ts = (int64_t)time(NULL);
    mbuf_write_uint64(mbuf, (uint64_t)cur_ts);
    mbuf_write_char(mbuf, ',');
    mbuf_write_uint8(mbuf, sp->ctx->dyn_state);
    mbuf_write_char(mbuf, ',');

    unsigned char *broadcast_addr = (unsigned char *)get_broadcast_address(sp);
    mbuf_write_bytes(mbuf, broadcast_addr, (int)dn_strlen(broadcast_addr));

    //for each peer, send a registered msg
    for (i = 0; i < nelem; i++) {
        struct peer *peer = (struct peer *) array_get(peers, i);
        if (peer->is_local)
            continue;

        log_debug(LOG_VVERB, "Gossiping to node  '%.*s'", peer->name.len, peer->name.data);

        struct conn * conn = dnode_peer_conn(peer);
        if (conn == NULL) {
            //running out of connection due to memory exhaust
            log_debug(LOG_DEBUG, "Unable to obtain a connection object");
            return DN_ERROR;
        }


        status = conn_connect(sp->ctx, conn);
        if (status != DN_OK ) {
            dnode_peer_close(sp->ctx, conn);
            log_debug(LOG_DEBUG, "Error happened in connecting on conn %d", conn->p.sd);
            return DN_ERROR;
        }

        //conn->

        dnode_peer_gossip_forward(sp->ctx, conn, mbuf);
        //peer_gossip_forward1(sp->ctx, conn, sp->data_store, &data);
    }

    //free this as nobody else will do
    //mbuf_put(mbuf);

    return DN_OK;
}


static void
dnode_peer_relink_conn_owner(struct server_pool *sp)
{
    struct topology *topo = sp->topo;
    struct array *peers = &topo->peers;

    uint32_t i,nelem;
    nelem = array_n(peers);
    for (i = 0; i < nelem; i++) {
        struct peer *peer = (struct peer *) array_get(peers, i);
        if (peer->conn)
            peer->conn->owner = peer; //re-link to the owner in case of an resize/allocation
    }
}


static rstatus_t
dnode_peer_add_node(struct server_pool *sp, struct gossip_node *node)
{
    rstatus_t status;
    struct topology *topo = sp->topo;
    struct array *peers = &topo->peers;
    struct peer *s = array_push(peers);

    s->owner = sp;
    s->idx = array_idx(peers, s);

    //log_debug(LOG_VERB, "node rack_name         : '%.*s'", node->rack.len, node->rack.data);
    //log_debug(LOG_VERB, "node dc_name        : '%.*s'", node->dc.len, node->dc.data);
    //log_debug(LOG_VERB, "node address          : '%.*s'", node->endpoint.pname.len, node->endpoint.pname.data);
    //log_debug(LOG_VERB, "node ip         : '%.*s'", node->name.len, node->name.data);


    string_copy(&s->endpoint.pname, node->pname.data, node->pname.len);
    string_copy(&s->name, node->name.data, node->name.len);
    string_copy(&s->rack, node->rack.data, node->rack.len);
    string_copy(&s->dc, node->dc.data, node->dc.len);

    s->endpoint.port = (uint16_t) node->port;
    s->is_local = node->is_local;
    s->state = node->state;
    s->processed = 0;

    array_init(&s->tokens, 1, sizeof(struct dyn_token));
    struct dyn_token *src_token = &node->token;
    struct dyn_token *dst_token = array_push(&s->tokens);
    copy_dyn_token(src_token, dst_token);

    struct sockinfo  *info =  dn_alloc(sizeof(*info)); //need to free this
    dn_resolve(&s->name, s->endpoint.port, info);
    s->endpoint.family = info->family;
    s->endpoint.addrlen = info->addrlen;
    s->endpoint.addr = (struct sockaddr *)&info->addr;  //TODOs: fix this by copying, not reference
    s->conn = NULL;

    s->next_retry = 0ULL;
    s->failure_count = 0;
    s->is_seed = node->is_seed;

    log_debug(LOG_VERB, "add a node to peer %"PRIu32" '%.*s'",
            s->idx, s->endpoint.pname.len, s->endpoint.pname.data);

    dnode_peer_relink_conn_owner(sp);

    status = topo_update_now(sp->topo);
    if (status != DN_OK)
        return status;

    status = dnode_peer_each_preconnect(s, NULL);

    return status;
}

rstatus_t
dnode_peer_add(void *rmsg)
{
    rstatus_t status;
    struct ring_msg *msg = rmsg;
    struct server_pool *sp = msg->sp;
    struct gossip_node *node = array_get(&msg->nodes, 0);
    log_debug(LOG_VVERB, "dyn: peer has an added message '%.*s'", node->name.len, node->name.data);
    status = dnode_peer_add_node(sp, node);

    return status;
}

/*
rstatus_t
dnode_peer_add(struct server_pool *sp, struct gossip_node *node)
{
    rstatus_t status;

    log_debug(LOG_VVERB, "dyn: peer has an added message '%.*s'", node->name.len, node->name.data);
    status = dnode_peer_add_node(sp, node);

    return status;
}
*/

rstatus_t
dnode_peer_replace(void *rmsg)
{
    //rstatus_t status;
    struct ring_msg *msg = rmsg;
    struct server_pool *sp = msg->sp;
    struct gossip_node *node = array_get(&msg->nodes, 0);
    log_debug(LOG_VVERB, "dyn: peer has a replaced message '%.*s'", node->name.len, node->name.data);
    struct topology *topo = sp->topo;
    struct array *peers = &topo->peers;
    struct peer *s = NULL;

    uint32_t i,nelem;
    //bool node_exist = false;
    //TODOs: use hash table here
    for (i=1, nelem = array_n(peers); i< nelem; i++) {
        struct peer * peer = (struct peer *) array_get(peers, i);
        if (string_compare(&peer->rack, &node->rack) == 0) {
            //TODOs: now only compare 1st token and support vnode later - use hash string on a tokens for comparison
            struct dyn_token *ptoken = (struct dyn_token *) array_get(&peer->tokens, 0);
            struct dyn_token *ntoken = &node->token;

            if (cmp_dyn_token(ptoken, ntoken) == 0) {
                s = peer; //found a node to replace
            }
        }
    }


    if (s != NULL) {
        log_debug(LOG_INFO, "Found an old node to replace '%.*s'", s->name.len, s->name.data);
        log_debug(LOG_INFO, "Replace with address '%.*s'", node->name.len, node->name.data);

        string_deinit(&s->endpoint.pname);
        string_deinit(&s->name);
        string_copy(&s->endpoint.pname, node->pname.data, node->pname.len);
        string_copy(&s->name, node->name.data, node->name.len);

        //TODOs: need to free the previous s->endpoint.addr?
        //if (s->endpoint.addr != NULL) {
        //   dn_free(s->endpoint.addr);
        //}

        struct sockinfo  *info =  dn_alloc(sizeof(*info)); //need to free this
        dn_resolve(&s->name, s->endpoint.port, info);
        s->endpoint.family = info->family;
        s->endpoint.addrlen = info->addrlen;
        s->endpoint.addr = (struct sockaddr *)&info->addr;  //TODOs: fix this by copying, not reference


        dnode_peer_each_disconnect(s, NULL);
        dnode_peer_each_preconnect(s, NULL);
    } else {
        log_debug(LOG_INFO, "Unable to find any node matched the token");
    }

    return DN_OK;
}


/*
rstatus_t
dnode_peer_replace(struct server_pool *sp, struct gossip_node *node)
{
    //rstatus_t status;
    log_debug(LOG_VVERB, "dyn: peer has a replaced message '%.*s'", node->name.len, node->name.data);
    struct array *peers = &sp->peers;
    struct peer *s = NULL;

    uint32_t i,nelem;
    //bool node_exist = false;
    //TODOs: use hash table here
    for (i=1, nelem = array_n(peers); i< nelem; i++) {
        struct peer * peer = (struct peer *) array_get(peers, i);
        if (string_compare(&peer->rack, &node->rack) == 0) {
            //TODOs: now only compare 1st token and support vnode later - use hash string on a tokens for comparison
            struct dyn_token *ptoken = (struct dyn_token *) array_get(&peer->tokens, 0);
            struct dyn_token *ntoken = &node->token;

            if (cmp_dyn_token(ptoken, ntoken) == 0) {
                s = peer; //found a node to replace
            }
        }
    }


    if (s != NULL) {
        log_debug(LOG_INFO, "Found an old node to replace '%.*s'", s->name.len, s->name.data);
        log_debug(LOG_INFO, "Replace with address '%.*s'", node->name.len, node->name.data);

        string_deinit(&s->endpoint.pname);
        string_deinit(&s->name);
        string_copy(&s->endpoint.pname, node->pname.data, node->pname.len);
        string_copy(&s->name, node->name.data, node->name.len);

        //TODOs: need to free the previous s->endpoint.addr?
        //if (s->endpoint.addr != NULL) {
        //   dn_free(s->endpoint.addr);
        //}

        struct sockinfo  *info =  dn_alloc(sizeof(*info)); //need to free this
        dn_resolve(&s->name, s->endpoint.port, info);
        s->endpoint.family = info->family;
        s->endpoint.addrlen = info->addrlen;
        s->endpoint.addr = (struct sockaddr *)&info->addr;  //TODOs: fix this by copying, not reference


        dnode_peer_each_disconnect(s, NULL);
        dnode_peer_each_preconnect(s, NULL);
    } else {
        log_debug(LOG_INFO, "Unable to find any node matched the token");
    }

    return DN_OK;
}
*/


void
dnode_peer_connected(struct context *ctx, struct conn *conn)
{
    struct peer *server = conn->owner;

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->connecting && !conn->connected);

    stats_pool_incr(ctx, peer_connections);

    conn->connecting = 0;
    conn->connected = 1;

    if (log_loggable(LOG_INFO)) {
        log_debug(LOG_INFO, "dyn: peer connected on sd %d to server '%.*s'", conn->p.sd,
                server->endpoint.pname.len, server->endpoint.pname.data);
    }
}

static void
dnode_peer_ok(struct context *ctx, struct conn *conn)
{
    struct peer *server = conn->owner;

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->connected);

    if (server->failure_count != 0) {
        log_debug(LOG_VERB, "dyn: reset peer '%.*s' failure count from %"PRIu32
                " to 0", server->endpoint.pname.len, server->endpoint.pname.data,
                server->failure_count);
        server->failure_count = 0;
        server->next_retry = 0ULL;
    }
}

static struct conn *
dnode_peer_active_conn(struct context *ctx, struct peer *peer)
{
    if (peer->state == DOWN) {
        log_debug(LOG_WARN, "Detecting peer '%.*s' is set with state Down", peer->name);
        return NULL;
    }
    struct conn *p_conn = dnode_peer_conn(peer);
    if (peer->state == RESET) {
        log_debug(LOG_WARN, "Detecting peer '%.*s' is set with state Reset. Disconnecting and Reconnecting", peer->name);
        // close existing connection and recreate it.
        dnode_peer_close_socket(p_conn);
    }

    rstatus_t status = conn_connect(ctx, p_conn);
    if (status != DN_OK) {
        p_conn->err = EHOSTDOWN;
        dnode_peer_close(ctx, p_conn);
        return NULL;
    }

    peer->state = NORMAL;
    return p_conn;
}

rstatus_t
dnode_peer_pool_preconnect(struct context *ctx)
{
    rstatus_t status;
    struct server_pool *sp = &ctx->pool;
    struct topology *topo = sp->topo;

    if (!sp->preconnect) {
        return DN_OK;
    }

    status = array_each(&topo->peers, dnode_peer_each_preconnect, NULL);
    if (status != DN_OK) {
        return status;
    }

    return DN_OK;
}


void
dnode_peer_pool_disconnect(struct context *ctx)
{
    rstatus_t status;
    struct server_pool *sp = &ctx->pool;
    struct topology *topo = sp->topo;

    status = array_each(&topo->peers, dnode_peer_each_disconnect, NULL);
    IGNORE_RET_VAL(status);
}

/*
static void
dnode_peer_pool_deinit(struct array *server_pool)
{
    uint32_t i, npool;

    for (i = 0, npool = array_n(server_pool); i < npool; i++) {
        struct server_pool *sp;

        sp = array_pop(server_pool);
        ASSERT(sp->p_conn == NULL);
        //fixe me to use different variables
        ASSERT(TAILQ_EMPTY(&sp->c_conn_q) && sp->dn_conn_q == 0);


        dnode_peer_deinit(&sp->peers);
        array_each(&sp->datacenters, datacenter_destroy, NULL);
        array_deinit(&sp->datacenters);

        log_debug(LOG_DEBUG, "dyn: deinit peer pool %"PRIu32" '%.*s'", sp->idx,
                sp->name.len, sp->name.data);
    }

    //array_deinit(server_pool);

    log_debug(LOG_DEBUG, "deinit %"PRIu32" peer pools", npool);
}
*/

static struct msg *
dnode_rsp_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    return rsp_recv_next(ctx, conn, alloc);
}

static bool
dnode_rsp_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    if (msg_empty(msg)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "dyn: filter empty rsp %"PRIu64" on s %d", msg->id,
                conn->p.sd);
        rsp_put(msg);
        return true;
    }

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL) {
        log_debug(LOG_INFO, "dyn: filter stray rsp %"PRIu64" len %"PRIu32" on s %d expect_datastore_reply %d",
                msg->id, msg->mlen, conn->p.sd, msg->expect_datastore_reply);
        rsp_put(msg);
        return true;
    }
    ASSERT(pmsg->peer == NULL);
    ASSERT(pmsg->request && !pmsg->done);

    return false;
}

static void
dnode_rsp_forward_stats(struct context *ctx, struct msg *msg)
{
    ASSERT(!msg->request);
    stats_pool_incr(ctx, peer_responses);
    stats_pool_incr_by(ctx, peer_response_bytes, msg->mlen);
}

static void
dnode_rsp_swallow(struct context *ctx, struct conn *peer_conn,
                  struct msg *req, struct msg *rsp)
{
    conn_dequeue_outq(ctx, peer_conn, req);
    req->done = 1;
    log_debug(LOG_VERB, "conn %p swallow %p", peer_conn, req);
    if (rsp) {
        log_debug(LOG_INFO, "dyn: swallow rsp %"PRIu64" len %"PRIu32" of req "
                  "%"PRIu64" on s %d", rsp->id, rsp->mlen, req->id,
                  peer_conn->p.sd);
        rsp_put(rsp);
    }
    req_put(req);
}

static void
peer_mismatch_stats_incr(struct conn *peer_conn, struct conn *c_conn, struct msg *req,
                   struct msg *rsp)
{
    // Report a mismatch and try to rectify
    log_error("MISMATCH: dnode %s %d rsp_dmsg_id %u req %u:%u dnode rsp %u:%u",
            conn_get_type_string(peer_conn),
            peer_conn->p.sd, rsp->dmsg->id, req->id, req->parent_id, rsp->id,
            rsp->parent_id);
    if (c_conn && conn_to_ctx(c_conn))
        stats_pool_incr(conn_to_ctx(c_conn), peer_mismatch_requests);
    return;
}

static void
update_remote_region_stats(struct context *ctx, struct conn *peer_conn, struct msg *req)
{
    if (!peer_conn->same_dc && req->remote_region_send_time) {
        struct stats *st = ctx->stats;
        usec_t delay = dn_usec_now() - req->remote_region_send_time;
        histo_add(&st->cross_region_histo, delay);
    }
}

static void
forward_response_upstream(struct conn *c_conn, struct msg *req, struct msg *rsp)
{
    // Here, if the client belongs to different thread, forward the response
    // to that thread
    rsp->req_id = req->parent_id ? req->parent_id : req->id;
    rsp->client_conn = c_conn;
    if (c_conn->ptctx != g_ptctx) {
        thread_ctx_forward_rsp(c_conn->ptctx, rsp);
        return;
    }
    rstatus_t status = conn_handle_response(c_conn, rsp);
    IGNORE_RET_VAL(status);
    if (req->swallow) {
        log_info("swallow request %d:%d", req->id, req->parent_id);
        req_put(req);
    }
}

/* Description: link data from a peer connection to a client-facing connection
 * peer_conn: a peer connection
 * msg      : msg with data from the peer connection after parsing
 */
static void
dnode_rsp_forward_match(struct context *ctx, struct conn *peer_conn, struct msg *rsp)
{
    struct msg *req;

    req = TAILQ_FIRST(&peer_conn->omsg_q);

    /* if client consistency is dc_one forward the response from only the
       local node. Since dyn_dnode_peer is always a remote node, drop the rsp */
    if (req->consistency == DC_ONE) {
        if (req->swallow) {
            dnode_rsp_swallow(ctx, peer_conn, req, rsp);
            return;
        }
        //log_warn("req %d:%d with DC_ONE consistency is not being swallowed");
    }

    /* if client consistency is dc_quorum, forward the response from only the
       local region/DC. */
    if ((req->consistency == DC_QUORUM) && !peer_conn->same_dc) {
        if (req->swallow) {
            dnode_rsp_swallow(ctx, peer_conn, req, rsp);
            return;
        }
        //log_warn("req %d:%d with DC_QUORUM consistency is not being swallowed");
    }

    log_debug(LOG_DEBUG, "DNODE RSP RECEIVED %s %d dmsg->id %u req %u:%u rsp %u:%u, ",
              conn_get_type_string(peer_conn),
              peer_conn->p.sd, rsp->dmsg->id,
              req->id, req->parent_id, rsp->id, rsp->parent_id);
    ASSERT(req != NULL && req->peer == NULL);
    ASSERT(req->request && !req->done);

    if (log_loggable(LOG_VVERB)) {
        loga("Dumping content for response:   ");
        msg_dump(rsp);

        loga("rsp id %d", rsp->id);

        loga("Dumping content for request:");
        msg_dump(req);

        loga("req id %d", req->id);
    }

    conn_dequeue_outq(ctx, peer_conn, req);

    dnode_rsp_forward_stats(ctx, rsp);
    struct conn *c_conn = req->owner;
    ASSERT_LOG((c_conn->p.type == CONN_CLIENT) ||
               (c_conn->p.type == CONN_DNODE_PEER_CLIENT),
               "c_conn type %s", conn_get_type_string(c_conn));
    req->done = 1;
    log_info("c_conn:%p %d:%d <-> %d:%d", c_conn, req->id, req->parent_id,
             rsp->id, rsp->parent_id);
    /* establish rsp <-> req (response <-> request) link */
    req->peer = rsp;
    rsp->peer = req;

    g_pre_coalesce(rsp);

    forward_response_upstream(c_conn, req, rsp);
}

/* There are chances that the request to the remote peer or its response got dropped.
 * Hence we may not always receive a response to the request at the head of the FIFO.
 * Hence what we do is we mark that request as errored and move on the next one
 * in the outgoing queue. This works since we always have message ids in monotonically
 * increasing order.
 */
static void
dnode_rsp_forward(struct context *ctx, struct conn *peer_conn, struct msg *rsp)
{
    struct msg *req;
    struct conn *c_conn;

    ASSERT(peer_conn->p.type == CONN_DNODE_PEER_SERVER);

    /* response from a peer implies that peer is ok and heartbeating */
    dnode_peer_ok(ctx, peer_conn);

    /* dequeue peer message (request) from peer conn */
    while (true) {
        req = TAILQ_FIRST(&peer_conn->omsg_q);
        log_debug(LOG_VERB, "dnode_rsp_forward entering req %p rsp %p...", req, rsp);
        c_conn = req->owner;

        if (req->id == rsp->dmsg->id) {
            update_remote_region_stats(ctx, peer_conn, req);
            dnode_rsp_forward_match(ctx, peer_conn, rsp);
            return;
        }
        peer_mismatch_stats_incr(peer_conn, c_conn, req, rsp);

        // TODO : should you be worried about message id getting wrapped around to 0?
        if (rsp->dmsg->id < req->id) {
            // We received a response from the past. This indeed proves out of order
            // responses. A blunder to the architecture. Log it and drop the response.
            log_error("MISMATCH: received response from the past. Dropping it");
            rsp_put(rsp);
            return;
        }

        if (req->consistency == DC_ONE) {
            if (req->swallow) {
                // swallow the request and move on the next one
                dnode_rsp_swallow(ctx, peer_conn, req, NULL);
                continue;
            }
        }

        if ((req->consistency == DC_QUORUM) && !peer_conn->same_dc) {
            if (req->swallow) {
                // swallow the request and move on the next one
                dnode_rsp_swallow(ctx, peer_conn, req, NULL);
                continue;
            }
            log_warn("req %d:%d with DC_QUORUM consistency is not being swallowed");
        }

        log_error("MISMATCHED DNODE RSP RECEIVED %s %d dmsg->id %u req %u:%u rsp %u:%u, skipping....",
                  conn_get_type_string(peer_conn),
                 peer_conn->p.sd, rsp->dmsg->id,
                 req->id, req->parent_id, rsp->id, rsp->parent_id);
        ASSERT(req != NULL && req->peer == NULL);
        ASSERT(req->request && !req->done);

        if (log_loggable(LOG_VVERB)) {
            loga("skipping req:   ");
            msg_dump(req);
        }

        conn_dequeue_outq(ctx, peer_conn, req);
        req->done = 1;

        // Create an appropriate response for the request so its propagated up;
        struct msg *err_rsp = msg_get(peer_conn, false, __FUNCTION__);
        err_rsp->error = req->error = 1;
        err_rsp->err = req->err = BAD_FORMAT;
        err_rsp->dyn_error = req->dyn_error = BAD_FORMAT;
        err_rsp->dmsg = dmsg_get();
        err_rsp->dmsg->id = req->id;
        log_debug(LOG_VERB, "%p <-> %p", req, err_rsp);
        /* establish err_rsp <-> req (response <-> request) link */
        req->peer = err_rsp;
        err_rsp->peer = req;

        log_error("Peer connection s %d skipping request %u:%u, dummy err_rsp %u:%u",
                 peer_conn->p.sd, req->id, req->parent_id, err_rsp->id, err_rsp->parent_id);
        forward_response_upstream(c_conn, req, err_rsp);
    }
}

static void
dnode_rsp_recv_done(struct context *ctx, struct conn *conn,
                    struct msg *msg, struct msg *nmsg)
{
    log_debug(LOG_VERB, "dnode_rsp_recv_done entering ...");

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);
    ASSERT(msg != NULL && conn->rmsg == msg);
    ASSERT(!msg->request);
    ASSERT(msg->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->request);

    if (log_loggable(LOG_VVERB)) {
       loga("Dumping content for msg:   ");
       msg_dump(msg);

       if (nmsg != NULL) {
          loga("Dumping content for nmsg :");
          msg_dump(nmsg);
       }
    }

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

    if (dnode_rsp_filter(ctx, conn, msg)) {
        return;
    }
    dnode_rsp_forward(ctx, conn, msg);
}



//TODOs: fix this in using dmsg_write with encrypted msgs
//         It is not in use now.
/*
void
dnode_rsp_gos_syn(struct context *ctx, struct conn *p_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;

    //ASSERT(p_conn->p.type == CONN_DNODE_PEER_CLIENT);

    //add messsage
    struct mbuf *nbuf = mbuf_get();
    if (nbuf == NULL) {
        log_debug(LOG_ERR, "Error happened in calling mbuf_get");
        return;  //TODOs: need to address this further
    }

    msg->done = 1;

    //TODOs: need to free the old msg object
    pmsg = msg_get(p_conn, 0, msg->redis);
    if (pmsg == NULL) {
        mbuf_put(nbuf);
        return;
    }

    pmsg->done = 1;
    // establish msg <-> pmsg (response <-> request) link
    msg->peer = pmsg;
    pmsg->peer = msg;
    g_pre_coalesce(pmsg);
    pmsg->owner = p_conn;

    //dyn message's meta data
    uint64_t msg_id = msg->dmsg->id;
    uint8_t type = GOSSIP_SYN_REPLY;
    struct string data = string("SYN_REPLY_OK");

    dmsg_write(nbuf, msg_id, type, p_conn, 0);
    mbuf_insert(&pmsg->mhdr, nbuf);

    //dnode_rsp_recv_done(ctx, p_conn, msg, pmsg);
    //should we do this?
    //conn_dequeue_outq(ctx, s_conn, pmsg);



     //p_conn->enqueue_outq(ctx, p_conn, pmsg);
     //if (TAILQ_FIRST(&p_conn->omsg_q) != NULL && req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
     //   status = conn_add_out(p_conn);
     //   if (status != DN_OK) {
     //      p_conn->err = errno;
     //   }
     //}


    if (TAILQ_FIRST(&p_conn->omsg_q) != NULL && req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
        status = conn_add_out(p_conn);
        if (status != DN_OK) {
            p_conn->err = errno;
        }
    }

    //dnode_rsp_forward_stats(ctx, msg);
}

*/
static struct msg *
dnode_req_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    // TODO: Not the right way to use time_t directly. FIXME
    uint32_t now = (uint32_t)time(NULL);
    //throttling the sending traffics here
    if (!conn->same_dc) {
        if (conn->last_sent != 0) {
            uint32_t elapsed_time = now - conn->last_sent;
            uint32_t earned_tokens = elapsed_time * msgs_per_sec();
            conn->avail_tokens = (conn->avail_tokens + earned_tokens) < msgs_per_sec()?
                    conn->avail_tokens + earned_tokens : msgs_per_sec();

        }

        conn->last_sent = now;
        if (conn->avail_tokens > 0) {
            conn->avail_tokens--;
            return req_send_next(ctx, conn);
        }

        //requeue
        status = thread_ctx_add_out(conn->ptctx, conn_get_pollable(conn));
        IGNORE_RET_VAL(status);

        return NULL;
    }

    conn->last_sent = now;
    return req_send_next(ctx, conn);
}

static void
dnode_req_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    if (log_loggable(LOG_DEBUG)) {
       log_debug(LOG_VERB, "dnode_req_send_done entering!!!");
    }
    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);
    // TODO: crashes because dmsg is NULL :(
    /*log_debug(LOG_DEBUG, "DNODE REQ SEND %s %d dmsg->id %u",
              conn_get_type_string(conn), conn->p.sd, msg->dmsg->id);*/

    if (!conn->same_dc)
        msg->remote_region_send_time = dn_usec_now();
    req_send_done(ctx, conn, msg);
}

static void
dnode_req_peer_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    log_debug(LOG_VERB, "conn %p enqueue inq %d:%d calling req_server_enqueue_imsgq",
              conn, msg->id, msg->parent_id);
    if (msg->expect_datastore_reply) {
        pthread_ctx ptctx = conn->ptctx;
        msg_tmo_insert(&ptctx->tmo, conn, msg);
    }
    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p enqueue inq %d:%d", conn, msg->id, msg->parent_id);

    conn->imsg_count++;
    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_in_queue, conn->imsg_count);
        stats_pool_incr(ctx, peer_in_queue);
        stats_pool_incr_by(ctx, peer_in_queue_bytes, msg->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_in_queue, conn->imsg_count);
        stats_pool_incr(ctx, remote_peer_in_queue);
        stats_pool_incr_by(ctx, remote_peer_in_queue_bytes, msg->mlen);
    }

}

static void
dnode_req_peer_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    TAILQ_REMOVE(&conn->imsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p dequeue inq %d:%d", conn, msg->id, msg->parent_id);

    conn->imsg_count--;
    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_in_queue, conn->imsg_count);
        stats_pool_decr(ctx, peer_in_queue);
        stats_pool_decr_by(ctx, peer_in_queue_bytes, msg->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_in_queue, conn->imsg_count);
        stats_pool_decr(ctx, remote_peer_in_queue);
        stats_pool_decr_by(ctx, remote_peer_in_queue_bytes, msg->mlen);
    }
}

static void
dnode_req_peer_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, s_tqe);
    log_debug(LOG_VERB, "conn %p enqueue outq %d:%d", conn, msg->id, msg->parent_id);

    conn->omsg_count++;
    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_out_queue, conn->omsg_count);
        stats_pool_incr(ctx, peer_out_queue);
        stats_pool_incr_by(ctx, peer_out_queue_bytes, msg->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_out_queue, conn->omsg_count);
        stats_pool_incr(ctx, remote_peer_out_queue);
        stats_pool_incr_by(ctx, remote_peer_out_queue_bytes, msg->mlen);
    }
}

static void
dnode_req_peer_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    ASSERT(msg->request);
    ASSERT(conn->p.type == CONN_DNODE_PEER_SERVER);

    pthread_ctx ptctx = conn->ptctx;
    msg_tmo_delete(&ptctx->tmo, msg);

    TAILQ_REMOVE(&conn->omsg_q, msg, s_tqe);
    log_debug(LOG_VVERB, "conn %p dequeue outq %p", conn, msg);

    conn->omsg_count--;
    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_out_queue, conn->omsg_count);
        stats_pool_decr(ctx, peer_out_queue);
        stats_pool_decr_by(ctx, peer_out_queue_bytes, msg->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_out_queue, conn->omsg_count);
        stats_pool_decr(ctx, remote_peer_out_queue);
        stats_pool_decr_by(ctx, remote_peer_out_queue_bytes, msg->mlen);
    }
}

static void
dnode_peer_req_forward_stats(struct context *ctx, struct msg *msg)
{
    ASSERT(msg->request);
    stats_pool_incr(ctx, peer_requests);
    stats_pool_incr_by(ctx, peer_request_bytes, msg->mlen);
}

void
dnode_req_forward_error(struct context *ctx, struct conn *p_conn, struct msg *msg,
                        err_t error)
{
    rstatus_t status;

    msg->done = msg->error = 1;
    msg->err = error;

    if (!msg->expect_datastore_reply || msg->swallow) {
        req_put(msg);
        return;
    }

    if (!p_conn)
        return;
    pthread_ctx ptctx = p_conn->ptctx;
    msg_tmo_delete(&ptctx->tmo, msg);
    ASSERT(p_conn->p.type == CONN_DNODE_PEER_CLIENT);
    if (req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
        status = thread_ctx_add_out(p_conn->ptctx, conn_get_pollable(p_conn));
        if (status != DN_OK) {
            p_conn->err = errno;
        }
    }
}

/* Forward request msg to peer on rack rack.*/
/* Requirements: peer is not a local node. So it will put the msg on the outQ
 */
rstatus_t
dnode_peer_req_forward(struct context *ctx,
                       struct peer *peer, struct msg *msg)
{
    if (g_ptctx != peer->ptctx) {
        // This peer belongs to a different thread, forward the msg to that thread
        msg->dst_peer = peer;
        THROW_STATUS(thread_ctx_forward_req(peer->ptctx, msg));
    }
    struct conn *p_conn = dnode_peer_active_conn(ctx, peer);
    if (p_conn == NULL)
    {
        // No active connection to this peer. Mark the message as err
        // TODO: SHAILESH How does the client connection know that it is supposed to
        // read from its outqueue i.e a message is done??
        dnode_req_forward_error(ctx, NULL, msg, DN_EHOST_DOWN);
        return DN_EHOST_DOWN;
    }

    /* enqueue the message (request) into peer inq */
    rstatus_t status = thread_ctx_add_out(p_conn->ptctx, conn_get_pollable(p_conn));
    if (status != DN_OK) {
        log_warn("Error on connection to '%.*s'. Marking it to RESET", peer->name);
        dnode_req_forward_error(ctx, p_conn, msg, DN_ENOMEM);
        peer->state = RESET;
        p_conn->err = errno;
        return status;
    }

    struct mbuf *header_buf = mbuf_get();
    if (header_buf == NULL) {
        loga("Unable to obtain an mbuf for dnode msg's header!");
        dnode_req_forward_error(ctx, p_conn, msg, DN_ENOMEM);
        return DN_ENOMEM;
    }

    dmsg_type_t msg_type = !p_conn->same_dc ? DMSG_REQ_FORWARD : DMSG_REQ;

    if (p_conn->dnode_secured) {
        log_debug(LOG_VVERB, "AES encryption key: %s\n", base64_encode(p_conn->aes_key, AES_KEYLEN));

        // TODO: SHAILESH why #define, use a config variable
        if (ENCRYPTION) {
            status = dyn_aes_encrypt_msg(msg, p_conn->aes_key);
            if (status == DN_ERROR) {
                loga("OOM to obtain an mbuf for encryption!");
                mbuf_put(header_buf);
                dnode_req_forward_error(ctx, p_conn, msg, DN_ENOMEM);
                return status;
            }

            log_debug(LOG_VVERB, "#encrypted bytes : %d", status);

            dmsg_write(header_buf, msg->id, msg_type, p_conn, msg_length(msg));
        } else {
            log_debug(LOG_VVERB, "no encryption on the msg payload");
            dmsg_write(header_buf, msg->id, msg_type, p_conn, msg_length(msg));
        }

    } else {
        //write dnode header
        dmsg_write(header_buf, msg->id, msg_type, p_conn, msg_length(msg));
    }

    mbuf_insert_head(&msg->mhdr, header_buf);

    if (log_loggable(LOG_VVERB)) {
        log_hexdump(LOG_VVERB, header_buf->pos, mbuf_length(header_buf), "dyn message header: ");
        msg_dump(msg);
    }

    conn_enqueue_inq(ctx, p_conn, msg);

    dnode_peer_req_forward_stats(ctx, msg);

    return DN_OK;
}



struct conn_ops dnode_peer_ops = {
    msg_recv,
    dnode_rsp_recv_next,
    dnode_rsp_recv_done,
    msg_send,
    dnode_req_send_next,
    dnode_req_send_done,
    dnode_peer_close,
    dnode_peer_active,
    dnode_peer_ref,
    dnode_peer_unref,
    dnode_req_peer_enqueue_imsgq,
    dnode_req_peer_dequeue_imsgq,
    dnode_req_peer_enqueue_omsgq,
    dnode_req_peer_dequeue_omsgq,
    conn_cant_handle_response
};

void
init_dnode_peer_conn(struct conn *conn)
{
    conn->p.type = CONN_DNODE_PEER_SERVER;
    conn->ops = &dnode_peer_ops;
}

static int
rack_name_cmp(const void *t1, const void *t2)
{
    const struct rack *s1 = t1, *s2 = t2;

    return string_compare(s1->name, s2->name);
}

// The idea here is to have a designated rack in each remote region to replicate
// data to. This is used to replicate writes to remote regions
void
preselect_remote_rack_for_replication(struct context *ctx)
{
    struct server_pool *sp = &ctx->pool;
    struct topology *topo = sp->topo;
    uint32_t dc_cnt = array_n(&topo->datacenters);
    uint32_t dc_index = 0, rack_cnt = 0;
    uint32_t my_rack_index = 0;
    struct datacenter *dc = NULL;
    struct rack *rack = NULL;

    // Get the index of my rack in the current dc.
    // While at it, also sort other DC racks.
    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {
        dc = array_get(&topo->datacenters, dc_index);
        // sort the racks.
        array_sort(&dc->racks, rack_name_cmp);

        // skip if its not a local dc.
        if (string_compare(dc->name, &sp->dc_name) == 0)
            continue;

        // if the dc is a local dc, get the rack_idx
        uint32_t rack_index;
        rack_cnt = array_n(&dc->racks);
        for(rack_index = 0; rack_index < rack_cnt; rack_index++) {
            rack = array_get(&dc->racks, rack_index);
            if (string_compare(rack->name, &sp->rack_name) == 0) {
                my_rack_index = rack_index;
                log_notice("my rack index %u", my_rack_index);
                break;
            }
        }
    }

    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {
        dc = array_get(&topo->datacenters, dc_index);
        dc->preselected_rack_for_replication = NULL;

        // Nothing to do for local DC, continue;
        if (string_compare(dc->name, &sp->dc_name) == 0)
            continue;

        // if no racks, keep preselected_rack_for_replication as NULL
        rack_cnt = array_n(&dc->racks);
        if (rack_cnt == 0)
            continue;

        // if the dc is a remote dc, get the rack at rack_idx
        // use that as preselected rack for replication
        uint32_t this_rack_index = my_rack_index % rack_cnt;
        dc->preselected_rack_for_replication = array_get(&dc->racks,
                                                         this_rack_index);
        log_notice("Selected rack %.*s for replication to remote region %.*s",
                   dc->preselected_rack_for_replication->name->len,
                   dc->preselected_rack_for_replication->name->data,
                   dc->name->len, dc->name->data);
    }
}
