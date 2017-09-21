/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#include "dyn_core.h"
#include "dyn_conf.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"
#include "dyn_node_snitch.h"
#include "dyn_task.h"
#include "dyn_token.h"
#include "dyn_vnode.h"

static rstatus_t dnode_peer_pool_update(struct server_pool *pool);

static void
dnode_peer_ref(struct conn *conn, void *owner)
{
    struct node *peer = owner;

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->owner == NULL);

    conn->family = peer->endpoint.family;
    conn->addrlen = peer->endpoint.addrlen;
    conn->addr = peer->endpoint.addr;
    string_duplicate(&conn->pname, &peer->endpoint.pname);

    conn->owner = peer;

    conn->dnode_secured = peer->is_secure;
    conn->crypto_key_sent = 0;
    conn->same_dc = peer->is_same_dc;

    if (log_loggable(LOG_VVERB)) {
       log_debug(LOG_VVERB, "dyn: ref peer conn %p owner %p into '%.*s", conn, peer,
                   peer->endpoint.pname.len, peer->endpoint.pname.data);
    }
}

static void
dnode_peer_unref(struct conn *conn)
{
    struct node *peer;

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->owner != NULL);
    conn_event_del_conn(conn);

    peer = conn->owner;
    conn->owner = NULL;
    // if this is the last connection, mark the peer as down.
    if (conn_pool_active_count(peer->conn_pool) ==  1) {
        log_notice("Marking %M as down", peer);
        peer->state = DOWN;
    }

    log_debug(LOG_VVERB, "dyn: unref peer conn %p owner %p from '%.*s'", conn, peer,
              peer->endpoint.pname.len, peer->endpoint.pname.data);
}

msec_t
dnode_peer_timeout(struct msg *req, struct conn *conn)
{
    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    struct node *peer = conn->owner;
    struct server_pool *pool = peer->owner;
    msec_t additional_timeout = 0;

   if (peer->is_same_dc)
       additional_timeout = 200;
   else
       additional_timeout = 5000;

   if (!req->is_read) //make sure write request has a longer timeout so we almost never want to drop it
       additional_timeout += 20000;

    return pool->timeout + additional_timeout;
}

static bool
dnode_peer_active(struct conn *conn)
{
    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    if (!TAILQ_EMPTY(&conn->imsg_q)) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    if (conn->rmsg != NULL) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    if (conn->smsg != NULL) {
        log_debug(LOG_VVERB, "dyn: s %d is active", conn->sd);
        return true;
    }

    log_debug(LOG_VVERB, "dyn: s %d is inactive", conn->sd);

    return false;
}

static int
_print_node(FILE *stream, const struct object *obj)
{
    ASSERT(obj->type == OBJ_NODE);
    struct node *node = (struct node *)obj;
    return fprintf(stream, "<NODE %p %.*s %.*s %.*s secured:%d>",
            node, node->name.len, node->name.data, node->dc.len, node->dc.data,
            node->rack.len, node->rack.data, node->is_secure);
}

static void
_init_peer_struct(struct node *node)
{
    memset(node, 0, sizeof(*node));
    init_object(&node->obj, OBJ_NODE, _print_node);
}

static rstatus_t
dnode_peer_add_local(struct server_pool *pool, struct node *self)
{
    ASSERT(self != NULL);

    _init_peer_struct(self);
    self->owner = pool;
    
    // Initialize the endpoint
    struct string *p_pname = &pool->dnode_proxy_endpoint.pname;
    string_duplicate(&self->endpoint.pname, p_pname);
    self->endpoint.port = pool->dnode_proxy_endpoint.port;
    self->endpoint.family = pool->dnode_proxy_endpoint.family;
    self->endpoint.addrlen = pool->dnode_proxy_endpoint.addrlen;
    self->endpoint.addr = pool->dnode_proxy_endpoint.addr;

    uint8_t *p = p_pname->data + p_pname->len - 1;
    uint8_t *start = p_pname->data;
    string_copy(&self->name, start, (uint32_t)(dn_strrchr(p, start, ':') - start));

    string_duplicate(&self->rack, &pool->rack);
    string_duplicate(&self->dc, &pool->dc);
    self->tokens = pool->tokens;


    self->is_local = true;
    self->is_same_dc = true;
    self->processed = 0;
    self->is_secure = false;
    self->state = JOINING;

    log_notice("Initialized local peer: %M", self);

    return DN_OK;
}

void
dnode_peer_deinit(struct array *nodes)
{
    uint32_t i, nnode;

    for (i = 0, nnode = array_n(nodes); i < nnode; i++) {
        struct node *s = *(struct node **)array_pop(nodes);
        if (s->conn_pool) {
            conn_pool_destroy(s->conn_pool);
            s->conn_pool = NULL;
        }
    }
    array_deinit(nodes);
}

static rstatus_t
dnode_peer_pool_run(struct server_pool *pool)
{
    ASSERT(array_n(&pool->peers) != 0);
    return vnode_update(pool);
}

static void
dnode_create_connection_pool(struct server_pool *sp, struct node *peer)
{
    if (peer->conn_pool)
        return;
    struct context *ctx = sp->ctx;
    if (!peer->is_local) {
        uint8_t max_connections = peer->is_same_dc ? sp->max_local_peer_connections :
                                                     sp->max_remote_peer_connections;
        peer->conn_pool = conn_pool_create(ctx, peer, max_connections,
                                           init_dnode_peer_conn, sp->server_failure_limit,
                                           MAX_WAIT_BEFORE_RECONNECT_IN_SECS);
    }

}

static rstatus_t
dnode_initialize_peer_each(void *elem, void *data1, void *data2)
{
    struct context *ctx = data1;
    struct server_pool *sp = &ctx->pool;

    struct conf_server *cseed = elem;
    ASSERT(cseed->valid);
    struct array *peers = data2;
    struct node **sptr = array_push(peers);
    struct node *s = dn_zalloc(sizeof(struct node));
    if (!s || !sptr)
        return DN_ENOMEM;
    *sptr = s;
    _init_peer_struct(s);

    s->idx = array_idx(peers, sptr);
    s->owner = sp;

    string_copy(&s->endpoint.pname, cseed->pname.data, cseed->pname.len);
    s->endpoint.port = (uint16_t)cseed->port;
    s->endpoint.family = cseed->info.family;
    s->endpoint.addrlen = cseed->info.addrlen;
    s->endpoint.addr = (struct sockaddr *)&cseed->info.addr;  //TODOs: fix this by copying, not reference

    uint8_t *p = cseed->name.data + cseed->name.len - 1;
    uint8_t *start = cseed->name.data;
    string_copy(&s->name, start, (uint32_t)(dn_strrchr(p, start, ':') - start));

    string_copy(&s->rack, cseed->rack.data, cseed->rack.len);
    string_copy(&s->dc, cseed->dc.data, cseed->dc.len);

    s->tokens = cseed->tokens;

    s->is_local = false;
    s->is_same_dc = (string_compare(&sp->dc, &s->dc) == 0);
    s->processed = 0;

    s->is_secure = is_secure(sp->secure_server_option, &sp->dc, &sp->rack, &s->dc,
                             &s->rack);
    s->state = DOWN;//assume peers are down initially
    dnode_create_connection_pool(sp, s);
    log_notice("added peer %M", s);

    return DN_OK;
}

rstatus_t
dnode_initialize_peers(struct context *ctx)
{
    struct server_pool *sp = &ctx->pool;
    struct array *conf_seeds = &sp->conf_pool->dyn_seeds;

    struct array *peers = &sp->peers;
    uint32_t nseed;

    /* initialize peers list = seeds list */
    ASSERT(array_n(peers) == 0);

    /* init seeds list */
    nseed = array_n(conf_seeds);

    log_debug(LOG_INFO, "Adding local node to the peer list");

    THROW_STATUS(array_init(peers, nseed + 1, sizeof(struct node *)));

    // Add self node
    struct node **selfptr = array_push(peers);
    struct node *self = dn_zalloc(sizeof(struct node));
    if (!self || !selfptr)
        return DN_ENOMEM;
    *selfptr = self;
    THROW_STATUS(dnode_peer_add_local(sp, self));

    // Add the peer nodes
    THROW_STATUS(array_each_2(conf_seeds, dnode_initialize_peer_each, ctx, peers));

    ASSERT(array_n(peers) == (nseed + 1));

    THROW_STATUS(dnode_peer_pool_run(sp));

    log_debug(LOG_DEBUG, "init %"PRIu32" peers in pool %M'", nseed, sp);

    return DN_OK;
}

static struct conn *
dnode_peer_conn(struct node *peer, int tag)
{
    return conn_pool_get(peer->conn_pool, tag);
}

static void
dnode_peer_ack_err(struct context *ctx, struct conn *conn, struct msg *req)
{
    if ((req->swallow && !req->expect_datastore_reply) || // no reply
        (req->swallow && (req->consistency == DC_ONE)) || // dc one
        (req->swallow && ((req->consistency == DC_QUORUM) || (req->consistency == DC_SAFE_QUORUM)) // remote dc request
                      && (!conn->same_dc)) ||
        (req->owner == conn)) // a gossip message that originated on this conn
    {
        log_info("%M Closing, swallow req %u:%u len %"PRIu32" type %d",
                 conn, req->id, req->parent_id, req->mlen, req->type);
        req_put(req);
        return;
    }
    struct conn *c_conn = req->owner;
    // At other connections, these responses would be swallowed.
    ASSERT_LOG((c_conn->type == CONN_CLIENT) ||
               (c_conn->type == CONN_DNODE_PEER_CLIENT),
               "conn:%M c_conn:%M, req %d:%d", conn, c_conn, req->id, req->parent_id);

    // Create an appropriate response for the request so its propagated up;
    // This response gets dropped in rsp_make_error anyways. But since this is
    // an error path its ok with the overhead.
    struct msg *rsp = msg_get(conn, false, __FUNCTION__);
    req->done = 1;
    rsp->peer = req;
    rsp->is_error = req->is_error = 1;
    rsp->error_code = req->error_code = conn->err;
    rsp->dyn_error_code = req->dyn_error_code = PEER_CONNECTION_REFUSE;
    rsp->dmsg = dmsg_get();
    rsp->dmsg->id =  req->id;

    log_info("%M Closing req %u:%u len %"PRIu32" type %d %c %s", conn,
             req->id, req->parent_id, req->mlen, req->type,
             conn->err ? ':' : ' ', conn->err ? strerror(conn->err): " ");
    rstatus_t status =
            conn_handle_response(c_conn, req->parent_id ? req->parent_id : req->id,
                                 rsp);
    IGNORE_RET_VAL(status);
    if (req->swallow)
        req_put(req);
}


static void
dnode_peer_failure(struct context *ctx, struct node *peer)
{
    struct server_pool *pool = peer->owner;
    conn_pool_notify_conn_errored(peer->conn_pool);
    stats_pool_set_ts(ctx, peer_ejected_at, (int64_t)dn_msec_now());

    if (dnode_peer_pool_update(peer->owner) != DN_OK) {
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
    struct msg *req, *nmsg; /* current and next message */

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    struct node *peer = conn->owner;

    dnode_peer_close_stats(ctx, conn);

    if (conn->sd < 0) {
        conn_unref(conn);
        conn_put(conn);
        dnode_peer_failure(ctx, peer);
        return;
    }
    uint32_t out_counter = 0;
    for (req = TAILQ_FIRST(&conn->omsg_q); req != NULL; req = nmsg) {
        nmsg = TAILQ_NEXT(req, s_tqe);

        /* dequeue the message (request) from peer outq */
        conn_dequeue_outq(ctx, conn, req);
        dnode_peer_ack_err(ctx, conn, req);
        out_counter++;
    }

    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    uint32_t in_counter = 0;
    for (req = TAILQ_FIRST(&conn->imsg_q); req != NULL; req = nmsg) {
        nmsg = TAILQ_NEXT(req, s_tqe);

        /* dequeue the message (request) from peer inq */
        conn_dequeue_inq(ctx, conn, req);
        // We should also remove the req from the timeout rbtree.
        // for outq, its already taken care of
        msg_tmo_delete(req);
        dnode_peer_ack_err(ctx, conn, req);
        in_counter++;

        if (conn->same_dc)
            stats_pool_incr(ctx, peer_dropped_requests);
        else
            stats_pool_incr(ctx, remote_peer_dropped_requests);
    }

    ASSERT(TAILQ_EMPTY(&conn->imsg_q));

    log_warn("%M Closing, Dropped %u outqueue & %u inqueue requests",
             conn, out_counter, in_counter);

    struct msg *rsp = conn->rmsg;
    if (rsp != NULL) {
        conn->rmsg = NULL;

        ASSERT(!rsp->is_request);
        ASSERT(rsp->peer == NULL);

        rsp_put(rsp);

        log_debug(LOG_INFO, "dyn: close s %d discarding rsp %"PRIu64" len %"PRIu32" "
                "in error", conn->sd, rsp->id, rsp->mlen);
    }

    ASSERT(conn->smsg == NULL);

    conn_unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("dyn: close s %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
    dnode_peer_failure(ctx, peer);

}

static rstatus_t
dnode_peer_each_preconnect(void *elem)
{
    struct node *peer = *(struct node **)elem;

    if (peer->is_local)  //don't bother to connect if it is a self-connection
        return DN_OK;

    return conn_pool_preconnect(peer->conn_pool);
}

static rstatus_t
dnode_peer_each_disconnect(void *elem)
{
    struct node *peer = *(struct node **)elem;

    if (peer->conn_pool) {
        conn_pool_destroy(peer->conn_pool);
        peer->conn_pool = NULL;
    }

    return DN_OK;
}

rstatus_t
dnode_peer_forward_state(void *rmsg)
{
    rstatus_t status;
    struct ring_msg *msg = rmsg;
    struct server_pool *sp = msg->sp;

    log_debug(LOG_VVERB, "dnode_peer_forward_state: forwarding");

    //we assume one mbuf is enough for now - will enhance with multiple mbufs later
    struct mbuf *mbuf = mbuf_get();
    if (mbuf == NULL) {
        log_debug(LOG_VVERB, "Too bad, not enough memory!");
        return DN_ENOMEM;
    }

    mbuf_copy(mbuf, msg->data, msg->len);

    struct array *peers = &sp->peers;
    uint32_t nelem = array_n(peers);

    //pick a random peer
    uint32_t ran_index = (uint32_t)rand() % nelem;

    if (ran_index == 0)
       ran_index += 1;

    struct node *peer = *(struct node **) array_get(peers, ran_index);

    //log_debug(LOG_VVERB, "Gossiping to node  '%.*s'", peer->name.len, peer->name.data);

    struct conn * conn = dnode_peer_conn(peer, 0);
    if (conn == NULL) {
        //running out of connection due to memory exhaust
        log_debug(LOG_ERR, "Unable to obtain a connection object");
        return DN_ERROR;
    }

    status = conn_connect(sp->ctx, conn);
    if (status != DN_OK ) {
        conn_close(sp->ctx, conn);
        log_debug(LOG_ERR, "Error happened in connecting on conn %d", conn->sd);
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
    log_debug(LOG_VVERB, "dyn: handshaking peers");
    struct array *peers = &sp->peers;

    uint32_t i,nelem;
    nelem = array_n(peers);

    //we assume one mbuf is enough for now - will enhance with multiple mbufs later
    struct mbuf *mbuf = mbuf_get();
    if (mbuf == NULL) {
        log_debug(LOG_VVERB, "Too bad, not enough memory!");
        return DN_ENOMEM;
    }

    //annoucing myself by sending msg: 'dc$rack$token,started_ts,node_state,node_dns'
    mbuf_write_string(mbuf, &sp->dc);
    mbuf_write_char(mbuf, '$');
    mbuf_write_string(mbuf, &sp->rack);
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
        struct node *peer = *(struct node **) array_get(peers, i);
        if (peer->is_local)
            continue;

        log_debug(LOG_VVERB, "Gossiping to node  '%.*s'", peer->name.len, peer->name.data);

        struct conn * conn = dnode_peer_conn(peer, 0);
        if (conn == NULL) {
            //running out of connection due to memory exhaust
            log_debug(LOG_DEBUG, "Unable to obtain a connection object");
            return DN_ERROR;
        }


        status = conn_connect(sp->ctx, conn);
        if (status != DN_OK ) {
            conn_close(sp->ctx, conn);
            log_debug(LOG_DEBUG, "Error happened in connecting on conn %d", conn->sd);
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

static rstatus_t
dnode_peer_add_node(struct server_pool *sp, struct gossip_node *node)
{
    rstatus_t status;
    struct array *peers = &sp->peers;
    struct node **sptr = array_push(peers);
    struct node *s = dn_zalloc(sizeof(struct node));
    if (!s || !sptr)
        return DN_ENOMEM;
    *sptr = s;
    _init_peer_struct(s);

    s->idx = array_idx(peers, sptr);
    s->owner = sp;

    string_copy(&s->endpoint.pname, node->pname.data, node->pname.len);
    s->endpoint.port = (uint16_t) node->port;
    string_copy(&s->name, node->name.data, node->name.len);
    struct sockinfo  *info =  dn_alloc(sizeof(*info)); //need to free this
    dn_resolve(&s->name, s->endpoint.port, info);
    s->endpoint.family = info->family;
    s->endpoint.addrlen = info->addrlen;
    s->endpoint.addr = (struct sockaddr *)&info->addr;  //TODOs: fix this by copying, not reference

    string_copy(&s->rack, node->rack.data, node->rack.len);
    string_copy(&s->dc, node->dc.data, node->dc.len);

    array_init(&s->tokens, 1, sizeof(struct dyn_token));
    struct dyn_token *dst_token = array_push(&s->tokens);
    copy_dyn_token(&node->token, dst_token);

    s->is_local = node->is_local;
    s->is_same_dc = (string_compare(&sp->dc, &s->dc) == 0);
    s->processed = 0;

    s->is_secure = is_secure(sp->secure_server_option, &sp->dc, &sp->rack, &s->dc,
                             &s->rack);
    s->state = node->state;

    dnode_create_connection_pool(sp, s);
    log_notice("added peer %M", s);

    status = dnode_peer_pool_run(sp);
    if (status != DN_OK)
        return status;

    status = dnode_peer_each_preconnect(&s);

    return status;
}

rstatus_t
dnode_peer_add(void *rmsg)
{
    rstatus_t status;
    struct ring_msg *msg = rmsg;
    struct server_pool *sp = msg->sp;
    struct gossip_node *node = array_get(&msg->nodes, 0);
    log_debug(LOG_NOTICE, "dyn: peer has an added message '%.*s'", node->name.len, node->name.data);
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
    struct array *peers = &sp->peers;
    struct node *s = NULL;

    uint32_t i,nelem;
    //bool node_exist = false;
    //TODOs: use hash table here
    for (i=1, nelem = array_n(peers); i< nelem; i++) {
        struct node * peer = *(struct node **) array_get(peers, i);
        if (string_compare(&peer->rack, &node->rack) == 0) {
            //TODOs: now only compare 1st token and support vnode later - use hash string on a tokens for comparison
            struct dyn_token *ptoken = (struct dyn_token *) array_get(&peer->tokens, 0);
            struct dyn_token *ntoken = &node->token;

            if (cmp_dyn_token(ptoken, ntoken) == 0) {
                s = peer; //found a node to replace
                break;
            }
        }
    }


    if (s != NULL) {
        log_notice("Found an old node to replace '%.*s'", s->name.len, s->name.data);
        log_notice("Replace with address '%.*s'", node->name.len, node->name.data);

        dnode_peer_each_disconnect(&s);
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

        dnode_create_connection_pool(sp, s);

        dnode_peer_each_preconnect(&s);
    } else {
        log_debug(LOG_INFO, "Unable to find any node matched the token");
    }

    return DN_OK;
}

void
dnode_peer_connected(struct context *ctx, struct conn *conn)
{
    struct node *peer = conn->owner;

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->connecting && !conn->connected);

    stats_pool_incr(ctx, peer_connections);

    conn->connecting = 0;
    conn->connected = 1;
    peer->state = NORMAL;
    conn_pool_connected(peer->conn_pool, conn);

    log_notice("%M connected", conn);
}

static void
dnode_peer_ok(struct context *ctx, struct conn *conn)
{
    struct node *server = conn->owner;

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT(conn->connected);

    log_debug(LOG_VERB, "dyn: reset peer '%.*s' failure count from %"PRIu32
            " to 0", server->endpoint.pname.len, server->endpoint.pname.data,
            server->failure_count);
    server->failure_count = 0;
}

static rstatus_t
dnode_peer_pool_update(struct server_pool *pool)
{
    msec_t now = dn_msec_now();
    if (now < 0) {
        return DN_ERROR;
    }

    if (now <= pool->next_rebuild) {
        return DN_OK;
    }

    pool->next_rebuild = now + WAIT_BEFORE_UPDATE_PEERS_IN_MILLIS;
    return dnode_peer_pool_run(pool);

}

uint32_t
dnode_peer_idx_for_key_on_rack(struct server_pool *pool, struct rack *rack,
                               uint8_t *key, uint32_t keylen)
{
    struct dyn_token token;
    pool->key_hash((char *)key, keylen, &token);
    return vnode_dispatch(rack->continuum, rack->ncontinuum, &token);
}

static struct node *
dnode_peer_for_key_on_rack(struct server_pool *pool, struct rack *rack,
                           uint8_t *key, uint32_t keylen)
{
    struct node *server;
    uint32_t idx;

    ASSERT(array_n(&pool->peers) != 0);

    if (keylen == 0) {
        idx = 0; //for no argument command
    } else {
        idx = dnode_peer_idx_for_key_on_rack(pool, rack, key, keylen);
    }

    ASSERT(idx < array_n(&pool->peers));

    server = *(struct node **)array_get(&pool->peers, idx);

    if (log_loggable(LOG_VERB)) {
        log_debug(LOG_VERB, "dyn: key '%.*s' maps to server '%.*s'", keylen,
                key, server->endpoint.pname.len, server->endpoint.pname.data);
    }

    return server;
}

struct node *
dnode_peer_pool_server(struct context *ctx, struct server_pool *pool,
                       struct rack *rack, uint8_t *key, uint32_t keylen,
                       msg_routing_t msg_routing)
{
    rstatus_t status;
    struct node *peer;

    log_debug(LOG_VERB, "Entering dnode_peer_pool_conn ................................");

    status = dnode_peer_pool_update(pool);
    if (status != DN_OK) {
        loga("status is not OK");
        return NULL;
    }

    if (msg_routing == ROUTING_LOCAL_NODE_ONLY) {  //always local
        peer = *(struct node **)array_get(&pool->peers, 0);
    } else {
        /* from a given {key, keylen} pick a peer from pool */
        peer = dnode_peer_for_key_on_rack(pool, rack, key, keylen);
        if (peer == NULL) {
            log_debug(LOG_VERB, "What? There is no such peer in rack '%.*s' for key '%.*s'",
                    rack->name, keylen, key);
            return NULL;
        }
    }
    return peer;
}

struct conn *
dnode_peer_get_conn(struct context *ctx, struct node *peer, int tag)
{
    ASSERT(!peer->is_local);

    if (peer->state == RESET) {
        log_debug(LOG_WARN, "Detecting peer '%.*s' is set with state Reset", peer->name);
        if (peer->conn_pool) {
            conn_pool_destroy(peer->conn_pool);
            peer->conn_pool = NULL;
        }

        dnode_create_connection_pool(&ctx->pool, peer);
        if (conn_pool_preconnect(peer->conn_pool) != DN_OK)
            return NULL;
    }
    /* pick a connection to a given peer */
    struct conn *conn = dnode_peer_conn(peer, tag);
    if (conn == NULL) {
        return NULL;
    }

    if (conn_connect(ctx, conn) != DN_OK) {
        conn_close(ctx, conn);
        return NULL;
    }

    return conn;
}


rstatus_t
dnode_peer_pool_preconnect(struct context *ctx)
{
    rstatus_t status;
    struct server_pool *sp = &ctx->pool;

    if (!sp->preconnect) {
        return DN_OK;
    }

    status = array_each(&sp->peers, dnode_peer_each_preconnect);
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

    status = array_each(&sp->peers, dnode_peer_each_disconnect);
    IGNORE_RET_VAL(status);
}

static bool
dnode_rsp_filter(struct context *ctx, struct conn *conn, struct msg *rsp)
{
    struct msg *req;

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    if (msg_empty(rsp)) {
        ASSERT(conn->rmsg == NULL);
        log_debug(LOG_VERB, "dyn: filter empty rsp %"PRIu64" on s %d", rsp->id,
                conn->sd);
        rsp_put(rsp);
        return true;
    }

    req = TAILQ_FIRST(&conn->omsg_q);
    if (req == NULL) {
        log_debug(LOG_INFO, "dyn: filter stray rsp %"PRIu64" len %"PRIu32" on s %d",
                  rsp->id, rsp->mlen, conn->sd);
        rsp_put(rsp);
        return true;
    }
    ASSERT(req->is_request && !req->done);

    return false;
}

static void
dnode_rsp_forward_stats(struct context *ctx, struct msg *rsp)
{
    ASSERT(!rsp->is_request);
    stats_pool_incr(ctx, peer_responses);
    stats_pool_incr_by(ctx, peer_response_bytes, rsp->mlen);
}

static void
dnode_rsp_swallow(struct context *ctx, struct conn *peer_conn,
                  struct msg *req, struct msg *rsp)
{
    conn_dequeue_outq(ctx, peer_conn, req);
    req->done = 1;
    log_debug(LOG_VERB, "conn %p swallow %p", peer_conn, req);
    if (rsp) {
        log_debug(LOG_INFO, "%M %M SWALLOW %M len %"PRIu32,
                  peer_conn, req, rsp, rsp->mlen);
        rsp_put(rsp);
    }
    req_put(req);
}

/* Description: link data from a peer connection to a client-facing connection
 * peer_conn: a peer connection
 * msg      : msg with data from the peer connection after parsing
 */
static void
dnode_rsp_forward_match(struct context *ctx, struct conn *peer_conn, struct msg *rsp)
{
    rstatus_t status;
    struct msg *req;
    struct conn *c_conn;

    req = TAILQ_FIRST(&peer_conn->omsg_q);
    c_conn = req->owner;

    /* if client consistency is dc_one forward the response from only the
       local node. Since dyn_dnode_peer is always a remote node, drop the rsp */
    if (req->consistency == DC_ONE) {
        if (req->swallow) {
            dnode_rsp_swallow(ctx, peer_conn, req, rsp);
            return;
        }
        //log_warn("req %d:%d with DC_ONE consistency is not being swallowed");
    }

    /* if client consistency is dc_quorum or dc_safe_quorum, forward the response from only the
       local region/DC. */
    if (((req->consistency == DC_QUORUM) || (req->consistency == DC_SAFE_QUORUM))
        && !peer_conn->same_dc) {
        if (req->swallow) {
            dnode_rsp_swallow(ctx, peer_conn, req, rsp);
            return;
        }
    }

    log_debug(LOG_DEBUG, "%M DNODE RSP RECEIVED dmsg->id %u req %u:%u rsp %u:%u, ",
              peer_conn, rsp->dmsg->id, req->id, req->parent_id, rsp->id, rsp->parent_id);
    ASSERT(req != NULL);
    ASSERT(req->is_request);

    if (log_loggable(LOG_VVERB)) {
        loga("%M Dumping content:", rsp);
        msg_dump(LOG_VVERB, rsp);

        loga("%M Dumping content:", req);
        msg_dump(LOG_VVERB, req);
    }

    conn_dequeue_outq(ctx, peer_conn, req);
    req->done = 1;

    log_info("%M %M RECEIVED %M", c_conn, req, rsp);

    ASSERT_LOG((c_conn->type == CONN_CLIENT) ||
               (c_conn->type == CONN_DNODE_PEER_CLIENT), "c_conn %M", c_conn);

    dnode_rsp_forward_stats(ctx, rsp);
    // c_conn owns respnse now
    status = conn_handle_response(c_conn, req->parent_id ? req->parent_id : req->id,
                                  rsp);
    IGNORE_RET_VAL(status);
    if (req->swallow) {
        log_info("swallow request %d:%d", req->id, req->parent_id);
        req_put(req);
    }
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

    ASSERT(peer_conn->type == CONN_DNODE_PEER_SERVER);

    /* response from a peer implies that peer is ok and heartbeating */
    dnode_peer_ok(ctx, peer_conn);

    /* dequeue peer message (request) from peer conn */
    while (true) {
        req = TAILQ_FIRST(&peer_conn->omsg_q);
        log_debug(LOG_VERB, "dnode_rsp_forward entering req %p rsp %p...", req, rsp);
        c_conn = req->owner;

        if (req->request_send_time) {
            struct stats *st = ctx->stats;
            uint64_t delay = dn_usec_now() - req->request_send_time;
            if (!peer_conn->same_dc)
                histo_add(&st->cross_region_latency_histo, delay);
            else
                histo_add(&st->cross_zone_latency_histo, delay);
        }

        if (req->id == rsp->dmsg->id) {
            dnode_rsp_forward_match(ctx, peer_conn, rsp);
            return;
        }
        // Report a mismatch and try to rectify
        log_error("%M MISMATCH: rsp_dmsg_id %u req %u:%u dnode rsp %u:%u",
                  peer_conn,
                  rsp->dmsg->id, req->id, req->parent_id, rsp->id,
                  rsp->parent_id);
        if (c_conn && conn_to_ctx(c_conn))
            stats_pool_incr(conn_to_ctx(c_conn),
                    peer_mismatch_requests);

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
            log_warn("req %d:%d with DC_ONE consistency is not being swallowed");
        }

        if (((req->consistency == DC_QUORUM) || (req->consistency == DC_SAFE_QUORUM))
            && !peer_conn->same_dc) {
            if (req->swallow) {
                // swallow the request and move on the next one
                dnode_rsp_swallow(ctx, peer_conn, req, NULL);
                continue;
            }
        }

        log_error("%M MISMATCHED DNODE RSP RECEIVED dmsg->id %u req %u:%u rsp %u:%u, skipping....",
                  peer_conn, rsp->dmsg->id,
                 req->id, req->parent_id, rsp->id, rsp->parent_id);
        ASSERT(req != NULL);
        ASSERT(req->is_request && !req->done);

        if (log_loggable(LOG_VVERB)) {
            loga("skipping req:   ");
            msg_dump(LOG_VVERB, req);
        }


        conn_dequeue_outq(ctx, peer_conn, req);
        req->done = 1;

        // Create an appropriate response for the request so its propagated up;
        struct msg *err_rsp = msg_get(peer_conn, false, __FUNCTION__);
        err_rsp->is_error = req->is_error = 1;
        err_rsp->error_code = req->error_code = BAD_FORMAT;
        err_rsp->dyn_error_code = req->dyn_error_code = BAD_FORMAT;
        err_rsp->dmsg = dmsg_get();
        err_rsp->dmsg->id = req->id;
        log_debug(LOG_VERB, "%p <-> %p", req, err_rsp);
        /* establish err_rsp <-> req (response <-> request) link */
        err_rsp->peer = req;

        log_error("Peer connection s %d skipping request %u:%u, dummy err_rsp %u:%u",
                 peer_conn->sd, req->id, req->parent_id, err_rsp->id, err_rsp->parent_id);
        rstatus_t status =
            conn_handle_response(c_conn, req->parent_id ? req->parent_id : req->id,
                                err_rsp);
        IGNORE_RET_VAL(status);
        if (req->swallow) {
                log_debug(LOG_INFO, "swallow request %d:%d", req->id, req->parent_id);
            req_put(req);
        }
    }
}

static void
dnode_rsp_recv_done(struct context *ctx, struct conn *conn,
                    struct msg *rsp, struct msg *nmsg)
{
    log_debug(LOG_VERB, "dnode_rsp_recv_done entering ...");

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    ASSERT(rsp != NULL && conn->rmsg == rsp);
    ASSERT(!rsp->is_request);
    ASSERT(rsp->owner == conn);
    ASSERT(nmsg == NULL || !nmsg->is_request);

    if (log_loggable(LOG_VVERB)) {
       loga("Dumping content for rsp:   ");
       msg_dump(LOG_VVERB, rsp);

       if (nmsg != NULL) {
          loga("Dumping content for nmsg :");
          msg_dump(LOG_VVERB, nmsg);
       }
    }

    /* enqueue next message (response), if any */
    conn->rmsg = nmsg;

    if (dnode_rsp_filter(ctx, conn, rsp)) {
        return;
    }
    dnode_rsp_forward(ctx, conn, rsp);
}



//TODOs: fix this in using dmsg_write with encrypted msgs
//         It is not in use now.
/*
void
dnode_rsp_gos_syn(struct context *ctx, struct conn *p_conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *pmsg;

    //ASSERT(p_conn->type == CONN_DNODE_PEER_CLIENT);

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
    msg->selected_rsp = pmsg;
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
     //   status = conn_event_add_out(p_conn);
     //   if (status != DN_OK) {
     //      p_conn->err = errno;
     //   }
     //}


    if (TAILQ_FIRST(&p_conn->omsg_q) != NULL && req_done(p_conn, TAILQ_FIRST(&p_conn->omsg_q))) {
        status = conn_event_add_out(p_conn);
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

    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

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
        status = conn_event_add_out(conn);
        IGNORE_RET_VAL(status);

        return NULL;
    }

    conn->last_sent = now;
    return req_send_next(ctx, conn);
}

static void
dnode_req_peer_enqueue_imsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);
    req->request_inqueue_enqueue_time_us = dn_usec_now();

    if (req->expect_datastore_reply) {
        msg_tmo_insert(req, conn);
    }
    TAILQ_INSERT_TAIL(&conn->imsg_q, req, s_tqe);
    log_debug(LOG_VERB, "conn %p enqueue inq %d:%d", conn, req->id, req->parent_id);

    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_in_queue, TAILQ_COUNT(&conn->imsg_q));
        stats_pool_incr(ctx, peer_in_queue);
        stats_pool_incr_by(ctx, peer_in_queue_bytes, req->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_in_queue, TAILQ_COUNT(&conn->imsg_q));
        stats_pool_incr(ctx, remote_peer_in_queue);
        stats_pool_incr_by(ctx, remote_peer_in_queue_bytes, req->mlen);
    }

}

static void
dnode_req_peer_dequeue_imsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    usec_t delay_us = 0;
    if (req->request_inqueue_enqueue_time_us) {
        delay_us = dn_usec_now() - req->request_inqueue_enqueue_time_us;
        if (conn->same_dc)
            histo_add(&ctx->stats->cross_zone_queue_wait_time_histo, delay_us);
        else
            histo_add(&ctx->stats->cross_region_queue_wait_time_histo, delay_us);
    }
    TAILQ_REMOVE(&conn->imsg_q, req, s_tqe);
    log_debug(LOG_VERB, "conn %p dequeue inq %d:%d", conn, req->id, req->parent_id);

    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_in_queue, TAILQ_COUNT(&conn->imsg_q));
        stats_pool_decr(ctx, peer_in_queue);
        stats_pool_decr_by(ctx, peer_in_queue_bytes, req->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_in_queue, TAILQ_COUNT(&conn->imsg_q));
        stats_pool_decr(ctx, remote_peer_in_queue);
        stats_pool_decr_by(ctx, remote_peer_in_queue_bytes, req->mlen);
    }
}

static void
dnode_req_peer_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    TAILQ_INSERT_TAIL(&conn->omsg_q, req, s_tqe);
    log_debug(LOG_VERB, "conn %p enqueue outq %d:%d", conn, req->id, req->parent_id);

    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_out_queue, TAILQ_COUNT(&conn->omsg_q));
        stats_pool_incr(ctx, peer_out_queue);
        stats_pool_incr_by(ctx, peer_out_queue_bytes, req->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_out_queue, TAILQ_COUNT(&conn->omsg_q));
        stats_pool_incr(ctx, remote_peer_out_queue);
        stats_pool_incr_by(ctx, remote_peer_out_queue_bytes, req->mlen);
    }
}

static void
dnode_req_peer_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *req)
{
    ASSERT(req->is_request);
    ASSERT(conn->type == CONN_DNODE_PEER_SERVER);

    msg_tmo_delete(req);

    TAILQ_REMOVE(&conn->omsg_q, req, s_tqe);
    log_debug(LOG_VVERB, "conn %p dequeue outq %p", conn, req);

    if (conn->same_dc) {
        histo_add(&ctx->stats->peer_out_queue, TAILQ_COUNT(&conn->omsg_q));
        stats_pool_decr(ctx, peer_out_queue);
        stats_pool_decr_by(ctx, peer_out_queue_bytes, req->mlen);
    } else {
        histo_add(&ctx->stats->remote_peer_out_queue, TAILQ_COUNT(&conn->omsg_q));
        stats_pool_decr(ctx, remote_peer_out_queue);
        stats_pool_decr_by(ctx, remote_peer_out_queue_bytes, req->mlen);
    }
}


struct conn_ops dnode_peer_ops = {
    msg_recv,
    rsp_recv_next,
    dnode_rsp_recv_done,
    msg_send,
    dnode_req_send_next,
    req_send_done,
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
    conn->dyn_mode = 1;
    conn->type = CONN_DNODE_PEER_SERVER;
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
    uint32_t dc_cnt = array_n(&sp->datacenters);
    uint32_t dc_index;
    uint32_t my_rack_index = 0;

    // Sort the racks in the dcs
    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {
        struct datacenter *dc = array_get(&sp->datacenters, dc_index);
        // sort the racks.
        array_sort(&dc->racks, rack_name_cmp);
    }

    // Find the rack index for the local rack
    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {
        struct datacenter *dc = array_get(&sp->datacenters, dc_index);

        if (string_compare(dc->name, &sp->dc) != 0)
            continue;

        // if the dc is a local dc, get the rack_idx
        uint32_t rack_index;
        uint32_t rack_cnt = array_n(&dc->racks);
        for(rack_index = 0; rack_index < rack_cnt; rack_index++) {
            struct rack *rack = array_get(&dc->racks, rack_index);
            if (string_compare(rack->name, &sp->rack) == 0) {
                my_rack_index = rack_index;
                log_notice("my rack index %u", my_rack_index);
                break;
            }
        }
    }

    // For every remote DC, find the corresponding rack to replicate to.
    for(dc_index = 0; dc_index < dc_cnt; dc_index++) {
        struct datacenter *dc = array_get(&sp->datacenters, dc_index);
        dc->preselected_rack_for_replication = NULL;

        // Nothing to do for local DC, continue;
        if (string_compare(dc->name, &sp->dc) == 0)
            continue;

        // if no racks, keep preselected_rack_for_replication as NULL
        uint32_t rack_cnt = array_n(&dc->racks);
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
