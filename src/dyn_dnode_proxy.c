/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <sys/un.h>
#include <arpa/inet.h>

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_dnode_peer.h"
#include "dyn_dnode_proxy.h"

static void
dnode_ref(struct conn *conn, void *owner)
{
    struct server_pool *pool = owner;

    ASSERT(conn->type == CONN_DNODE_PEER_PROXY);
    ASSERT(conn->owner == NULL);

    conn->family = pool->dnode_proxy_endpoint.family;
    conn->addrlen = pool->dnode_proxy_endpoint.addrlen;
    conn->addr = pool->dnode_proxy_endpoint.addr;
    string_duplicate(&conn->pname, &pool->dnode_proxy_endpoint.pname);

    pool->d_conn = conn;

    /* owner of the proxy connection is the server pool */
    conn->owner = owner;

    log_debug(LOG_VVERB, "ref conn %p owner %p", conn, pool);
}

static void
dnode_unref(struct conn *conn)
{
    struct server_pool *pool;

    ASSERT(conn->type == CONN_DNODE_PEER_PROXY);
    ASSERT(conn->owner != NULL);

    conn_event_del_conn(conn);
    pool = conn->owner;
    conn->owner = NULL;

    pool->d_conn = NULL;

    log_debug(LOG_VVERB, "unref conn %p owner %p", conn, pool);
}

static void
dnode_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    
    ASSERT(conn->type == CONN_DNODE_PEER_PROXY);

    if (conn->sd < 0) {
        conn_unref(conn);
        conn_put(conn);
        return;
    }

    ASSERT(conn->rmsg == NULL);
    ASSERT(conn->smsg == NULL);
    ASSERT(TAILQ_EMPTY(&conn->imsg_q));
    ASSERT(TAILQ_EMPTY(&conn->omsg_q));

    conn_unref(conn);

    status = close(conn->sd);
    if (status < 0) {
        log_error("close p %d failed, ignored: %s", conn->sd, strerror(errno));
    }
    conn->sd = -1;

    conn_put(conn);
}

/*
 * Initialize the Dynomite node. Check the connection and backend data store,
 * then log a message with the socket descriptor, the Dynomite
 */
rstatus_t
dnode_init(struct context *ctx)
{
    rstatus_t status;
    struct server_pool *pool = &ctx->pool;
    struct conn *p = conn_get_dnode(pool);
    if (p == NULL) {
        return DN_ENOMEM;
    }

    status = conn_listen(pool->ctx, p);
    if (status != DN_OK) {
        conn_close(pool->ctx, p);
        return status;
    }

    const char * log_datastore = "not selected data store";
    if (g_data_store == DATA_REDIS){
    	log_datastore = "redis";
    }
    else if (g_data_store == DATA_MEMCACHE){
    	log_datastore = "memcache";
    }

    log_debug(LOG_NOTICE, "dyn: p %d listening on '%.*s' in %s pool '%.*s'",
              p->sd, pool->dnode_proxy_endpoint.pname.len,
              pool->dnode_proxy_endpoint.pname.data,
			  log_datastore,
              pool->name.len, pool->name.data );
    return DN_OK;
}

void
dnode_deinit(struct context *ctx)
{
    struct server_pool *pool = &ctx->pool;
    struct conn *p = pool->d_conn;
    if (p != NULL) {
        conn_close(pool->ctx, p);
    }

    log_debug(LOG_VVERB, "deinit dnode");
}

static rstatus_t
dnode_accept(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct conn *c;
    struct sockaddr_in client_address;
    socklen_t client_len = sizeof(client_address);
    int sd = 0;

    ASSERT(p->type == CONN_DNODE_PEER_PROXY);
    ASSERT(p->sd > 0);
    ASSERT(p->recv_active && p->recv_ready);

    
    for (;;) {
        sd = accept(p->sd, (struct sockaddr *)&client_address, &client_len);
        if (sd < 0) {
            if (errno == EINTR) {
                log_warn("dyn: accept on %s %d not ready - eintr",
                         conn_get_type_string(p), p->sd);
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                p->recv_ready = 0;
                return DN_OK;
            }

            /*
             * FIXME: On EMFILE or ENFILE mask out IN event on the proxy; mask
             * it back in when some existing connection gets closed
             */

            log_error("dyn: accept on %s %d failed: %s", conn_get_type_string(p),
                      p->sd, strerror(errno));
            return DN_ERROR;
        }

        break;
    }

    char clntName[INET_ADDRSTRLEN];

    if(inet_ntop(AF_INET, &client_address.sin_addr.s_addr, clntName, sizeof(clntName))!=NULL){
       loga("Accepting client connection from %s%c%d on sd %d",clntName,'/',ntohs(client_address.sin_port), sd);
    } else {
       loga("Unable to get client's address for accept on sd %d\n", sd);
    }

    c = conn_get_peer(p->owner, true);
    if (c == NULL) {
        log_error("dyn: get conn client peer for PEER_CLIENT %d from %s %d failed: %s",
                  sd, conn_get_type_string(p), p->sd, strerror(errno));
        status = close(sd);
        if (status < 0) {
            log_error("dyn: close c %d failed, ignored: %s", sd, strerror(errno));
        }
        return DN_ENOMEM;
    }
    c->sd = sd;

    stats_pool_incr(ctx, dnode_client_connections);

    status = dn_set_nonblocking(c->sd);
    if (status < 0) {
        log_error("dyn: set nonblock on s %d from peer socket %d failed: %s",
                  c->sd, p->sd, strerror(errno));
        conn_close(ctx, c);
        return status;
    }

    if (p->family == AF_INET || p->family == AF_INET6) {
        status = dn_set_tcpnodelay(c->sd);
        if (status < 0) {
            log_warn("dyn: set tcpnodelay on %d from peer socket %d failed, ignored: %s",
                     c->sd, p->sd, strerror(errno));
        }
    }

    status = conn_event_add_conn(c);
    if (status < 0) {
        log_error("dyn: event add conn from %s %d failed: %s",
                  conn_get_type_string(p), p->sd, strerror(errno));
        conn_close(ctx, c);
        return status;
    }

    log_notice("dyn: accepted %s %d on %s %d from '%s'",
               conn_get_type_string(c), c->sd, conn_get_type_string(p), p->sd,
               dn_unresolve_peer_desc(c->sd));

    return DN_OK;
}

static rstatus_t
dnode_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    ASSERT(conn->type == CONN_DNODE_PEER_PROXY);
    ASSERT(conn->recv_active);
 
    conn->recv_ready = 1;
    do {
        status = dnode_accept(ctx, conn);
        if (status != DN_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return DN_OK;
}

struct conn_ops dnode_server_ops = {
    dnode_recv,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    dnode_close,
    NULL,
    dnode_ref,
    dnode_unref,
    NULL,
    NULL,
    NULL,
    NULL,
    conn_cant_handle_response
};

void
init_dnode_proxy_conn(struct conn *conn)
{
    conn->type = CONN_DNODE_PEER_PROXY;
    conn->ops = &dnode_server_ops;
}
