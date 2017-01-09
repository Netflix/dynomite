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

#include <sys/uio.h>

#include "dyn_core.h"
#include "dyn_server.h"
#include "dyn_client.h"
#include "dyn_proxy.h"
#include "dyn_dnode_proxy.h"
#include "dyn_dnode_peer.h"
#include "dyn_dnode_client.h"
#include "event/dyn_event.h"

#include "proto/dyn_proto.h"

/*
 *                   dyn_connection.[ch]
 *                Connection (struct conn)
 *                 +         +          +
 *                 |         |          |
 *                 |       Proxy        |
 *                 |     dyn_proxy.[ch]  |
 *                 /                    \
 *              Client                Server
 *           dyn_client.[ch]         dyn_server.[ch]
 *
 * Dynomite essentially multiplexes m client connections over n server
 * connections. Usually m >> n, so that dynomite can pipeline requests
 * from several clients over a server connection and hence use the connection
 * bandwidth to the server efficiently
 *
 * Client and server connection maintain two fifo queues for requests:
 *
 * 1). in_q (imsg_q):  queue of incoming requests
 * 2). out_q (omsg_q): queue of outstanding (outgoing) requests
 *
 * Request received over the client connection are forwarded to the server by
 * enqueuing the request in the chosen server's in_q. From the client's
 * perspective once the request is forwarded, it is outstanding and is tracked
 * in the client's out_q (unless the request was tagged as !expect_datastore_reply). The server
 * in turn picks up requests from its own in_q in fifo order and puts them on
 * the wire. Once the request is outstanding on the wire, and a response is
 * expected for it, the server keeps track of outstanding requests it in its
 * own out_q.
 *
 * The server's out_q enables us to pair a request with a response while the
 * client's out_q enables us to pair request and response in the order in
 * which they are received from the client.
 *
 *
 *      Clients                             Servers
 *                                    .
 *    in_q: <empty>                   .
 *    out_q: req11 -> req12           .   in_q:  req22
 *    (client1)                       .   out_q: req11 -> req21 -> req12
 *                                    .   (server1)
 *    in_q: <empty>                   .
 *    out_q: req21 -> req22 -> req23  .
 *    (client2)                       .
 *                                    .   in_q:  req23
 *                                    .   out_q: <empty>
 *                                    .   (server2)
 *
 * In the above example, client1 has two pipelined requests req11 and req12
 * both of which are outstanding on the server connection server1. On the
 * other hand, client2 has three requests req21, req22 and req23, of which
 * only req21 is outstanding on the server connection while req22 and
 * req23 are still waiting to be put on the wire. The fifo of client's
 * out_q ensures that we always send back the response of request at the head
 * of the queue, before sending out responses of other completed requests in
 * the queue.
 *
 * TODOs: Minh: add explanation for peer-to-peer communication
 */

#define DYN_KEEPALIVE_INTERVAL_S 15 /* seconds */
static uint32_t nfree_connq;       /* # free conn q */
static struct conn_tqh free_connq; /* free conn q */

consistency_t g_read_consistency = DEFAULT_READ_CONSISTENCY;
consistency_t g_write_consistency = DEFAULT_WRITE_CONSISTENCY;

inline char *
conn_get_type_string(struct conn *conn)
{
    switch(conn->type) {
        case CONN_UNSPECIFIED: return "UNSPEC";
        case CONN_PROXY : return "PROXY";
        case CONN_CLIENT: return "CLIENT";
        case CONN_SERVER: return "SERVER";
        case CONN_DNODE_PEER_PROXY: return "PEER_PROXY";
        case CONN_DNODE_PEER_CLIENT: return conn->same_dc ?
                                            "LOCAL_PEER_CLIENT" : "REMOTE_PEER_CLIENT";
        case CONN_DNODE_PEER_SERVER: return conn->same_dc ?
                                            "LOCAL_PEER_SERVER" : "REMOTE_PEER_SERVER";
    }
    return "INVALID";
}

bool
conn_is_req_first_in_outqueue(struct conn *conn, struct msg *req)
{
    struct msg *first_req_in_outqueue = TAILQ_FIRST(&conn->omsg_q);
    return req == first_req_in_outqueue;
}

/*
 * Return the context associated with this connection.
 */
struct context *
conn_to_ctx(struct conn *conn)
{
    struct datastore *server;
    struct node *peer;
    struct server_pool *pool;
    switch(conn->type) {
        case CONN_PROXY:
        case CONN_CLIENT:
        case CONN_DNODE_PEER_PROXY:
        case CONN_DNODE_PEER_CLIENT:
            pool = conn->owner;
            break;
        case CONN_SERVER:
            server = conn->owner;
            pool = server ? server->owner : NULL;
            break;
        case CONN_DNODE_PEER_SERVER:
            peer = conn->owner;
            pool = peer ? peer->owner : NULL;
            break;
        default:
            return NULL;
    }

    return pool ? pool->ctx : NULL;
}

static struct conn *
_conn_get(void)
{
    struct conn *conn;

    if (!TAILQ_EMPTY(&free_connq)) {
        ASSERT(nfree_connq > 0);

        conn = TAILQ_FIRST(&free_connq);
        nfree_connq--;
        TAILQ_REMOVE(&free_connq, conn, conn_tqe);
    } else {
        conn = dn_alloc(sizeof(*conn));
        if (conn == NULL) {
            return NULL;
        }
        memset(conn, 0, sizeof(*conn));
    }

    conn->object_type = OBJ_CONN;
    conn->owner = NULL;

    conn->sd = -1;
    string_init(&conn->pname);
    /* {family, addrlen, addr} are initialized in enqueue handler */

    TAILQ_INIT(&conn->imsg_q);
    conn->imsg_count = 0;

    TAILQ_INIT(&conn->omsg_q);
    conn->omsg_count = 0;

    conn->rmsg = NULL;
    conn->smsg = NULL;

    /*
     * Callbacks {recv, recv_next, recv_done}, {send, send_next, send_done},
     * {close, active}, parse, {ref, unref}, {enqueue_inq, dequeue_inq} and
     * {enqueue_outq, dequeue_outq} are initialized by the wrapper.
     */

    conn->send_bytes = 0;
    conn->recv_bytes = 0;

    conn->events = 0;
    conn->err = 0;
    conn->recv_active = 0;
    conn->recv_ready = 0;
    conn->send_active = 0;
    conn->send_ready = 0;

    conn->connecting = 0;
    conn->connected = 0;
    conn->eof = 0;
    conn->done = 0;
    conn->waiting_to_unref = 0;

    /* for dynomite */
    conn->dyn_mode = 0;
    conn->dnode_secured = 0;
    conn->dnode_crypto_state = 0;

    conn->same_dc = 1;
    conn->avail_tokens = msgs_per_sec();
    conn->last_sent = 0;
    conn->attempted_reconnect = 0;
    //conn->non_bytes_send = 0;
    conn_set_read_consistency(conn, g_read_consistency);
    conn_set_write_consistency(conn, g_write_consistency);
    conn->type = CONN_UNSPECIFIED;

    unsigned char *aes_key = generate_aes_key();
    strncpy((char *)conn->aes_key, (char *)aes_key, strlen((char *)aes_key)); //generate a new key for each connection

    return conn;
}

int
print_conn(FILE *stream, struct conn *conn)
{
    return fprintf(stream, "<%s %p %d>",
                   conn_get_type_string(conn), conn, conn->sd);
}

inline void
conn_set_read_consistency(struct conn *conn, consistency_t cons)
{
    conn->read_consistency = cons;
}

inline consistency_t
conn_get_read_consistency(struct conn *conn)
{
    //return conn->read_consistency;
    return g_read_consistency;
}

inline void
conn_set_write_consistency(struct conn *conn, consistency_t cons)
{
    conn->write_consistency = cons;
}

inline consistency_t
conn_get_write_consistency(struct conn *conn)
{
    //return conn->write_consistency;
    return g_write_consistency;
}

struct conn *
test_conn_get(void)
{
   return _conn_get();
}

static void
add_to_ready_q(struct context *ctx, struct conn *conn)
{
    // This check is required to check if the connection is already
    // on the ready queue
    if (conn->ready_tqe.tqe_prev == NULL) {
        struct server_pool *pool = &ctx->pool;
        TAILQ_INSERT_TAIL(&pool->ready_conn_q, conn, ready_tqe);
    }
}

static void
remove_from_ready_q(struct context *ctx, struct conn *conn)
{
    // This check is required to check if the connection is already
    // on the ready queue
    if (conn->ready_tqe.tqe_prev != NULL) {
        struct server_pool *pool = &ctx->pool;
        TAILQ_REMOVE(&pool->ready_conn_q, conn, ready_tqe);
    }
}

rstatus_t
conn_event_del_conn(struct conn *conn)
{
    struct context *ctx = conn_to_ctx(conn);
    remove_from_ready_q(ctx, conn);
    if (conn->sd != -1)
        return event_del_conn(ctx->evb, conn);
    return DN_OK;
}

rstatus_t
conn_event_add_out(struct conn *conn)
{
    struct context *ctx = conn_to_ctx(conn);
    add_to_ready_q(ctx, conn);
    return event_add_out(ctx->evb, conn);
}

rstatus_t
conn_event_add_conn(struct conn *conn)
{
    struct context *ctx = conn_to_ctx(conn);
    add_to_ready_q(ctx, conn);
    return event_add_conn(ctx->evb, conn);
}

rstatus_t
conn_event_del_out(struct conn *conn)
{
    struct context *ctx = conn_to_ctx(conn);
    remove_from_ready_q(ctx, conn);
    return event_del_out(ctx->evb, conn);
}

struct conn *
conn_get_peer(void *owner, bool client)
{
    struct conn *conn;

    conn = _conn_get();
    if (conn == NULL) {
        return NULL;
    }

    conn->dyn_mode = 1;

    if (client) {
        /* incoming peer connection to dnode server
         * dyn client receives a request, possibly parsing it, and sends a
         * response downstream.
         */
        init_dnode_client_conn(conn);
    } else {
        /*
         * outgoing peer connection
         * dyn server receives a response, possibly parsing it, and sends a
         * request upstream.
         */
        init_dnode_peer_conn(conn);
    }

    conn_ref(conn, owner);

    log_debug(LOG_VVERB, "get conn %p %s", conn, conn_get_type_string(conn));

    return conn;
}

struct conn *
conn_get(void *owner, bool client)
{
    struct conn *conn;

    conn = _conn_get();
    if (conn == NULL) {
        return NULL;
    }

    /* connection handles the data store messages (redis, memcached or other) */

    conn->dyn_mode = 0;

    if (client) {
        /*
         * client receives a request, possibly parsing it, and sends a
         * response downstream.
         */
        init_client_conn(conn);
    } else {
        /*
         * server receives a response, possibly parsing it, and sends a
         * request upstream.
         */
        init_server_conn(conn);
    }

    conn_ref(conn, owner);

    log_debug(LOG_VVERB, "get conn %p %s", conn, conn_get_type_string(conn));

    return conn;
}

struct conn *
conn_get_dnode(void *owner)
{
    struct conn *conn;

    conn = _conn_get();
    if (conn == NULL) {
        return NULL;
    }

    conn->dyn_mode = 1;
    init_dnode_proxy_conn(conn);
    conn_ref(conn, owner);

    log_debug(LOG_VVERB, "get conn %p %s", conn, conn_get_type_string(conn));

    return conn;
}

struct conn *
conn_get_proxy(void *owner)
{
    struct conn *conn;

    conn = _conn_get();
    if (conn == NULL) {
        return NULL;
    }


    conn->dyn_mode = 0;
    init_proxy_conn(conn);
    conn_ref(conn, owner);

    log_debug(LOG_VVERB, "get conn %p %s", conn, conn_get_type_string(conn));

    return conn;
}

static void
conn_free(struct conn *conn)
{
    log_debug(LOG_VVERB, "free conn %p", conn);
    dn_free(conn);
}

void
conn_put(struct conn *conn)
{
    ASSERT(conn->sd < 0);
    ASSERT(conn->owner == NULL);

    log_debug(LOG_VVERB, "putting %M", conn);

    nfree_connq++;
    TAILQ_INSERT_HEAD(&free_connq, conn, conn_tqe);
}

/**
 * Initialize connections.
 */
void
conn_init(void)
{
    log_debug(LOG_DEBUG, "conn size %d", sizeof(struct conn));
    nfree_connq = 0;
    TAILQ_INIT(&free_connq);
}

void
conn_deinit(void)
{
    struct conn *conn, *nconn; /* current and next connection */

    for (conn = TAILQ_FIRST(&free_connq); conn != NULL;
         conn = nconn, nfree_connq--) {
        ASSERT(nfree_connq > 0);
        nconn = TAILQ_NEXT(conn, conn_tqe);
        conn_free(conn);
    }
    ASSERT(nfree_connq == 0);
}

static rstatus_t
conn_reuse(struct conn *p)
{
    rstatus_t status;
    struct sockaddr_un *un;

    switch (p->family) {
    case AF_INET:
    case AF_INET6:
        status = dn_set_reuseaddr(p->sd);
        break;

    case AF_UNIX:
        /*
         * bind() will fail if the pathname already exist. So, we call unlink()
         * to delete the pathname, in case it already exists. If it does not
         * exist, unlink() returns error, which we ignore
         */
        un = (struct sockaddr_un *) p->addr;
        unlink(un->sun_path);
        status = DN_OK;
        break;

    default:
        NOT_REACHED();
        status = DN_ERROR;
    }

    return status;
}

rstatus_t
conn_listen(struct context *ctx, struct conn *p)
{
    rstatus_t status;
    struct server_pool *pool = &ctx->pool;

    ASSERT((p->type == CONN_PROXY) ||
           (p->type == CONN_DNODE_PEER_PROXY));

    p->sd = socket(p->family, SOCK_STREAM, 0);
    if (p->sd < 0) {
        log_error("socket failed: %s", strerror(errno));
        return DN_ERROR;
    }

    status = conn_reuse(p);
    if (status < 0) {
        log_error("reuse of addr '%.*s' for listening on p %d failed: %s",
                  p->pname.len, p->pname.data, p->sd,
                  strerror(errno));
        return DN_ERROR;
    }

    status = bind(p->sd, p->addr, p->addrlen);
    if (status < 0) {
        log_error("bind on p %d to addr '%.*s' failed: %s", p->sd,
                  p->pname.len, p->pname.data, strerror(errno));
        return DN_ERROR;
    }

    status = listen(p->sd, pool->backlog);
    if (status < 0) {
        log_error("listen on p %d on addr '%.*s' failed: %s", p->sd,
                  p->pname.len, p->pname.data, strerror(errno));
        return DN_ERROR;
    }

    status = dn_set_nonblocking(p->sd);
    if (status < 0) {
        log_error("set nonblock on p %d on addr '%.*s' failed: %s", p->sd,
                  p->pname.len, p->pname.data, strerror(errno));
        return DN_ERROR;
    }

    status = conn_event_add_conn(p);
    if (status < 0) {
        log_error("event add conn p %d on addr '%.*s' failed: %s",
                  p->sd, p->pname.len, p->pname.data,
                  strerror(errno));
        return DN_ERROR;
    }

    status = conn_event_del_out(p);
    if (status < 0) {
        log_error("event del out p %d on addr '%.*s' failed: %s",
                  p->sd, p->pname.len, p->pname.data,
                  strerror(errno));
        return DN_ERROR;
    }

    return DN_OK;
}

rstatus_t
conn_connect(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    // Outgoing connection to another Dynomite node and admin mode is disabled
    if ((conn->type == CONN_DNODE_PEER_SERVER) && (ctx->admin_opt > 0))
        return DN_OK;

    // Only continue if the connection type is:
    // 1. CONN_DNODE_PEER_SERVER: Outbound connection to another Dynomite node
    // 2. CONN_SERVER: Outbound connection to backend datastore (Redis, ARDB)
    ASSERT((conn->type == CONN_DNODE_PEER_SERVER) ||
            (conn->type == CONN_SERVER));

    if (conn->sd > 0) {
        /* already connected on peer connection */
        return DN_OK;
    }

    conn->sd = socket(conn->family, SOCK_STREAM, 0);
    if (conn->sd < 0) {
        log_error("dyn: socket for '%.*s' failed: %s", conn->pname.len,
                conn->pname.data, strerror(errno));
        status = DN_ERROR;
        goto error;
    }
    log_warn("connecting to %s '%.*s' from sd %d", conn_get_type_string(conn),
             conn->pname.len, conn->pname.data, conn->sd);

    status = dn_set_nonblocking(conn->sd);
    if (status != DN_OK) {
        log_error("set nonblock on s %d for '%.*s' failed: %s",
                conn->sd,  conn->pname.len, conn->pname.data,
                strerror(errno));
        goto error;
    }
    status = dn_set_keepalive(conn->sd, DYN_KEEPALIVE_INTERVAL_S);
    if (status != DN_OK) {
        log_error("set keepalive on s %d for '%.*s' failed: %s",
                conn->sd,  conn->pname.len, conn->pname.data,
                strerror(errno));
        // Continue since this is not catastrophic
    }

    if (conn->pname.data[0] != '/') {
        status = dn_set_tcpnodelay(conn->sd);
        if (status != DN_OK) {
            log_warn("set tcpnodelay on s %d for '%.*s' failed, ignored: %s",
                    conn->sd, conn->pname.len, conn->pname.data,
                    strerror(errno));
        }
    }

    status = conn_event_add_conn(conn);
    if (status != DN_OK) {
        log_error("event add conn s %d for '%.*s' failed: %s",
                conn->sd, conn->pname.len, conn->pname.data,
                strerror(errno));
        goto error;
    }

    ASSERT(!conn->connecting && !conn->connected);

    status = connect(conn->sd, conn->addr, conn->addrlen);

    if (status != DN_OK) {
        if (errno == EINPROGRESS) {
            conn->connecting = 1;
            log_debug(LOG_DEBUG, "connecting on s %d to '%.*s'",
                    conn->sd, conn->pname.len, conn->pname.data);
            return DN_OK;
        }

        log_error("connect on s %d to '%.*s' failed: %s", conn->sd,
                conn->pname.len, conn->pname.data, strerror(errno));

        goto error;
    }

    ASSERT(!conn->connecting);
    conn->connected = 1;
    log_debug(LOG_WARN, "%M connected to '%.*s'", conn,
            conn->pname.len, conn->pname.data);

    return DN_OK;

    error:
    conn->err = errno;
    return status;
}

ssize_t
conn_recv_data(struct conn *conn, void *buf, size_t size)
{
    ssize_t n;

    ASSERT(buf != NULL);
    ASSERT(size > 0);
    ASSERT(conn->recv_ready);

    for (;;) {
        n = dn_read(conn->sd, buf, size);

        log_debug(LOG_VERB, "%M recv %zd of %zu", conn, n, size);

        if (n > 0) {
            if (n < (ssize_t) size) {
                conn->recv_ready = 0;
            }
            conn->recv_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            conn->recv_ready = 0;
            conn->eof = 1;
            log_debug(LOG_NOTICE, "%M recv eof rb %zu sb %zu", conn,
                      conn->recv_bytes, conn->send_bytes);
            return n;
        }

        if (errno == EINTR) {
            log_debug(LOG_VERB, "%M recv not ready - eintr", conn);
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->recv_ready = 0;
            log_debug(LOG_VERB, "%M recv not ready - eagain", conn);
            return DN_EAGAIN;
        } else {
            conn->recv_ready = 0;
            conn->err = errno;
            log_error("%M recv failed: %s", conn, strerror(errno));
            return DN_ERROR;
        }
    }

    NOT_REACHED();

    return DN_ERROR;
}

ssize_t
conn_sendv_data(struct conn *conn, struct array *sendv, size_t nsend)
{
    ssize_t n;

    ASSERT(array_n(sendv) > 0);
    ASSERT(nsend != 0);
    ASSERT(conn->send_ready);

    for (;;) {
        n = dn_writev(conn->sd, sendv->elem, sendv->nelem);

        log_debug(LOG_VERB, "sendv on sd %d %zd of %zu in %"PRIu32" buffers",
                  conn->sd, n, nsend, sendv->nelem);

        if (n > 0) {
            if (n < (ssize_t) nsend) {
                conn->send_ready = 0;
            }
            conn->send_bytes += (size_t)n;
            //conn->non_bytes_send = 0;
            return n;
        }

        if (n == 0) {
            log_warn("sendv on sd %d returned zero", conn->sd);
            conn->send_ready = 0;
            //conn->non_bytes_send++;
            //if (conn->dyn_mode && conn->non_bytes_send > MAX_CONN_ALLOWABLE_NON_SEND) {
            //    conn->err = ENOTRECOVERABLE;
            //}
            return 0;
        }

        if (errno == EINTR) {
            log_debug(LOG_VERB, "sendv on sd %d not ready - eintr", conn->sd);
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->send_ready = 0;
            log_debug(LOG_VERB, "sendv on sd %d not ready - eagain", conn->sd);
            return DN_EAGAIN;
        } else {
            conn->send_ready = 0;
            conn->err = errno;
            log_error("sendv on sd %d failed: %s", conn->sd, strerror(errno));
            return DN_ERROR;
        }
    }

    NOT_REACHED();

    return DN_ERROR;
}

void
conn_print(struct conn *conn)
{
	log_debug(LOG_VERB, "sd %d", conn->sd);
	log_debug(LOG_VERB, "Type: %s", conn_get_type_string(conn));

	log_debug(LOG_VERB, "dyn_mode %d", conn->dyn_mode);

	log_debug(LOG_VERB, "dnode_crypto_state %d", conn->dnode_crypto_state);
	log_debug(LOG_VERB, "dnode_secured %d", conn->dnode_secured);

	log_debug(LOG_VERB, "connected %d", conn->connected);
	log_debug(LOG_VERB, "done %d", conn->done);


	log_debug(LOG_VERB, "send_active %d", conn->send_active);
	log_debug(LOG_VERB, "send_bytes %d", conn->send_bytes);
	log_debug(LOG_VERB, "send_ready %d", conn->send_ready);

	log_debug(LOG_VERB, "recv_active %d", conn->recv_active);
	log_debug(LOG_VERB, "recv_bytes %d", conn->recv_bytes);
	log_debug(LOG_VERB, "recv_ready %d", conn->recv_ready);

	log_debug(LOG_VERB, "events %d", conn->events);
	log_debug(LOG_VERB, "eof %d", conn->eof);
	log_debug(LOG_VERB, "err %d", conn->err);
}

