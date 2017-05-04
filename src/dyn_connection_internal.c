/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

/*
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

#include "dyn_connection_internal.h"
#include "event/dyn_event.h"

static uint32_t nfree_connq;       /* # free conn q */
static struct conn_tqh free_connq; /* free conn q */

inline char *
_conn_get_type_string(struct conn *conn)
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

static int
_print_conn(FILE *stream, const struct object *obj)
{
    ASSERT(obj->type == OBJ_CONN);
    struct conn *conn = (struct conn *)obj;
    if ((conn->type == CONN_DNODE_PEER_PROXY) ||
        (conn->type == CONN_PROXY)) {
        return fprintf(stream, "<%s %p %d listening on '%.*s'>",
                   _conn_get_type_string(conn), conn, conn->sd,
                   conn->pname.len, conn->pname.data);
    }
    if ((conn->type == CONN_DNODE_PEER_CLIENT) ||
        (conn->type == CONN_CLIENT)) {
        return fprintf(stream, "<%s %p %d from '%.*s'>",
                   _conn_get_type_string(conn), conn, conn->sd,
                   conn->pname.len, conn->pname.data);
    }
    if ((conn->type == CONN_DNODE_PEER_SERVER) ||
        (conn->type == CONN_SERVER)) {
        return fprintf(stream, "<%s %p %d to '%.*s'>",
                   _conn_get_type_string(conn), conn, conn->sd,
                   conn->pname.len, conn->pname.data);
    }

    return fprintf(stream, "<%s %p %d>",
                   _conn_get_type_string(conn), conn, conn->sd);
}


struct conn *
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

    init_object(&conn->object, OBJ_CONN, _print_conn);
    conn->owner = NULL;

    conn->sd = -1;
    string_init(&conn->pname);
    /* {family, addrlen, addr} are initialized in enqueue handler */

    TAILQ_INIT(&conn->imsg_q);

    TAILQ_INIT(&conn->omsg_q);

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
    //conn->non_bytes_send = 0;
    conn_set_read_consistency(conn, g_read_consistency);
    conn_set_write_consistency(conn, g_write_consistency);
    conn->type = CONN_UNSPECIFIED;

    unsigned char *aes_key = generate_aes_key();
    strncpy((char *)conn->aes_key, (char *)aes_key, strlen((char *)aes_key)); //generate a new key for each connection

    return conn;
}

void
_add_to_ready_q(struct context *ctx, struct conn *conn)
{
    // This check is required to check if the connection is already
    // on the ready queue
    if (conn->ready_tqe.tqe_prev == NULL) {
        struct server_pool *pool = &ctx->pool;
        TAILQ_INSERT_TAIL(&pool->ready_conn_q, conn, ready_tqe);
    }
}

void
_remove_from_ready_q(struct context *ctx, struct conn *conn)
{
    // This check is required to check if the connection is already
    // on the ready queue
    if (conn->ready_tqe.tqe_prev != NULL) {
        struct server_pool *pool = &ctx->pool;
        TAILQ_REMOVE(&pool->ready_conn_q, conn, ready_tqe);
    }
}

static void
_conn_free(struct conn *conn)
{
    log_debug(LOG_VVERB, "free conn %p", conn);
    dn_free(conn);
}

void
_conn_put(struct conn *conn)
{
    nfree_connq++;
    TAILQ_INSERT_HEAD(&free_connq, conn, conn_tqe);
}

/**
 * Initialize connections.
 */
void
_conn_init(void)
{
    log_debug(LOG_DEBUG, "conn size %d", sizeof(struct conn));
    nfree_connq = 0;
    TAILQ_INIT(&free_connq);
}

void
_conn_deinit(void)
{
    struct conn *conn, *nconn; /* current and next connection */

    for (conn = TAILQ_FIRST(&free_connq); conn != NULL;
         conn = nconn, nfree_connq--) {
        ASSERT(nfree_connq > 0);
        nconn = TAILQ_NEXT(conn, conn_tqe);
        _conn_free(conn);
    }
    ASSERT(nfree_connq == 0);
}

rstatus_t
_conn_reuse(struct conn *p)
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
