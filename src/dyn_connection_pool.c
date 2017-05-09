#include "dyn_core.h"
#include "dyn_connection_pool.h"
#include "dyn_array.h"

struct conn_pool {
    struct object   obj;
    uint8_t max_connections; // connections this conn_pool owns
    void    *owner;          // the owner of this conn pool, this gets passed
                             // to each connection
    struct context      *ctx;
    func_conn_init_t    func_conn_init; // initializtion function for each connection
    struct conn_tqh     active_conn_q;         /* pool connection q */
    // A hash table
};

static int
print_conn_pool(FILE *stream, const struct object *obj)
{
    ASSERT(obj->type == OBJ_CONN_POOL);
    conn_pool_t *cp = (conn_pool_t *)obj;
    return fprintf(stream, "<CONN_POOL %p active_conn:%lu max %u>", cp,
                   TAILQ_COUNT(&cp->active_conn_q), cp->max_connections);
}

conn_pool_t *
conn_pool_create(struct context *ctx, void *owner, uint8_t max_connections,
                 func_conn_init_t func_conn_init)
{
    conn_pool_t *cp = dn_alloc(sizeof(struct conn_pool));
    if (!cp)
        return NULL;

    init_object(&cp->obj, OBJ_CONN_POOL, print_conn_pool);
    cp->ctx = ctx;
    cp->max_connections = max_connections;
    cp->owner = owner;
    cp->func_conn_init = func_conn_init;
    TAILQ_INIT(&cp->active_conn_q);
    uint8_t index = 0;
    for (index = 0; index < max_connections; index++) {
        struct conn *conn = conn_get(owner, func_conn_init);
        if (conn == NULL) {
            continue;
        }
        conn->conn_pool = cp;
        log_notice("created %M", conn);
        TAILQ_INSERT_TAIL(&cp->active_conn_q, conn, pool_tqe);
    }
    log_notice("created %M", cp);
    return cp;
}

rstatus_t
conn_pool_preconnect(conn_pool_t *cp)
{
    // for each conn in array, call conn_connect
    rstatus_t overall_status = DN_OK;
    struct conn *conn, *nconn;
    TAILQ_FOREACH_SAFE(conn, &cp->active_conn_q, pool_tqe, nconn) {
        log_notice("conn %M nconn %M", conn, nconn);
        rstatus_t s = conn_connect(cp->ctx, conn);
        if (s == DN_OK) {
            continue;
        }

        TAILQ_REMOVE(&cp->active_conn_q, conn, pool_tqe);
        ASSERT(TAILQ_COUNT(&cp->active_conn_q) > 0);
        conn_close(cp->ctx, conn);
        overall_status = s;
    }
    return overall_status;
}

struct conn *
conn_pool_get(conn_pool_t *cp, uint16_t tag)
{
    // use tag to get conn in the hashtable.
    // if conn found, return that
    // get a new connection to use from the currenly active connections
    // add tag->new_conn in hash table
    // return new_conn

    // Attempt reconnect if connections are few.
    if (TAILQ_COUNT(&cp->active_conn_q) < cp->max_connections) {
        struct conn *conn = conn_get(cp->owner, cp->func_conn_init);
        if (conn != NULL) {
            conn->conn_pool = cp;
            log_notice("created %M", conn);
            TAILQ_INSERT_TAIL(&cp->active_conn_q, conn, pool_tqe);
        }
    }


    // TODO: First cut: just return a random connection in the queue and recycle
    if (TAILQ_COUNT(&cp->active_conn_q) > 0) {
        struct conn *conn = TAILQ_FIRST(&cp->active_conn_q);
        //recycle connection
        TAILQ_REMOVE(&cp->active_conn_q, conn, pool_tqe);
        TAILQ_INSERT_TAIL(&cp->active_conn_q, conn, pool_tqe);
        //log_notice("returning %M", conn);
        return conn;
    }
    return NULL;
}

rstatus_t
conn_pool_reset(conn_pool_t *cp)
{
    // clear everything in hash table.
    // for every connection, kill it
    return DN_OK;
}

void
conn_pool_notify_conn_close(conn_pool_t *cp, struct conn *conn)
{
    log_notice("%M Removing %M", cp, conn);
    TAILQ_REMOVE(&cp->active_conn_q, conn, pool_tqe);
}
