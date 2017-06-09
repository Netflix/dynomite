#include "dyn_core.h"
#include "dyn_connection_pool.h"
#include "dyn_task.h"

#define MIN_WAIT_BEFORE_RECONNECT_IN_SECS    1ULL

struct conn_pool {
    struct object   obj;
    uint8_t max_connections; // connections this conn_pool owns
    void    *owner;          // the owner of this conn pool, this gets passed
                             // to each connection
    struct context      *ctx;
    func_conn_init_t    func_conn_init; // initializtion function for each connection
    struct conn_tqh     active_conn_q;         /* pool connection q */
    uint8_t             failure_count;
    uint8_t             max_failure_count;
    msec_t              current_timeout_sec;
    msec_t              max_timeout_sec;
    struct task         *scheduled_reconnect_task;
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

static void
_create_missing_connections(conn_pool_t *cp)
{
    // Attempt reconnect if connections are few.
    uint8_t failures = 0;
    while (TAILQ_COUNT(&cp->active_conn_q) < cp->max_connections) {
        struct conn *conn = conn_get(cp->owner, cp->func_conn_init);
        if (conn != NULL) {
            conn->conn_pool = cp;
            log_notice("%M %M created %M", cp->owner, cp, conn);
            TAILQ_INSERT_TAIL(&cp->active_conn_q, conn, pool_tqe);
        } else {
            if (++failures == 3) {
                return;
            }
        }
    }

}

conn_pool_t *
conn_pool_create(struct context *ctx, void *owner, uint8_t max_connections,
                 func_conn_init_t func_conn_init, uint8_t max_failures,
                 sec_t max_timeout)
{
    conn_pool_t *cp = dn_alloc(sizeof(struct conn_pool));
    if (!cp)
        return NULL;
    init_object(&cp->obj, OBJ_CONN_POOL, print_conn_pool);
    cp->max_connections = max_connections;
    cp->owner = owner;
    cp->ctx = ctx;
    cp->func_conn_init = func_conn_init;
    TAILQ_INIT(&cp->active_conn_q);
    cp->failure_count = 0;
    cp->max_failure_count = max_failures;
    cp->current_timeout_sec = 0;
    cp->max_timeout_sec = max_timeout;
    cp->scheduled_reconnect_task = NULL;

    log_notice("%M Creating %M", cp->owner, cp);
    _create_missing_connections(cp);
    return cp;
}

rstatus_t
conn_pool_preconnect(conn_pool_t *cp)
{
    log_notice("%M %M Preconnecting", cp->owner, cp);
    _create_missing_connections(cp);
    // for each conn in array, call conn_connect
    rstatus_t overall_status = DN_OK;
    struct conn *conn, *nconn;
    TAILQ_FOREACH_SAFE(conn, &cp->active_conn_q, pool_tqe, nconn) {
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

// TODO: Decide the behaviour for this function.
rstatus_t
conn_pool_destroy(conn_pool_t *cp)
{
    // clear everything in hash table.
    // for every connection, kill it
    if (cp->scheduled_reconnect_task) {
        log_info("%M %M Cancelling task %p", cp->owner, cp,
                 cp->scheduled_reconnect_task);
        cancel_task(cp->scheduled_reconnect_task);
    }
    cp->scheduled_reconnect_task = NULL;
    struct conn *conn, *nconn;
    TAILQ_FOREACH_SAFE(conn, &cp->active_conn_q, pool_tqe, nconn) {
        log_notice("%M Closing %M", cp, conn);
        conn_close(cp->ctx, conn);
        TAILQ_REMOVE(&cp->active_conn_q, conn, pool_tqe);
    }
    log_notice("%M Destroying", cp);
    dn_free(cp);
    return DN_OK;
}

void
conn_pool_notify_conn_close(conn_pool_t *cp, struct conn *conn)
{
    log_notice("%M Removing %M", cp, conn);
    TAILQ_REMOVE(&cp->active_conn_q, conn, pool_tqe);
}

static void
_conn_pool_reconnect_task(void *arg1)
{
    conn_pool_t *cp = arg1;
    cp->scheduled_reconnect_task = NULL;
    conn_pool_preconnect(cp);
}

void
conn_pool_notify_conn_errored(conn_pool_t *cp)
{
    // check if reconnect task is active
    // if so , never mind
    if (cp->scheduled_reconnect_task) {
        return;
    }
    // else increase erorr count, and schedule a task after the backoff wait
    cp->failure_count++;

    if (cp->current_timeout_sec < (MIN_WAIT_BEFORE_RECONNECT_IN_SECS))
        cp->current_timeout_sec = MIN_WAIT_BEFORE_RECONNECT_IN_SECS;

    log_notice("%M %M Scheduling reconnect task after %u secs", cp->owner, cp,
               cp->current_timeout_sec);
    cp->scheduled_reconnect_task = schedule_task_1(_conn_pool_reconnect_task,
                                                   cp, cp->current_timeout_sec * 1000);
    log_info("%M %M Scheduled %p", cp->owner, cp, cp->scheduled_reconnect_task);

    cp->current_timeout_sec = 2 * cp->current_timeout_sec;
    if (cp->current_timeout_sec > cp->max_timeout_sec)
        cp->current_timeout_sec = cp->max_timeout_sec;
}
    
void
conn_pool_connected(conn_pool_t *cp, struct conn *conn)
{
    cp->failure_count = 0;
    cp->current_timeout_sec = 0;
}

uint8_t
conn_pool_active_count(conn_pool_t *cp)
{
    return (uint8_t)TAILQ_COUNT(&cp->active_conn_q);
}
