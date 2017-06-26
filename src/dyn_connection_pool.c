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

    // connection state
    struct array        active_connections;         /* pool connections */
    uint8_t             active_conn_count;    /* Count of currently good connections */

    // backoff logic
    uint8_t             failure_count;
    uint8_t             max_failure_count;
    msec_t              current_timeout_sec;
    msec_t              max_timeout_sec;
    struct task         *scheduled_reconnect_task;
};

static int
_print_conn_pool(FILE *stream, const struct object *obj)
{
    ASSERT(obj->type == OBJ_CONN_POOL);
    conn_pool_t *cp = (conn_pool_t *)obj;
    return fprintf(stream, "<CONN_POOL %p active_conn:%u in array %u max %u>", cp,
                   cp->active_conn_count, array_n(&cp->active_connections), cp->max_connections);
}

static void
_create_missing_connections(conn_pool_t *cp)
{
    // create connections if they are less than required.
    uint8_t idx = 0, failures = 0;
    uint32_t count = array_n(&cp->active_connections);
    while (idx < count) {
        struct conn** pconn = array_get(&cp->active_connections, idx);
        if (*pconn != NULL) {
            idx++;
            continue;
        }
        struct conn *conn = conn_get(cp->owner, cp->func_conn_init);
        if (conn != NULL) {
            conn->conn_pool = cp;
            log_notice("%M %M created %M", cp->owner, cp, conn);
            *pconn = conn; // set that in the array
            cp->active_conn_count++;
            idx++;
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
    init_object(&cp->obj, OBJ_CONN_POOL, _print_conn_pool);
    cp->max_connections = max_connections;
    cp->owner = owner;
    cp->ctx = ctx;
    cp->func_conn_init = func_conn_init;

    cp->active_conn_count = 0;
    if (array_init(&cp->active_connections, max_connections, sizeof(struct conn *))
            != DN_OK)
    {
        log_notice("%M Failed to initialize conn array", owner);
        dn_free(cp);
        return NULL;
    }
    cp->failure_count = 0;
    cp->max_failure_count = max_failures;
    cp->current_timeout_sec = 0;
    cp->max_timeout_sec = max_timeout;
    cp->scheduled_reconnect_task = NULL;

    log_notice("%M Creating %M", cp->owner, cp);
    uint8_t idx = 0;
    for (idx = 0; idx < max_connections; idx++) {
        struct conn **pconn = array_push(&cp->active_connections);
        *pconn = NULL;
    }
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
    uint8_t idx = 0;
    uint32_t count = array_n(&cp->active_connections);
    for (idx = 0; idx < count; idx++) {
        struct conn **pconn = array_get(&cp->active_connections, idx);
        if (*pconn == NULL)
            continue;
        struct conn *conn = *pconn;
        rstatus_t s = conn_connect(cp->ctx, conn);
        if (s == DN_OK) {
            continue;
        }
        // this will remove the connection from the array
        conn_close(cp->ctx, conn);
        overall_status = s;
    }
    return overall_status;
}

struct conn *
conn_pool_get(conn_pool_t *cp, int tag)
{
    struct conn **pconn = array_get(&cp->active_connections,
                                    (uint32_t)tag % array_n(&cp->active_connections));
    if (*pconn) {
        if ((*pconn)->connected) {
            return *pconn;
        } else {
            return NULL;
        }
    }
    return *pconn;
}

rstatus_t
conn_pool_destroy(conn_pool_t *cp)
{
    if (cp->scheduled_reconnect_task) {
        log_info("%M %M Cancelling task %p", cp->owner, cp,
                 cp->scheduled_reconnect_task);
        cancel_task(cp->scheduled_reconnect_task);
    }
    cp->scheduled_reconnect_task = NULL;
    uint8_t idx = 0;
    uint32_t count = array_n(&cp->active_connections);
    for (idx = 0; idx < count; idx++) {
        struct conn** pconn = array_get(&cp->active_connections, idx);
        if (*pconn == NULL) {
            continue;
        }
        struct conn *conn = *pconn;
        log_notice("%M Closing %M", cp, conn);
        conn_close(cp->ctx, conn);
        *pconn = NULL;
    }
    log_notice("%M Destroying", cp);
    dn_free(cp);
    return DN_OK;
}

void
conn_pool_notify_conn_close(conn_pool_t *cp, struct conn *conn)
{
    log_notice("%M Removing %M", cp, conn);
    if (conn == NULL)
        return;

    uint8_t idx = 0;
    uint32_t count = array_n(&cp->active_connections);
    for (idx = 0; idx < count; idx++) {
        struct conn** pconn = array_get(&cp->active_connections, idx);
        if (*pconn == conn) {
            *pconn = NULL;
            cp->active_conn_count--;
            return;
        }
    }
    log_warn("%M did not find %M", cp, conn);
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
    // else increase error count, and schedule a task after the backoff wait
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
    return cp->active_conn_count;
}
