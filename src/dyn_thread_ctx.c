#include <dyn_core.h>
#include <dyn_thread_ctx.h>
#include <dyn_server.h>

static tid_t tid_counter = 0;
__thread tid_t g_ptctx = NULL;
pthread_ctx
thread_ctx_create(void)
{
    pthread_ctx ptctx = dn_zalloc(sizeof(*ptctx));
    return ptctx;
}

static rstatus_t
thread_ctx_core(void *arg, uint32_t events)
{
	rstatus_t status;
	struct conn *conn = arg;
	struct context *ctx = conn_to_ctx(conn);

    log_debug(LOG_VVVERB, "event %04"PRIX32" on %s %d", events,
              conn_get_type_string(conn), conn->p.sd);

	conn->events = events;

	/* error takes precedence over read | write */
	if (events & EVENT_ERR) {
        log_debug(LOG_VVVERB, "handing error on %s %d",
                  conn_get_type_string(conn), conn->p.sd);
		if (conn->err && conn->dyn_mode) {
			loga("conn err on dnode EVENT_ERR: %d", conn->err);
		}
		conn_error(ctx, conn);

		return DN_ERROR;
	}

	/* read takes precedence over write */
	if (events & EVENT_READ) {
        log_debug(LOG_VVVERB, "handing read on %s %d",
                  conn_get_type_string(conn), conn->p.sd);
		status = conn_recv(ctx, conn);

		if (status != DN_OK || conn->done || conn->err) {
			if (conn->dyn_mode) {
				if (conn->err) {
					loga("conn err on dnode EVENT_READ: %d", conn->err);
					conn_close(ctx, conn);
					return DN_ERROR;
				}
				return DN_OK;
			}

			conn_close(ctx, conn);
			return DN_ERROR;
		}
	}

	if (events & EVENT_WRITE) {
        log_debug(LOG_VVVERB, "handing write on %s %d",
                  conn_get_type_string(conn), conn->p.sd);
		status = conn_send(ctx, conn);
		if (status != DN_OK || conn->done || conn->err) {
			if (conn->dyn_mode) {
				if (conn->err) {
					loga("conn err on dnode EVENT_WRITE: %d", conn->err);
					conn_close(ctx, conn);
					return DN_ERROR;
				}
				return DN_OK;
			}

			conn_close(ctx, conn);
			return DN_ERROR;
		}
	}

	return DN_OK;
}

rstatus_t
thread_ctx_datastore_preconnect(void *elem, void *arg)
{
    pthread_ctx ptctx = elem;
    struct context *ctx = ptctx->ctx;
    struct server_pool *pool = &ctx->pool;

    if (ptctx->datastore_conn == NULL)
        ptctx->datastore_conn = get_datastore_conn(ctx, pool);
    if (ptctx->datastore_conn == NULL) {
        log_error("Could not preconnect to datastore");
        return DN_ERROR;
    }
	return DN_OK;
}

rstatus_t
thread_ctx_init(pthread_ctx ptctx, struct context *ctx)
{
    ptctx->ctx = ctx;
    ptctx->tid = tid_counter++;
	ptctx->evb = event_base_create(EVENT_SIZE, &thread_ctx_core);
    ptctx->datastore_conn = NULL;
    msg_tmo_init(&ptctx->tmo, ptctx);
	if (ptctx->evb == NULL) {
		loga("Failed to create socket event handling!!!");
		return DN_ERROR;
	}
    return DN_OK;
}

rstatus_t
thread_ctx_deinit(void *elem, void *arg)
{
    pthread_ctx ptctx = elem;
	event_base_destroy(ptctx->evb);
    msg_tmo_deinit(&ptctx->tmo, ptctx);
    conn_close(ptctx->ctx, ptctx->datastore_conn);
    ptctx->datastore_conn = NULL;
    return DN_OK;
}

rstatus_t
thread_ctx_add_conn(pthread_ctx ptctx, struct conn *conn)
{
    log_debug(LOG_VVVERB, "ptctx %p: adding conn %p, %s", ptctx, conn, conn_get_type_string(conn));
    return event_add_conn(ptctx->evb, conn);
}

rstatus_t
thread_ctx_del_conn(pthread_ctx ptctx, struct conn *conn)
{
    log_debug(LOG_VVVERB, "ptctx %p: deleting conn %p, %s", ptctx, conn, conn_get_type_string(conn));
    return event_del_conn(ptctx->evb, &conn->p);
}

rstatus_t
thread_ctx_add_out(pthread_ctx ptctx, struct conn *conn)
{
    log_debug(LOG_VVVERB, "ptctx %p: adding out conn %p, %s", ptctx, conn, conn_get_type_string(conn));
    return event_add_out(ptctx->evb, &conn->p);
}

rstatus_t
thread_ctx_del_out(pthread_ctx ptctx, struct conn *conn)
{
    log_debug(LOG_VVVERB, "ptctx %p: deleting out conn %p, %s", ptctx, conn, conn_get_type_string(conn));
    return event_del_out(ptctx->evb, &conn->p);
}

rstatus_t
thread_ctx_add_in(pthread_ctx ptctx, struct conn *conn)
{
    log_debug(LOG_VVVERB, "ptctx %p: adding in conn %p, %s", ptctx, conn, conn_get_type_string(conn));
    return event_add_in(ptctx->evb, &conn->p);
}

static void
thread_ctx_timeout(pthread_ctx ptctx)
{
    struct context *ctx = ptctx->ctx;
	for (;;) {
		struct msg *msg;
		struct conn *conn;
		msec_t now, then;

		msg = msg_tmo_min(&ptctx->tmo);
		if (msg == NULL) {
			ptctx->timeout = ctx->max_timeout;
			return;
		}

		/* skip over req that are in-error or done */

		if (msg->error || msg->done) {
			msg_tmo_delete(&ptctx->tmo, msg);
			continue;
		}

		/*
		 * timeout expired req and all the outstanding req on the timing
		 * out server
		 */

		conn = msg->tmo_rbe.data;
		then = msg->tmo_rbe.key;

		now = dn_msec_now();
		if (now < then) {
			msec_t delta = then - now;
			ptctx->timeout = MIN(delta, ctx->max_timeout);
			return;
		}

        log_warn("req %"PRIu64" on %s %d timedout, timeout was %d", msg->id,
                 conn_get_type_string(conn), conn->p.sd, msg->tmo_rbe.timeout);

		msg_tmo_delete(&ptctx->tmo, msg);

		if (conn->dyn_mode) {
			if (conn->p.type == CONN_DNODE_PEER_SERVER) { //outgoing peer requests
                if (conn->same_dc)
			        stats_pool_incr(ctx, peer_timedout_requests);
                else
			        stats_pool_incr(ctx, remote_peer_timedout_requests);
			}
		} else {
			if (conn->p.type == CONN_SERVER) { //storage server requests
			   stats_server_incr(ctx, server_dropped_requests);
			}
		}

		conn->err = ETIMEDOUT;

		conn_close(ctx, conn);
	}
}

rstatus_t
thread_ctx_run_once(pthread_ctx ptctx)
{
	int nsd;
    g_ptctx = ptctx;
	nsd = event_wait(ptctx->evb, (int)ptctx->timeout);
	if (nsd < 0) {
		return nsd;
	}
	thread_ctx_timeout(ptctx);
    return DN_OK;
}
