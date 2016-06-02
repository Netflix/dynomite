#include <dyn_core.h>
#include <dyn_thread_ctx.h>

static tid_t tid_counter = 0;
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
              conn_get_type_string(conn), conn->sd);

	conn->events = events;

	/* error takes precedence over read | write */
	if (events & EVENT_ERR) {
		if (conn->err && conn->dyn_mode) {
			loga("conn err on dnode EVENT_ERR: %d", conn->err);
		}
		conn_error(ctx, conn);

		return DN_ERROR;
	}

	/* read takes precedence over write */
	if (events & EVENT_READ) {
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

static rstatus_t
thread_ctx_init_each(void *elem, void *arg)
{
    pthread_ctx ptctx = elem;
    struct context *ctx = arg;
    ptctx->ctx = ctx;
    ptctx->tid = tid_counter++;
	ptctx->evb = event_base_create(EVENT_SIZE, &thread_ctx_core);
	if (ptctx->evb == NULL) {
		loga("Failed to create socket event handling!!!");
		return DN_ERROR;
	}
    return DN_OK;
}

rstatus_t
thread_ctx_init(struct context *ctx)
{
    array_null(&ctx->thread_ctxs);
    THROW_STATUS(array_init(&ctx->thread_ctxs, 1, sizeof(thread_ctx)));
    pthread_ctx ptctx = array_push(&ctx->thread_ctxs);
    thread_ctx_init_each(ptctx, ctx);
}

static rstatus_t
thread_ctx_deinit_each(void *elem, void *arg)
{
    pthread_ctx ptctx = elem;
	event_base_destroy(ptctx->evb);
    return DN_OK;
}

void
thread_ctx_deinit(struct context *ctx)
{
    rstatus_t status = array_each(&ctx->thread_ctxs, thread_ctx_deinit_each, NULL);
    IGNORE_RET_VAL(status);
}

rstatus_t
thread_ctx_run_once(pthread_ctx ptctx)
{
	int nsd;
	nsd = event_wait(ptctx->evb, ptctx->ctx->timeout);
	if (nsd < 0) {
		return nsd;
	}

}
