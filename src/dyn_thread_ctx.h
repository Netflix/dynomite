#pragma once

#include <dyn_error.h>
#include <dyn_msg_tmo.h>
struct context;
struct event_base;
typedef uint16_t tid_t;

typedef struct thread_ctx {
    struct context      *ctx;
    tid_t               tid;
    struct event_base   *evb;
    struct msg_tmo      tmo;
    msec_t              timeout;     /* timeout in msec */
}thread_ctx;
typedef struct thread_ctx *pthread_ctx;

pthread_ctx thread_ctx_create(void);

// Run functions
rstatus_t thread_ctx_run_once(pthread_ctx ptctx);

// Init and Deinit
rstatus_t thread_ctx_init(struct context *ctx);
void thread_ctx_deinit(struct context *ctx);

rstatus_t thread_ctx_add_conn(pthread_ctx ptctx, struct conn *conn);
rstatus_t thread_ctx_del_conn(pthread_ctx ptctx, struct conn *conn);
rstatus_t thread_ctx_add_out(pthread_ctx ptctx, struct conn *conn);
rstatus_t thread_ctx_del_out(pthread_ctx ptctx, struct conn *conn);
