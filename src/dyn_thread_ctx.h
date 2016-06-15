#pragma once

#include <dyn_error.h>
#include <dyn_msg_tmo.h>
#include <dyn_thread_ipc.h>
struct context;
struct event_base;
typedef uint16_t tid_t;
typedef struct thread_ctx *pthread_ctx;
extern __thread pthread_ctx g_ptctx;

typedef struct thread_ctx {
    struct context      *ctx;
    tid_t               tid;
    pthread_t           pthread_id;
    struct event_base   *evb;
    struct conn         *datastore_conn;
    pthread_ipc         ptipc;
    struct msg_tmo      tmo;
    msec_t              timeout;     /* timeout in msec */
}thread_ctx;

pthread_ctx thread_ctx_create(void);

// Run functions
void *thread_ctx_run(void *arg);

// Init and Deinit
rstatus_t thread_ctx_init(pthread_ctx ptctx, struct context *ctx);
rstatus_t thread_ctx_deinit(void *elem, void *arg);

// Preconnection functions
rstatus_t thread_ctx_datastore_preconnect(void *elem, void *arg);

// event interfaces
rstatus_t thread_ctx_add_conn(pthread_ctx ptctx, struct pollable *conn);
rstatus_t thread_ctx_del_conn(pthread_ctx ptctx, struct pollable *conn);
rstatus_t thread_ctx_add_out(pthread_ctx ptctx, struct pollable *conn);
rstatus_t thread_ctx_del_out(pthread_ctx ptctx, struct pollable *conn);
rstatus_t thread_ctx_add_in(pthread_ctx ptctx, struct pollable *conn);

// forward msg to this thread
rstatus_t thread_ctx_forward_req(pthread_ctx ptctx, struct msg *msg);
rstatus_t thread_ctx_forward_rsp(pthread_ctx ptctx, struct msg *msg);
