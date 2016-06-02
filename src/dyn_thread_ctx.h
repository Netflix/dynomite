#pragma once

#include <dyn_error.h>
struct context;
struct event_base;
typedef uint16_t tid_t;

typedef struct thread_ctx {
    struct context      *ctx;
    tid_t               tid;
    struct event_base   *evb;
}thread_ctx;
typedef struct thread_ctx *pthread_ctx;

pthread_ctx thread_ctx_create(void);

// Run functions
rstatus_t thread_ctx_run_once(pthread_ctx ptctx);

// Init and Deinit
rstatus_t thread_ctx_init(struct context *ctx);
void thread_ctx_deinit(struct context *ctx);
