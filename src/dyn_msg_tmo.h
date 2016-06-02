#pragma once
// TODO: SHAILESH, not good to pull in dyn_core everywhere just for the types.
#include <dyn_core.h>

extern uint8_t g_timeout_factor;
struct thread_ctx;
struct msg_tmo {
    struct rbtree       tmo_rbt;    /* timeout rbtree */
    // SHAILESH: TODO: Not sure what is this
    struct rbnode       tmo_rbs;    /* timeout rbtree sentinel */
    struct thread_ctx   *ptctx;
};

struct msg *msg_tmo_min(struct msg_tmo *tmo);
void msg_tmo_insert(struct msg_tmo *tmo, struct conn *conn, struct msg *msg);
void msg_tmo_delete(struct msg_tmo *tmo, struct msg *msg);
void msg_tmo_init(struct msg_tmo *tmo, struct thread_ctx *ptctx);
void msg_tmo_deinit(struct msg_tmo *tmo, struct thread_ctx *ptctx);
