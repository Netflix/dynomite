#pragma once

#include "dyn_types.h"

// Forward declarations.
struct conn;
struct context;

extern void _conn_deinit(void);
extern void _conn_init(void);
extern struct conn *_conn_get(void);
extern void _conn_put(struct conn *conn);
extern char *_conn_get_type_string(struct conn *conn);
extern void _add_to_ready_q(struct context *ctx, struct conn *conn);
extern void _remove_from_ready_q(struct context *ctx, struct conn *conn);
extern rstatus_t _conn_reuse(struct conn *p);
