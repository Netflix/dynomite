/*
 * dyn_dnode_tls_server.h
 */
#include "dyn_core.h"

#ifndef DYN_DNODE_TLS_SERVER_H_
#define DYN_DNODE_TLS_SERVER_H_


void dnode_tls_ref(struct conn *conn, void *owner);
void dnode_tls_unref(struct conn *conn);
void dnode_tls_close(struct context *ctx, struct conn *conn);

rstatus_t dnode_tls_each_init(void *elem, void *data);
rstatus_t dnode_tls_each_deinit(void *elem, void *data);

rstatus_t dnode_tls_init(struct context *ctx);
void dnode_tls_deinit(struct context *ctx);
rstatus_t dnode_tls_recv(struct context *ctx, struct conn *conn);

#endif /* DYN_DNODE_TLS_SERVER_H_ */
