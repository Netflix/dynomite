
#ifndef _DYN_DNODE_H_
#define _DYN_DNODE_H_

#include <nc_core.h>

void dnode_ref(struct conn *conn, struct server_pool *owner);
void dnode_unref(struct conn *conn);
void dnode_close(struct context *ctx, struct conn *conn);

rstatus_t dnode_each_init(void *elem, void *data);
rstatus_t dnode_each_deinit(void *elem, void *data);

rstatus_t dnode_init(struct context *ctx);
void dnode_deinit(struct context *ctx);
rstatus_t dnode_recv(struct context *ctx, struct conn *conn);

#endif

