#ifndef _DYN_CLIENT_H_
#define _DYN_CLIENT_H_

#include <nc_core.h>

bool dyn_client_active(struct conn *conn);
void dyn_client_ref(struct conn *conn, void *owner);
void dyn_client_unref(struct conn *conn);
void dyn_client_close(struct context *ctx, struct conn *conn);

#endif
