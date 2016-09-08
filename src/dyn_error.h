#pragma once

#define DN_NOOPS     1
#define DN_OK        0
#define DN_ERROR    -1
#define DN_EAGAIN   -2
#define DN_ENOMEM   -3
#define DN_ENO_IMPL -4
#define DN_ENOHOST  -5
#define DN_EHOST_DOWN -6
#define DN_EHOST_STATE_INVALID -7

typedef int rstatus_t; /* return type */
typedef int err_t;     /* error type */

extern char *dn_client_strerror(err_t err);
