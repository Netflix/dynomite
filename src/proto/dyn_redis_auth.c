/*
 * Datastore Authentification
 * 
 * If Dynomite is configured to require a password via config option `requirepass`
 * the following behaviour will be applied:
 * 
 * Prerequisite: The datastore(s), here Redis, needs to be configured to require 
 *               the same password.
 * 
 * 1. On Dynomite startup, the server authenticates with the backend itself
 *    by calling the datastore agnostic function g_datastore_auth.
 * 2. The corresponding Redis response will be handeled in g_is_authenticated. 
 *    Dynomite will exit if authentification to the datatstore was not successful.
 * 3. Each newly created client connection will require authentification.
 * 4. Clients can authentificate itself by issue the AUTH command against dynomite.
 * 5. Dynomite will check the password and simulate an AUTH response.
 * 6. If AUTH was successful, the auth_required flag on the connection is reset and
 *    the client can process further commands through this connection.
 */

#include "../dyn_core.h"
#include "../dyn_message.h"
#include "dyn_proto.h"

struct msg *craft_auth_rsp(struct context *ctx, struct conn *conn,
    struct msg *req, char *auth_msg);
rstatus_t simulate_auth_rsp(struct context *ctx, struct conn *conn,
                           struct msg *msg, char *auth_msg);
static int extract_password(unsigned char *string, unsigned int len, char *passwd,
                            uint32_t *passwd_len);

static const char *AUTH_UNKNOWN = "-Unknown cmd";
static const char *AUTH_OK = "+OK\r\n";
static const char *AUTH_ERR = "-ERR invalid password\r\n";
static const char *AUTH_NOAUTH = "-NOAUTH Authentication required\r\n";

/*
 * Issue the auth command against datastore
 */
void redis_datatstore_auth(struct context *ctx, struct conn *conn) {
#define REDIS_AUTH_CMD "*2\r\n$4\r\nAUTH\r\n"
  struct server_pool *pool;
  struct msg *msg;
  struct mbuf *mbuf;
  int n;
  char auth[1024];

  pool = &ctx->pool;
  if (pool->requirepass.len > 0) {
    msg = msg_get(conn, true, __FUNCTION__);
    if (msg == NULL) {
      return;
    }

    mbuf = mbuf_get();
    if (mbuf == NULL) {
      return;
    }

    n = snprintf(auth, sizeof(auth), REDIS_AUTH_CMD "$%d\r\n%s\r\n",
                 pool->requirepass.len, pool->requirepass.data);

    memcpy(mbuf->last, auth, (size_t)n);
    mbuf->last += n;
    mbuf_insert(&msg->mhdr, mbuf);
    msg->pos = mbuf->pos;
    msg->mlen = (uint32_t)n;
    TAILQ_INSERT_TAIL(&conn->imsg_q, msg, s_tqe);
  }
#undef REDIS_AUTH_CMD
}

/*
 * Check if AUTH repsonse against datastore is +OK
 */
bool redis_is_authenticated(struct msg *rsp) {
  int res = memcmp(AUTH_OK, rsp->mhdr.stqh_first->start, rsp->mlen);
  return (res == 0);
}

/*
 * Process the Redis AUTH command for client connections 
 */
bool redis_authenticate_conn(struct context *ctx, struct conn *conn, struct msg *req) {
  struct server_pool *pool;
  // Default: AUTH required
  char *auth_msg = AUTH_NOAUTH; 
  bool authenticated = false;

  // Only handle redis auth requests
  if (req->type == MSG_REQ_REDIS_AUTH) {
    pool = &ctx->pool;
    char passwd[128];
    uint32_t passwd_len;
    
    // Default: Wrong password
    auth_msg = AUTH_ERR;
    
    if (extract_password(req->mhdr.stqh_first->start,
                           req->mlen, passwd, &passwd_len) != 0) {
      // No password provided
      auth_msg = AUTH_UNKNOWN;
    }
    else if (passwd_len == pool->requirepass.len) {
      if (memcmp(pool->requirepass.data, passwd, passwd_len) == 0) {
        // Password matches
        auth_msg = AUTH_OK;
        authenticated = true;
      }
    }
  }
  
  IGNORE_RET_VAL(simulate_auth_rsp(ctx, conn, req, auth_msg));
  return authenticated;
}

struct msg *craft_auth_rsp(struct context *ctx, struct conn *conn,
    struct msg *req, char *auth_msg) {

  ASSERT(req->is_request);
  
  struct msg *rsp = msg_get(conn, false, __FUNCTION__);
  if (rsp == NULL) {
    conn->err = errno;
    return NULL;
  }

  rstatus_t append_status = msg_append(rsp, auth_msg, strlen(auth_msg));
  if (append_status != DN_OK) {
    rsp_put(rsp);
    return NULL;
  }

  rsp->peer = req;
  rsp->is_request = 0;

  req->done = 1;

  return rsp;
}

rstatus_t simulate_auth_rsp(struct context *ctx, struct conn *conn,
    struct msg *msg, char *auth_msg) {
  // Create an AUTH response.
  struct msg *auth_rsp = craft_auth_rsp(ctx, conn, msg, auth_msg);

  // Add it to the outstanding messages dictionary, so that 'conn_handle_response'
  // can process it appropriately.
  dictAdd(conn->outstanding_msgs_dict, &msg->id, msg);

  // Enqueue the message in the outbound queue so that the code on the response
  // path can find it.
  conn_enqueue_outq(ctx, conn, msg);

  THROW_STATUS(conn_handle_response(ctx, conn,
      msg->parent_id ? msg->parent_id : msg->id, auth_rsp));

  return DN_OK;
}

int extract_password(unsigned char *string, unsigned int len, char *passwd,
                     uint32_t *passwd_len) {
  char *p;
  char *pos;
  char buff[128];
  int cmdlen, nargc;
  size_t l;

  p = (char *)string;
  if (p[0] != '*') {
    return -1;
  }

  /* deal with nargc */
  pos = strstr(p + 1, CRLF);
  if (!pos) return -1;
  l = pos - (p + 1);
  memcpy(buff, p + 1, l);
  buff[l] = '\0';
  nargc = atoi(buff);
  if (nargc != 2) return -1;

  /* deal with cmd */
  p = pos + 2;
  if (*p != '$') return -1;
  pos = strstr(p + 1, CRLF);
  if (!pos) return -1;
  l = pos - (p + 1);
  memcpy(buff, p + 1, l);
  buff[l] = '\0';
  cmdlen = atoi(buff);
  p = pos + 2;
  pos = strstr(p, CRLF);
  if (!pos) return -1;
  l = pos - p;
  if (l != cmdlen) return -1;
  memcpy(buff, p, l);
  buff[l] = '\0';
  if (strcasecmp("AUTH", buff) != 0) return -1;

  /* password */
  p = pos + 2;
  if (*p != '$') return -1;
  pos = strstr(p + 1, CRLF);
  if (!pos) return -1;
  l = pos - (p + 1);
  memcpy(buff, p + 1, l);
  buff[l] = '\0';
  *passwd_len = atoi(buff);
  p = pos + 2;
  pos = strstr(p, CRLF);
  if (!pos) return -1;
  l = pos - p;
  memcpy(passwd, p, l);
  passwd[l] = '\0';
  return 0;
}
