/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * In twemproxy there are 3 types of connections:
 * PROXY - listens for client connections (default: 8102)
 * CLIENT - incoming connection from the client
 * SERVER - outgoing connection to the underlying data store.
 *
 * Dynomite extended this same concept and added 3 other types of connections
 * DNODE_PEER_PROXY - listens to connections from other dynomite node (default
 * 8101) DNODE_PEER_CLIENT - incoming connection from other dnode
 * DNODE_PEER_SERVER - outgoing connection to other dnode.
 *
 */

#ifndef _DYN_CONNECTION_H_
#define _DYN_CONNECTION_H_

#include <sys/socket.h>

#include "dyn_message.h"
#include "dyn_queue.h"
#include "dyn_string.h"
#include "dyn_types.h"

#define MAX_CONN_QUEUE_SIZE 20000
#define MAX_CONN_ALLOWABLE_NON_RECV 5
#define MAX_CONN_ALLOWABLE_NON_SEND 5

typedef rstatus_t (*func_recv_t)(struct context *, struct conn *);
typedef struct msg *(*func_recv_next_t)(struct context *, struct conn *, bool);
typedef void (*func_recv_done_t)(struct context *, struct conn *, struct msg *,
                                 struct msg *);

typedef rstatus_t (*func_send_t)(struct context *, struct conn *);
typedef struct msg *(*func_send_next_t)(struct context *, struct conn *);
typedef void (*func_send_done_t)(struct context *, struct conn *, struct msg *);

typedef void (*func_close_t)(struct context *, struct conn *);
typedef bool (*func_active_t)(struct conn *);

typedef void (*func_ref_t)(struct conn *, void *);
typedef void (*func_unref_t)(struct conn *);

typedef void (*func_msgq_t)(struct context *, struct conn *, struct msg *);
typedef rstatus_t (*func_response_handler)(struct context *ctx, struct conn *, msgid_t reqid,
                                           struct msg *rsp);
struct conn_pool;

struct conn_ops {
  func_recv_t recv;           /* recv (read) handler */
  func_recv_next_t recv_next; /* recv next message handler */
  func_recv_done_t recv_done; /* read done handler */
  func_send_t send;           /* send (write) handler */
  func_send_next_t send_next; /* write next message handler */
  func_send_done_t send_done; /* write done handler */
  func_close_t close;         /* close handler */
  func_active_t active;       /* active? handler */

  func_ref_t ref;     /* connection reference handler */
  func_unref_t unref; /* connection unreference handler */

  func_msgq_t enqueue_inq;  /* connection inq msg enqueue handler */
  func_msgq_t dequeue_inq;  /* connection inq msg dequeue handler */
  func_msgq_t enqueue_outq; /* connection outq msg enqueue handler */
  func_msgq_t dequeue_outq; /* connection outq msg dequeue handler */
  func_response_handler rsp_handler;
};

typedef enum connection_type {
  CONN_UNSPECIFIED,
  CONN_PROXY,   // a dynomite proxy (listening) connection
  CONN_CLIENT,  // this is connected to a client connection
  CONN_SERVER,  // this is connected to underlying datastore ...redis/memcache
  CONN_DNODE_PEER_PROXY,   // this is a dnode (listening) connection...default
                           // 8101
  CONN_DNODE_PEER_CLIENT,  // this is connected to a dnode peer client
  CONN_DNODE_PEER_SERVER,  // this is connected to a dnode peer server
} connection_type_t;

struct conn {
  object_t object;
  TAILQ_ENTRY(conn) conn_tqe;  /* link in server_pool / server / free q */
  TAILQ_ENTRY(conn) ready_tqe; /* link in ready connection q */
  void *owner;                 /* connection owner - server_pool / server */
  struct conn_pool *conn_pool;

  int sd; /* socket descriptor */
  struct string pname;
  int family;            /* socket address family */
  socklen_t addrlen;     /* socket length */
  struct sockaddr *addr; /* socket address (ref in server or server_pool) */

  struct msg_tqh imsg_q; /* incoming request Q */
  struct msg_tqh omsg_q; /* outstanding request Q */

  struct msg *rmsg; /* current message being rcvd */
  struct msg *smsg; /* current message being sent */

  struct conn_ops *ops;
  size_t recv_bytes; /* received (read) bytes */
  size_t send_bytes; /* sent (written) bytes */

  uint32_t events;          /* connection io events */
  err_t err;                /* connection errno */
  unsigned recv_active : 1; /* recv active? */
  unsigned recv_ready : 1;  /* recv ready? */
  unsigned send_active : 1; /* send active? */
  unsigned send_ready : 1;  /* send ready? */

  unsigned connecting : 1;       /* connecting? */
  unsigned connected : 1;        /* connected? */
  unsigned eof : 1;              /* eof? aka passive close? */
  unsigned waiting_to_unref : 1; /* eof? aka passive close? */
  unsigned done : 1;             /* done? aka close? */
  unsigned dyn_mode : 1;         /* is a dyn connection? */
  unsigned dnode_secured : 1;    /* is a secured connection? */
  unsigned crypto_key_sent : 1;  /* crypto state */
  unsigned char aes_key[50];  // aes_key[34];              /* a place holder for
                              // AES key */
  unsigned same_dc : 1;  /* bit to indicate whether a peer conn is same DC */
  uint32_t avail_tokens; /* used to throttle the traffics */
  uint32_t last_sent;    /* ts in sec used to determine the last sent time */
  // uint32_t           non_bytes_send;        /* #times or epoll triggers that
  // we are not able to send any bytes */
  consistency_t read_consistency;
  consistency_t write_consistency;
  dict *outstanding_msgs_dict;
  connection_type_t type;
};

static inline rstatus_t conn_cant_handle_response(struct context *ctx, struct conn *conn,
                                                  msgid_t reqid,
                                                  struct msg *resp) {
  return DN_ENO_IMPL;
}

static inline rstatus_t conn_handle_response(struct context *ctx, struct conn *conn,
                                             msgid_t msgid,
                                             struct msg *rsp) {
  return conn->ops->rsp_handler(ctx, conn, msgid, rsp);
}

#define conn_recv(ctx, conn) (conn)->ops->recv(ctx, conn)
#define conn_recv_next(ctx, conn, alloc) \
  (conn)->ops->recv_next(ctx, conn, alloc)
#define conn_recv_done(ctx, conn, msg, nmsg) \
  (conn)->ops->recv_done(ctx, conn, msg, nmsg)

#define conn_send(ctx, conn) (conn)->ops->send(ctx, conn)
#define conn_send_next(ctx, conn) (conn)->ops->send_next(ctx, conn)
#define conn_send_done(ctx, conn, msg) (conn)->ops->send_done(ctx, conn, msg)

#define conn_close(ctx, conn) (conn)->ops->close(ctx, conn)
#define conn_active(conn) (conn)->ops->active(conn)
#define conn_ref(conn, owner) (conn)->ops->ref(conn, owner)
#define conn_unref(conn) (conn)->ops->unref(conn)

#define conn_enqueue_inq(ctx, conn, msg) \
  (conn)->ops->enqueue_inq(ctx, conn, msg)
#define conn_dequeue_inq(ctx, conn, msg) \
  (conn)->ops->dequeue_inq(ctx, conn, msg)
#define conn_enqueue_outq(ctx, conn, msg) \
  (conn)->ops->enqueue_outq(ctx, conn, msg)
#define conn_dequeue_outq(ctx, conn, msg) \
  (conn)->ops->dequeue_outq(ctx, conn, msg)
TAILQ_HEAD(conn_tqh, conn);

void conn_set_write_consistency(struct conn *conn, consistency_t cons);
consistency_t conn_get_write_consistency(struct conn *conn);
void conn_set_read_consistency(struct conn *conn, consistency_t cons);
consistency_t conn_get_read_consistency(struct conn *conn);
struct context *conn_to_ctx(struct conn *conn);
struct conn *test_conn_get(void);

typedef void (*func_conn_init_t)(struct conn *conn);
struct conn *conn_get(void *owner, func_conn_init_t func_conn_init);
void conn_put(struct conn *conn);
rstatus_t conn_listen(struct context *ctx, struct conn *p);

/**
 * Open an outgoing socket connection to either another Dynomite node or to a
 * backend data store (such as Redis or ARDB).
 * @param[in] ctx Dynomite server context.
 * @param[in,out] conn Outbound socket connection.
 * @return rstatus_t Return status code.
 */
rstatus_t conn_connect(struct context *ctx, struct conn *conn);

ssize_t conn_recv_data(struct conn *conn, void *buf, size_t size);
ssize_t conn_sendv_data(struct conn *conn, struct array *sendv, size_t nsend);
void conn_init(void);
void conn_deinit(void);

bool conn_is_req_first_in_outqueue(struct conn *conn, struct msg *req);
rstatus_t conn_event_add_conn(struct conn *conn);
rstatus_t conn_event_add_out(struct conn *conn);
rstatus_t conn_event_del_conn(struct conn *conn);
rstatus_t conn_event_del_out(struct conn *conn);
#endif
