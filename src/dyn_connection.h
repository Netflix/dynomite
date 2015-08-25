/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
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


#ifndef _DYN_CONNECTION_H_
#define _DYN_CONNECTION_H_
#include "dyn_core.h"

#define MAX_CONN_QUEUE_SIZE           20000
#define MAX_CONN_ALLOWABLE_NON_RECV   5
#define MAX_CONN_ALLOWABLE_NON_SEND   5

typedef rstatus_t (*conn_recv_t)(struct context *, struct conn*);
typedef struct msg* (*conn_recv_next_t)(struct context *, struct conn *, bool);
typedef void (*conn_recv_done_t)(struct context *, struct conn *, struct msg *, struct msg *);

typedef rstatus_t (*conn_send_t)(struct context *, struct conn*);
typedef struct msg* (*conn_send_next_t)(struct context *, struct conn *);
typedef void (*conn_send_done_t)(struct context *, struct conn *, struct msg *);

typedef void (*conn_close_t)(struct context *, struct conn *);
typedef bool (*conn_active_t)(struct conn *);

typedef void (*conn_ref_t)(struct conn *, void *);
typedef void (*conn_unref_t)(struct conn *);

typedef void (*conn_msgq_t)(struct context *, struct conn *, struct msg *);
typedef rstatus_t (*conn_response_handler)(struct conn *, msgid_t reqid,
                                           struct msg *rsp);
typedef enum connection_type {
    CONN_UNSPECIFIED,
    CONN_PROXY, // a dynomite proxy (listening) connection 
    CONN_CLIENT, // this is connected to a client connection
    CONN_DNODE_PEER_CLIENT, // this is connected to a dnode peer client
    CONN_DNODE_PEER_SERVER, // this is connected to a dnode peer server
    CONN_DNODE_SERVER, // this is a dnode (listening) connection...default 8101
    CONN_SERVER, // this is connected to underlying datastore ...redis/memcache
} connection_type_t;

struct conn {
    TAILQ_ENTRY(conn)  conn_tqe;      /* link in server_pool / server / free q */
    void               *owner;        /* connection owner - server_pool / server */

    int                sd;            /* socket descriptor */
    int                family;        /* socket address family */
    socklen_t          addrlen;       /* socket length */
    struct sockaddr    *addr;         /* socket address (ref in server or server_pool) */

    struct msg_tqh     imsg_q;        /* incoming request Q */
    uint32_t           imsg_count;    /* counter for incoming request Q */

    struct msg_tqh     omsg_q;        /* outstanding request Q */
    uint32_t           omsg_count;    /* counter for outstanding request Q */

    struct msg         *rmsg;         /* current message being rcvd */
    struct msg         *smsg;         /* current message being sent */

    conn_recv_t        recv;          /* recv (read) handler */
    conn_recv_next_t   recv_next;     /* recv next message handler */
    conn_recv_done_t   recv_done;     /* read done handler */
    conn_send_t        send;          /* send (write) handler */
    conn_send_next_t   send_next;     /* write next message handler */
    conn_send_done_t   send_done;     /* write done handler */
    conn_close_t       close;         /* close handler */
    conn_active_t      active;        /* active? handler */

    conn_ref_t         ref;           /* connection reference handler */
    conn_unref_t       unref;         /* connection unreference handler */

    conn_msgq_t        enqueue_inq;   /* connection inq msg enqueue handler */
    conn_msgq_t        dequeue_inq;   /* connection inq msg dequeue handler */
    conn_msgq_t        enqueue_outq;  /* connection outq msg enqueue handler */
    conn_msgq_t        dequeue_outq;  /* connection outq msg dequeue handler */

    size_t             recv_bytes;    /* received (read) bytes */
    size_t             send_bytes;    /* sent (written) bytes */

    uint32_t           events;        /* connection io events */
    err_t              err;           /* connection errno */
    unsigned           recv_active:1; /* recv active? */
    unsigned           recv_ready:1;  /* recv ready? */
    unsigned           send_active:1; /* send active? */
    unsigned           send_ready:1;  /* send ready? */

    unsigned           client:1;      /* client? or server? */
    unsigned           proxy:1;       /* proxy? */
    unsigned           connecting:1;  /* connecting? */
    unsigned           connected:1;   /* connected? */
    unsigned           eof:1;         /* eof? aka passive close? */
    unsigned           done:1;        /* done? aka close? */
    unsigned           dnode_server:1;       /* dnode server connection? */
    unsigned           dnode_client:1;       /* dnode client? */
    unsigned           dyn_mode:1;           /* is a dyn connection? */
    unsigned           dnode_secured:1;      /* is a secured connection? */
    unsigned           dnode_crypto_state:1; /* crypto state */
    unsigned char      aes_key[50]; //aes_key[34];              /* a place holder for AES key */

    int				   data_store;
    unsigned           same_dc:1;            /* bit to indicate whether a peer conn is same DC */
    uint32_t           avail_tokens;          /* used to throttle the traffics */
    uint32_t           last_sent;             /* ts in sec used to determine the last sent time */
    uint32_t           last_received;         /* last ts to receive a byte */
    uint32_t           attempted_reconnect;   /* #attempted reconnect before calling close */
    uint32_t           non_bytes_recv;        /* #times or epoll triggers we receive no bytes */
    //uint32_t           non_bytes_send;        /* #times or epoll triggers that we are not able to send any bytes */
    consistency_t      read_consistency;
    consistency_t      write_consistency;
    dict               *outstanding_msgs_dict;
    connection_type_t  type;
    conn_response_handler rsp_handler;
};

static inline rstatus_t
conn_handle_response(struct conn *conn, msgid_t msgid, struct msg *rsp)
{
    return conn->rsp_handler(conn, msgid, rsp);
}

TAILQ_HEAD(conn_tqh, conn);

void conn_set_write_consistency(struct conn *conn, consistency_t cons);
consistency_t conn_get_write_consistency(struct conn *conn);
void conn_set_read_consistency(struct conn *conn, consistency_t cons);
consistency_t conn_get_read_consistency(struct conn *conn);
struct context *conn_to_ctx(struct conn *conn);
struct conn *test_conn_get(void);
struct conn *conn_get(void *owner, bool client, int data_store);
struct conn *conn_get_proxy(void *owner);
struct conn *conn_get_peer(void *owner, bool client, int data_store);
struct conn *conn_get_dnode(void *owner);
void conn_put(struct conn *conn);
ssize_t conn_recv(struct conn *conn, void *buf, size_t size);
ssize_t conn_sendv(struct conn *conn, struct array *sendv, size_t nsend);
void conn_init(void);
void conn_deinit(void);
void conn_print(struct conn *conn);

#endif
