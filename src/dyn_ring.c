/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed storages.
 * Copyright (C) 2014 Netflix, Inc.
 */ 

#include <stdlib.h>
#include <unistd.h>

#include "dyn_core.h"
#include "dyn_conf.h"
#include "dyn_ring.h"
#include "dyn_dnode_peer.h"
#include "dyn_server.h"
#include "hashkit/dyn_token.h"


rstatus_t dyn_ring_init(struct array *peers, struct server_pool *sp)
{
       sp->ring.ring_nodes = peers;
       sp->ring.owner = sp;
       
       return DN_OK;
}


rstatus_t dyn_gos_run(struct context *ctx)
{
        log_debug(LOG_DEBUG, "Running gossip serviceeeeeeeeeeeeeeeeeeeeeeeeeee");
        rstatus_t status;
        struct server_pool *sp = array_get(&ctx->pool, 0); //use the 1st one only

        struct array *peers = sp->ring.ring_nodes;
        uint32_t ncontinuum = array_n(peers);

        ASSERT(ncontinuum > 0);

        if (ncontinuum == 1) {
           return DN_OK;
        }

        //struct server *rnode = array_get(peers, random() % ncontinuum);
        struct server *rnode = array_get(peers, 1);
  
        if (rnode->is_local) {
             log_debug(LOG_DEBUG, "dyn_gossip picked a local node");
        } else {
             log_debug(LOG_DEBUG, "dyn_gossip picked a peer node");
             struct conn * conn = dnode_peer_conn(rnode);
             if (conn == NULL) {
                //running out of connection due to memory exhaust
                 log_debug(LOG_DEBUG, "Unable to obtain a connection object");
                 return DN_ERROR;
             }

             status = dnode_peer_connect(ctx, rnode, conn);
             if (status != DN_OK ) {
                  dnode_peer_close(ctx, conn);
                  log_debug(LOG_DEBUG, "Error happened in dyn_gos_run");
                  return DN_OK;
             }

             /* enqueue the message (request) into peer inq */
             if (TAILQ_EMPTY(&conn->imsg_q)) {
                 status = event_add_out(ctx->evb, conn);
                 if (status != DN_OK) {
                      conn->err = errno;
                      log_debug(LOG_DEBUG, "Error happened in calling event_add_out");
                      return DN_ERROR;
                  }
             }

             struct mbuf *nbuf = mbuf_get();
             if (nbuf == NULL) {
                 log_debug(LOG_DEBUG, "Error happened in calling mbuf_get");
                 return DN_ERROR;
             }

             struct msg *msg = msg_get(conn, 1, 0);
             if (msg == NULL) {
                 mbuf_put(nbuf);
                 return DN_ERROR;
             }

             //dyn message's meta data
              uint64_t msg_id = 1234;
              uint8_t type = GOSSIP_PING;
              uint8_t version = 1;
              struct string data = string("Ping");

              dmsg_write(nbuf, msg_id, type, version, &data);
              mbuf_insert_head(&msg->mhdr, nbuf);

              //expect a response
              //conn->enqueue_outq(ctx, conn, msg);
              msg->owner = conn;


              conn->enqueue_inq(ctx, conn, msg);

              //fix me - gossip stats
              //req_forward_stats(ctx, s_conn->owner, msg);
              log_debug(LOG_VERB, "gossip to s %d req %"PRIu64" len %"PRIu32
                        " type %d", conn->sd, msg->id, msg->mlen, msg->type);
      }

      return DN_OK;
}
