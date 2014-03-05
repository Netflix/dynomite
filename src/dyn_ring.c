#include <stdlib.h>
#include <unistd.h>

#include <nc_core.h>
#include <nc_conf.h>
#include <dyn_ring.h>
#include <dyn_peer.h>
#include <nc_server.h>
#include <dyn_token.h>


rstatus_t dyn_ring_init(struct array *peers, struct server_pool *sp)
{
       sp->ring.ring_nodes = peers;
       sp->ring.owner = sp;
       
       return NC_OK;
}


rstatus_t dyn_gos_run(struct context *ctx)
{
        loga("Running gossip serviceeeeeeeeeeeeeeeeeeeeeeeeeee");
        rstatus_t status;
        struct server_pool *sp = array_get(&ctx->pool, 0); //use the 1st one only

        struct array *peers = sp->ring.ring_nodes;
        uint32_t ncontinuum = array_n(peers);

        ASSERT(ncontinuum > 0);

        //struct peer *rnode = array_get(peers, random() % ncontinuum);
        struct peer *rnode = array_get(peers, 1);

        if (rnode->is_local) {
             loga("dyn_gossip picked a local node");
        } else {
             loga("dyn_gossip picked a peer node");
             struct conn * conn = dyn_peer_conn(rnode);
             if (conn == NULL) {
                //running out of connection due to memory exhaust
                 loga("Unable to obtain a connection object");
                 return NC_ERROR;
             }

             status = dyn_peer_connect(ctx, rnode, conn);
             if (status != NC_OK ) {
                  dyn_peer_close(ctx, conn);
                  loga("Error happened in dyn_gos_run");
                  return NC_OK;
             }



             /* enqueue the message (request) into peer inq */
             if (TAILQ_EMPTY(&conn->imsg_q)) {
                 status = event_add_out(ctx->evb, conn);
                 if (status != NC_OK) {
                      conn->err = errno;
                      loga("Error happened in calling event_add_out");
                      return NC_ERROR;
                  }
             }

             struct mbuf *nbuf = mbuf_get();
             if (nbuf == NULL) {
                 loga("Error happened in calling mbuf_get");
                 return NC_ERROR;
             }

             struct msg *msg = msg_get(conn, 1, 0);
             if (msg == NULL) {
                 mbuf_put(nbuf);
                 return NC_ERROR;
             }

             //dyn message's meta data
              uint64_t msg_id = 1234;
              uint8_t type = GOSSIP_PING;
              uint8_t version = 1;
              struct string data = string("Ping");

              dmsg_write(nbuf, msg_id, type, version, &data);
              mbuf_insert(&msg->mhdr, nbuf);

              //expect a response
              //conn->enqueue_outq(ctx, conn, msg);



              conn->enqueue_inq(ctx, conn, msg);

              //fix me - gossip stats
              //req_forward_stats(ctx, s_conn->owner, msg);
              log_debug(LOG_VERB, "gossip to s %d req %"PRIu64" len %"PRIu32
                        " type %d", conn->sd, msg->id, msg->mlen, msg->type);
      }

      return NC_OK;
}
