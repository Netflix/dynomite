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
   return NC_OK;
}
