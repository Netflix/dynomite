#include <stdlib.h>
#include <unistd.h>

#include <nc_core.h>
#include <nc_conf.h>
#include <dyn_ring.h>
#include <dyn_peer.h>
#include <nc_server.h>
#include <dyn_token.h>

static rstatus_t
dyn_ring_each_set_owner(void *elem, void *data)
{
    struct ring_node *rn = elem;
    struct server_pool *sp = data;

    rn->owner = sp;

    return NC_OK;
}

rstatus_t
dyn_ring_init(struct array *conf_seeds, struct server_pool *sp)
{
    uint32_t seed_cnt = array_n(conf_seeds);
    if(seed_cnt == 0) {
        return NC_OK;
    }

    struct array *nodes = &sp->ring.ring_nodes;

    rstatus_t status = array_init(nodes, seed_cnt, sizeof(struct ring_node));
    if (status != NC_OK) {
        return status;
    }

    /* transform conf seeds to ring_nodes */
    status = array_each(conf_seeds, conf_seed_ring_each_transform, nodes);
    if (status != NC_OK) {
        //TODO: do some deinit'ing here on barf time
        //dyn_peer_deinit(seeds);
        return status;
    }
    ASSERT(array_n(nodes) == seed_cnt);

    status = array_each(nodes, dyn_ring_each_set_owner, sp);
    if (status != NC_OK) {
        //TODO: do some deinit'ing here on barf time
        return status;
    }
    
    return NC_OK;
}
