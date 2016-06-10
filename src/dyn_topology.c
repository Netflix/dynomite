#include "dyn_conf.h"
#include "dyn_core.h"
#include "dyn_topology.h"
#include "dyn_dnode_peer.h"
#include "hashkit/dyn_hashkit.h"

dictType dc_string_dict_type = {
		dict_string_hash,            /* hash function */
		NULL,                        /* key dup */
		NULL,                        /* val dup */
		dict_string_key_compare,     /* key compare */
		dict_string_destructor,      /* key destructor */
		NULL                         /* val destructor */
};

static rstatus_t
rack_init(struct rack *rack)
{
	rack->continuum = dn_alloc(sizeof(struct continuum));
	rack->ncontinuum = 0;
	rack->nserver_continuum = 0;
	rack->name = dn_alloc(sizeof(struct string));
	string_init(rack->name);

	rack->dc = dn_alloc(sizeof(struct string));
	string_init(rack->dc);

	return DN_OK;
}


static rstatus_t
rack_deinit(struct rack *rack)
{
	if (rack->continuum != NULL) {
		dn_free(rack->continuum);
	}

	return DN_OK;
}

static rstatus_t
dc_init(struct datacenter *dc)
{
	rstatus_t status;

	dc->dict_rack = dictCreate(&dc_string_dict_type, NULL);
	dc->name = dn_alloc(sizeof(struct string));
	string_init(dc->name);
    dc->preselected_rack_for_replication = NULL;

	status = array_init(&dc->racks, 3, sizeof(struct rack));

	return status;
}

static rstatus_t
rack_destroy(void *elem, void *data)
{
	struct rack *rack = elem;
	return rack_deinit(rack);
}

static rstatus_t
dc_deinit(struct datacenter *dc)
{
	array_each(&dc->racks, rack_destroy, NULL);
	string_deinit(dc->name);
	//dictRelease(dc->dict_rack);
	return DN_OK;
}

rstatus_t
datacenter_destroy(void *elem, void *data)
{
	struct datacenter *dc = elem;
	dc_deinit(dc);
	return DN_OK;
}



struct datacenter *
topo_get_dc(struct topology *topo, struct string *dcname)
{
	struct datacenter *dc;
	uint32_t i, len;


	log_debug(LOG_DEBUG, "%.*s'", dcname->len, dcname->data);
	for (i = 0, len = array_n(&topo->datacenters); i < len; i++) {
		dc = (struct datacenter *) array_get(&topo->datacenters, i);
		ASSERT(dc != NULL);
		ASSERT(dc->name != NULL);

		if (string_compare(dc->name, dcname) == 0) {
			return dc;
		}
	}

	dc = array_push(&topo->datacenters);
	log_debug(LOG_DEBUG, "%.*s' dc:%p idx:%d", dcname->len, dcname->data, dc, len);
	dc_init(dc);
	string_copy(dc->name, dcname->data, dcname->len);

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_dc about to exit dc '%.*s'",
				dc->name->len, dc->name->data);
	}

	return dc;
}

struct rack *
topo_get_rack(struct datacenter *dc, struct string *rackname)
{
	ASSERT(dc != NULL);
	ASSERT(dc->dict_rack != NULL);
	ASSERT(dc->name != NULL);

	/*
   struct rack *rack = dictFetchValue(dc->dict_rack, rackname);
   if (rack == NULL) {
      rack = array_push(&dc->racks);
      rack_init(rack);
      string_copy(rack->name, rackname->data, rackname->len);
      string_copy(rack->dc, dc->name->data, dc->name->len);
      rack->continuum = dn_alloc(sizeof(struct continuum));

   	dictAdd(dc->dict_rack, rackname, rack);
   }
	 */

	struct rack *rack;
	uint32_t i, len;
	for (i = 0, len = array_n(&dc->racks); i < len; i++) {
		rack = (struct rack *) array_get(&dc->racks, i);

		if (string_compare(rack->name, rackname) == 0) {
			return rack;
		}
	}

	rack = array_push(&dc->racks);
	log_debug(LOG_DEBUG, "'%.*s' rack:%p idx:%d", rackname->len, rackname->data, rack, len);
	rack_init(rack);
	string_copy(rack->name, rackname->data, rackname->len);
	string_copy(rack->dc, dc->name->data, dc->name->len);

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_rack exiting  '%.*s'",
				rack->name->len, rack->name->data);
	}

	return rack;
}

rstatus_t
topo_update_now(struct topology *topo)
{
    return vnode_update(topo);
}

static struct peer *
get_token_owner_reroute(struct topology *topo, struct rack *rack,
                        uint8_t *key, uint32_t keylen)
{
    uint32_t pos = 0;
    struct peer *peer = NULL;
    struct continuum *entry;

    if (rack->ncontinuum > 1) {
        do {
            // TODO: SHAILESH: Not sure how this works, using uint32 to get offset etc
            pos = pos % rack->ncontinuum;
            entry = rack->continuum + pos;
            peer = array_get(&topo->peers, entry->index);
            pos++;
        } while (peer->state == DOWN && pos < rack->ncontinuum);
    }

    //TODO: SHAILESH pick another server in another rack of the same DC if we don't have any good server
    return peer;
}

static struct dyn_token *
get_key_hash(struct topology *topo, uint8_t *key, uint32_t keylen)
{
    ASSERT(array_n(&topo->peers) != 0);
    ASSERT(key != NULL && keylen != 0);

    struct dyn_token *token = dn_alloc(sizeof(struct dyn_token));
    if (token == NULL) {
        return NULL;
    }
    init_dyn_token(token);

    rstatus_t status = topo->key_hash((char *)key, keylen, token);
    if (status != DN_OK) {
        log_error("Failed to get key_hash for key %.*s", keylen, key);
        dn_free(token);
        return NULL;
    }

    return token;
}

static struct peer *
get_token_owner_on_rack(struct topology *topo, struct rack *rack,
                        uint8_t *key, uint32_t keylen)
{
    uint32_t idx;
    struct dyn_token *token = NULL;

    ASSERT(array_n(&topo->peers) != 0);

    if (keylen == 0) {
        idx = 0; //for no argument command, local_only
    } else {
        token = get_key_hash(topo, key, keylen);
        if (!token)
            return NULL;

        //print_dyn_token(token, 1);
        idx = vnode_dispatch(rack->continuum, rack->ncontinuum, token);

        //TODOs: should reuse the token
        deinit_dyn_token(token);
        dn_free(token);
    }

    ASSERT(idx < array_n(&topo->peers));

    struct peer *peer = array_get(&topo->peers, idx);

    if (peer->state == DOWN) {
        if (!peer_is_same_dc(peer)) {
            //pick another reroute peer in the remote DC
            peer = get_token_owner_reroute(topo, rack, key, keylen);
        }
    }

    if (peer)
        log_debug(LOG_VERB, "dyn: key '%.*s' on dist %d maps to peer '%.*s'", keylen,
                  key, topo->dist_type, peer->endpoint.pname.len, peer->endpoint.pname.data);

    return peer;
}

struct peer*
topo_get_peer_in_rack_for_key(struct topology *topo, struct rack *rack,
                              uint8_t *key, uint32_t keylen, uint8_t msg_type)
{
    if (msg_type == 1) {  //always local
        return array_get(&topo->peers, 0);
    }

    rstatus_t status = topo_peer_update(topo);
    if (status != DN_OK) {
        log_error("Failed to update topology");
        return NULL;
    }

    /* from a given {key, keylen} pick a peer from pool */
    struct peer *peer = get_token_owner_on_rack(topo, rack, key, keylen);
    if (peer == NULL) {
        log_error("What? There is no such peer in rack '%.*s' for key '%.*s'",
                  rack->name->len, rack->name->data, keylen, key);
    }
    return peer;
}

void
topo_deinit_seeds_peers(struct context *ctx)
{
    struct topology *topo = ctx_get_topology(ctx);
    struct array *seeds = &topo->seeds;
    struct array *peers= &topo->peers;
    dnode_peer_deinit(seeds);
    dnode_peer_deinit(peers);
}

rstatus_t
topo_init_seeds_peers(struct context *ctx)
{
    struct server_pool *sp = ctx_get_pool(ctx);
    struct topology *topo = ctx_get_topology(ctx);
    struct array *conf_seeds = &sp->conf_pool->dyn_seeds;
    struct array * seeds = &topo->seeds;
    struct array * peers = &topo->peers;
    rstatus_t status = DN_OK;
    uint32_t nseed;

    /* init seeds list */
    nseed = array_n(conf_seeds);
    if(nseed == 0) {
        log_info("dyn: look like you are running with no seeds defined. This is ok for running with just one node.");

        // add current node to peers array
        status = array_init(peers, 1, sizeof(struct peer));
        if (status != DN_OK) {
            return status;
        }

        struct peer *self = array_push(peers);
        ASSERT(self != NULL);
        THROW_STATUS(dnode_peer_add_local(sp, self));
        THROW_STATUS(array_each(peers, dnode_peer_each_set_owner, sp));
        THROW_STATUS(array_each(peers, dnode_peer_each_set_ptctx, ctx));
        topo_update_now(topo);
        return status;
    }

    ASSERT(array_n(seeds) == 0);

    status = array_init(seeds, nseed, sizeof(struct peer));
    if (status != DN_OK) {
        return status;
    }

    /* transform conf seeds to seeds */
    THROW_STATUS(array_each(conf_seeds, conf_seed_each_transform, seeds));
    ASSERT(array_n(seeds) == nseed);

    /* set seed owner */
    THROW_STATUS(array_each(seeds, dnode_peer_each_set_owner, sp));
    THROW_STATUS(array_each(seeds, dnode_peer_each_set_ptctx, ctx));

    /* initialize peers list = seeds list */
    ASSERT(array_n(peers) == 0);

    // add current node to peers array
    uint32_t peer_cnt = nseed + 1;
    THROW_STATUS(array_init(peers, peer_cnt, sizeof(struct peer)));

    struct peer *peer = array_push(peers);
    ASSERT(peer != NULL);
    THROW_STATUS(dnode_peer_add_local(sp, peer));

    THROW_STATUS(array_each(conf_seeds, conf_seed_each_transform, peers));
    IGNORE_RET_VAL(peer_cnt);
    ASSERT(array_n(peers) == peer_cnt);

    THROW_STATUS(array_each(peers, dnode_peer_each_set_owner, sp));
    THROW_STATUS(array_each(peers, dnode_peer_each_set_ptctx, ctx));

    THROW_STATUS(topo_update_now(topo));

    log_debug(LOG_DEBUG, "init %"PRIu32" seeds and peers in pool '%.*s'",
              nseed, sp->name.len, sp->name.data);

    return DN_OK;
}

void
topo_print(struct topology *topo)
{
    uint32_t j, n;
    for (j = 0, n = array_n(&topo->peers); j < n; j++) {
        log_debug(LOG_VERB, "==============================================");
        struct peer *peer = (struct peer *) array_get(&topo->peers, j);
        log_debug(LOG_VERB, "\tPeer DC            : '%.*s'",peer ->dc);
        log_debug(LOG_VERB, "\tPeer Rack          : '%.*s'", peer->rack);

        log_debug(LOG_VERB, "\tPeer name          : '%.*s'", peer->name);
        log_debug(LOG_VERB, "\tPeer pname         : '%.*s'", peer->endpoint.pname);

        log_debug(LOG_VERB, "\tPeer state         : %"PRIu32"", peer->state);
        log_debug(LOG_VERB, "\tPeer port          : %"PRIu32"", peer->endpoint.port);
        log_debug(LOG_VERB, "\tPeer is_local      : %"PRIu32" ", peer->is_local);
        log_debug(LOG_VERB, "\tPeer failure_count : %"PRIu32" ", peer->failure_count);
        log_debug(LOG_VERB, "\tPeer num tokens    : %d", array_n(&peer->tokens));

        uint32_t k;
        for (k = 0; k < array_n(&peer->tokens); k++) {
            struct dyn_token *token = (struct dyn_token *) array_get(&peer->tokens, k);
            print_dyn_token(token, 12);
        }
    }

    log_debug(LOG_VERB, "Peers Datacenters/racks/nodes .................................................");
    uint32_t dc_index, dc_len;
    for(dc_index = 0, dc_len = array_n(&topo->datacenters); dc_index < dc_len; dc_index++) {
        struct datacenter *dc = array_get(&topo->datacenters, dc_index);
        log_debug(LOG_VERB, "Peer datacenter........'%.*s' (%p)", dc->name->len, dc->name->data, dc);
        uint32_t rack_index, rack_len;
        for(rack_index=0, rack_len = array_n(&dc->racks); rack_index < rack_len; rack_index++) {
            struct rack *rack = array_get(&dc->racks, rack_index);
            log_debug(LOG_VERB, "\tPeer rack........'%.*s' (%p)", rack->name->len, rack->name->data, rack);
            log_debug(LOG_VERB, "\tPeer rack ncontinuumm    : %d", rack->ncontinuum);
            log_debug(LOG_VERB, "\tPeer rack nserver_continuum    : %d", rack->nserver_continuum);
        }
    }
	log_debug(LOG_VERB, "...............................................................................");

}

rstatus_t
topo_peer_update(struct topology *topo)
{
    msec_t now = dn_msec_now();
    if (now < topo->next_rebuild) {
        return DN_OK;
    }

    topo->next_rebuild = now + WAIT_BEFORE_UPDATE_PEERS_IN_MILLIS;
    //TODO: SHAILESH: Make this async
    return topo_update_now(topo);
}

struct topology *
topo_create(void)
{
    struct topology *t = dn_zalloc(sizeof(struct topology));
    array_null(&t->datacenters);
    array_init(&t->datacenters, 1, sizeof(struct datacenter));
    array_null(&t->seeds);
    array_init(&t->seeds, 1, sizeof(struct peer));
    array_null(&t->peers);
    array_init(&t->peers, 1, sizeof(struct peer));
    string_init(&t->seed_provider);
    t->next_rebuild = 0;
    return t;
}
