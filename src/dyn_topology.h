#pragma once
#include <hashkit/dyn_hashkit.h>
struct continuum;
struct rack {
	struct string      *name;
	struct string      *dc;
	uint32_t           ncontinuum;           /* # continuum points */
	uint32_t           nserver_continuum;    /* # servers - live and dead on continuum (const) */
	struct continuum   *continuum;           /* continuum */
};


struct datacenter {
	struct string      *name;            /* datacenter name */
	struct array       racks;           /* list of racks in a datacenter */
    struct rack        *preselected_rack_for_replication;
	dict               *dict_rack;
};

struct topology
{
    struct array datacenters;
    struct string      seed_provider;
    struct array       seeds;            /*dyn seeds */
    struct array       peers;
    struct context     *ctx;
    msec_t             next_rebuild;     /* next distribution rebuild time */
    dist_type_t        dist_type;            /* distribution type (dist_type_t) */
    hash_type_t        key_hash_type;        /* key hash type (hash_type_t) */
    hash_t             key_hash;             /* key hasher */
    struct string      hash_tag;             /* key hash tag (ref in conf_pool) */
};

// Access functions
struct datacenter *topo_get_dc(struct topology *topo, struct string *dcname);
struct rack *topo_get_rack(struct datacenter *dc, struct string *rackname);
void topo_print(struct topology *t);

// init functions
rstatus_t datacenter_destroy(void *elem, void *data);
rstatus_t topo_update_now(struct topology *topo);
struct peer;
struct peer *topo_get_peer_in_rack_for_key(struct topology *topo, struct rack *rack,
                              uint8_t *key, uint32_t keylen, uint8_t msg_type);
rstatus_t topo_peer_update(struct topology *topo);
rstatus_t topo_init_seeds_peers(struct context *ctx);
void topo_deinit_seeds_peers(struct context *ctx);
struct topology *topo_create(void);
