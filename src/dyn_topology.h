#pragma once
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
    struct array       seeds;                /*dyn seeds */
    struct array       peers;
    struct context     *ctx;
};

// Access functions
struct datacenter *topo_get_dc(struct topology *topo, struct string *dcname);
struct rack *topo_get_rack(struct datacenter *dc, struct string *rackname);
void topo_print(struct topology *t);

// init functions
rstatus_t datacenter_destroy(void *elem, void *data);
rstatus_t topo_update(struct topology *topo);
rstatus_t topo_init_seeds_peers(struct context *ctx);
void topo_deinit_seeds_peers(struct context *ctx);
struct topology *topo_create(void);
