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
};
struct datacenter *topo_get_dc(struct topology *topo, struct string *dcname);
struct rack *topo_get_rack(struct datacenter *dc, struct string *rackname);
void topo_print(struct topology *t);
struct topology *topo_create(void);
